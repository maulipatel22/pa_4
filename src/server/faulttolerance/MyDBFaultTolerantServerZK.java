package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import server.MyDBSingleServer;
import server.ReplicatedServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ZooKeeper-based fault-tolerant server.
 *
 * Each server:
 *  - Has its own Cassandra keyspace = myID (e.g., "server0").
 *  - Uses a common ZK log under /requests to record all CQL commands
 *    as persistent sequential znodes.
 *  - On startup, replays any log entries with znode name > lastAppliedZnode
 *    to catch up.
 *  - Maintains lastAppliedZnode in a Cassandra table "checkpoint".
 *
 * The grader sends plain CQL strings (INSERT / UPDATE on table "grade")
 * to this server; we just execute them on our local keyspace.
 *
 * IMPORTANT: We never use Cassandra for coordination between replicas.
 * All cross-replica ordering/coordination is via ZooKeeper.
 */
public class MyDBFaultTolerantServerZK extends MyDBSingleServer implements Watcher {

    private static final Logger log =
            Logger.getLogger(MyDBFaultTolerantServerZK.class.getName());

    public static final int SLEEP = 1000;
    public static final boolean DROP_TABLES_AFTER_TESTS = true;
    public static final int MAX_LOG_SIZE = 400;
    public static final int DEFAULT_PORT = 2181;

    private static final String REQUESTS_PATH = "/requests";
    private static final String SERVERS_PATH  = "/servers";
    private static final String CHECKPOINT_TABLE = "checkpoint";

    // This MUST match what the grader and your GP app use.
    private static final String DATA_TABLE = "grade";

    private final NodeConfig<String> nodeConfig;
    private final String myID;  // lower-cased server ID (keyspace name)

    private ZooKeeper zk;
    private final CountDownLatch zkConnectedLatch = new CountDownLatch(1);

    private final Cluster cluster;
    private final Session session;

    // Name of the last applied znode under /requests (e.g. "req_0000000012")
    private volatile String lastAppliedZnode = null;

    // Ensures ZK initialization logic runs only once
    private final AtomicBoolean zkInitialized = new AtomicBoolean(false);

    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig,
                                     String myID,
                                     InetSocketAddress cassandraAddress) throws IOException {

        // Listen on the exact port from servers.properties (e.g., 2000, 2001, 2002)
        super(
                new InetSocketAddress(
                        nodeConfig.getNodeAddress(myID),
                        nodeConfig.getNodePort(myID)   // no extra offsets here
                ),
                cassandraAddress,
                myID.toLowerCase()                  // keyspace = server ID (per-server)
        );

        this.nodeConfig = nodeConfig;
        this.myID = myID.toLowerCase();

        try {
            // Set up Cassandra
            this.cluster = Cluster.builder()
                    .addContactPoint(cassandraAddress.getHostString())
                    .withPort(cassandraAddress.getPort())
                    .build();

            // Create keyspace if needed (per-server keyspace)
            Session sys = cluster.connect();
            String ks = this.myID;
            String createKs = "CREATE KEYSPACE IF NOT EXISTS " + ks
                    + " WITH replication = {'class':'SimpleStrategy','replication_factor':1};";
            sys.execute(createKs);
            sys.close();

            // Use this server's keyspace
            this.session = cluster.connect(ks);

            // Connect to ZooKeeper
            connectToZookeeper();

            // Wait a bit for SyncConnected so that we can eagerly sync from ZK.
            try {
                if (!zkConnectedLatch.await(5, TimeUnit.SECONDS)) {
                    log.warning("Timed out waiting for ZooKeeper connection for " + myID);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log.log(Level.WARNING, "Interrupted while waiting for ZK connection for " + myID, ie);
            }

            // Initialize ZK state (paths + registration) and replay existing log.
            if (zk != null) {
                try {
                    initializeZookeeperStateIfNeeded();
                } catch (KeeperException | InterruptedException e) {
                    log.log(Level.WARNING,
                            "Error during initial ZooKeeper state setup for " + myID, e);
                }
                // Ensure DB schema
                initDataTable();        // creates TABLE "grade" if needed
                initCheckpointTable();

                // Load last checkpoint, if any
                loadCheckpoint();

                // Replay any pending log entries from /requests
                replayPendingRequests();
            } else {
                // ZK unavailable at startup: we still init tables so the server is usable,
                // but will not get replicated ordering until ZK connects.
                initDataTable();
                initCheckpointTable();
            }

        } catch (Exception e) {
            // If constructor throws, the process dies and the grader sees
            // connection refused on this server's port.
            log.log(Level.SEVERE, "Error initializing MyDBFaultTolerantServerZK for " + myID, e);
            throw new IOException(e);
        }
    }

    /**
     * Create the ZooKeeper client (does not block for connection).
     */
    private void connectToZookeeper() throws IOException {
        this.zk = new ZooKeeper("localhost:" + DEFAULT_PORT, 15000, this);
        log.info("Initiated ZooKeeper connection to localhost:" + DEFAULT_PORT + " for " + myID);
    }

    /**
     * Watcher callback for ZooKeeper events.
     */
    @Override
    public void process(WatchedEvent event) {
        try {
            if (event.getState() == Event.KeeperState.SyncConnected) {
                log.info("ZooKeeper SyncConnected for " + myID);
                zkConnectedLatch.countDown();
                initializeZookeeperStateIfNeeded();
                replayPendingRequests();
            }

            if (event.getType() == Event.EventType.NodeChildrenChanged
                    && REQUESTS_PATH.equals(event.getPath())) {
                // New log entries added
                replayPendingRequests();
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error in ZooKeeper watcher for " + myID, e);
        }
    }

    /**
     * One-time ZK initialization: ensure the parent znodes exist and
     * register this server's address.
     */
    private void initializeZookeeperStateIfNeeded() throws KeeperException, InterruptedException {
        if (!zkInitialized.compareAndSet(false, true)) {
            return; // already initialized
        }
        if (zk == null) return;

        ensureZNodeExists(REQUESTS_PATH);
        ensureZNodeExists(SERVERS_PATH);
        registerMyAddress();
        log.info("ZK paths initialized and server registered for " + myID);
    }

    private void ensureZNodeExists(String path) throws KeeperException, InterruptedException {
        if (zk == null) return;
        Stat stat = zk.exists(path, false);
        if (stat == null) {
            try {
                zk.create(path, new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                log.info("Created znode " + path + " in ZooKeeper");
            } catch (KeeperException.NodeExistsException ignore) {
                // Another server raced to create it; that's fine.
            }
        }
    }

    /**
     * Register this server’s client-facing address under /servers/myID.
     */
    private void registerMyAddress() throws KeeperException, InterruptedException {
        if (zk == null) return;
        String path = SERVERS_PATH + "/" + myID;
        String address = nodeConfig.getNodeAddress(myID) + ":"
                + nodeConfig.getNodePort(myID);
        byte[] data = address.getBytes(StandardCharsets.UTF_8);

        Stat stat = zk.exists(path, false);
        if (stat == null) {
            try {
                zk.create(path, data,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                zk.setData(path, data, -1);
            }
        } else {
            zk.setData(path, data, -1);
        }
        log.info("Registered server " + myID + " at " + address + " in ZooKeeper");
    }

    /**
     * Ensure the data table exists in this server's keyspace.
     * This table name must match what the grader expects.
     */
    private void initDataTable() {
        try {
            String cql = "CREATE TABLE IF NOT EXISTS " + DATA_TABLE + " ("
                    + "key bigint, "
                    + "seq int, "
                    + "value int, "
                    + "PRIMARY KEY (key, seq)"
                    + ");";
            session.execute(cql);
            log.info("Ensured data table '" + DATA_TABLE + "' exists in keyspace " + myID);
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error creating data table '" + DATA_TABLE + "' for " + myID, e);
        }
    }

    private void initCheckpointTable() {
        try {
            String cql = "CREATE TABLE IF NOT EXISTS " + CHECKPOINT_TABLE + " ("
                    + "server_id text PRIMARY KEY, "
                    + "last_znode text"
                    + ");";
            session.execute(cql);
            log.info("Ensured checkpoint table exists in keyspace " + myID);
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error creating checkpoint table for " + myID, e);
        }
    }

    private void loadCheckpoint() {
        try {
            String cql = "SELECT last_znode FROM " + CHECKPOINT_TABLE
                    + " WHERE server_id='" + myID + "';";
            ResultSet rs = session.execute(cql);
            Row row = rs.one();
            if (row != null) {
                this.lastAppliedZnode = row.getString("last_znode");
                log.info("Loaded checkpoint for " + myID
                        + " last_znode=" + this.lastAppliedZnode);
            } else {
                log.info("No existing checkpoint for " + myID + "; starting from scratch");
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error reading checkpoint for " + myID, e);
        }
    }

    private void persistCheckpoint() {
        try {
            if (lastAppliedZnode != null) {
                String cql = "INSERT INTO " + CHECKPOINT_TABLE
                        + " (server_id, last_znode) VALUES ('"
                        + myID + "', '" + lastAppliedZnode + "');";
                session.execute(cql);
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error persisting checkpoint for " + myID, e);
        }
    }

    /**
     * Apply a single CQL request string to this server's Cassandra keyspace.
     */
    private void applyRequestToDB(String request) {
        if (request == null) return;
        String trimmed = request.trim();
        if (trimmed.isEmpty()) return;

        try {
            session.execute(trimmed);
        } catch (Exception e) {
            log.log(Level.SEVERE,
                    "Error applying request to DB (treating as CQL): " + trimmed, e);
        }
    }

    /**
     * Replay all ZK log entries under /requests with znode name strictly
     * greater than lastAppliedZnode.
     */
    private synchronized void replayPendingRequests() {
        if (zk == null) {
            return;
        }
        try {
            // Re-register watch on /requests
            List<String> children = zk.getChildren(REQUESTS_PATH, true);
            if (children.isEmpty()) {
                log.fine("No requests in ZK log; nothing to apply for " + myID);
                return;
            }
            Collections.sort(children);
            for (String child : children) {
                if (lastAppliedZnode != null && child.compareTo(lastAppliedZnode) <= 0) {
                    continue; // already applied
                }
                String fullPath = REQUESTS_PATH + "/" + child;
                byte[] data = zk.getData(fullPath, false, null);
                String request = new String(data, StandardCharsets.UTF_8);
                applyRequestToDB(request);
                lastAppliedZnode = child;
                persistCheckpoint();
                log.fine("Applied and checkpointed znode " + child + " on " + myID);
            }
        } catch (KeeperException.NoNodeException nne) {
            log.log(Level.FINE,
                    "No " + REQUESTS_PATH + " znode yet; nothing to replay for " + myID);
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error updating from ZK log on " + myID, e);
        }
    }

    /**
     * Called by SingleServer when a client message arrives.
     * We:
     *  1. Log the CQL string to ZooKeeper under /requests as a
     *     persistent sequential znode.
     *  2. Apply any pending log entries in order to this server's DB.
     *  3. Send "OK" back to the client.
     */
    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        String request = new String(bytes, StandardCharsets.UTF_8).trim();
        if (request.isEmpty()) {
            return;
        }

        try {
            boolean loggedInZK = false;

            if (zk != null) {
                try {
                    // Make sure /requests and /servers exist and this server
                    // is registered.
                    initializeZookeeperStateIfNeeded();
                    ensureZNodeExists(REQUESTS_PATH);

                    String path = REQUESTS_PATH + "/req_";
                    zk.create(
                            path,
                            request.getBytes(StandardCharsets.UTF_8),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT_SEQUENTIAL
                    );
                    loggedInZK = true;

                } catch (KeeperException | InterruptedException e) {
                    log.log(Level.WARNING,
                            "Error logging client request in ZooKeeper on " + myID
                                    + "; falling back to local apply", e);
                }
            }

            if (!loggedInZK) {
                // No ZK available; just apply locally (still keeps server usable).
                applyRequestToDB(request);
            } else {
                // Process any new log entries (including the one we just added).
                replayPendingRequests();
            }

            try {
                clientMessenger.send(header.sndr, "OK".getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                log.log(Level.SEVERE, "Error sending response to client on " + myID, e);
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Unexpected error handling client request on " + myID, e);
        }
    }

    // No inter-server messages used in this ZK-based design.
    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        // Intentionally empty for the ZK-based approach.
    }

    @Override
    public void close() {
        try {
            if (zk != null) zk.close();
        } catch (InterruptedException e) {
            log.log(Level.WARNING, "Interrupted while closing ZooKeeper for " + myID, e);
            Thread.currentThread().interrupt();
        }
        try {
            if (session != null) session.close();
        } catch (Exception e) {
            log.log(Level.WARNING, "Error closing Cassandra session for " + myID, e);
        }
        try {
            if (cluster != null) cluster.close();
        } catch (Exception e) {
            log.log(Level.WARNING, "Error closing Cassandra cluster for " + myID, e);
        }
        super.close();
    }

    public static enum CheckpointRecovery {
        CHECKPOINT, RESTORE;
    }

    public static void main(String[] args) throws IOException {
        new MyDBFaultTolerantServerZK(
                NodeConfigUtils.getNodeConfigFromFile(
                        args[0],
                        ReplicatedServer.SERVER_PREFIX   // use ports directly from servers.properties
                ),
                args[1],
                args.length > 2
                        ? Util.getInetSocketAddressFromString(args[2])
                        : new InetSocketAddress("localhost", 9042)
        );
    }
}
