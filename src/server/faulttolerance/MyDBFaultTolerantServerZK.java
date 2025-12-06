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
import java.util.logging.Level;
import java.util.logging.Logger;

public class MyDBFaultTolerantServerZK extends MyDBSingleServer implements Watcher {

    private static final Logger log = Logger.getLogger(MyDBFaultTolerantServerZK.class.getName());

    public static final int SLEEP = 1000;
    public static final boolean DROP_TABLES_AFTER_TESTS = true;
    public static final int MAX_LOG_SIZE = 400;
    public static final int DEFAULT_PORT = 2181;

    private static final String REQUESTS_PATH = "/requests";
    private static final String SERVERS_PATH = "/servers";
    private static final String CHECKPOINT_TABLE = "checkpoint";

    private final NodeConfig<String> nodeConfig;
    private final String myID;

    private ZooKeeper zk;
    private CountDownLatch zkConnectedSignal;

    private final Cluster cluster;
    private final Session session;

    private volatile String lastAppliedZnode = null;

    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig,
                                     String myID,
                                     InetSocketAddress cassandraAddress) throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                        nodeConfig.getNodePort(myID)),
                cassandraAddress,
                myID.toLowerCase());
        this.nodeConfig = nodeConfig;
        this.myID = myID.toLowerCase();
        try {
            this.cluster = Cluster.builder()
                    .addContactPoint(cassandraAddress.getHostString())
                    .withPort(cassandraAddress.getPort())
                    .build();
            Session sys = cluster.connect();
            String ks = this.myID;
            String createKs = "CREATE KEYSPACE IF NOT EXISTS " + ks
                    + " WITH replication = {'class':'SimpleStrategy','replication_factor':1};";
            sys.execute(createKs);
            sys.close();
            this.session = cluster.connect(ks);
            connectToZookeeper();
            ensureZNodeExists(REQUESTS_PATH);
            ensureZNodeExists(SERVERS_PATH);
            registerMyAddress();
            initKVTable();
            initCheckpointTable();
            loadCheckpoint();
            replayPendingRequests();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void connectToZookeeper() throws IOException, InterruptedException {
        this.zkConnectedSignal = new CountDownLatch(1);
        this.zk = new ZooKeeper("localhost:" + DEFAULT_PORT, 3000, this);
        zkConnectedSignal.await();
        log.info("Connected to ZooKeeper at localhost:" + DEFAULT_PORT);
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            if (event.getState() == Event.KeeperState.SyncConnected && zkConnectedSignal != null) {
                zkConnectedSignal.countDown();
            }
            if (event.getType() == Event.EventType.NodeChildrenChanged
                    && REQUESTS_PATH.equals(event.getPath())) {
                replayPendingRequests();
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error in ZooKeeper watcher", e);
        }
    }

    private void ensureZNodeExists(String path) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(path, false);
        if (stat == null) {
            zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            log.info("Created znode " + path + " in ZooKeeper");
        }
    }

    private void registerMyAddress() throws KeeperException, InterruptedException {
        String path = SERVERS_PATH + "/" + myID;
        String address = nodeConfig.getNodeAddress(myID) + ":"
                + (nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET);
        byte[] data = address.getBytes(StandardCharsets.UTF_8);
        Stat stat = zk.exists(path, false);
        if (stat == null) {
            try {
                zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                zk.setData(path, data, -1);
            }
        } else {
            zk.setData(path, data, -1);
        }
        log.info("Registered server " + myID + " at " + address + " in ZooKeeper");
    }

    private void initKVTable() {
        String cql = "CREATE TABLE IF NOT EXISTS kv ("
                + "key bigint, "
                + "seq int, "
                + "value int, "
                + "PRIMARY KEY (key, seq)"
                + ");";
        session.execute(cql);
        log.info("Ensured kv table exists in keyspace " + myID);
    }

    private void initCheckpointTable() {
        String cql = "CREATE TABLE IF NOT EXISTS " + CHECKPOINT_TABLE + " ("
                + "server_id text PRIMARY KEY, "
                + "last_znode text"
                + ");";
        session.execute(cql);
        log.info("Ensured checkpoint table exists in keyspace " + myID);
    }

    private void loadCheckpoint() {
        try {
            ResultSet rs = session.execute("SELECT last_znode FROM " + CHECKPOINT_TABLE
                    + " WHERE server_id='" + myID + "';");
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

    private void applyRequestToDB(String request) {
        try {
            String[] parts = request.trim().split("\\s+");
            if (parts.length < 2) {
                return;
            }
            String op = parts[0].toUpperCase();
            long key = Long.parseLong(parts[1]);
            if (op.equals("PUT") || op.equals("ADD")) {
                if (parts.length < 3) {
                    return;
                }
                int value = Integer.parseInt(parts[2]);
                ResultSet rs = session.execute("SELECT seq FROM kv WHERE key=" + key
                        + " ORDER BY seq DESC LIMIT 1;");
                Row row = rs.one();
                int nextSeq = row == null ? 0 : row.getInt("seq") + 1;
                String cql = "INSERT INTO kv (key, seq, value) VALUES ("
                        + key + ", " + nextSeq + ", " + value + ");";
                session.execute(cql);
            } else if (op.equals("SET")) {
                if (parts.length < 3) {
                    return;
                }
                int value = Integer.parseInt(parts[2]);
                session.execute("DELETE FROM kv WHERE key=" + key + ";");
                String cql = "INSERT INTO kv (key, seq, value) VALUES ("
                        + key + ", 0, " + value + ");";
                session.execute(cql);
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error applying request to DB: " + request, e);
        }
    }

    private synchronized void replayPendingRequests() {
        try {
            List<String> children = zk.getChildren(REQUESTS_PATH, true);
            if (children.isEmpty()) {
                log.info("No requests in ZK log; nothing to apply");
                return;
            }
            Collections.sort(children);
            for (String child : children) {
                if (lastAppliedZnode != null && child.compareTo(lastAppliedZnode) <= 0) {
                    continue;
                }
                String fullPath = REQUESTS_PATH + "/" + child;
                byte[] data = zk.getData(fullPath, false, null);
                String request = new String(data, StandardCharsets.UTF_8);
                applyRequestToDB(request);
                lastAppliedZnode = child;
                persistCheckpoint();
                log.info("Applied and checkpointed znode " + child);
            }
        } catch (KeeperException.NoNodeException nne) {
            log.log(Level.INFO, "No /requests znode yet; nothing to replay");
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error updating from ZK log", e);
        }
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        String request = new String(bytes, StandardCharsets.UTF_8);
        try {
            zk.create(
                    REQUESTS_PATH + "/req_",
                    request.getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT_SEQUENTIAL
            );
            replayPendingRequests();
            try {
                clientMessenger.send(header.sndr, "OK".getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                log.log(Level.SEVERE, "Error sending response to client", e);
            }
        } catch (KeeperException | InterruptedException e) {
            log.log(Level.SEVERE, "Error logging client request in ZooKeeper", e);
        } catch (Exception e) {
            log.log(Level.SEVERE, "Unexpected error handling client request", e);
        }
    }

    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
    }

    @Override
    public void close() {
        try {
            if (zk != null) zk.close();
        } catch (InterruptedException e) {
            log.log(Level.WARNING, "Interrupted while closing ZooKeeper", e);
        }
        try {
            if (session != null) session.close();
        } catch (Exception e) {
            log.log(Level.WARNING, "Error closing Cassandra session", e);
        }
        try {
            if (cluster != null) cluster.close();
        } catch (Exception e) {
            log.log(Level.WARNING, "Error closing Cassandra cluster", e);
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
                        ReplicatedServer.SERVER_PREFIX,
                        ReplicatedServer.SERVER_PORT_OFFSET
                ),
                args[1],
                args.length > 2
                        ? Util.getInetSocketAddressFromString(args[2])
                        : new InetSocketAddress("localhost", 9042)
        );
    }
}
