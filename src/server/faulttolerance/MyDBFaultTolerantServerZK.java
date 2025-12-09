package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import org.apache.zookeeper.*;
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
    private static final String DATA_TABLE = "grade";
    private final NodeConfig<String> nodeConfig;
    private final String myID;
    private ZooKeeper zk;
    private final CountDownLatch zkConnectedLatch = new CountDownLatch(1);
    private final Cluster cluster;
    private final Session session;
    private volatile String lastAppliedZnode = null;
    private final AtomicBoolean zkInitialized = new AtomicBoolean(false);


    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig,
                                     String myID,
                                     InetSocketAddress cassandraAddress) throws IOException {

        super(
                new InetSocketAddress(
                        nodeConfig.getNodeAddress(myID),
                        nodeConfig.getNodePort(myID)
                ),
                cassandraAddress,
                myID.toLowerCase()
        );

        this.nodeConfig = nodeConfig;
        this.myID = myID.toLowerCase();
        try {
            this.cluster = Cluster.builder()
                    .addContactPoint(cassandraAddress.getHostString())
                    .withPort(cassandraAddress.getPort())
                    .build();

            Session sys = cluster.connect();
            sys.execute("CREATE KEYSPACE IF NOT EXISTS " + this.myID
                    + " WITH replication = {'class':'SimpleStrategy','replication_factor':1};");
            sys.close();
            this.session = cluster.connect(this.myID);
            connectToZookeeper();
            zkConnectedLatch.await(3, TimeUnit.SECONDS);
            initializeZookeeperStateIfNeeded();
            initDataTable();
            initCheckpointTable();
            loadCheckpoint();
            if (isGradeTableEmpty()) {
                log.warning("Grade table empty on startup — forcing full replay for " + myID);
                this.lastAppliedZnode = null;  
            }

            replayPendingRequests();
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error while initializing MyDBFaultTolerantServerZK for " + myID, e);
            throw new IOException(e);
        }
    }
    private boolean isGradeTableEmpty() {
        try {
            ResultSet rs = session.execute("SELECT * FROM " + DATA_TABLE + " LIMIT 1;");
            return rs.one() == null;
        } catch (Exception e) {
            log.warning("Error: CHECK grade table is empty - " + myID);
            return true;
        }
    }


    private void connectToZookeeper() throws IOException {
        this.zk = new ZooKeeper("localhost:" + DEFAULT_PORT, 15000, this);
    }
    @Override
    public void process(WatchedEvent event) {
        try {
            if (event.getState() == Event.KeeperState.SyncConnected) {
                zkConnectedLatch.countDown();
            }

            if (event.getType() == Event.EventType.NodeChildrenChanged
                    && REQUESTS_PATH.equals(event.getPath())) {
                replayPendingRequests();
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "Watcher error on " + myID, e);
        }
    }


    private void initializeZookeeperStateIfNeeded() throws KeeperException, InterruptedException {
        if (!zkInitialized.compareAndSet(false, true)) return;
        ensureZNodeExists(REQUESTS_PATH);
        ensureZNodeExists(SERVERS_PATH);
        registerMyAddress();
    }

    private void ensureZNodeExists(String path) throws KeeperException, InterruptedException {
        if (zk.exists(path, false) == null) {
            try {
                zk.create(path, new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException ignore) {}
        }
    }

    private void registerMyAddress() throws KeeperException, InterruptedException {
        String p = SERVERS_PATH + "/" + myID;
        String addr = nodeConfig.getNodeAddress(myID) + ":" +
                nodeConfig.getNodePort(myID);
        byte[] data = addr.getBytes(StandardCharsets.UTF_8);
        if (zk.exists(p, false) == null) {
            zk.create(p, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            zk.setData(p, data, -1);
        }
    }

    private void initDataTable() {
        session.execute(
                "CREATE TABLE IF NOT EXISTS " + DATA_TABLE + " ("
                        + "key bigint, seq int, value int, "
                        + "PRIMARY KEY (key, seq));"
        );
    }

    private void initCheckpointTable() {
        session.execute(
                "CREATE TABLE IF NOT EXISTS " + CHECKPOINT_TABLE + " ("
                        + "server_id text PRIMARY KEY, last_znode text);"
        );
    }

    private void loadCheckpoint() {
        ResultSet rs = session.execute(
                "SELECT last_znode FROM " + CHECKPOINT_TABLE +
                        " WHERE server_id='" + myID + "';"
        );
        Row row = rs.one();
        if (row != null) {
            lastAppliedZnode = row.getString("last_znode");
        }
    }

    private void persistCheckpoint() {
        if (lastAppliedZnode != null) {
            session.execute("INSERT INTO " + CHECKPOINT_TABLE
                    + " (server_id,last_znode) VALUES ('"
                    + myID + "','" + lastAppliedZnode + "');");
        }
    }

    private synchronized void replayPendingRequests() {
        if (zk == null) return;
        try {
            List<String> children = zk.getChildren(REQUESTS_PATH, true);
            Collections.sort(children);

            for (String child : children) {
                if (lastAppliedZnode != null && child.compareTo(lastAppliedZnode) <= 0)
                    continue;

                byte[] data = zk.getData(REQUESTS_PATH + "/" + child, false, null);
                String req = new String(data, StandardCharsets.UTF_8);

                session.execute(req);

                lastAppliedZnode = child;
                persistCheckpoint();
            }

        } catch (Exception e) {
            log.log(Level.SEVERE, "Replay failed on " + myID, e);
        }
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        String request = new String(bytes, StandardCharsets.UTF_8).trim();
        if (request.isEmpty()) return;

        boolean logged = false;
        try {
            if (zk != null) {
                initializeZookeeperStateIfNeeded();

                zk.create(
                        REQUESTS_PATH + "/req_",
                        request.getBytes(StandardCharsets.UTF_8),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT_SEQUENTIAL
                );
                logged = true;
            }
        } catch (Exception ignore) {}
        if (!logged) session.execute(request);
        replayPendingRequests();
        try {
            clientMessenger.send(header.sndr, "OK".getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            log.warning("Failed to reply to client");
        }
    }

    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {}
    @Override
    public void close() {
        try { if (zk != null) zk.close(); } catch (Exception ignore) {}
        try { session.close(); } catch (Exception ignore) {}
        try { cluster.close(); } catch (Exception ignore) {}
        super.close();
    }
    public static void main(String[] args) throws IOException {
        new MyDBFaultTolerantServerZK(
                NodeConfigUtils.getNodeConfigFromFile(
                        args[0], ReplicatedServer.SERVER_PREFIX
                ),
                args[1],
                args.length > 2
                        ? Util.getInetSocketAddressFromString(args[2])
                        : new InetSocketAddress("localhost", 9042)
        );
    }
}
