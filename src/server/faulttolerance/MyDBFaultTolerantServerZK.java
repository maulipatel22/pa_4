package server.faulttolerance;

import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import server.MyDBSingleServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;


public class MyDBFaultTolerantServerZK extends MyDBSingleServer implements Watcher {

    private static final Logger log = Logger.getLogger(MyDBFaultTolerantServerZK.class.getName());

    public static final int MAX_LOG_SIZE = 400;
    public static final int DEFAULT_PORT = 2181;
    public static final int SLEEP = 1000;  
    public static final boolean DROP_TABLES_AFTER_TESTS = true;  
    private final int myPort;

    private final String myID;
    private final NodeConfig<String> nodeConfig;
    private ZooKeeper zk;
    private final LinkedList<String> requestLog = new LinkedList<>();

    private static final String REQUESTS_PATH = "/requests";
    private static final String CHECKPOINT_TABLE = "checkpoint";

    private Cluster cluster;
    private com.datastax.driver.core.Session session;

    private static int resolvePort(NodeConfig<String> nodeConfig, String myID) {
        int p = nodeConfig.getNodePort(myID);
        // If config doesn't specify a valid port (e.g., -1), fall back to 0 (ephemeral)
        if (p < 0 || p > 65535) {
            return 0;
        }
        return p;
    }



    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String myID,
                                    InetSocketAddress isaDB) throws IOException {

        // Use a sanitized port value here so InetSocketAddress never sees -1
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                                    resolvePort(nodeConfig, myID)),
            isaDB,
            myID);

        this.myID = myID;
        this.nodeConfig = nodeConfig;
        this.myPort = resolvePort(nodeConfig, myID);

        try {
            connectToCassandra(isaDB);
            connectToZookeeper();
            registerMyAddress();
            ensureZNodeExists(REQUESTS_PATH);
            recoverFromCheckpoint();
            replayPendingRequests();
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void registerMyAddress() throws KeeperException, InterruptedException {
        String path = "/servers/" + myID;
        String address = nodeConfig.getNodeAddress(myID) + ":" + myPort; // ✅ use myPort here
        byte[] data = address.getBytes(StandardCharsets.UTF_8);

        Stat stat = zk.exists(path, false);
        if (stat == null) {
            zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            zk.setData(path, data, -1);
        }

        log.info("Registered server at " + address + " in Zookeeper");
    }


    private void connectToCassandra(InetSocketAddress isaDB) {
        cluster = Cluster.builder()
                .addContactPoint(isaDB.getHostString())
                .withPort(isaDB.getPort())
                .build();
        session = cluster.connect();
        log.info("Connected to Cassandra at " + isaDB);
    }

    private void connectToZookeeper() throws IOException, InterruptedException {
        CountDownLatch connectedSignal = new CountDownLatch(1);
        this.zk = new ZooKeeper("localhost:" + DEFAULT_PORT, 3000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedSignal.countDown();
            }
        });
        connectedSignal.await();
        log.info("Connected to Zookeeper");
    }

    private void ensureZNodeExists(String path) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(path, false);
        if (stat == null) {
            zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    @Override
    public void process(WatchedEvent event) {}

    private void recoverFromCheckpoint() {
        try {
            session.execute("CREATE TABLE IF NOT EXISTS " + CHECKPOINT_TABLE +
                    " (server_id text PRIMARY KEY, executed list<text>)");
            ResultSet rs = session.execute("SELECT executed FROM " + CHECKPOINT_TABLE +
                    " WHERE server_id='" + myID + "';");
            Row row = rs.one();
            if (row != null) {
                List<String> executed = row.getList("executed", String.class);
                requestLog.addAll(executed);
            }
            log.info("Recovered " + requestLog.size() + " requests from checkpoint");
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error recovering checkpoint", e);
        }
    }

    private void replayPendingRequests() {
        try {
            List<String> children = zk.getChildren(REQUESTS_PATH, false);
            Collections.sort(children);
            for (String node : children) {
                byte[] data = zk.getData(REQUESTS_PATH + "/" + node, false, null);
                String request = new String(data, StandardCharsets.UTF_8);
                synchronized (requestLog) {
                    if (!requestLog.contains(request)) {
                        session.execute(request);
                        requestLog.add(request);
                        truncateLogIfNeeded();
                    }
                }
            }
            persistCheckpoint();
            log.info("Replayed pending requests from Zookeeper");
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error replaying Zookeeper requests", e);
        }
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, edu.umass.cs.nio.nioutils.NIOHeader header) {
        try {
            String request = new String(bytes, StandardCharsets.UTF_8);

            zk.create(REQUESTS_PATH + "/req_", request.getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

            session.execute(request);
            synchronized (requestLog) {
                requestLog.add(request);
                truncateLogIfNeeded();
            }

            persistCheckpoint();

            clientMessenger.send(header.sndr, "OK".getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error handling client request", e);
        }
    }

    protected void handleMessageFromServer(byte[] bytes, edu.umass.cs.nio.nioutils.NIOHeader header) {}

    private void truncateLogIfNeeded() {
        while (requestLog.size() > MAX_LOG_SIZE) {
            requestLog.removeFirst();
        }
    }

    private void persistCheckpoint() {
        try {
            session.execute("INSERT INTO " + CHECKPOINT_TABLE + " (server_id, executed) VALUES ('" +
                    myID + "', ?)", requestLog);
        } catch (Exception e) {
            log.log(Level.SEVERE, "Error persisting checkpoint", e);
        }
    }

    @Override
    public void close() {
        try {
            if (zk != null) zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (session != null) session.close();
        if (cluster != null) cluster.close();
        super.close();
    }

    public static void main(String[] args) {
        try {
            NodeConfig<String> nodeConfig = NodeConfigUtils.getNodeConfigFromFile(
                    args[0], "server", 0);
            String myID = args[1];
            InetSocketAddress isaDB = args.length > 2 ? Util.getInetSocketAddressFromString(args[2])
                    : new InetSocketAddress("localhost", 9042);

            new MyDBFaultTolerantServerZK(nodeConfig, myID, isaDB);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
