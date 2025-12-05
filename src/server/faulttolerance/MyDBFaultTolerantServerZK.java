package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.utils.Util;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import server.MyDBSingleServer;
import server.ReplicatedServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;

public class MyDBFaultTolerantServerZK extends MyDBSingleServer implements Watcher {

    public static final int SLEEP = 1000;
    public static final boolean DROP_TABLES_AFTER_TESTS = true;
    public static final int MAX_LOG_SIZE = 400;
    public static final int DEFAULT_PORT = 2181;

    private final String myID;
    private final Cluster cluster;
    private final Session session;
    private ZooKeeper zk;
    private final String zkBase = "/seq";

    private final SortedMap<String, String> pendingOps = Collections.synchronizedSortedMap(new TreeMap<>());
    private String lastAppliedOp = null;

    private final ExecutorService applyExecutor = Executors.newSingleThreadExecutor();

    public MyDBFaultTolerantServerZK(edu.umass.cs.nio.interfaces.NodeConfig<String> nodeConfig,
                                     String myID, InetSocketAddress isaDB) throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET), isaDB, myID);

        this.myID = myID;
        this.cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        this.session = this.cluster.connect(myID);

        try {
            CountDownLatch connectedSignal = new CountDownLatch(1);
            this.zk = new ZooKeeper("localhost:" + DEFAULT_PORT, 3000, event -> {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected)
                    connectedSignal.countDown();
            });
            connectedSignal.await();

            if (zk.exists(zkBase, false) == null) {
                zk.create(zkBase, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            List<String> children = zk.getChildren(zkBase, false);
            Collections.sort(children);
            for (String child : children) {
                byte[] data = zk.getData(zkBase + "/" + child, false, null);
                pendingOps.put(child, new String(data, StandardCharsets.UTF_8));
            }

            if (!pendingOps.isEmpty()) {
                lastAppliedOp = pendingOps.lastKey();
            }

            applyPendingOps(); 

        } catch (Exception e) {
            throw new IOException("Failed to initialize ZooKeeper client", e);
        }

        log.log(Level.INFO, "ZK-based server {0} started and connected to Cassandra keyspace {1}",
                new Object[]{this.myID, myID});
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        final String request = new String(bytes, StandardCharsets.UTF_8);
        log.info(this.myID + " received client request: " + request + " from " + header.sndr);

        applyExecutor.submit(() -> {
            try {
                String created = zk.create(zkBase + "/op-", request.getBytes(StandardCharsets.UTF_8),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                String nodeName = created.substring(zkBase.length() + 1);

                pendingOps.put(nodeName, request);
                applyPendingOps();

            } catch (KeeperException | InterruptedException e) {
                log.log(Level.SEVERE, "Failed to create sequential node in Zookeeper", e);
            }
        });
    }

    private void applyPendingOps() {
        synchronized (pendingOps) {
            List<String> keys = new ArrayList<>(pendingOps.keySet());
            Collections.sort(keys);

            for (String nodeName : keys) {
                if (lastAppliedOp == null || nodeName.compareTo(lastAppliedOp) > 0) {
                    String cmd = pendingOps.get(nodeName);
                    try {
                        if (cmd != null && !cmd.trim().isEmpty()) {
                            session.execute(cmd);
                        }
                        lastAppliedOp = nodeName;

                        if (pendingOps.size() > MAX_LOG_SIZE) {
                            int removeCount = pendingOps.size() - MAX_LOG_SIZE;
                            Iterator<String> it = pendingOps.keySet().iterator();
                            while (removeCount-- > 0 && it.hasNext()) {
                                it.next();
                                it.remove();
                            }
                        }
                    } catch (Exception e) {
                        log.log(Level.SEVERE, "Failed to apply op " + nodeName + " command:" + cmd, e);
                    }
                }
            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
    }

    @Override
    public void close() {
        super.close();
        applyExecutor.shutdownNow();
        try {
            if (zk != null) zk.close();
        } catch (InterruptedException ignored) {}
        if (session != null) session.close();
        if (cluster != null) cluster.close();
    }

    public static void main(String[] args) throws IOException {
        new MyDBFaultTolerantServerZK(
                edu.umass.cs.nio.nioutils.NodeConfigUtils.getNodeConfigFromFile(
                        args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer.SERVER_PORT_OFFSET),
                args[1],
                args.length > 2 ? Util.getInetSocketAddressFromString(args[2])
                        : new InetSocketAddress("localhost", 9042)
        );
    }
}
