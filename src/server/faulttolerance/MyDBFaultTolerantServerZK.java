package server.faulttolerance;

import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import server.AVDBReplicatedServer;
import server.ReplicatedServer;
import server.MyDBSingleServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.json.JSONObject;

public class MyDBFaultTolerantServerZK extends MyDBSingleServer {

    public static final int SLEEP = 1000;
    public static final boolean DROP_TABLES_AFTER_TESTS = true;
    public static final int MAX_LOG_SIZE = 400;

    private final Cluster cluster;
    private final Session session;

    private final server.ReplicatedServer.ServerMessenger serverMessenger;

    private final List<JSONObject> requestLog = new CopyOnWriteArrayList<>();
    private final Map<Long, JSONObject> pendingAcks = new ConcurrentHashMap<>();

    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String myID,
                                     InetSocketAddress isaDB) throws IOException {
        super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
                        nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET),
                isaDB, myID);

        cluster = Cluster.builder().addContactPoint(isaDB.getHostString())
                .withPort(isaDB.getPort()).build();
        session = cluster.connect();

        serverMessenger = new server.ReplicatedServer.ServerMessenger(nodeConfig, myID, DEFAULT_PORT);

        recoverFromCheckpoint();
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        try {
            String msg = new String(bytes);
            JSONObject req = new JSONObject(msg);

            session.execute(req.getString("query"));

            requestLog.add(req);
            if (requestLog.size() > MAX_LOG_SIZE) {
                requestLog.remove(0);
            }

            for (String node : serverMessenger.getNodeConfig().getNodeIDs()) {
                if (!node.equals(header.sndr.toString())) {
                    serverMessenger.send(node, msg.getBytes());
                }
            }

            sendAck(header.sndr, req.getLong("reqId"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        try {
            String msg = new String(bytes);
            JSONObject req = new JSONObject(msg);

            long reqId = req.getLong("reqId");
            if (!pendingAcks.containsKey(reqId)) {
                session.execute(req.getString("query"));
                pendingAcks.put(reqId, req);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendAck(InetSocketAddress client, long reqId) {
        try {
            JSONObject ack = new JSONObject();
            ack.put("reqId", reqId);
            ack.put("status", "OK");
            serverMessenger.send(client, ack.toString().getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        if (serverMessenger != null) serverMessenger.close();
        if (session != null) session.close();
        if (cluster != null) cluster.close();
    }

    private void recoverFromCheckpoint() {

        System.out.println("Recovering from checkpoint (not implemented yet).");
    }

    public static void main(String[] args) throws IOException {
        NodeConfig<String> nodeConfig = NodeConfigUtils.getNodeConfigFromFile(
                args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer.SERVER_PORT_OFFSET);
        String myID = args[1];
        InetSocketAddress isaDB = args.length > 2 ? Util.getInetSocketAddressFromString(args[2])
                : new InetSocketAddress("localhost", 9042);
        new MyDBFaultTolerantServerZK(nodeConfig, myID, isaDB);
    }
}
