package server.faulttolerance;

import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import server.AVDBReplicatedServer;
import server.MyDBSingleServer;
import server.ReplicatedServer;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Fault-tolerant replicated server using Zookeeper for leader election and request ordering.
 * Implements AVDBReplicatedServer-style proposal + ack protocol.
 */
public class MyDBFaultTolerantServerZK extends MyDBSingleServer implements Watcher {

    private static final String ZK_ROOT = "/replicated_db";
    private static final String ZK_LEADER = ZK_ROOT + "/leader";
    private static final int SESSION_TIMEOUT = 5000;

    private ZooKeeper zk;
    private boolean isLeader = false;
    private String myID;

    // Request queue and sequencing
    private final ConcurrentHashMap<Long, JSONObject> requestQueue = new ConcurrentHashMap<>();
    private long nextRequestId = 0; // local request counter
    private long expectedId = 0;    // next request to execute

    // Leader tracking acks
    private CopyOnWriteArrayList<String> notAcked;

    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String myID, InetSocketAddress isaDB) throws IOException, KeeperException, InterruptedException {
        super(isaDB, myID);
        this.myID = myID;

        // Connect to Zookeeper
        zk = new ZooKeeper("localhost:2181", SESSION_TIMEOUT, this);

        // Ensure root exists
        if (zk.exists(ZK_ROOT, false) == null) {
            zk.create(ZK_ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        // Elect leader
        electLeader();
    }

    /** Zookeeper watcher for leader deletion */
    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeDeleted && event.getPath().equals(ZK_LEADER)) {
            try {
                electLeader();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /** Leader election via ephemeral ZNode */
    private void electLeader() throws KeeperException, InterruptedException {
        try {
            zk.create(ZK_LEADER, myID.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            isLeader = true;
            System.out.println(myID + " is elected leader.");
        } catch (KeeperException.NodeExistsException e) {
            isLeader = false;
            zk.exists(ZK_LEADER, true); // watch for leader deletion
            System.out.println(myID + " is a follower.");
        }
    }

    /** Handle client requests */
    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        try {
            JSONObject req = new JSONObject(new String(bytes));
            req.put("client", header.sndr);
            req.put("id", nextRequestId++);

            if (isLeader) {
                // Leader directly proposes request
                proposeRequest(req);
            } else {
                // Forward request to leader
                String leaderId = new String(zk.getData(ZK_LEADER, false, new Stat()));
                serverMessenger.send(leaderId, req.toString().getBytes());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /** Handle server messages (proposals and acks) */
    @Override
    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        try {
            JSONObject json = new JSONObject(new String(bytes));
            String type = json.getString("type");

            if (type.equals("PROPOSAL")) {
                long reqId = json.getLong("id");
                String query = json.getString("request");

                // Execute in order
                requestQueue.put(reqId, json);
                executePendingRequests();

                // Send acknowledgement back to leader
                JSONObject ack = new JSONObject();
                ack.put("type", "ACK");
                ack.put("id", reqId);
                ack.put("node", myID);
                serverMessenger.send(header.sndr, ack.toString().getBytes());

            } else if (type.equals("ACK") && isLeader) {
                String node = json.getString("node");
                notAcked.remove(node);

                if (notAcked.isEmpty()) {
                    // All followers acknowledged, proceed to next
                    expectedId++;
                    executeNextProposal();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /** Leader proposes a request to all nodes */
    private void proposeRequest(JSONObject req) throws Exception {
        long reqId = req.getLong("id");
        req.put("type", "PROPOSAL");
        requestQueue.put(reqId, req);

        // Track acknowledgements
        notAcked = new CopyOnWriteArrayList<>(serverMessenger.getNodeConfig().getNodeIDs());

        // Broadcast to all nodes
        broadcast(req);
    }

    /** Broadcast a JSON message to all nodes */
    private void broadcast(JSONObject json) throws IOException {
        for (String node : serverMessenger.getNodeConfig().getNodeIDs()) {
            serverMessenger.send(node, json.toString().getBytes());
        }
    }

    /** Execute pending requests in order */
    private void executePendingRequests() throws Exception {
        while (requestQueue.containsKey(expectedId)) {
            JSONObject req = requestQueue.remove(expectedId);
            String client = req.getString("client");
            String query = req.getString("request");

            // Execute query on Cassandra backend
            session.execute(query);

            // Respond to client
            String response = "[success:" + query + "]";
            serverMessenger.send(client, response.getBytes());

            expectedId++;
        }
    }

    /** Leader executes next proposal after all ACKs received */
    private void executeNextProposal() throws Exception {
        if (requestQueue.containsKey(expectedId)) {
            JSONObject req = requestQueue.get(expectedId);
            broadcast(req);
        }
    }

    /** Graceful shutdown */
    @Override
    public void close() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        super.close();
    }

    /** Main method */
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: MyDBFaultTolerantServerZK <server.properties> <myID> [DB address]");
            System.exit(1);
        }

        InetSocketAddress isaDB = args.length > 2
                ? new InetSocketAddress(args[2].split(":")[0], Integer.parseInt(args[2].split(":")[1]))
                : new InetSocketAddress("localhost", 9042);

        new MyDBFaultTolerantServerZK(
                edu.umass.cs.nio.nioutils.NodeConfigUtils.getNodeConfigFromFile(args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer.SERVER_PORT_OFFSET),
                args[1],
                isaDB
        );
    }
}
