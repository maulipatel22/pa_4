package server.faulttolerance;

import com.datastax.driver.core.*;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Util;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MyDBReplicableAppGP implements Replicable {

    public static final int SLEEP = 100; // used by tests
    private final Cluster cluster;
    private final Session session;
    private final String keyspace;

    private static final int MAX_LOG_SIZE = 400;
    private final Queue<String> requestLog = new ConcurrentLinkedQueue<>();
    private final File checkpointFile;

    public MyDBReplicableAppGP(String[] args) throws IOException {
        if (args == null || args.length < 1)
            throw new IllegalArgumentException("Must provide keyspace name as args[0]");

        this.keyspace = args[0];
        this.checkpointFile = new File(keyspace + "_checkpoint.dat");

        InetSocketAddress isaDB = args.length > 1 ?
                Util.getInetSocketAddressFromString(args[1]) :
                new InetSocketAddress("localhost", 9042);

        cluster = Cluster.builder()
                .addContactPoint(isaDB.getHostString())
                .withPort(isaDB.getPort())
                .build();
        session = cluster.connect();

        session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace +
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
        session.execute("USE " + keyspace + ";");

        restoreCheckpoint();
    }

    @Override
    public boolean execute(Request request) {
        return execute(request, false);
    }
    @Override
    public boolean execute(Request request, boolean b) {
        if (!(request instanceof RequestPacket))
            throw new IllegalArgumentException("Expected RequestPacket");

        RequestPacket rp = (RequestPacket) request;
        String command = rp.toString();  // <-- fixed

        try {
            session.execute(command);
            requestLog.add(command);

            if (requestLog.size() >= MAX_LOG_SIZE) {
                checkpointState();
                requestLog.clear();
            }

            Thread.sleep(SLEEP);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public Request getRequest(String s) throws RequestParseException {
        return new RequestPacket(s, false);  // <-- fixed
    }


    @Override
    public String checkpoint(String s) {
        try {
            checkpointState();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return checkpointFile.getAbsolutePath();
    }

    @Override
    public boolean restore(String filePath, String s1) {
        try {
            restoreCheckpoint();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }


    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<>();
    }

    public void close() {
        if (session != null) session.close();
        if (cluster != null) cluster.close();
    }

    private void checkpointState() throws IOException {
        try (ObjectOutputStream oos = new ObjectOutputStream(
                new FileOutputStream(checkpointFile))) {

            List<String> logCopy = new ArrayList<>(requestLog);
            oos.writeObject(logCopy);
        }
    }

    @SuppressWarnings("unchecked")
    private void restoreCheckpoint() throws IOException {
        if (!checkpointFile.exists()) return;

        try (ObjectInputStream ois = new ObjectInputStream(
                new FileInputStream(checkpointFile))) {

            Object obj = ois.readObject();
            if (!(obj instanceof List)) {
                throw new IOException("Invalid checkpoint format");
            }

            List<String> savedLog = (List<String>) obj;
            for (String cmd : savedLog) {
                try {
                    session.execute(cmd);
                    requestLog.add(cmd);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }
}
