package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

public class MyDBReplicableAppGP implements Replicable {

    private static final int SLEEP = 1000;

    private Cluster cluster;
    private Session session;
    private final String keyspace;


    public MyDBReplicableAppGP(String[] args) throws IOException {
        if (args == null || args.length < 1) {
            throw new IllegalArgumentException("Must provide keyspace name as args[0]");
        }
        this.keyspace = args[0];

        InetSocketAddress isaDB = args.length > 1 ?
                NodeConfigUtils.getInetSocketAddressFromString(args[1]) :
                new InetSocketAddress("localhost", 9042);

        cluster = Cluster.builder()
                .addContactPoint(isaDB.getHostString())
                .withPort(isaDB.getPort())
                .build();
        session = cluster.connect();

        session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace +
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");

        session.execute("USE " + keyspace + ";");
    }

    @Override
    public boolean execute(Request request) {
        return execute(request, false);
    }

    @Override
    public boolean execute(Request request, boolean b) {
        if (!(request instanceof RequestPacket)) {
            throw new IllegalArgumentException("Expected RequestPacket");
        }

        RequestPacket rp = (RequestPacket) request;

        String command = rp.getCommand();

        try {
            session.execute(command);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public String checkpoint(String s) {
        return "";
    }

    @Override
    public boolean restore(String s, String s1) {
        return true;
    }

    @Override
    public Request getRequest(String s) throws RequestParseException {
        return null;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<>();
    }

    public void close() {
        if (session != null) session.close();
        if (cluster != null) cluster.close();
    }
}
