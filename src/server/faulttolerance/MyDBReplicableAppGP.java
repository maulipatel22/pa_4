package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class MyDBReplicableAppGP implements Replicable {

    public static final int SLEEP = 1000;

    private final Cluster cluster;
    private final Session session;
    private final String keyspace;
    private static final String TABLE = "kv";

    public MyDBReplicableAppGP(String[] args) throws IOException {
        if (args == null || args.length == 0 || args[0] == null) {
            throw new IllegalArgumentException(
                    "MyDBReplicableAppGP requires args[0] = keyspace");
        }

        this.keyspace = args[0];

        String cassandraHost = (args.length > 1 && args[1] != null)
                ? args[1]
                : "127.0.0.1";
        int cassandraPort = 9042;
        if (args.length > 2 && args[2] != null) {
            cassandraPort = Integer.parseInt(args[2]);
        }

        try {
            this.cluster = Cluster.builder()
                    .addContactPoint(cassandraHost)
                    .withPort(cassandraPort)
                    .build();
            this.session = this.cluster.connect();
            initSchema();
        } catch (Exception e) {
            throw new IOException("Error initializing Cassandra", e);
        }
    }

    private void initSchema() {
        String ksCql = "CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH " +
                "replication = {'class':'SimpleStrategy','replication_factor':1}";
        session.execute(ksCql);

        session.execute("USE " + keyspace);

        String tableCql =
                "CREATE TABLE IF NOT EXISTS " + TABLE + " (" +
                        "k text PRIMARY KEY, " +
                        "v int" +
                        ")";
        session.execute(tableCql);
    }

    @Override
    public boolean execute(Request request, boolean doNotReplyToClient) {
        return execute(request);
    }

    @Override
    public boolean execute(Request request) {
        if (!(request instanceof RequestPacket)) {
            return false;
        }

        RequestPacket rp = (RequestPacket) request;
        String reqString = rp.getRequestValue();
        if (reqString == null) {
            return false;
        }

        try {
            session.execute(reqString);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public String checkpoint(String name) {
        return "";
    }

    @Override
    public boolean restore(String name, String state) {
        return true;
    }

    @Override
    public Request getRequest(String s) throws RequestParseException {
        return null;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<IntegerPacketType>();
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            if (session != null) session.close();
            if (cluster != null) cluster.close();
        } finally {
            super.finalize();
        }
    }
}
