package server.faulttolerance;

import com.datastax.driver.core.*;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;

import java.io.IOException;
import java.util.*;

public class MyDBReplicableAppGP implements Replicable {

    private final Cluster cluster;
    private final Session session;
    private final String keyspace;

    private static final String TABLE = "grade";

    public MyDBReplicableAppGP(String[] args) throws IOException {
        if (args == null || args.length == 0)
            throw new IOException("Keyspace missing");

        this.keyspace = args[0];
        String host = (args.length > 1 ? args[1] : "127.0.0.1");
        int port = (args.length > 2 ? Integer.parseInt(args[2]) : 9042);

        try {
            this.cluster = Cluster.builder()
                    .addContactPoint(host)
                    .withPort(port)
                    .build();

            this.session = this.cluster.connect();
            initSchema();
        } catch (Exception e) {
            throw new IOException("Failed to initialize Cassandra", e);
        }
    }

    private void initSchema() {
        session.execute(
                "CREATE KEYSPACE IF NOT EXISTS " + keyspace +
                        " WITH replication = {'class':'SimpleStrategy','replication_factor':1};"
        );

        session.execute("USE " + keyspace);

        session.execute(
                "CREATE TABLE IF NOT EXISTS " + TABLE + " (" +
                        "id int PRIMARY KEY, " +
                        "events list<int>" +
                        ");"
        );
    }

    @Override
    public boolean execute(Request request, boolean doNotReply) {
        return execute(request);
    }

    @Override
    public boolean execute(Request request) {
    if (!(request instanceof RequestPacket))
        return false;

    RequestPacket rp = (RequestPacket) request;

    String raw = rp.getRequestValue();
    System.out.println(">>> RAW RECEIVED: " + raw);

    String cql = extractCQL(raw);
    System.out.println(">>> CQL EXTRACTED: " + cql);

    if (cql == null || cql.trim().isEmpty()) {
        rp.setResponse("ERROR: empty CQL");
        return false;
    }

    try {
        boolean isSelect = cql.trim().toUpperCase().startsWith("SELECT");
        ResultSet rs = session.execute(cql);

        if (isSelect) {
            StringBuilder sb = new StringBuilder();
            for (Row row : rs) sb.append(row.toString()).append("\n");
            rp.setResponse(sb.toString());
        } else {
            rp.setResponse("OK");
        }

        return true;

    } catch (Exception e) {
        rp.setResponse("ERROR: " + e.getMessage());
        return false;
    }
}

    private String extractCQL(String raw) {
        if (raw == null) return null;
        raw = raw.trim();

        if (!raw.startsWith("{"))
            return raw;

        try {
            int qvIndex = raw.indexOf("\"QV\"");
            if (qvIndex < 0) return null;

            int colon = raw.indexOf(":", qvIndex);
            if (colon < 0) return null;

            int start = raw.indexOf("\"", colon) + 1;

            int end = start;
            while (end < raw.length()) {
                if (raw.charAt(end) == '"' &&
                    raw.charAt(end - 1) != '\\' &&
                    (end + 1 >= raw.length() || raw.charAt(end + 1) == ',' || raw.charAt(end + 1) == '}')) {
                    break;
                }
                end++;
            }

            return raw.substring(start, end);

        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public String checkpoint(String name) {
        try {
            session.execute("USE " + keyspace);

            ResultSet rs = session.execute("SELECT id, events FROM grade");
            StringBuilder sb = new StringBuilder("{");

            boolean first = true;
            for (Row row : rs) {
                if (!first) sb.append(",");
                first = false;

                List<Integer> events = row.getList("events", Integer.class);

                sb.append("{\"id\":")
                        .append(row.getInt("id"))
                        .append(",\"events\":")
                        .append(events != null ? events.toString() : "[]")
                        .append("}");
            }

            sb.append("}");
            return sb.toString();

        } catch (Exception e) {
            return "{}";
        }
    }

    @Override
    public boolean restore(String name, String state) {
        try {
            session.execute("USE " + keyspace);
            session.execute("TRUNCATE grade");

            if (state == null || state.trim().equals("{}"))
                return true;

            String trimmed = state.trim();
            trimmed = trimmed.substring(1, trimmed.length() - 1).trim();  // strip {}
            if (trimmed.isEmpty()) return true;

            String[] entries = trimmed.split("\\},\\{");

            for (String entry : entries) {
                entry = entry.replace("{", "").replace("}", "");

                String[] parts = entry.split(",\"events\":");

                int id = Integer.parseInt(parts[0].split(":")[1].trim());

                String eventsStr = parts[1].replace("[", "").replace("]", "").trim();

                List<Integer> events = new ArrayList<>();
                if (!eventsStr.isEmpty()) {
                    for (String e : eventsStr.split(","))
                        events.add(Integer.parseInt(e.trim()));
                }

                PreparedStatement ps = session.prepare(
                        "INSERT INTO grade (id, events) VALUES (?, ?)"
                );
                session.execute(ps.bind(id, events));
            }

            return true;

        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public Request getRequest(String s) {
        return null;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<>();
    }
}
