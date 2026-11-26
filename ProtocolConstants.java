package ro.ubm.sd.common;

public final class ProtocolConstants {
    public static final String MULTICAST_GROUP = "230.0.0.1";
    public static final int MULTICAST_PORT = 4446;

    // Portul RMI este derivat automat din portul TCP: ex. TCP 7001 -> RMI 8001
    public static final int RMI_PORT_OFFSET = 1000;
    public static final String RMI_SERVICE_NAME = "StorageNodeService";

    public static final int REPLICATION_FACTOR = 2;
    public static final long MAX_FILE_SIZE_BYTES = 512L * 1024 * 1024; // 512 MB

    // Timpi pentru Etapa 3 (detectare sub 5s) si Etapa 5 (Bully sub 10s)
    public static final long HEARTBEAT_INTERVAL_MS = 1000;
    public static final long SUSPECT_TIMEOUT_MS = 2500;
    public static final long DEAD_TIMEOUT_MS = 4500;

    private ProtocolConstants() {}

    // Compara ID-uri precum n1, n2, n10 numeric daca au sufix numeric, altfel alfabetic
    public static int compareNodeIds(String id1, String id2) {
        Integer num1 = extractTrailingNumber(id1);
        Integer num2 = extractTrailingNumber(id2);
        if (num1 != null && num2 != null) {
            int cmp = Integer.compare(num1, num2);
            if (cmp != 0) return cmp;
        }
        return id1.compareTo(id2);
    }

    private static Integer extractTrailingNumber(String id) {
        if (id == null || id.isEmpty()) return null;
        int i = id.length() - 1;
        while (i >= 0 && Character.isDigit(id.charAt(i))) {
            i--;
        }
        if (i == id.length() - 1) return null;
        try {
            return Integer.parseInt(id.substring(i + 1));
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
