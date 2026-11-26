package ro.ubm.sd.common;

import java.nio.file.Path;
import java.nio.file.Paths;

public class NodeConfig {
    private final String nodeId;
    private final String host;
    private final int port;
    private final Path dataDir;
    private final boolean coordinator;

    public NodeConfig(String nodeId, String host, int port, Path dataDir, boolean coordinator) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.dataDir = dataDir;
        this.coordinator = coordinator;
    }

    public static NodeConfig fromArgs(String[] args) {
        String id = null;
        String host = "127.0.0.1";
        int port = -1;
        String dir = null;
        boolean coordinator = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--id" -> {
                    if (i + 1 >= args.length) throw new IllegalArgumentException("Lipseste valoarea pentru --id");
                    id = args[++i];
                }
                case "--host" -> {
                    if (i + 1 >= args.length) throw new IllegalArgumentException("Lipseste valoarea pentru --host");
                    host = args[++i];
                }
                case "--port" -> {
                    if (i + 1 >= args.length) throw new IllegalArgumentException("Lipseste valoarea pentru --port");
                    port = Integer.parseInt(args[++i]);
                }
                case "--dir" -> {
                    if (i + 1 >= args.length) throw new IllegalArgumentException("Lipseste valoarea pentru --dir");
                    dir = args[++i];
                }
                case "--coordinator" -> coordinator = true;
                default -> throw new IllegalArgumentException("Argument necunoscut: " + args[i]);
            }
        }

        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Lipseste argumentul obligatoriu --id");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Argumentul --port trebuie sa fie intre 1 si 65535");
        }
        if (dir == null || dir.isBlank()) {
            throw new IllegalArgumentException("Lipseste argumentul obligatoriu --dir");
        }

        return new NodeConfig(id, host, port, Paths.get(dir), coordinator);
    }

    public String getNodeId() { return nodeId; }
    public String getHost() { return host; }
    public int getPort() { return port; }
    public Path getDataDir() { return dataDir; }
    public boolean isCoordinator() { return coordinator; }
}
