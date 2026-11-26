package ro.ubm.sd.node;

import ro.ubm.sd.common.FileMetadata;
import ro.ubm.sd.common.TcpUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TcpDataServer {

    private final String nodeId;
    private final int port;
    private final LocalStorageManager storage;
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private volatile boolean running = true;
    private ServerSocket serverSocket;

    public TcpDataServer(String nodeId, int port, LocalStorageManager storage) {
        this.nodeId = nodeId;
        this.port = port;
        this.storage = storage;
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        Thread acceptThread = new Thread(() -> {
            while (running) {
                try {
                    Socket client = serverSocket.accept();
                    pool.submit(() -> handleConnection(client));
                } catch (IOException e) {
                    if (running) {
                        System.err.printf("[%s] Eroare accept TCP: %s%n", nodeId, e.getMessage());
                    }
                }
            }
        }, "tcp-server-" + nodeId);
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    public void stop() {
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {
        }
        pool.shutdownNow();
    }

    private void handleConnection(Socket socket) {
        try (socket) {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            String header = TcpUtil.readLine(in);
            if (header == null || header.isBlank()) {
                return;
            }

            String[] parts = header.split(" ");
            String cmd = parts[0];

            switch (cmd) {
                case "PUT" -> handlePut(parts, in, out);
                case "REPLICATE" -> handleReplicate(parts, in, out);
                case "GET" -> handleGet(parts, out);
                default -> TcpUtil.writeLine(out, "ERROR Comanda necunoscuta: " + cmd);
            }
        } catch (Exception e) {
            System.err.printf("[%s] Eroare conexiune TCP: %s%n", nodeId, e.getMessage());
        }
    }

    // PUT <uuid> <encodedName> <size> <checksum> <secondaryHost> <secondaryPort>
    private void handlePut(String[] parts, InputStream in, OutputStream out) throws IOException {
        if (parts.length < 7) {
            TcpUtil.writeLine(out, "ERROR Antet PUT incomplet");
            return;
        }
        UUID id = UUID.fromString(parts[1]);
        String name = URLDecoder.decode(parts[2], StandardCharsets.UTF_8);
        long size = Long.parseLong(parts[3]);
        String checksum = parts[4];
        String secondaryHost = parts[5];
        int secondaryPort = Integer.parseInt(parts[6]);

        try {
            storage.saveFromStream(id, name, size, checksum, in);
            System.out.printf("[%s] Stocat local %s (%s, %d octeti). Replicare catre %s:%d...%n",
                    nodeId, name, id, size, secondaryHost, secondaryPort);

            boolean replicated = sendReplicaToSecondary(id, name, size, checksum, secondaryHost, secondaryPort);
            if (!replicated) {
                storage.deleteReplica(id);
                TcpUtil.writeLine(out, "ERROR Replicarea catre nodul secundar a esuat");
                return;
            }
            TcpUtil.writeLine(out, "STORED");
        } catch (IOException e) {
            storage.deleteReplica(id);
            TcpUtil.writeLine(out, "ERROR " + e.getMessage());
        }
    }

    // REPLICATE <uuid> <encodedName> <size> <checksum>
    private void handleReplicate(String[] parts, InputStream in, OutputStream out) throws IOException {
        if (parts.length < 5) {
            TcpUtil.writeLine(out, "ERROR Antet REPLICATE incomplet");
            return;
        }
        UUID id = UUID.fromString(parts[1]);
        String name = URLDecoder.decode(parts[2], StandardCharsets.UTF_8);
        long size = Long.parseLong(parts[3]);
        String checksum = parts[4];

        try {
            storage.saveFromStream(id, name, size, checksum, in);
            System.out.printf("[%s] Replica primita si verificata: %s (%s)%n", nodeId, name, id);
            TcpUtil.writeLine(out, "STORED");
        } catch (IOException e) {
            TcpUtil.writeLine(out, "ERROR " + e.getMessage());
        }
    }

    // GET <uuid>
    private void handleGet(String[] parts, OutputStream out) throws IOException {
        if (parts.length < 2) {
            TcpUtil.writeLine(out, "ERROR Antet GET incomplet");
            return;
        }
        UUID id = UUID.fromString(parts[1]);
        Path dataPath = storage.getDataFilePath(id);
        FileMetadata meta = storage.readMetadataFile(id);

        if (meta == null || !Files.exists(dataPath)) {
            TcpUtil.writeLine(out, "NOT_FOUND");
            return;
        }

        TcpUtil.writeLine(out, "OK " + meta.getSize() + " " + meta.getChecksum());
        Files.copy(dataPath, out);
        out.flush();
    }

    private boolean sendReplicaToSecondary(UUID id, String name, long size, String checksum,
                                           String host, int port) {
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(15000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            String encodedName = URLEncoder.encode(name, StandardCharsets.UTF_8);
            TcpUtil.writeLine(out, String.format("REPLICATE %s %s %d %s", id, encodedName, size, checksum));
            Files.copy(storage.getDataFilePath(id), out);
            out.flush();

            String response = TcpUtil.readLine(in);
            return "STORED".equals(response);
        } catch (IOException e) {
            System.err.printf("[%s] Eroare la replicarea catre %s:%d: %s%n", nodeId, host, port, e.getMessage());
            return false;
        }
    }
}
