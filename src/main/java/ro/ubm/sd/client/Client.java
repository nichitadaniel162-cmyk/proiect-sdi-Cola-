package ro.ubm.sd.client;

import ro.ubm.sd.common.ChecksumUtil;
import ro.ubm.sd.common.ClientConfig;
import ro.ubm.sd.common.FileMetadata;
import ro.ubm.sd.common.NodeInfo;
import ro.ubm.sd.common.ProtocolConstants;
import ro.ubm.sd.common.TcpUtil;
import ro.ubm.sd.rmi.AllocationPlan;
import ro.ubm.sd.rmi.LocateResult;
import ro.ubm.sd.rmi.NodeRemoteService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Enumeration;
import java.util.List;
import java.util.Scanner;

public class Client {

    private volatile String coordinatorHost;
    private volatile int coordinatorRmiPort;
    private volatile boolean running = true;

    public Client(String coordinatorAddress) {
        String[] parts = coordinatorAddress.split(":");
        this.coordinatorHost = parts[0];
        int tcpPort = parts.length > 1 ? Integer.parseInt(parts[1]) : 7001;
        this.coordinatorRmiPort = tcpPort + ProtocolConstants.RMI_PORT_OFFSET;
    }

    public void start() {
        System.setProperty("java.net.preferIPv4Stack", "true");
        startCoordinatorDiscoveryListener();

        System.out.printf("[client] coordonator initial: %s (RMI %d); comenzi: put, get, ls, rm, nodes, exit%n",
                coordinatorHost, coordinatorRmiPort);

        try (Scanner scanner = new Scanner(System.in)) {
            while (true) {
                System.out.print("> ");
                if (!scanner.hasNextLine()) break;

                String line = scanner.nextLine().trim();
                if (line.isEmpty()) continue;
                if ("exit".equalsIgnoreCase(line)) {
                    running = false;
                    System.out.println("[client] inchidere...");
                    break;
                }
                executeCommand(line);
            }
        }
    }

    private void startCoordinatorDiscoveryListener() {
        Thread t = new Thread(() -> {
            try (MulticastSocket socket = new MulticastSocket(ProtocolConstants.MULTICAST_PORT)) {
                socket.setReuseAddress(true);
                InetAddress group = InetAddress.getByName(ProtocolConstants.MULTICAST_GROUP);
                InetSocketAddress groupAddr = new InetSocketAddress(group, ProtocolConstants.MULTICAST_PORT);
                try {
                    socket.joinGroup(groupAddr, null);
                } catch (IOException e) {
                    Enumeration<NetworkInterface> nifs = NetworkInterface.getNetworkInterfaces();
                    while (nifs.hasMoreElements()) {
                        NetworkInterface nif = nifs.nextElement();
                        if (nif.isUp() && nif.supportsMulticast()) {
                            try {
                                socket.joinGroup(groupAddr, nif);
                                break;
                            } catch (IOException ignored) {
                            }
                        }
                    }
                }

                byte[] buf = new byte[1024];
                while (running) {
                    DatagramPacket packet = new DatagramPacket(buf, buf.length);
                    socket.receive(packet);
                    String msg = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8).trim();
                    String[] parts = msg.split("\\|");

                    if ("COORDINATOR".equals(parts[0]) && parts.length >= 5) {
                        updateCoordinator(parts[2], Integer.parseInt(parts[4]), parts[1]);
                    } else if ("HEARTBEAT".equals(parts[0]) && parts.length >= 7 && Boolean.parseBoolean(parts[5])) {
                        updateCoordinator(parts[2], Integer.parseInt(parts[4]), parts[1]);
                    }
                }
            } catch (IOException ignored) {
            }
        }, "client-discovery");
        t.setDaemon(true);
        t.start();
    }

    private void updateCoordinator(String host, int rmiPort, String coordId) {
        if (!host.equals(coordinatorHost) || rmiPort != coordinatorRmiPort) {
            coordinatorHost = host;
            coordinatorRmiPort = rmiPort;
            System.out.printf("%n[client] Coordonator actualizat automat: %s (%s, RMI %d)%n> ",
                    coordId, host, rmiPort);
        }
    }

    private NodeRemoteService getCoordinatorStub() throws Exception {
        Registry registry = LocateRegistry.getRegistry(coordinatorHost, coordinatorRmiPort);
        return (NodeRemoteService) registry.lookup(ProtocolConstants.RMI_SERVICE_NAME);
    }

    private void executeCommand(String line) {
        String[] parts = line.split("\\s+");
        String cmd = parts[0].toLowerCase();

        try {
            switch (cmd) {
                case "put" -> {
                    if (parts.length < 2) {
                        System.out.println("Utilizare: put <cale_fisier>");
                        return;
                    }
                    handlePut(parts[1]);
                }
                case "get" -> {
                    if (parts.length < 2) {
                        System.out.println("Utilizare: get <nume_sau_id> [director_destinatie]");
                        return;
                    }
                    String destDir = parts.length >= 3 ? parts[2] : "./out";
                    handleGet(parts[1], destDir);
                }
                case "ls" -> handleLs();
                case "rm" -> {
                    if (parts.length < 2) {
                        System.out.println("Utilizare: rm <nume_sau_id>");
                        return;
                    }
                    handleRm(parts[1]);
                }
                case "nodes" -> handleNodes();
                default -> System.out.println("Comanda necunoscuta. Comenzi disponibile: put, get, ls, rm, nodes, exit");
            }
        } catch (Exception e) {
            System.err.println("[client] Eroare: " + extractMessage(e));
        }
    }

    private void handlePut(String localPathStr) throws Exception {
        Path localPath = Paths.get(localPathStr);
        if (!Files.exists(localPath) || !Files.isRegularFile(localPath)) {
            System.err.println("[client] Fisierul local nu exista: " + localPathStr);
            return;
        }

        String fileName = localPath.getFileName().toString();
        long size = Files.size(localPath);
        String checksum = ChecksumUtil.computeSha256(localPath);

        NodeRemoteService coordinator = getCoordinatorStub();
        AllocationPlan plan = coordinator.allocateUpload(fileName, size);

        NodeInfo primary = plan.getTargetNodes().get(0);
        NodeInfo secondary = plan.getTargetNodes().get(1);

        try (Socket socket = new Socket(primary.getHost(), primary.getPort())) {
            socket.setSoTimeout(20000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            String encodedName = URLEncoder.encode(fileName, StandardCharsets.UTF_8);
            TcpUtil.writeLine(out, String.format("PUT %s %s %d %s %s %d",
                    plan.getFileId(), encodedName, size, checksum,
                    secondary.getHost(), secondary.getPort()));
            Files.copy(localPath, out);
            out.flush();

            String response = TcpUtil.readLine(in);
            if (!"STORED".equals(response)) {
                System.err.println("[client] Incarcare esuata pe noduri: " + response);
                return;
            }
        }

        FileMetadata metadata = new FileMetadata(
                plan.getFileId(), fileName, size, checksum,
                List.of(primary.getNodeId(), secondary.getNodeId())
        );
        coordinator.commitUpload(metadata);

        System.out.printf("[client] Incarcat cu succes! ID: %s | Noduri: [%s, %s] | SHA-256: %s%n",
                metadata.getId(), primary.getNodeId(), secondary.getNodeId(), checksum);
    }

    private void handleGet(String nameOrId, String destDirStr) throws Exception {
        NodeRemoteService coordinator = getCoordinatorStub();
        LocateResult locate = coordinator.locateFile(nameOrId);
        if (locate == null) {
            System.err.println("[client] Fisierul nu a fost gasit: " + nameOrId);
            return;
        }

        FileMetadata meta = locate.getMetadata();
        List<NodeInfo> nodes = locate.getReplicaNodes();
        if (nodes.isEmpty()) {
            System.err.println("[client] Nicio copie accesibila in acest moment pentru: " + meta.getName());
            return;
        }

        Path destDir = Paths.get(destDirStr);
        Files.createDirectories(destDir);
        Path targetFile = destDir.resolve(meta.getName());
        Path tempFile = destDir.resolve(meta.getName() + ".part");

        for (NodeInfo node : nodes) {
            try (Socket socket = new Socket(node.getHost(), node.getPort())) {
                socket.setSoTimeout(15000);
                OutputStream out = socket.getOutputStream();
                InputStream in = socket.getInputStream();

                TcpUtil.writeLine(out, "GET " + meta.getId());
                String header = TcpUtil.readLine(in);
                if (header == null || !header.startsWith("OK ")) {
                    System.err.printf("[client] Nodul %s nu detine copia (%s), incerc alt nod...%n",
                            node.getNodeId(), header);
                    continue;
                }

                String[] headerParts = header.split(" ");
                long size = Long.parseLong(headerParts[1]);

                String actualChecksum;
                try (OutputStream fileOut = Files.newOutputStream(tempFile)) {
                    actualChecksum = TcpUtil.copyExactAndHash(in, fileOut, size);
                }

                if (!actualChecksum.equalsIgnoreCase(meta.getChecksum())) {
                    Files.deleteIfExists(tempFile);
                    System.err.printf("[client] Checksum gresit de la %s, incerc a doua copie...%n", node.getNodeId());
                    continue;
                }

                Files.move(tempFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
                System.out.printf("[client] Descarcat de pe %s -> %s (checksum SHA-256 verificat OK)%n",
                        node.getNodeId(), targetFile);
                return;
            } catch (IOException e) {
                Files.deleteIfExists(tempFile);
                System.err.printf("[client] Eroare la descarcarea de pe %s (%s), incerc a doua copie...%n",
                        node.getNodeId(), e.getMessage());
            }
        }

        System.err.println("[client] Descarcare esuata: nicio copie valida nu a putut fi citita.");
    }

    private void handleLs() throws Exception {
        NodeRemoteService coordinator = getCoordinatorStub();
        List<FileMetadata> files = coordinator.listFiles();
        if (files.isEmpty()) {
            System.out.println("(nu exista fisiere stocate)");
            return;
        }
        System.out.printf("%-36s | %-20s | %10s | %s%n", "ID", "NUME", "OCTETI", "NODURI");
        System.out.println("-".repeat(90));
        for (FileMetadata f : files) {
            System.out.printf("%-36s | %-20s | %10d | %s%n",
                    f.getId(), f.getName(), f.getSize(), f.getReplicas());
        }
    }

    private void handleRm(String nameOrId) throws Exception {
        NodeRemoteService coordinator = getCoordinatorStub();
        String result = coordinator.deleteFile(nameOrId);
        System.out.println("[client] " + result);
    }

    private void handleNodes() throws Exception {
        NodeRemoteService coordinator = getCoordinatorStub();
        List<NodeInfo> nodes = coordinator.getClusterNodes();
        System.out.println("=== Noduri in cluster ===");
        for (NodeInfo n : nodes) {
            System.out.println("  " + n);
        }
    }

    private String extractMessage(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return root.getMessage() != null ? root.getMessage() : root.toString();
    }

    public static void main(String[] args) {
        System.setProperty("java.net.preferIPv4Stack", "true");
        try {
            ClientConfig config = ClientConfig.fromArgs(args);
            new Client(config.getCoordinatorAddress()).start();
        } catch (IllegalArgumentException e) {
            System.err.println("Eroare argumente: " + e.getMessage());
            System.err.println("Utilizare: Client --coordinator <host:port>");
            System.exit(1);
        }
    }
}
