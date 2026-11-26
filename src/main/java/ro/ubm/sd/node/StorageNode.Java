package ro.ubm.sd.node;

import ro.ubm.sd.common.FileMetadata;
import ro.ubm.sd.common.NodeConfig;
import ro.ubm.sd.common.NodeInfo;
import ro.ubm.sd.common.NodeStatus;
import ro.ubm.sd.common.ProtocolConstants;
import ro.ubm.sd.rmi.AllocationPlan;
import ro.ubm.sd.rmi.LocateResult;
import ro.ubm.sd.rmi.NodeRemoteService;

import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class StorageNode implements NodeRemoteService {

    private final NodeConfig config;
    private final NodeInfo self;
    private final LocalStorageManager storage;
    private final TcpDataServer tcpDataServer;
    private final ClusterManager clusterManager;

    private final Map<UUID, FileMetadata> metadataTable = new ConcurrentHashMap<>();
    private final Map<String, Set<UUID>> pendingDeletes = new ConcurrentHashMap<>();

    public StorageNode(NodeConfig config) throws IOException {
        this.config = config;
        int rmiPort = config.getPort() + ProtocolConstants.RMI_PORT_OFFSET;
        this.self = new NodeInfo(
                config.getNodeId(),
                config.getHost(),
                config.getPort(),
                rmiPort,
                NodeStatus.ACTIV,
                System.currentTimeMillis()
        );
        this.self.setCoordinator(config.isCoordinator());

        this.storage = new LocalStorageManager(config.getDataDir());
        this.tcpDataServer = new TcpDataServer(config.getNodeId(), config.getPort(), storage);
        this.clusterManager = new ClusterManager(this, self);
    }

    public void start() throws IOException, InterruptedException {
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("java.rmi.server.hostname", config.getHost());

        NodeRemoteService stub = (NodeRemoteService) UnicastRemoteObject.exportObject(this, 0);
        Registry registry = LocateRegistry.createRegistry(self.getRmiPort());
        registry.rebind(ProtocolConstants.RMI_SERVICE_NAME, stub);

        tcpDataServer.start();
        clusterManager.start();

        if (self.isCoordinator()) {
            rebuildMetadataFromCluster();
        }

        String role = self.isCoordinator() ? "coordonator" : "nod de stocare";
        System.out.printf("[%s] pornit: TCP %d, RMI %d, director %s, rol: %s%n",
                self.getNodeId(), self.getPort(), self.getRmiPort(), config.getDataDir(), role);

        CountDownLatch keepAlive = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.printf("[%s] oprire nod...%n", self.getNodeId());
            clusterManager.stop();
            tcpDataServer.stop();
            keepAlive.countDown();
        }));
        keepAlive.await();
    }

    public int getLocalReplicaCount() {
        return storage.countLocalFiles();
    }

    private void ensureCoordinator() throws RemoteException {
        if (!self.isCoordinator()) {
            throw new RemoteException("Acest nod (" + self.getNodeId()
                    + ") nu este coordonator. Coordonator curent: " + clusterManager.getCurrentCoordinatorId());
        }
    }

    @Override
    public AllocationPlan allocateUpload(String fileName, long size) throws RemoteException {
        ensureCoordinator();
        if (size <= 0 || size > ProtocolConstants.MAX_FILE_SIZE_BYTES) {
            throw new RemoteException("Dimensiune invalida (maxim 512 MB).");
        }
        boolean nameExists = metadataTable.values().stream()
                .anyMatch(m -> m.getName().equalsIgnoreCase(fileName));
        if (nameExists) {
            throw new RemoteException("ALREADY_EXISTS: Exista deja un fisier cu numele " + fileName);
        }

        List<NodeInfo> selected = clusterManager.getKnownNodes().values().stream()
                .filter(n -> n.getStatus() == NodeStatus.ACTIV)
                .sorted(Comparator.comparingInt(NodeInfo::getStoredReplicasCount)
                        .thenComparing(NodeInfo::getNodeId))
                .limit(ProtocolConstants.REPLICATION_FACTOR)
                .toList();

        if (selected.size() < ProtocolConstants.REPLICATION_FACTOR) {
            throw new RemoteException("Insuficiente noduri active (necesar minim 2, active: " + selected.size() + ")");
        }

        UUID fileId = UUID.randomUUID();
        return new AllocationPlan(fileId, new ArrayList<>(selected));
    }

    @Override
    public void commitUpload(FileMetadata metadata) throws RemoteException {
        ensureCoordinator();
        metadataTable.put(metadata.getId(), metadata);
        for (String nodeId : metadata.getReplicas()) {
            NodeInfo n = clusterManager.getKnownNodes().get(nodeId);
            if (n != null) {
                n.setStoredReplicasCount(n.getStoredReplicasCount() + 1);
            }
        }
        System.out.printf("[%s] Metadate inregistrate: %s (%s) pe %s%n",
                self.getNodeId(), metadata.getName(), metadata.getId(), metadata.getReplicas());
    }

    @Override
    public LocateResult locateFile(String nameOrId) throws RemoteException {
        ensureCoordinator();
        FileMetadata meta = findMetadata(nameOrId);
        if (meta == null) {
            return null;
        }
        List<NodeInfo> replicaNodes = new ArrayList<>();
        for (String replicaId : meta.getReplicas()) {
            NodeInfo info = clusterManager.getKnownNodes().get(replicaId);
            if (info != null && info.getStatus() != NodeStatus.CAZUT) {
                replicaNodes.add(info);
            }
        }
        return new LocateResult(meta, replicaNodes);
    }

    @Override
    public List<FileMetadata> listFiles() throws RemoteException {
        ensureCoordinator();
        return new ArrayList<>(metadataTable.values());
    }

    @Override
    public String deleteFile(String nameOrId) throws RemoteException {
        ensureCoordinator();
        FileMetadata meta = findMetadata(nameOrId);
        if (meta == null) {
            return "NOT_FOUND";
        }
        metadataTable.remove(meta.getId());

        List<String> cleaned = new ArrayList<>();
        List<String> pending = new ArrayList<>();

        for (String replicaNodeId : meta.getReplicas()) {
            NodeInfo node = clusterManager.getKnownNodes().get(replicaNodeId);
            boolean deleted = false;
            if (node != null && node.getStatus() == NodeStatus.ACTIV) {
                deleted = callDeleteReplicaOnNode(node, meta.getId());
            }
            if (deleted) {
                cleaned.add(replicaNodeId);
            } else {
                pendingDeletes.computeIfAbsent(replicaNodeId, k -> ConcurrentHashMap.newKeySet()).add(meta.getId());
                pending.add(replicaNodeId);
            }
        }

        if (pending.isEmpty()) {
            return "Sters complet (" + meta.getName() + ") de pe nodurile: " + cleaned;
        }
        return "Sters din metadate (" + meta.getName() + "). Curatat pe " + cleaned
                + "; programat pentru curatare la reconectare pe: " + pending;
    }

    @Override
    public List<NodeInfo> getClusterNodes() throws RemoteException {
        ensureCoordinator();
        return new ArrayList<>(clusterManager.getKnownNodes().values().stream()
                .sorted(Comparator.comparing(NodeInfo::getNodeId, ProtocolConstants::compareNodeIds))
                .toList());
    }

    @Override
    public List<FileMetadata> getLocalFiles() throws RemoteException {
        return storage.scanLocalFiles();
    }

    @Override
    public boolean deleteLocalReplica(UUID fileId) throws RemoteException {
        try {
            return storage.deleteReplica(fileId);
        } catch (IOException e) {
            throw new RemoteException("Eroare la stergerea locala: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean receiveElection(String senderNodeId) throws RemoteException {
        System.out.printf("[%s] Primit ELECTION de la %s -> raspund OK si preiau alegerea%n",
                self.getNodeId(), senderNodeId);
        clusterManager.startBullyElection();
        return true;
    }

    @Override
    public void announceCoordinator(String coordinatorId, String host, int tcpPort, int rmiPort) throws RemoteException {
        clusterManager.setCoordinatorInfo(coordinatorId, host, tcpPort, rmiPort);
    }

    public void rebuildMetadataFromCluster() {
        metadataTable.clear();
        System.out.printf("[%s] Reconstruire tabela de metadate din nodurile active...%n", self.getNodeId());

        for (NodeInfo node : clusterManager.getKnownNodes().values()) {
            if (node.getStatus() == NodeStatus.CAZUT) continue;
            try {
                List<FileMetadata> files;
                if (node.getNodeId().equals(self.getNodeId())) {
                    files = storage.scanLocalFiles();
                } else {
                    Registry reg = LocateRegistry.getRegistry(node.getHost(), node.getRmiPort());
                    NodeRemoteService remote = (NodeRemoteService) reg.lookup(ProtocolConstants.RMI_SERVICE_NAME);
                    files = remote.getLocalFiles();
                }
                for (FileMetadata reported : files) {
                    metadataTable.compute(reported.getId(), (id, existing) -> {
                        if (existing == null) {
                            List<String> replicas = new ArrayList<>();
                            replicas.add(node.getNodeId());
                            return new FileMetadata(reported.getId(), reported.getName(),
                                    reported.getSize(), reported.getChecksum(), replicas);
                        } else {
                            existing.addReplica(node.getNodeId());
                            return existing;
                        }
                    });
                }
            } catch (Exception e) {
                System.err.printf("[%s] Nu am putut interoga nodul %s la reconstructie: %s%n",
                        self.getNodeId(), node.getNodeId(), e.getMessage());
            }
        }
        System.out.printf("[%s] Metadate reconstruite: %d fisiere gasite.%n",
                self.getNodeId(), metadataTable.size());
    }

    public void flushPendingDeletesForNode(NodeInfo node) {
        Set<UUID> toDelete = pendingDeletes.get(node.getNodeId());
        if (toDelete == null || toDelete.isEmpty()) return;

        for (UUID id : new ArrayList<>(toDelete)) {
            if (callDeleteReplicaOnNode(node, id)) {
                toDelete.remove(id);
                System.out.printf("[%s] Tombstone curatat pe nodul revenit %s pentru fisierul %s%n",
                        self.getNodeId(), node.getNodeId(), id);
            }
        }
    }

    private boolean callDeleteReplicaOnNode(NodeInfo node, UUID fileId) {
        try {
            if (node.getNodeId().equals(self.getNodeId())) {
                return storage.deleteReplica(fileId);
            }
            Registry reg = LocateRegistry.getRegistry(node.getHost(), node.getRmiPort());
            NodeRemoteService remote = (NodeRemoteService) reg.lookup(ProtocolConstants.RMI_SERVICE_NAME);
            return remote.deleteLocalReplica(fileId);
        } catch (Exception e) {
            return false;
        }
    }

    private FileMetadata findMetadata(String nameOrId) {
        for (FileMetadata meta : metadataTable.values()) {
            if (meta.getName().equalsIgnoreCase(nameOrId) || meta.getId().toString().equalsIgnoreCase(nameOrId)) {
                return meta;
            }
        }
        return null;
    }

    public static void main(String[] args) {
        System.setProperty("java.net.preferIPv4Stack", "true");
        try {
            NodeConfig config = NodeConfig.fromArgs(args);
            StorageNode node = new StorageNode(config);
            node.start();
        } catch (IllegalArgumentException e) {
            System.err.println("Eroare argumente: " + e.getMessage());
            System.err.println("Utilizare: StorageNode --id <id> --port <port> --dir <director> [--host <host>] [--coordinator]");
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Eroare I/O la pornirea nodului: " + e.getMessage());
            System.exit(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
