package ro.ubm.sd.node;

import ro.ubm.sd.common.NodeInfo;
import ro.ubm.sd.common.NodeStatus;
import ro.ubm.sd.common.ProtocolConstants;
import ro.ubm.sd.rmi.NodeRemoteService;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterManager {

    private final StorageNode owner;
    private final NodeInfo self;
    private final Map<String, NodeInfo> knownNodes = new ConcurrentHashMap<>();

    private volatile String currentCoordinatorId;
    private volatile boolean running = true;
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);

    private MulticastSocket multicastSocket;
    private InetAddress groupAddress;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final ExecutorService bullyPool = Executors.newCachedThreadPool();

    public ClusterManager(StorageNode owner, NodeInfo self) {
        this.owner = owner;
        this.self = self;
        this.knownNodes.put(self.getNodeId(), self);
        if (self.isCoordinator()) {
            this.currentCoordinatorId = self.getNodeId();
        }
    }

    public void start() throws IOException {
        groupAddress = InetAddress.getByName(ProtocolConstants.MULTICAST_GROUP);
        multicastSocket = new MulticastSocket(ProtocolConstants.MULTICAST_PORT);
        multicastSocket.setReuseAddress(true);
        multicastSocket.setTimeToLive(1);
        joinMulticastGroup(multicastSocket, groupAddress);

        Thread listener = new Thread(this::listenLoop, "multicast-listener-" + self.getNodeId());
        listener.setDaemon(true);
        listener.start();

        scheduler.scheduleAtFixedRate(this::sendHeartbeat, 200,
                ProtocolConstants.HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);

        scheduler.scheduleAtFixedRate(this::checkNodeHealth, 1500, 1000, TimeUnit.MILLISECONDS);
    }

    private void joinMulticastGroup(MulticastSocket socket, InetAddress group) throws IOException {
        InetSocketAddress groupAddr = new InetSocketAddress(group, ProtocolConstants.MULTICAST_PORT);
        try {
            socket.joinGroup(groupAddr, null);
        } catch (IOException e) {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface nif = interfaces.nextElement();
                if (nif.isUp() && nif.supportsMulticast()) {
                    try {
                        socket.joinGroup(groupAddr, nif);
                        return;
                    } catch (IOException ignored) {
                    }
                }
            }
            throw e;
        }
    }

    public void stop() {
        running = false;
        scheduler.shutdownNow();
        bullyPool.shutdownNow();
        if (multicastSocket != null) {
            multicastSocket.close();
        }
    }

    public Map<String, NodeInfo> getKnownNodes() {
        return knownNodes;
    }

    public String getCurrentCoordinatorId() {
        return currentCoordinatorId;
    }

    public void sendHeartbeat() {
        try {
            self.setLastHeartbeat(System.currentTimeMillis());
            self.setStoredReplicasCount(owner.getLocalReplicaCount());

            String msg = String.format("HEARTBEAT|%s|%s|%d|%d|%b|%d",
                    self.getNodeId(), self.getHost(), self.getPort(), self.getRmiPort(),
                    self.isCoordinator(), self.getStoredReplicasCount());
            sendMulticast(msg);
        } catch (IOException e) {
            if (running) {
                System.err.printf("[%s] Eroare trimitere heartbeat: %s%n", self.getNodeId(), e.getMessage());
            }
        }
    }

    private void sendMulticast(String msg) throws IOException {
        byte[] data = msg.getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(data, data.length, groupAddress, ProtocolConstants.MULTICAST_PORT);
        multicastSocket.send(packet);
    }

    private void listenLoop() {
        byte[] buf = new byte[1024];
        while (running) {
            try {
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                multicastSocket.receive(packet);
                String msg = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8).trim();
                handleMulticastMessage(msg);
            } catch (IOException e) {
                if (running) {
                    System.err.printf("[%s] Eroare receptie multicast: %s%n", self.getNodeId(), e.getMessage());
                }
            }
        }
    }

    private void handleMulticastMessage(String msg) {
        String[] parts = msg.split("\\|");
        if (parts.length < 2) return;

        if ("HEARTBEAT".equals(parts[0]) && parts.length >= 7) {
            String senderId = parts[1];
            if (senderId.equals(self.getNodeId())) return;

            String host = parts[2];
            int tcpPort = Integer.parseInt(parts[3]);
            int rmiPort = Integer.parseInt(parts[4]);
            boolean isCoord = Boolean.parseBoolean(parts[5]);
            int replicaCount = Integer.parseInt(parts[6]);

            long now = System.currentTimeMillis();
            boolean isNew = !knownNodes.containsKey(senderId);

            NodeInfo node = knownNodes.compute(senderId, (k, existing) -> {
                if (existing == null) {
                    return new NodeInfo(senderId, host, tcpPort, rmiPort, NodeStatus.ACTIV, now);
                }
                if (existing.getStatus() == NodeStatus.CAZUT) {
                    System.out.printf("[%s] Nodul %s a revenit -> ACTIV%n", self.getNodeId(), senderId);
                }
                existing.setStatus(NodeStatus.ACTIV);
                existing.setLastHeartbeat(now);
                return existing;
            });

            node.setCoordinator(isCoord);
            node.setStoredReplicasCount(replicaCount);

            if (isNew) {
                System.out.printf("[%s] Descoperit nod nou: %s (%s:%d)%n", self.getNodeId(), senderId, host, tcpPort);
            }

            if (isCoord) {
                currentCoordinatorId = senderId;
                if (self.isCoordinator() && ProtocolConstants.compareNodeIds(senderId, self.getNodeId()) > 0) {
                    self.setCoordinator(false);
                }
            }

            if (self.isCoordinator()) {
                owner.flushPendingDeletesForNode(node);
            }
        } else if ("COORDINATOR".equals(parts[0]) && parts.length >= 5) {
            String coordId = parts[1];
            setCoordinatorInfo(coordId, parts[2], Integer.parseInt(parts[3]), Integer.parseInt(parts[4]));
        }
    }

    public void setCoordinatorInfo(String coordId, String host, int tcpPort, int rmiPort) {
        this.currentCoordinatorId = coordId;
        for (NodeInfo n : knownNodes.values()) {
            n.setCoordinator(n.getNodeId().equals(coordId));
        }
        if (!coordId.equals(self.getNodeId())) {
            self.setCoordinator(false);
        }
        electionInProgress.set(false);
        System.out.printf("[%s] Coordonator curent actualizat: %s (%s:%d)%n", self.getNodeId(), coordId, host, tcpPort);
    }

    private void checkNodeHealth() {
        long now = System.currentTimeMillis();
        boolean coordinatorDied = false;

        for (NodeInfo node : knownNodes.values()) {
            if (node.getNodeId().equals(self.getNodeId())) continue;

            long elapsed = now - node.getLastHeartbeat();
            if (elapsed > ProtocolConstants.DEAD_TIMEOUT_MS && node.getStatus() != NodeStatus.CAZUT) {
                node.setStatus(NodeStatus.CAZUT);
                System.out.printf("[%s] Nodul %s marcat CAZUT (fara heartbeat de %d ms)%n",
                        self.getNodeId(), node.getNodeId(), elapsed);

                if (node.getNodeId().equals(currentCoordinatorId) || node.isCoordinator()) {
                    node.setCoordinator(false);
                    coordinatorDied = true;
                }
            } else if (elapsed > ProtocolConstants.SUSPECT_TIMEOUT_MS && node.getStatus() == NodeStatus.ACTIV) {
                node.setStatus(NodeStatus.SUSPECT);
                System.out.printf("[%s] Nodul %s marcat SUSPECT%n", self.getNodeId(), node.getNodeId());
            }
        }

        if (coordinatorDied && !self.isCoordinator()) {
            System.out.printf("[%s] Coordonatorul %s a cazut! Pornire alegere Bully...%n",
                    self.getNodeId(), currentCoordinatorId);
            startBullyElection();
        }
    }

    public void startBullyElection() {
        if (!electionInProgress.compareAndSet(false, true)) {
            return;
        }
        bullyPool.submit(() -> {
            try {
                List<NodeInfo> higherNodes = new ArrayList<>();
                for (NodeInfo n : knownNodes.values()) {
                    if (!n.getNodeId().equals(self.getNodeId())
                            && n.getStatus() != NodeStatus.CAZUT
                            && ProtocolConstants.compareNodeIds(n.getNodeId(), self.getNodeId()) > 0) {
                        higherNodes.add(n);
                    }
                }

                if (higherNodes.isEmpty()) {
                    becomeCoordinator();
                    return;
                }

                boolean gotOk = false;
                List<Future<Boolean>> futures = new ArrayList<>();
                for (NodeInfo higher : higherNodes) {
                    futures.add(bullyPool.submit(() -> sendElectionRmi(higher)));
                }

                for (Future<Boolean> f : futures) {
                    try {
                        if (Boolean.TRUE.equals(f.get(1500, TimeUnit.MILLISECONDS))) {
                            gotOk = true;
                        }
                    } catch (Exception ignored) {
                    }
                }

                if (!gotOk) {
                    becomeCoordinator();
                } else {
                    Thread.sleep(2500);
                    if (electionInProgress.get()) {
                        electionInProgress.set(false);
                        startBullyElection();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                electionInProgress.set(false);
            }
        });
    }

    private boolean sendElectionRmi(NodeInfo target) {
        try {
            Registry registry = LocateRegistry.getRegistry(target.getHost(), target.getRmiPort());
            NodeRemoteService remote = (NodeRemoteService) registry.lookup(ProtocolConstants.RMI_SERVICE_NAME);
            return remote.receiveElection(self.getNodeId());
        } catch (Exception e) {
            return false;
        }
    }

    private void becomeCoordinator() {
        self.setCoordinator(true);
        currentCoordinatorId = self.getNodeId();
        for (NodeInfo n : knownNodes.values()) {
            n.setCoordinator(n.getNodeId().equals(self.getNodeId()));
        }
        electionInProgress.set(false);

        System.out.printf("[%s] === ALES NOU COORDONATOR (Bully) ===%n", self.getNodeId());

        owner.rebuildMetadataFromCluster();

        try {
            String msg = String.format("COORDINATOR|%s|%s|%d|%d",
                    self.getNodeId(), self.getHost(), self.getPort(), self.getRmiPort());
            sendMulticast(msg);
        } catch (IOException ignored) {
        }

        for (NodeInfo n : knownNodes.values()) {
            if (!n.getNodeId().equals(self.getNodeId()) && n.getStatus() != NodeStatus.CAZUT) {
                bullyPool.submit(() -> {
                    try {
                        Registry reg = LocateRegistry.getRegistry(n.getHost(), n.getRmiPort());
                        NodeRemoteService remote = (NodeRemoteService) reg.lookup(ProtocolConstants.RMI_SERVICE_NAME);
                        remote.announceCoordinator(self.getNodeId(), self.getHost(), self.getPort(), self.getRmiPort());
                    } catch (Exception ignored) {
                    }
                });
            }
        }
    }
}
