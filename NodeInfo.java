package ro.ubm.sd.common;

import java.io.Serializable;

public class NodeInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String nodeId;
    private final String host;
    private final int port;
    private final int rmiPort;
    private NodeStatus status;
    private long lastHeartbeat;
    private int storedReplicasCount;
    private boolean coordinator;

    public NodeInfo(String nodeId, String host, int port, int rmiPort, NodeStatus status, long lastHeartbeat) {
        this.nodeId = nodeId;
        this.host = host;
        this.port = port;
        this.rmiPort = rmiPort;
        this.status = status;
        this.lastHeartbeat = lastHeartbeat;
        this.storedReplicasCount = 0;
        this.coordinator = false;
    }

    public String getNodeId() { return nodeId; }
    public String getHost() { return host; }
    public int getPort() { return port; }
    public int getRmiPort() { return rmiPort; }
    public NodeStatus getStatus() { return status; }
    public void setStatus(NodeStatus status) { this.status = status; }
    public long getLastHeartbeat() { return lastHeartbeat; }
    public void setLastHeartbeat(long lastHeartbeat) { this.lastHeartbeat = lastHeartbeat; }
    public int getStoredReplicasCount() { return storedReplicasCount; }
    public void setStoredReplicasCount(int storedReplicasCount) { this.storedReplicasCount = storedReplicasCount; }
    public boolean isCoordinator() { return coordinator; }
    public void setCoordinator(boolean coordinator) { this.coordinator = coordinator; }

    @Override
    public String toString() {
        return String.format("%s (%s:%d, RMI:%d) [%s]%s - replici: %d",
                nodeId, host, port, rmiPort, status,
                coordinator ? " *COORDONATOR*" : "", storedReplicasCount);
    }
}
