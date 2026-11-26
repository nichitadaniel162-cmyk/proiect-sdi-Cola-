package ro.ubm.sd.rmi;

import ro.ubm.sd.common.NodeInfo;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

public class AllocationPlan implements Serializable {
    private static final long serialVersionUID = 1L;

    private final UUID fileId;
    private final List<NodeInfo> targetNodes;

    public AllocationPlan(UUID fileId, List<NodeInfo> targetNodes) {
        this.fileId = fileId;
        this.targetNodes = targetNodes;
    }

    public UUID getFileId() { return fileId; }
    public List<NodeInfo> getTargetNodes() { return targetNodes; }
}
