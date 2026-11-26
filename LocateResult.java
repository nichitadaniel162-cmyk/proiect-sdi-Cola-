package ro.ubm.sd.rmi;

import ro.ubm.sd.common.FileMetadata;
import ro.ubm.sd.common.NodeInfo;

import java.io.Serializable;
import java.util.List;

public class LocateResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private final FileMetadata metadata;
    private final List<NodeInfo> replicaNodes;

    public LocateResult(FileMetadata metadata, List<NodeInfo> replicaNodes) {
        this.metadata = metadata;
        this.replicaNodes = replicaNodes;
    }

    public FileMetadata getMetadata() { return metadata; }
    public List<NodeInfo> getReplicaNodes() { return replicaNodes; }
}
