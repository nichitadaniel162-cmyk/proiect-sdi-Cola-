package ro.ubm.sd.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class FileMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    private final UUID id;
    private final String name;
    private final long size;
    private final String checksum;
    private final List<String> replicas;

    public FileMetadata(UUID id, String name, long size, String checksum, List<String> replicas) {
        this.id = id;
        this.name = name;
        this.size = size;
        this.checksum = checksum;
        this.replicas = new ArrayList<>(replicas);
    }

    public UUID getId() { return id; }
    public String getName() { return name; }
    public long getSize() { return size; }
    public String getChecksum() { return checksum; }
    public List<String> getReplicas() { return Collections.unmodifiableList(replicas); }

    public synchronized void addReplica(String nodeId) {
        if (!replicas.contains(nodeId)) {
            replicas.add(nodeId);
        }
    }
}
