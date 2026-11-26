package ro.ubm.sd.rmi;

import ro.ubm.sd.common.FileMetadata;
import ro.ubm.sd.common.NodeInfo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.UUID;

public interface NodeRemoteService extends Remote {

    // --- Operatii de Coordonator (apelate de Client) ---
    AllocationPlan allocateUpload(String fileName, long size) throws RemoteException;
    void commitUpload(FileMetadata metadata) throws RemoteException;
    LocateResult locateFile(String nameOrId) throws RemoteException;
    List<FileMetadata> listFiles() throws RemoteException;
    String deleteFile(String nameOrId) throws RemoteException;
    List<NodeInfo> getClusterNodes() throws RemoteException;

    // --- Operatii intre Noduri (Etapa 5 Bully & Etapa 6 Replicare/Recuperare) ---
    List<FileMetadata> getLocalFiles() throws RemoteException;
    boolean deleteLocalReplica(UUID fileId) throws RemoteException;
    boolean receiveElection(String senderNodeId) throws RemoteException;
    void announceCoordinator(String coordinatorId, String host, int tcpPort, int rmiPort) throws RemoteException;
}
