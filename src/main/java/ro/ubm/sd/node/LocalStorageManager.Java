package ro.ubm.sd.node;

import ro.ubm.sd.common.FileMetadata;
import ro.ubm.sd.common.TcpUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

public class LocalStorageManager {

    private final Path dataDir;

    public LocalStorageManager(Path dataDir) throws IOException {
        this.dataDir = dataDir;
        Files.createDirectories(dataDir);
    }

    public Path getDataFilePath(UUID id) {
        return dataDir.resolve(id + ".bin");
    }

    public Path getMetaFilePath(UUID id) {
        return dataDir.resolve(id + ".meta");
    }

    public void saveFromStream(UUID id, String name, long size, String expectedChecksum, InputStream in)
            throws IOException {
        Path tempFile = dataDir.resolve(id + ".tmp");
        String actualChecksum;
        try (OutputStream out = Files.newOutputStream(tempFile)) {
            actualChecksum = TcpUtil.copyExactAndHash(in, out, size);
        } catch (IOException e) {
            Files.deleteIfExists(tempFile);
            throw e;
        }

        if (!actualChecksum.equalsIgnoreCase(expectedChecksum)) {
            Files.deleteIfExists(tempFile);
            throw new IOException("Checksum invalid! Asteptat: " + expectedChecksum + ", calculat: " + actualChecksum);
        }

        Files.move(tempFile, getDataFilePath(id), StandardCopyOption.REPLACE_EXISTING);
        saveMetadataFile(id, name, size, actualChecksum);
    }

    private void saveMetadataFile(UUID id, String name, long size, String checksum) throws IOException {
        Properties props = new Properties();
        props.setProperty("id", id.toString());
        props.setProperty("name", name);
        props.setProperty("size", String.valueOf(size));
        props.setProperty("checksum", checksum);
        try (OutputStream out = Files.newOutputStream(getMetaFilePath(id))) {
            props.store(out, "sd-storage local metadata");
        }
    }

    public FileMetadata readMetadataFile(UUID id) throws IOException {
        Path metaPath = getMetaFilePath(id);
        if (!Files.exists(metaPath)) {
            return null;
        }
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(metaPath)) {
            props.load(in);
        }
        return new FileMetadata(
                UUID.fromString(props.getProperty("id")),
                props.getProperty("name"),
                Long.parseLong(props.getProperty("size")),
                props.getProperty("checksum"),
                Collections.emptyList()
        );
    }

    public boolean deleteReplica(UUID id) throws IOException {
        boolean deletedBin = Files.deleteIfExists(getDataFilePath(id));
        boolean deletedMeta = Files.deleteIfExists(getMetaFilePath(id));
        return deletedBin || deletedMeta;
    }

    public List<FileMetadata> scanLocalFiles() {
        List<FileMetadata> result = new ArrayList<>();
        try (Stream<Path> stream = Files.list(dataDir)) {
            stream.filter(p -> p.getFileName().toString().endsWith(".meta"))
                  .forEach(metaPath -> {
                      try {
                          String fileName = metaPath.getFileName().toString();
                          UUID id = UUID.fromString(fileName.substring(0, fileName.length() - 5));
                          if (Files.exists(getDataFilePath(id))) {
                              FileMetadata meta = readMetadataFile(id);
                              if (meta != null) {
                                  result.add(meta);
                              }
                          }
                      } catch (Exception ignored) {
                      }
                  });
        } catch (IOException ignored) {
        }
        return result;
    }

    public int countLocalFiles() {
        return scanLocalFiles().size();
    }
}
