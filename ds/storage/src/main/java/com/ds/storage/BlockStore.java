package com.ds.storage;

import com.ds.common.JsonSerde;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.HexFormat;

public class BlockStore {
  private final Path dataDir = Paths.get(System.getProperty("ds.data.dir", "./data/node1"));
  private final Path blocksDir = dataDir.resolve("blocks");
  private final Path metaDir = dataDir.resolve("meta");

  public BlockStore() throws IOException {
    Files.createDirectories(blocksDir);
    Files.createDirectories(metaDir);
  }

  public static final class PutResult {
    public final String checksumHex;
    public final long bytesWritten;

    public PutResult(String checksumHex, long bytesWritten) {
      this.checksumHex = checksumHex;
      this.bytesWritten = bytesWritten;
    }
  }

  private String sanitizedId(String blockId) {
    if (blockId == null || blockId.isBlank()) {
      return "";
    }
    return blockId.startsWith("/") ? blockId.substring(1) : blockId;
  }

  public Path blockPath(String blockId) {
    return blocksDir.resolve(sanitizedId(blockId));
  }

  public Path metaPath(String blockId) {
    return metaDir.resolve(sanitizedId(blockId) + ".json");
  }

  public static final class Meta {
    public String vectorClock = "";
    public String checksum = "";
    public String primary = "";
  }

  public Meta readMetaObj(String blockId) throws IOException {
    Path mp = metaPath(blockId);
    if (!Files.exists(mp)) {
      return new Meta();
    }
    try {
      return JsonSerde.read(Files.readAllBytes(mp), Meta.class);
    } catch (RuntimeException e) {
      return new Meta();
    }
  }

  public void writeMetaObj(String blockId, Meta meta) throws IOException {
    Meta m = meta == null ? new Meta() : meta;
    Path mp = metaPath(blockId);
    Path parent = mp.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    Files.writeString(
        mp,
        new String(JsonSerde.write(m), StandardCharsets.UTF_8),
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING);
  }

  public Path blockPathSibling(String blockId, String suffix) {
    return blocksDir.resolve(sanitizedId(blockId) + "." + suffix);
  }

  public Path metaPathSibling(String blockId, String suffix) {
    return metaDir.resolve(sanitizedId(blockId) + "." + suffix + ".json");
  }

  public PutResult writeStreaming(String blockId, Iterable<byte[]> chunks) throws Exception {
    Path path = blockPath(blockId);
    Files.createDirectories(path.getParent());
    try (FileChannel channel =
        FileChannel.open(
            path,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      long total = 0L;
      for (byte[] bytes : chunks) {
        if (bytes == null || bytes.length == 0) {
          continue;
        }
        digest.update(bytes);
        total += bytes.length;
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining()) {
          channel.write(buffer);
        }
      }
      String hex = HexFormat.of().formatHex(digest.digest());
      return new PutResult(hex, total);
    }
  }

  public void writeMeta(String blockId, String vectorClock, String checksumHex) throws IOException {
    Meta m = new Meta();
    m.vectorClock = vectorClock == null ? "" : vectorClock;
    m.checksum = checksumHex == null ? "" : checksumHex;
    m.primary = "";
    writeMetaObj(blockId, m);
  }

  public byte[] readMeta(String blockId) throws IOException {
    return JsonSerde.write(readMetaObj(blockId));
  }

  public long size(String blockId) throws IOException {
    Path path = blockPath(blockId);
    return Files.exists(path) ? Files.size(path) : -1L;
  }

  public Iterable<byte[]> streamRead(String blockId, int chunkSize) throws IOException {
    final Path path = blockPath(blockId);
    final int resolvedChunkSize = Math.max(64 * 1024, chunkSize);
    return () ->
        new java.util.Iterator<>() {
          FileChannel channel;
          long remaining;
          boolean closed;

          {
            try {
              channel = FileChannel.open(path, StandardOpenOption.READ);
              remaining = Files.size(path);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public boolean hasNext() {
            if (remaining <= 0 && !closed) {
              try {
                channel.close();
              } catch (IOException ignored) {
              }
              closed = true;
            }
            return remaining > 0;
          }

          @Override
          public byte[] next() {
            try {
              int toRead = (int) Math.min(resolvedChunkSize, remaining);
              ByteBuffer buffer = ByteBuffer.allocate(toRead);
              int read = channel.read(buffer);
              if (read <= 0) {
                remaining = 0;
                channel.close();
                closed = true;
                return new byte[0];
              }
              remaining -= read;
              return buffer.array();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        };
  }
}
