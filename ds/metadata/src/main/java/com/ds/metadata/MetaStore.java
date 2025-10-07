package com.ds.metadata;

import com.ds.common.JsonSerde;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.zookeeper.KeeperException;

public class MetaStore {
  public static final String FILES = "/ds/meta/files";
  public static final String BLOCKS = "/ds/meta/blocks";

  public static final class FileEntry {
    public List<String> blocks = new ArrayList<>();
    public long size;
    public long ctime;
    public long mtime;
  }

  public static final class BlockEntry {
    public List<String> replicas = new ArrayList<>();
    public long size;
  }

  private final CuratorFramework zk;

  public MetaStore(CuratorFramework zk) {
    this.zk = zk;
  }

  private static String esc(String path) {
    return URLEncoder.encode(path, StandardCharsets.UTF_8);
  }

  public void ensureRoots() throws Exception {
    try {
      zk.create().creatingParentsIfNeeded().forPath(FILES);
    } catch (KeeperException.NodeExistsException ignore) {
      // already present
    }
    try {
      zk.create().creatingParentsIfNeeded().forPath(BLOCKS);
    } catch (KeeperException.NodeExistsException ignore) {
      // already present
    }
  }

  public void putFile(String path, FileEntry fe) throws Exception {
    String p = FILES + "/" + esc(path);
    byte[] payload = JsonSerde.write(fe);
    try {
      zk.create().creatingParentsIfNeeded().forPath(p, payload);
    } catch (KeeperException.NodeExistsException e) {
      zk.setData().forPath(p, payload);
    }
  }

  public Optional<FileEntry> getFile(String path) throws Exception {
    String p = FILES + "/" + esc(path);
    if (zk.checkExists().forPath(p) == null) {
      return Optional.empty();
    }
    return Optional.of(JsonSerde.read(zk.getData().forPath(p), FileEntry.class));
  }

  public void putBlock(String blockId, BlockEntry be) throws Exception {
    String p = BLOCKS + "/" + esc(blockId);
    byte[] payload = JsonSerde.write(be);
    try {
      zk.create().creatingParentsIfNeeded().forPath(p, payload);
    } catch (KeeperException.NodeExistsException e) {
      zk.setData().forPath(p, payload);
    }
  }

  public Optional<BlockEntry> getBlock(String blockId) throws Exception {
    String p = BLOCKS + "/" + esc(blockId);
    if (zk.checkExists().forPath(p) == null) {
      return Optional.empty();
    }
    return Optional.of(JsonSerde.read(zk.getData().forPath(p), BlockEntry.class));
  }

  public void commit(String path, FileEntry fe, Map<String, BlockEntry> blocks) throws Exception {
    String fp = FILES + "/" + esc(path);
    byte[] fbytes = JsonSerde.write(fe);
    if (zk.checkExists().forPath(fp) != null) {
      FileEntry cur = JsonSerde.read(zk.getData().forPath(fp), FileEntry.class);
      boolean identical = Objects.equals(cur.blocks, fe.blocks) && cur.size == fe.size;
      if (identical) {
        boolean blocksIdentical = true;
    for (Map.Entry<String, BlockEntry> entry : blocks.entrySet()) {
          String bp = BLOCKS + "/" + esc(entry.getKey());
          if (zk.checkExists().forPath(bp) == null) {
            blocksIdentical = false;
            break;
          }
          BlockEntry existing = JsonSerde.read(zk.getData().forPath(bp), BlockEntry.class);
          if (!Objects.equals(existing.replicas, entry.getValue().replicas)
              || existing.size != entry.getValue().size) {
            blocksIdentical = false;
            break;
          }
        }
        if (blocksIdentical) {
          return;
        }
      }
    }
    CuratorTransactionFinal tx = zk.inTransaction().check().forPath(FILES).and();
    if (zk.checkExists().forPath(fp) == null) {
      tx = tx.create().forPath(fp, fbytes).and();
    } else {
      tx = tx.setData().forPath(fp, fbytes).and();
    }
    for (Map.Entry<String, BlockEntry> e : blocks.entrySet()) {
      String bp = BLOCKS + "/" + esc(e.getKey());
      byte[] bbytes = JsonSerde.write(e.getValue());
      if (zk.checkExists().forPath(bp) == null) {
        tx = tx.create().forPath(bp, bbytes).and();
      } else {
        tx = tx.setData().forPath(bp, bbytes).and();
      }
    }
    tx.commit();
  }
}
