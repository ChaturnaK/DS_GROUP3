package com.ds.metadata;

import com.ds.common.JsonSerde;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

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
    upsertWithCas(p, payload);
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
    upsertWithCas(p, payload);
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
    int attempts = 0;
    while (true) {
      attempts++;
      Stat fileStat = new Stat();
      boolean fileExists = false;
      FileEntry currentFile = null;
      try {
        byte[] existingFile = zk.getData().storingStatIn(fileStat).forPath(fp);
        fileExists = true;
        currentFile = JsonSerde.read(existingFile, FileEntry.class);
      } catch (KeeperException.NoNodeException ignore) {
        fileStat = null;
      }

      Map<String, Stat> blockStats = new HashMap<>();
      Map<String, byte[]> blockPayloads = new HashMap<>();
      boolean identical = fileExists
          && Objects.equals(currentFile.blocks, fe.blocks)
          && currentFile.size == fe.size;
      boolean blocksIdentical = identical;

      for (Map.Entry<String, BlockEntry> entry : blocks.entrySet()) {
        String bp = BLOCKS + "/" + esc(entry.getKey());
        byte[] payload = JsonSerde.write(entry.getValue());
        blockPayloads.put(bp, payload);
        Stat blockStat = new Stat();
        try {
          byte[] existingBlock = zk.getData().storingStatIn(blockStat).forPath(bp);
          blockStats.put(bp, blockStat);
          if (blocksIdentical) {
            BlockEntry existing = JsonSerde.read(existingBlock, BlockEntry.class);
            if (!Objects.equals(existing.replicas, entry.getValue().replicas)
                || existing.size != entry.getValue().size) {
              blocksIdentical = false;
            }
          }
        } catch (KeeperException.NoNodeException e) {
          blockStats.put(bp, null);
          blocksIdentical = false;
        }
      }

      if (identical && blocksIdentical) {
        return;
      }

      try {
        List<CuratorOp> ops = new ArrayList<>();
        ops.add(zk.transactionOp().check().forPath(FILES));
        if (!fileExists) {
          ops.add(zk.transactionOp().create().forPath(fp, fbytes));
        } else {
          ops.add(
              zk.transactionOp()
                  .setData()
                  .withVersion(fileStat.getVersion())
                  .forPath(fp, fbytes));
        }
        for (Map.Entry<String, BlockEntry> entry : blocks.entrySet()) {
          String bp = BLOCKS + "/" + esc(entry.getKey());
          byte[] payload = blockPayloads.get(bp);
          Stat stat = blockStats.get(bp);
          if (stat == null) {
            ops.add(zk.transactionOp().create().forPath(bp, payload));
          } else {
            ops.add(
                zk.transactionOp()
                    .setData()
                    .withVersion(stat.getVersion())
                    .forPath(bp, payload));
          }
        }
        zk.transaction().forOperations(ops);
        return;
      } catch (KeeperException.BadVersionException | KeeperException.NodeExistsException e) {
        if (attempts >= 5) {
          throw e;
        }
      } catch (Exception e) {
        if ((isBadVersion(e) || isNodeExists(e)) && attempts < 5) {
          continue;
        }
        throw e;
      }
    }
  }

  private void upsertWithCas(String path, byte[] payload) throws Exception {
    int attempts = 0;
    while (attempts++ < 10) {
      if (zk.checkExists().forPath(path) == null) {
        try {
          CuratorOp create = zk.transactionOp().create().forPath(path, payload);
          zk.transaction().forOperations(create);
          return;
        } catch (KeeperException.NodeExistsException e) {
          // Retry with fresh state
        } catch (Exception e) {
          if (isNodeExists(e)) {
            continue;
          }
          throw e;
        }
      } else {
        Stat stat = new Stat();
        try {
          zk.getData().storingStatIn(stat).forPath(path);
        } catch (KeeperException.NoNodeException e) {
          continue;
        }
        try {
          CuratorOp set =
              zk.transactionOp()
                  .setData()
                  .withVersion(stat.getVersion())
                  .forPath(path, payload);
          zk.transaction().forOperations(set);
          return;
        } catch (KeeperException.BadVersionException e) {
          // retry
        } catch (Exception e) {
          if (isBadVersion(e)) {
            continue;
          }
          throw e;
        }
      }
    }
    throw KeeperException.create(KeeperException.Code.BADVERSION);
  }

  private static boolean isBadVersion(Exception e) {
    return causedBy(e, KeeperException.BadVersionException.class);
  }

  private static boolean isNodeExists(Exception e) {
    return causedBy(e, KeeperException.NodeExistsException.class);
  }

  private static boolean causedBy(Throwable throwable, Class<? extends KeeperException> type) {
    Throwable current = throwable;
    while (current != null) {
      if (type.isInstance(current)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }
}
