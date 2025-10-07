package com.ds.metadata;

import com.ds.common.JsonSerde;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;

public class PlacementService {
  public static final String NODES = "/ds/nodes/storage";

  private final CuratorFramework zk;
  private final int replication;

  public PlacementService(CuratorFramework zk, int replication) {
    this.zk = zk;
    this.replication = replication;
  }

  public static final class NodeInfo {
    public String id;
    public String host;
    public int port;
    public String zone;
    public long freeBytes;
  }

  public List<NodeInfo> listLiveNodes() throws Exception {
    List<NodeInfo> out = new ArrayList<>();
    if (zk.checkExists().forPath(NODES) == null) {
      return out;
    }
    try {
      for (String child : zk.getChildren().forPath(NODES)) {
        byte[] data = zk.getData().forPath(NODES + "/" + child);
        NodeInfo ni = JsonSerde.read(data, NodeInfo.class);
        ni.id = child;
        out.add(ni);
      }
    } catch (KeeperException.NoNodeException ignore) {
      // path disappeared between calls - treat as no nodes
    }
    return out;
  }

  public List<NodeInfo> chooseReplicas(String blockId) throws Exception {
    List<NodeInfo> nodes = listLiveNodes();
    if (nodes.size() < replication) {
      throw new IllegalStateException(
          "Not enough replicas: have=" + nodes.size() + " need=" + replication);
    }
    nodes.sort(Comparator.comparing(n -> n.id));
    int start =
        Math.floorMod(Arrays.hashCode(blockId.getBytes(StandardCharsets.UTF_8)), nodes.size());
    LinkedHashMap<String, NodeInfo> chosen = new LinkedHashMap<>();
    for (int i = 0, idx = start;
        i < nodes.size() && chosen.size() < replication;
        i++, idx = (idx + 1) % nodes.size()) {
      NodeInfo ni = nodes.get(idx);
      boolean zoneAlreadyPresent =
          chosen.values().stream().anyMatch(x -> Objects.equals(x.zone, ni.zone));
      if (!zoneAlreadyPresent || chosen.size() >= nodes.size() / 2) {
        chosen.put(ni.id, ni);
      }
    }
    if (chosen.size() < replication) {
      for (NodeInfo ni : nodes) {
        if (chosen.size() >= replication) {
          break;
        }
        chosen.putIfAbsent(ni.id, ni);
      }
    }
    return new ArrayList<>(chosen.values());
  }
}
