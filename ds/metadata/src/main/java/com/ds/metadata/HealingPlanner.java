package com.ds.metadata;

import com.ds.common.Metrics;
import ds.Ack;
import ds.GetHdr;
import ds.StorageServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.List;
import java.util.Optional;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HealingPlanner implements Runnable {
  private static final Logger log = LoggerFactory.getLogger(HealingPlanner.class);
  private final CuratorFramework zk;
  private final PlacementService placement;
  private final MetaStore meta;
  private final String zkRootBlocks = MetaStore.BLOCKS;
  private final int replication;

  public HealingPlanner(
      CuratorFramework zk, PlacementService placement, MetaStore meta, int replication) {
    this.zk = zk;
    this.placement = placement;
    this.meta = meta;
    this.replication = replication;
  }

  @Override
  public void run() {
    double backlog = 0.0;
    try {
      if (zk.checkExists().forPath(zkRootBlocks) == null) {
        return;
      }
      for (String blockId : zk.getChildren().forPath(zkRootBlocks)) {
        MetaStore.BlockEntry be = meta.getBlock(blockId).orElse(null);
        if (be == null) {
          continue;
        }
        if (be.replicas.size() < replication) {
          backlog++;
          log.warn(
              "Under-replicated block {} have={} need={}",
              blockId,
              be.replicas.size(),
              replication);
          List<PlacementService.NodeInfo> live = placement.listLiveNodes();
          String src =
              be.replicas.stream()
                  .filter(
                      r ->
                          live.stream()
                              .anyMatch(n -> (n.host + ":" + n.port).equals(r)))
                  .findFirst()
                  .orElse(null);
          if (src == null) {
            log.warn("No live source for {}", blockId);
            continue;
          }
          Optional<PlacementService.NodeInfo> tgt =
              live.stream()
                  .filter(n -> be.replicas.stream().noneMatch(r -> r.equals(n.host + ":" + n.port)))
                  .findFirst();
          if (tgt.isEmpty()) {
            continue;
          }
          String targetUrl = tgt.get().host + ":" + tgt.get().port;
          log.info("Scheduling replication block={} src={} tgt={}", blockId, src, targetUrl);
          String blockAndSrc = blockId + "@" + src;
          String[] hp = targetUrl.split(":");
          ManagedChannel ch =
              ManagedChannelBuilder.forAddress(hp[0], Integer.parseInt(hp[1]))
                  .usePlaintext()
                  .build();
          StorageServiceGrpc.StorageServiceBlockingStub stub =
              StorageServiceGrpc.newBlockingStub(ch);
          try {
            Ack ack = stub.replicateBlock(GetHdr.newBuilder().setBlockId(blockAndSrc).build());
            log.info("Replication result block={} ok={} msg={}", blockId, ack.getOk(), ack.getMsg());
            if (ack.getOk()) {
              if (be.replicas.stream().noneMatch(r -> r.equals(targetUrl))) {
                be.replicas.add(targetUrl);
              }
              meta.putBlock(blockId, be);
            }
          } catch (Exception e) {
            log.error("Replication RPC failed: {}", e.toString());
          } finally {
            ch.shutdownNow();
          }
        }
      }
    } catch (KeeperException.NoNodeException ignore) {
      // ignore
    } catch (Exception e) {
      log.error("HealingPlanner run() error {}", e.toString());
    } finally {
      Metrics.gauge("replication_backlog", backlog);
    }
  }
}
