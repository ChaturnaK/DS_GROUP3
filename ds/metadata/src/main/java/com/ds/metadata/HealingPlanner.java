package com.ds.metadata;

import com.ds.common.Metrics;
import com.ds.time.ClusterClock;
import ds.Ack;
import ds.GetHdr;
import ds.StorageServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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
  private final ExecutorService healingPool = Executors.newFixedThreadPool(
      Math.max(2, Runtime.getRuntime().availableProcessors() / 2));
  private final ConcurrentLinkedQueue<String> recentTasks = new ConcurrentLinkedQueue<>();

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
    List<Future<?>> futures = new ArrayList<>();
    long startTime = ClusterClock.now();
    int totalTasks = 0;
    try {
      if (zk.checkExists().forPath(zkRootBlocks) == null) {
        return;
      }
      List<PlacementService.NodeInfo> liveNodes = placement.listLiveNodes();
      Set<String> liveReplicaSet = liveNodes.stream()
          .map(n -> n.host + ":" + n.port)
          .collect(Collectors.toSet());
      for (String encoded : zk.getChildren().forPath(zkRootBlocks)) {
        String blockId = URLDecoder.decode(encoded, StandardCharsets.UTF_8);
        MetaStore.BlockEntry be = meta.getBlock(blockId).orElse(null);
        if (be == null) {
          continue;
        }
        List<String> liveReplicas = be.replicas.stream().filter(liveReplicaSet::contains).distinct()
            .collect(Collectors.toList());
        List<String> staleReplicas = be.replicas.stream().filter(r -> !liveReplicaSet.contains(r)).distinct()
            .collect(Collectors.toList());
        if (!staleReplicas.isEmpty()) {
          log.info("Pruning stale replicas for block={} stale={}", blockId, staleReplicas);
          synchronized (be) {
            be.replicas.removeIf(r -> staleReplicas.contains(r));
            meta.putBlock(blockId, be);
          }
        }
        int currentReplicas = liveReplicas.size();
        int needed = replication - currentReplicas;
        if (log.isDebugEnabled()) {
          log.debug(
              "Healing scan block={} replicas={} live={} needed={}",
              blockId,
              be.replicas,
              liveReplicas,
              needed);
        }
        if (needed > 0) {
          backlog++;
          log.warn(
              "Under-replicated block {} have={} need={}",
              blockId,
              currentReplicas,
              needed);
          // Find all live sources (nodes that have the block)
          List<String> sources = new ArrayList<>(liveReplicas);
          if (sources.isEmpty()) {
            log.warn("No live source for {}", blockId);
            continue;
          }
          // Find all targets (live nodes that do not have the block)
          List<PlacementService.NodeInfo> targets = new ArrayList<>();
          for (PlacementService.NodeInfo n : liveNodes) {
            String nodeUrl = n.host + ":" + n.port;
            if (be.replicas.stream().noneMatch(r -> r.equals(nodeUrl))) {
              targets.add(n);
            }
          }
          int scheduled = 0;
          for (PlacementService.NodeInfo tgt : targets) {
            if (scheduled >= needed)
              break;
            String targetUrl = tgt.host + ":" + tgt.port;
            String src = sources.get(0); // pick first live source (could randomize for load balance)
            final String srcFinal = src;
            final String targetFinal = targetUrl;
            log.info("Scheduling replication block={} src={} tgt={}", blockId, src, targetUrl);
            totalTasks++;
            futures.add(healingPool.submit(() -> {
              long taskStart = ClusterClock.now();
              String blockAndSrc = blockId + "@" + src;
              String[] hp = targetUrl.split(":");
              ManagedChannel ch = ManagedChannelBuilder.forAddress(hp[0], Integer.parseInt(hp[1]))
                  .usePlaintext()
                  .build();
              StorageServiceGrpc.StorageServiceBlockingStub stub = StorageServiceGrpc.newBlockingStub(ch);
              boolean ok = false;
              String resultMsg = "";
              try {
                Ack ack = stub.replicateBlock(GetHdr.newBuilder().setBlockId(blockAndSrc).build());
                log.info("Replication result block={} ok={} msg={}", blockId, ack.getOk(), ack.getMsg());
                ok = ack.getOk();
                resultMsg = ack.getMsg();
                if (ack.getOk()) {
                  synchronized (be) {
                    if (be.replicas.stream().noneMatch(r -> r.equals(targetUrl))) {
                      be.replicas.add(targetUrl);
                    }
                    meta.putBlock(blockId, be);
                  }
                  Metrics.counter("healing_success").increment();
                } else {
                  Metrics.counter("healing_fail").increment();
                }
              } catch (Exception e) {
                log.error("Replication RPC failed: {}", e.toString());
                resultMsg = "rpc_error:" + e.getClass().getSimpleName();
                Metrics.counter("healing_fail").increment();
              } finally {
                ch.shutdownNow();
                long taskEnd = ClusterClock.now();
                Metrics.timer("healing_task_time").record(taskEnd - taskStart, TimeUnit.MILLISECONDS);
                recordTask(blockId, srcFinal, targetFinal, ok, resultMsg);
              }
            }));
            scheduled++;
          }
        }
      }
      // Wait for all healing tasks to finish
      for (Future<?> f : futures) {
        try {
          f.get();
        } catch (Exception e) {
          log.error("Healing task error: {}", e.toString());
        }
      }
    } catch (KeeperException.NoNodeException ignore) {
      // ignore
    } catch (Exception e) {
      log.error("HealingPlanner run() error {}", e.toString());
    } finally {
      Metrics.gauge("replication_backlog", backlog);
      long elapsed = ClusterClock.now() - startTime;
      Metrics.timer("healing_total_time").record(elapsed, TimeUnit.MILLISECONDS);
      Metrics.gauge("healing_tasks", totalTasks);
    }
  }

  private void recordTask(String blockId, String src, String tgt, boolean ok, String msg) {
    String outcome = ok ? "OK" : "FAIL";
    StringBuilder sb = new StringBuilder();
    sb.append("block=").append(blockId).append(' ').append(src).append("->").append(tgt).append(' ').append(outcome);
    if (msg != null && !msg.isBlank()) {
      sb.append(" (").append(msg).append(')');
    }
    recentTasks.add(sb.toString());
    while (recentTasks.size() > 20) {
      recentTasks.poll();
    }
  }

  public List<String> drainRecentTasks() {
    List<String> out = new ArrayList<>();
    String item;
    while ((item = recentTasks.poll()) != null) {
      out.add(item);
    }
    return out;
  }
}
