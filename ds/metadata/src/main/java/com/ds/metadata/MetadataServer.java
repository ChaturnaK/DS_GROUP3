package com.ds.metadata;

import com.ds.common.Metrics;
import com.ds.metadata.grpc.MetadataServiceImpl;
import com.ds.time.ClusterClock;
import com.ds.time.NtpSync;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.micrometer.core.instrument.Counter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.curator.utils.EnsurePath;

public class MetadataServer {
  public static void main(String[] args) {
    int port = 7000;
    String zkConnect = "localhost:2181";
    int replication = 3;
    long healInitialDelaySeconds = 2L;
    long healIntervalSeconds = 3L;

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--port":
          if (i + 1 < args.length) {
            port = Integer.parseInt(args[++i]);
          }
          break;
        case "--zk":
          if (i + 1 < args.length) {
            zkConnect = args[++i];
          }
          break;
        case "--replication":
          if (i + 1 < args.length) {
            replication = Integer.parseInt(args[++i]);
          }
          break;
        case "--heal-initial-delay":
          if (i + 1 < args.length) {
            healInitialDelaySeconds = Long.parseLong(args[++i]);
          }
          break;
        case "--heal-interval":
          if (i + 1 < args.length) {
            healIntervalSeconds = Long.parseLong(args[++i]);
          }
          break;
        default:
          // ignore unknown args
      }
    }

    ZkCoordinator coordinator = null;
    Server server = null;
    ScheduledExecutorService healExec = null;
    ScheduledExecutorService metricsExec = null;
    try {
      String host = resolveLocalHost();
      String serverId = host + ":" + port + ":" + UUID.randomUUID();

      coordinator = new ZkCoordinator(zkConnect, serverId);

      EnsurePath filesPath = new EnsurePath(MetaStore.FILES);
      EnsurePath blocksPath = new EnsurePath(MetaStore.BLOCKS);
      EnsurePath leaderPath = new EnsurePath("/ds/metadata/leader");
      filesPath.ensure(coordinator.client().getZookeeperClient());
      blocksPath.ensure(coordinator.client().getZookeeperClient());
      leaderPath.ensure(coordinator.client().getZookeeperClient());

      MetaStore metaStore = new MetaStore(coordinator.client());
      PlacementService placementService = new PlacementService(coordinator.client(), replication);
      MetadataServiceImpl service = new MetadataServiceImpl(metaStore, placementService, coordinator);
      HealingPlanner planner = new HealingPlanner(coordinator.client(), placementService, metaStore, replication);
      final ZkCoordinator coordRef = coordinator;
      final HealingPlanner plannerRef = planner;
      healExec = Executors.newSingleThreadScheduledExecutor();
      long finalHealInitialDelaySeconds = Math.max(0L, healInitialDelaySeconds);
      long finalHealIntervalSeconds = Math.max(1L, healIntervalSeconds);
      healExec.scheduleAtFixedRate(
          () -> {
            if (coordRef.isLeader()) {
              plannerRef.run();
            }
          },
          finalHealInitialDelaySeconds,
          finalHealIntervalSeconds,
          TimeUnit.SECONDS);

      metricsExec = Executors.newScheduledThreadPool(1);
      // Demo-friendly NTP: multi-host, relaxed thresholds (30s degraded, 20min unreliable)
      NtpSync ntpSync =
          new NtpSync(
              "pool.ntp.org,time.google.com,time.cloudflare.com",
              123,
              3,
              30_000L,
              1_200_000L);
      ClusterClock.registerHealthSupplier(ntpSync::health);
      metricsExec.scheduleAtFixedRate(ntpSync, 1, 120, TimeUnit.SECONDS);
      metricsExec.scheduleAtFixedRate(Metrics::dumpCsv, 5, 5, TimeUnit.SECONDS);
      final PlacementService placementRef = placementService;
      final ZkCoordinator coordHudRef = coordinator;
      metricsExec.scheduleAtFixedRate(
          () -> logClusterHud(coordHudRef, placementRef, plannerRef),
          5,
          5,
          TimeUnit.SECONDS);

      Counter leaderChanges = Metrics.counter("leader_changes");
      String leaderEndpoint = host + ":" + port;
      final ZkCoordinator listenerCoord = coordinator;
      coordinator.addLeadershipListener(
          isLeader -> {
            leaderChanges.increment();
            String state = isLeader ? "GRANTED" : "REVOKED";
            System.out.printf("[LEADER] %s -> %s%n", state, leaderEndpoint);
            if (isLeader) {
              listenerCoord.publishLeaderEndpoint(leaderEndpoint);
            } else {
              listenerCoord.clearLeaderEndpoint();
            }
          });
      if (listenerCoord.isLeader()) {
        leaderChanges.increment();
        listenerCoord.publishLeaderEndpoint(leaderEndpoint);
      }

      server = NettyServerBuilder.forPort(port)
          .addService(service)
          .addService(ProtoReflectionService.newInstance())
          .build()
          .start();

      System.out.println("[MetadataServer] gRPC started on :" + port + " (Stage 2)");

      ZkCoordinator coordForHook = coordinator;
      ScheduledExecutorService healExecForHook = healExec;
      ScheduledExecutorService metricsExecForHook = metricsExec;
      Server serverForHook = server;
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    System.out.println("Shutting down gracefully…");
                    try {
                      if (serverForHook != null) {
                        serverForHook.shutdown();
                        serverForHook.awaitTermination(5, TimeUnit.SECONDS);
                      }
                    } catch (Exception ignored) {
                      // ignored
                    }
                    try {
                      if (healExecForHook != null) {
                        healExecForHook.shutdownNow();
                      }
                    } catch (Exception ignored) {
                      // ignored
                    }
                    try {
                      if (metricsExecForHook != null) {
                        metricsExecForHook.shutdownNow();
                      }
                    } catch (Exception ignored) {
                      // ignored
                    }
                    try {
                      if (coordForHook != null) {
                        coordForHook.close();
                      }
                    } catch (Exception ignored) {
                      // ignored
                    }
                    System.out.println("Shutdown complete.");
                  }));

      server.awaitTermination();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    } finally {
      if (healExec != null) {
        healExec.shutdownNow();
      }
      if (metricsExec != null) {
        metricsExec.shutdownNow();
      }
      if (server != null) {
        server.shutdown();
      }
      if (coordinator != null) {
        try {
          coordinator.close();
        } catch (Exception ignore) {
          // ignore
        }
      }
    }
  }

  private static String resolveLocalHost() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      return "unknown-host";
    }
  }

  private static void logClusterHud(
      ZkCoordinator coord, PlacementService placement, HealingPlanner planner) {
    try {
      boolean isLeader = coord.isLeader();
      String publishedLeader = "";
      try {
        byte[] data =
            coord
                .client()
                .getData()
                .forPath("/ds/metadata/leader");
        if (data != null && data.length > 0) {
          publishedLeader = new String(data, StandardCharsets.UTF_8);
        }
      } catch (Exception ignored) {
        // ignore errors fetching the leader endpoint; leave blank
      }
      List<PlacementService.NodeInfo> liveNodes = placement.listLiveNodes();
      String nodeSummary =
          liveNodes.isEmpty()
              ? "none"
              : liveNodes.stream()
                  .map(
                      n ->
                          n.host
                              + ":"
                              + n.port
                              + (n.zone == null || n.zone.isBlank() ? "" : " (" + n.zone + ")"))
                  .sorted()
                  .reduce((a, b) -> a + ", " + b)
                  .orElse("none");
      double backlog = readGauge("replication_backlog");
      double tasks = readGauge("healing_tasks");
      double success = readCounter("healing_success");
      double fail = readCounter("healing_fail");
      var clockHealth = ClusterClock.currentHealth();
      System.out.printf(
          "[HUD] leader=%s published=%s live=%d [%s] backlog=%.0f tasks=%.0f success=%.0f fail=%.0f clock=%s%n",
          isLeader ? "self" : "other",
          publishedLeader.isBlank() ? "n/a" : publishedLeader,
          liveNodes.size(),
          nodeSummary,
          backlog,
          tasks,
          success,
          fail,
          clockHealth);
      if (planner != null) {
        List<String> recent = planner.drainRecentTasks();
        if (!recent.isEmpty()) {
          System.out.println("[HUD] healing-tasks: " + String.join(" | ", recent));
        }
      }
    } catch (Exception e) {
      System.out.println("[HUD] error: " + e.getMessage());
    }
  }

  private static double readGauge(String name) {
    try {
      var g = Metrics.reg().find(name).gauge();
      if (g == null) {
        return 0.0;
      }
      double v = g.value();
      return Double.isNaN(v) ? 0.0 : v;
    } catch (Exception e) {
      return 0.0;
    }
  }

  private static double readCounter(String name) {
    try {
      var c = Metrics.reg().find(name).counter();
      return c == null ? 0.0 : c.count();
    } catch (Exception e) {
      return 0.0;
    }
  }
}
