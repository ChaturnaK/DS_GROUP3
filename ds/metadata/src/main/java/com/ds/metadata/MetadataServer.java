package com.ds.metadata;

import com.ds.common.Metrics;
import com.ds.metadata.grpc.MetadataServiceImpl;
import com.ds.time.NtpSync;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.micrometer.core.instrument.Counter;
import java.net.InetAddress;
import java.net.UnknownHostException;
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
      HealingPlanner planner =
          new HealingPlanner(coordinator.client(), placementService, metaStore, replication);
      final ZkCoordinator coordRef = coordinator;
      final HealingPlanner plannerRef = planner;
      healExec = Executors.newSingleThreadScheduledExecutor();
      healExec.scheduleAtFixedRate(
          () -> {
            if (coordRef.isLeader()) {
              plannerRef.run();
            }
          },
          5,
          10,
          TimeUnit.SECONDS);

      metricsExec = Executors.newScheduledThreadPool(1);
      metricsExec.scheduleAtFixedRate(
          new NtpSync("pool.ntp.org", 123), 1, 600, TimeUnit.SECONDS);
      metricsExec.scheduleAtFixedRate(Metrics::dumpCsv, 5, 5, TimeUnit.SECONDS);

      Counter leaderChanges = Metrics.counter("leader_changes");
      String leaderEndpoint = host + ":" + port;
      final ZkCoordinator listenerCoord = coordinator;
      coordinator.addLeadershipListener(
          isLeader -> {
            leaderChanges.increment();
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

      server =
          NettyServerBuilder.forPort(port)
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
}
