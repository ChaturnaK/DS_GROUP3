package com.ds.storage;

import com.ds.common.JsonSerde;
import com.ds.storage.ChaosHooks;
import com.ds.storage.grpc.StorageServiceImpl;
import com.ds.storage.Replicator;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageNode {
  private static final Logger log = LoggerFactory.getLogger(StorageNode.class);
  private static final String STORAGE_NODES_PATH = "/ds/nodes/storage";

  public static void main(String[] args) throws Exception {
    int port = 8001;
    String dataDir = "./data/node1";
    String zkConnect = "localhost:2181";
    String zone = "z1";

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--port":
          if (i + 1 < args.length) {
            port = Integer.parseInt(args[++i]);
          }
          break;
        case "--data":
          if (i + 1 < args.length) {
            dataDir = args[++i];
          }
          break;
        case "--zk":
          if (i + 1 < args.length) {
            zkConnect = args[++i];
          }
          break;
        case "--zone":
          if (i + 1 < args.length) {
            zone = args[++i];
          }
          break;
        default:
          // ignore
      }
    }

    ChaosHooks.configureFromEnv();

    System.setProperty("ds.data.dir", dataDir);

    Path dataPath = Paths.get(dataDir);
    Files.createDirectories(dataPath);
    long freeBytes = Files.getFileStore(dataPath).getUsableSpace();

    String host = java.net.InetAddress.getLocalHost().getHostName();
    String nodeId = host + ":" + port;
    String nodePath = STORAGE_NODES_PATH + "/" + nodeId;

    CuratorFramework curator =
        CuratorFrameworkFactory.newClient(zkConnect, new ExponentialBackoffRetry(200, 10));
    curator.start();
    curator.blockUntilConnected();

    registerNode(curator, nodePath, host, port, zone, freeBytes);

    BlockStore store = new BlockStore();
    Replicator replicator = new Replicator(store);

    Server server =
        NettyServerBuilder.forPort(port)
            .addService(new StorageServiceImpl(store, replicator))
            .addService(ProtoReflectionService.newInstance())
            .build()
            .start();

    log.info(
        "gRPC StorageService started on :{} (Stage 3), data={}, zone={}, zk={}",
        port,
        dataDir,
        zone,
        zkConnect);

    ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    final String dataDirFinal = dataDir;
    final String hostFinal = host;
    final int portFinal = port;
    final String zoneFinal = zone;
    final CuratorFramework curatorFinal = curator;
    final String nodePathFinal = nodePath;
    ses.scheduleAtFixedRate(
        () -> {
          try {
            long free = Files.getFileStore(Paths.get(dataDirFinal)).getUsableSpace();
            String payload =
                String.format(
                    "{\"host\":\"%s\",\"port\":%d,\"zone\":\"%s\",\"freeBytes\":%d}",
                    hostFinal, portFinal, zoneFinal, free);
            curatorFinal.setData().forPath(nodePathFinal, payload.getBytes());
          } catch (Exception e) {
            log.warn("Failed to update storage node payload", e);
          }
        },
        5,
        5,
        TimeUnit.SECONDS);

    AtomicBoolean shuttingDown = new AtomicBoolean(false);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  if (shuttingDown.compareAndSet(false, true)) {
                    System.out.println("Shutting down gracefully…");
                    try {
                      server.shutdown();
                      server.awaitTermination(5, TimeUnit.SECONDS);
                    } catch (Exception ignored) {
                      // ignore
                    }
                    try {
                      ses.shutdownNow();
                    } catch (Exception ignored) {
                      // ignore
                    }
                    try {
                      curator.close();
                    } catch (Exception ignored) {
                      // ignore
                    }
                    System.out.println("Shutdown complete.");
                  }
                }));

    server.awaitTermination();
    if (shuttingDown.compareAndSet(false, true)) {
      ses.shutdownNow();
      curator.close();
    }
  }

  private static void registerNode(
      CuratorFramework curator,
      String nodePath,
      String host,
      int port,
      String zone,
      long freeBytes)
      throws Exception {
    NodePayload payload = new NodePayload(host, port, zone, freeBytes);
    byte[] data = JsonSerde.write(payload);
    try {
      curator
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.EPHEMERAL)
          .forPath(nodePath, data);
    } catch (KeeperException.NodeExistsException e) {
      curator.delete().forPath(nodePath);
      curator
          .create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.EPHEMERAL)
          .forPath(nodePath, data);
    }
  }

  private record NodePayload(String host, int port, String zone, long freeBytes) {}
}
