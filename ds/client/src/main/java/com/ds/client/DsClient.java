package com.ds.client;

import static com.ds.client.NetUtil.host;
import static com.ds.client.NetUtil.port;

import com.ds.common.VectorClock;
import ds.BlockPlan;
import ds.CommitBlock;
import ds.CommitReq;
import ds.FilePath;
import ds.GetHdr;
import ds.GetMeta;
import ds.LocateResp;
import ds.MetadataServiceGrpc;
import ds.PlanPutReq;
import ds.PlanPutResp;
import ds.StorageServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public class DsClient {
  private static final int DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024;
  private static final int CHUNK_SIZE = Math.max(1, Integer.getInteger("ds.chunk.size", DEFAULT_CHUNK_SIZE));
  private static final int QUORUM_W = Math.max(1, Integer.getInteger("ds.quorum.w", 2));
  private static final int QUORUM_R = Math.max(1, Integer.getInteger("ds.quorum.r", 2));
  private static final long WRITE_TIMEOUT_MS = Math.max(1L, Long.getLong("ds.quorum.timeout.ms", 20_000L));
  private static final long READ_TIMEOUT_MS = Math.max(1L, Long.getLong("ds.quorum.timeout.read.ms", 15_000L));
  private static final String CLIENT_ID =
      System.getProperty("ds.client.id", defaultClientId());
  private static final String CLIENT_EPOCH = CLIENT_ID + "#" + java.util.UUID.randomUUID();
  private static final ConcurrentHashMap<String, VectorClock> BLOCK_CLOCKS =
      new ConcurrentHashMap<>();

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage();
      return;
    }
    switch (args[0]) {
      case "putBlock" -> {
        if (args.length < 4) {
          System.out.println("putBlock requires host:port blockId file");
          return;
        }
        String[] hp = args[1].split(":");
        StreamTool.put(hp[0], Integer.parseInt(hp[1]), args[2], Paths.get(args[3]));
      }
      case "getBlock" -> {
        if (args.length < 4) {
          System.out.println("getBlock requires host:port blockId out");
          return;
        }
        String[] hp = args[1].split(":");
        StreamTool.get(hp[0], Integer.parseInt(hp[1]), args[2], Paths.get(args[3]));
      }
      case "put" -> {
        if (args.length < 3) {
          System.out.println("Usage: put <localFile> <remotePath>");
          return;
        }
        Path local = Paths.get(args[1]);
        String remote = args[2];
        long fileSize = Files.size(local);
        PlanPutResp plan =
            withLeader(
                stub ->
                    stub.planPut(
                        PlanPutReq.newBuilder()
                            .setPath(remote)
                            .setSize(fileSize)
                            .setChunkSize(CHUNK_SIZE)
                            .build()));
        byte[] buf = new byte[CHUNK_SIZE];
        List<Integer> actualSizes = new ArrayList<>();
        HashMap<String, List<String>> successfulReplicas = new HashMap<>();
        try (var in = Files.newInputStream(local)) {
          for (int i = 0; i < plan.getBlocksCount(); i++) {
            BlockPlan bp = plan.getBlocks(i);
            Path tmpChunk = Files.createTempFile("chunk", ".bin");
            int read = in.readNBytes(buf, 0, buf.length);
            if (read < 0) {
              read = 0;
            }
            Files.write(tmpChunk, java.util.Arrays.copyOf(buf, read));
            actualSizes.add(read);

            var replicas = bp.getReplicaUrlsList();
            List<String> ids = new ArrayList<>(replicas);
            List<java.util.function.Supplier<Boolean>> tasks = new ArrayList<>();
            String blockClock = nextVectorClock(bp.getBlockId());
            for (String hp : replicas) {
              tasks.add(
                  () -> {
                    try {
                      StreamTool.put(
                          host(hp),
                          port(hp),
                          bp.getBlockId(),
                          tmpChunk,
                          blockClock,
                          CLIENT_ID);
                      return Boolean.TRUE;
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    }
                  });
            }
            Quorum q =
                new Quorum(replicas.size(), Duration.ofMillis(WRITE_TIMEOUT_MS), QUORUM_W);
            try {
              var res = q.runRace(ids, tasks);
              successfulReplicas.put(
                  bp.getBlockId(),
                  new ArrayList<>(res.successes()));
              System.out.println(
                  "[PUT quorum] block="
                      + bp.getBlockId()
                      + " ok="
                      + res.successes()
                      + " fails="
                      + res.failures().size());
            } catch (TimeoutException te) {
              throw new RuntimeException(
                  "Failed to meet W="
                      + QUORUM_W
                      + " for block "
                      + bp.getBlockId()
                      + ": "
                      + te.getMessage());
            } catch (Exception e) {
              throw new RuntimeException("Quorum error for block " + bp.getBlockId(), e);
            } finally {
              q.shutdown();
              Files.deleteIfExists(tmpChunk);
            }
          }
        }
        CommitReq.Builder commitBuilder =
            CommitReq.newBuilder().setPath(remote).setSize(fileSize);
        for (int i = 0; i < plan.getBlocksCount(); i++) {
          BlockPlan bp = plan.getBlocks(i);
          int sz = i < actualSizes.size() ? actualSizes.get(i) : (int) bp.getSize();
          List<String> replicas = successfulReplicas.get(bp.getBlockId());
          if (replicas == null || replicas.isEmpty()) {
            replicas = bp.getReplicaUrlsList();
          }
          commitBuilder.addBlocks(
              CommitBlock.newBuilder()
                  .setBlockId(bp.getBlockId())
                  .setSize(sz)
                  .addAllReplicas(replicas)
                  .build());
        }
        withLeader(
            stub -> {
              stub.commit(commitBuilder.build());
              return Boolean.TRUE;
            });
        System.out.println("Upload complete for " + remote);
      }
      case "get" -> {
        if (args.length < 3) {
          System.out.println("Usage: get <remotePath> <localFile>");
          return;
        }
        String remote = args[1];
        Path localOut = Paths.get(args[2]);
        LocateResp loc =
            withLeader(
                stub -> stub.locate(FilePath.newBuilder().setPath(remote).build()));
        try (var out = Files.newOutputStream(localOut)) {
          for (BlockPlan bp : loc.getBlocksList()) {
            var replicas = bp.getReplicaUrlsList();
            Map<String, GetMeta> metaByReplica = new ConcurrentHashMap<>();
            List<String> ids = new ArrayList<>(replicas);
            List<java.util.function.Supplier<GetMeta>> tasks = new ArrayList<>();
            for (String hp : replicas) {
              tasks.add(
                  () -> {
                    ManagedChannel ch =
                        ManagedChannelBuilder.forAddress(host(hp), port(hp)).usePlaintext().build();
                    try {
                      StorageServiceGrpc.StorageServiceBlockingStub stub =
                          StorageServiceGrpc.newBlockingStub(ch);
                      var it =
                          stub.getBlock(
                              GetHdr.newBuilder().setBlockId(bp.getBlockId()).setMinR(1).build());
                      if (!it.hasNext()) {
                        throw new IllegalStateException("No response from " + hp);
                      }
                      var first = it.next();
                      if (!first.hasMeta()) {
                        throw new IllegalStateException("Missing meta from " + hp);
                      }
                      GetMeta metaResp = first.getMeta();
                      metaByReplica.put(hp, metaResp);
                      return metaResp;
                    } catch (Exception e) {
                      throw new RuntimeException(e);
                    } finally {
                      ch.shutdownNow();
                    }
                  });
            }
            Quorum qR =
                new Quorum(replicas.size(), Duration.ofMillis(READ_TIMEOUT_MS), QUORUM_R);
            Quorum.Result<GetMeta> r2;
            try {
              r2 = qR.runRace(ids, tasks);
              System.out.println(
                  "[GET quorum] block="
                      + bp.getBlockId()
                      + " ok="
                      + r2.successes()
                      + " fails="
                      + r2.failures().size());
            } catch (Exception e) {
              throw new RuntimeException(
                  "Failed to meet R="
                      + QUORUM_R
                      + " for block "
                      + bp.getBlockId()
                      + ": "
                      + e.getMessage());
            } finally {
              qR.shutdown();
            }

            String chosenHp = null;
            int bestClock = -1;
            for (String hp : r2.successes()) {
              GetMeta metaResp = metaByReplica.get(hp);
              if (metaResp == null) {
                continue;
              }
              int vcLen = metaResp.getVectorClock().length();
              if (vcLen > bestClock) {
                bestClock = vcLen;
                chosenHp = hp;
              }
            }
            if (chosenHp == null) {
              if (!replicas.isEmpty()) {
                chosenHp = replicas.get(0);
              } else {
                throw new RuntimeException(
                    "No replicas available to read block " + bp.getBlockId());
              }
            }

            Path tmp = Files.createTempFile("getchunk", ".bin");
            try {
              StreamTool.get(host(chosenHp), port(chosenHp), bp.getBlockId(), tmp);
              Files.copy(tmp, out);
            } finally {
              Files.deleteIfExists(tmp);
            }
          }
        }
        System.out.println("Download complete → " + localOut);
      }
      case "race" -> {
        if (args.length < 4) {
          System.out.println("Usage: race <localA> <localB> <remotePath>");
          return;
        }
        Path a = Paths.get(args[1]);
        Path b = Paths.get(args[2]);
        String remote = args[3];
        long sizeA = Files.size(a);
        PlanPutResp plan =
            withLeader(
                stub ->
                    stub.planPut(
                        PlanPutReq.newBuilder()
                            .setPath(remote)
                            .setSize(sizeA)
                            .setChunkSize(CHUNK_SIZE)
                            .build()));
        if (plan.getBlocksCount() == 0) {
          System.out.println("No blocks planned for " + remote);
          return;
        }
        BlockPlan bp0 = plan.getBlocks(0);
        if (bp0.getReplicaUrlsCount() < 2) {
          System.out.println("Need at least two replicas to race");
          return;
        }
        String hp0 = bp0.getReplicaUrls(0);
        String hp1 = bp0.getReplicaUrls(1);
        String clientA = CLIENT_ID + "-A";
        String clientB = CLIENT_ID + "-B";
        VectorClock vcA = new VectorClock();
        vcA.increment(clientA);
        VectorClock vcB = new VectorClock();
        vcB.increment(clientB);
        Thread t1 =
            new Thread(
                () -> {
                  try {
                    StreamTool.put(
                        host(hp0),
                        port(hp0),
                        bp0.getBlockId(),
                        a,
                        vcA.toJson(),
                        clientA);
                  } catch (Exception e) {
                    e.printStackTrace();
                  }
                });
        Thread t2 =
            new Thread(
                () -> {
                  try {
                    StreamTool.put(
                        host(hp1),
                        port(hp1),
                        bp0.getBlockId(),
                        b,
                        vcB.toJson(),
                        clientB);
                  } catch (Exception e) {
                    e.printStackTrace();
                  }
                });
        t1.start();
        t2.start();
        t1.join();
        t2.join();
      }
      case "leader" -> {
        String zk = System.getProperty("ds.zk", "localhost:2181");
        try (LeaderDiscovery ld = new LeaderDiscovery(zk)) {
          System.out.println(ld.hostPort());
        }
      }
      default -> System.out.println("Unknown command");
    }
  }

  private static void printUsage() {
    System.out.println("Usage:");
    System.out.println("  put <localFile> <remotePath>");
    System.out.println("  get <remotePath> <localFile>");
    System.out.println("  race <localA> <localB> <remotePath>");
    System.out.println("  leader");
    System.out.println("  putBlock <host:port> <blockId> <file>");
    System.out.println("  getBlock <host:port> <blockId> <out>");
  }

  private static String nextVectorClock(String blockId) {
    VectorClock vc =
        BLOCK_CLOCKS.compute(
            blockId,
            (k, existing) -> {
              VectorClock clock = existing == null ? new VectorClock() : existing;
              clock.increment(CLIENT_ID);
              clock.increment(CLIENT_EPOCH);
              return clock;
            });
    return vc.toJson();
  }

  private static String defaultClientId() {
    try {
      return java.net.InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      return "cli-" + java.util.UUID.randomUUID();
    }
  }

  private static <T> T withLeader(Function<MetadataServiceGrpc.MetadataServiceBlockingStub, T> call)
      throws Exception {
    String zk = System.getProperty("ds.zk", "localhost:2181");
    Exception last = null;
    try (LeaderDiscovery discovery = new LeaderDiscovery(zk)) {
      for (int attempt = 0; attempt < 5; attempt++) {
        ManagedChannel channel = null;
        try {
          String endpoint = discovery.hostPort();
          String leaderHost = host(endpoint);
          int leaderPort = port(endpoint);
          if (leaderPort <= 0) {
            throw new IllegalStateException("Invalid leader endpoint: " + endpoint);
          }
          channel =
              ManagedChannelBuilder.forAddress(leaderHost, leaderPort).usePlaintext().build();
          MetadataServiceGrpc.MetadataServiceBlockingStub stub =
              MetadataServiceGrpc.newBlockingStub(channel);
          T result = call.apply(stub);
          return result;
        } catch (io.grpc.StatusRuntimeException sre) {
          last = sre;
        } catch (Exception e) {
          last = e;
        } finally {
          if (channel != null) {
            channel.shutdownNow();
          }
        }
        try {
          Thread.sleep(500L * (attempt + 1));
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw ie;
        }
      }
    }
    if (last != null) {
      throw last;
    }
    throw new IllegalStateException("Unable to contact metadata leader");
  }
}
