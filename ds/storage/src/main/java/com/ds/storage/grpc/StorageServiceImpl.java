package com.ds.storage.grpc;

import com.ds.common.Metrics;
import com.ds.common.VectorClock;
import com.ds.storage.BlockStore;
import com.ds.storage.ChaosHooks;
import com.ds.storage.Replicator;
import ds.Ack;
import ds.BlockChunk;
import ds.GetHdr;
import ds.GetMeta;
import ds.GetResponse;
import ds.PutAck;
import ds.PutOpen;
import ds.PutRequest;
import ds.StorageServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Timer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class StorageServiceImpl extends StorageServiceGrpc.StorageServiceImplBase {
  private final BlockStore store;
  private final Replicator replicator;

  public StorageServiceImpl(BlockStore store, Replicator replicator) {
    this.store = store;
    this.replicator = replicator;
  }

  @Override
  public StreamObserver<PutRequest> putBlock(StreamObserver<PutAck> responseObserver) {
    return new StreamObserver<>() {
      String blockId;
      String vectorClock;
      final List<byte[]> chunks = new ArrayList<>();
      final long start = System.nanoTime();
      final Timer timer = Metrics.timer("put_latency_ms");

      boolean chaosEvaluated = false;
      boolean aborted = false;

      @Override
      public void onNext(PutRequest request) {
        if (!chaosEvaluated) {
          chaosEvaluated = true;
          if (ChaosHooks.shouldDrop()) {
            aborted = true;
            responseObserver.onError(
                Status.UNAVAILABLE.withDescription("CHAOS_DROP").asRuntimeException());
            return;
          }
          ChaosHooks.maybeDelay();
        }
        if (aborted) {
          return;
        }
        switch (request.getPayloadCase()) {
          case OPEN -> {
            PutOpen open = request.getOpen();
            blockId = open.getBlockId();
            vectorClock = open.getVectorClock();
          }
          case CHUNK -> chunks.add(request.getChunk().getData().toByteArray());
          case PAYLOAD_NOT_SET -> {
            // ignore
          }
        }
      }

      @Override
      public void onError(Throwable throwable) {
        responseObserver.onError(throwable);
      }

      @Override
      public void onCompleted() {
        if (aborted) {
          timer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
          return;
        }
        try {
          if (blockId == null || blockId.isBlank()) {
            responseObserver.onError(
                Status.INVALID_ARGUMENT.withDescription("Missing blockId").asRuntimeException());
            return;
          }
          VectorClock incoming = VectorClock.fromJson(vectorClock);
          BlockStore.Meta meta = store.readMetaObj(blockId);
          if (meta.vectorClock == null) {
            meta.vectorClock = "";
          }
          if (meta.checksum == null) {
            meta.checksum = "";
          }
          if (meta.primary == null) {
            meta.primary = "";
          }
          VectorClock current = VectorClock.fromJson(meta.vectorClock);
          VectorClock.Order order = current.compare(incoming);
          if (meta.checksum.isBlank()) {
            BlockStore.PutResult res = store.writeStreaming(blockId, chunks);
            meta.vectorClock = incoming.toJson();
            meta.checksum = res.checksumHex;
            meta.primary = "";
            store.writeMetaObj(blockId, meta);
            responseObserver.onNext(
                PutAck.newBuilder()
                    .setOk(true)
                    .setChecksum(res.checksumHex)
                    .setMsg("NEW")
                    .build());
            responseObserver.onCompleted();
            return;
          }

          switch (order) {
            case GREATER -> {
              responseObserver.onNext(
                  PutAck.newBuilder()
                      .setOk(false)
                      .setChecksum(meta.checksum)
                      .setMsg("STALE")
                      .build());
              responseObserver.onCompleted();
            }
            case EQUAL -> {
              responseObserver.onNext(
                  PutAck.newBuilder()
                      .setOk(true)
                      .setChecksum(meta.checksum)
                      .setMsg("IDEMPOTENT")
                      .build());
              responseObserver.onCompleted();
            }
            case LESS -> {
              BlockStore.PutResult res = store.writeStreaming(blockId, chunks);
              meta.vectorClock = incoming.toJson();
              meta.checksum = res.checksumHex;
              meta.primary = "";
              store.writeMetaObj(blockId, meta);
              responseObserver.onNext(
                  PutAck.newBuilder()
                      .setOk(true)
                      .setChecksum(res.checksumHex)
                      .setMsg("UPDATED")
                      .build());
              responseObserver.onCompleted();
            }
            case CONCURRENT -> {
              String suffix =
                  Files.exists(store.blockPathSibling(blockId, "a")) ? "b" : "a";
              BlockStore.PutResult res = store.writeStreaming(blockId + "." + suffix, chunks);
              BlockStore.Meta sibling = new BlockStore.Meta();
              sibling.vectorClock = incoming.toJson();
              sibling.checksum = res.checksumHex;
              sibling.primary = "";
              store.writeMetaObj(blockId + "." + suffix, sibling);
              meta.primary = suffix;
              meta.vectorClock = incoming.toJson();
              meta.checksum = res.checksumHex;
              store.writeMetaObj(blockId, meta);
              responseObserver.onNext(
                  PutAck.newBuilder()
                      .setOk(false)
                      .setChecksum(res.checksumHex)
                      .setMsg("CONFLICT:" + suffix)
                      .build());
              responseObserver.onCompleted();
            }
          }
        } catch (Exception e) {
          responseObserver.onError(
              Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        } finally {
          timer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        }
      }
    };
  }

  @Override
  public void getBlock(GetHdr request, StreamObserver<GetResponse> responseObserver) {
    String blockId = request.getBlockId();
    long bytes = 0L;
    long start = System.nanoTime();
    try {
      BlockStore.Meta baseMeta = store.readMetaObj(blockId);
      String chosen = blockId;
      if (baseMeta.primary != null && !baseMeta.primary.isBlank()) {
        chosen = blockId + "." + baseMeta.primary;
      } else if (!Files.exists(store.blockPath(blockId))) {
        if (Files.exists(store.blockPathSibling(blockId, "a"))) {
          chosen = blockId + ".a";
        } else if (Files.exists(store.blockPathSibling(blockId, "b"))) {
          chosen = blockId + ".b";
        }
      }
      BlockStore.Meta chosenMeta = store.readMetaObj(chosen);
      String vc = chosenMeta.vectorClock == null ? "" : chosenMeta.vectorClock;
      String checksum = chosenMeta.checksum == null ? "" : chosenMeta.checksum;
      GetMeta meta = GetMeta.newBuilder().setVectorClock(vc).setChecksum(checksum).build();
      responseObserver.onNext(GetResponse.newBuilder().setMeta(meta).build());

      for (byte[] buf : store.streamRead(chosen, 1024 * 1024)) {
        if (buf.length == 0) {
          continue;
        }
        bytes += buf.length;
        if (ChaosHooks.shouldDrop()) {
          responseObserver.onError(
              Status.UNAVAILABLE.withDescription("CHAOS_DROP").asRuntimeException());
          return;
        }
        ChaosHooks.maybeDelay();
        GetResponse chunkResp =
            GetResponse.newBuilder()
                .setChunk(
                    BlockChunk.newBuilder()
                        .setData(com.google.protobuf.ByteString.copyFrom(buf)))
                .build();
        responseObserver.onNext(chunkResp);
      }
      long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
      if (elapsedMs > 0) {
        double throughput = (bytes / 1_000_000.0) / (elapsedMs / 1000.0);
        Metrics.gauge("get_throughput_mb_s", throughput);
      }
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(
          Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException());
    }
  }

  @Override
  public void replicateBlock(GetHdr request, StreamObserver<Ack> responseObserver) {
    try {
      String[] parts = request.getBlockId().split("@");
      if (parts.length != 2) {
        throw new IllegalArgumentException("Expected blockId@host:port");
      }
      String blockId = parts[0];
      String[] hp = parts[1].split(":");
      if (hp.length != 2) {
        throw new IllegalArgumentException("Expected host:port");
      }
      boolean ok =
          replicator.replicate(blockId, hp[0], Integer.parseInt(hp[1]));
      responseObserver.onNext(
          Ack.newBuilder().setOk(ok).setMsg(ok ? "done" : "fail").build());
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(
          Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }
  }
}
