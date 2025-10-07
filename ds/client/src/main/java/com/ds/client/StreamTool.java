package com.ds.client;

import com.ds.common.VectorClock;
import com.google.protobuf.ByteString;
import ds.BlockChunk;
import ds.GetHdr;
import ds.GetResponse;
import ds.PutAck;
import ds.PutOpen;
import ds.PutRequest;
import ds.StorageServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class StreamTool {
  private static final String DEFAULT_CLIENT_ID =
      System.getProperty("ds.client.id", defaultClientId());

  private StreamTool() {}

  public static void put(String host, int port, String blockId, Path file) throws Exception {
    VectorClock vc = new VectorClock();
    vc.increment(DEFAULT_CLIENT_ID);
    put(host, port, blockId, file, vc.toJson(), DEFAULT_CLIENT_ID);
  }

  public static void put(
      String host, int port, String blockId, Path file, String vectorClockJson, String clientId)
      throws Exception {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    StorageServiceGrpc.StorageServiceStub stub = StorageServiceGrpc.newStub(channel);

    CountDownLatch done = new CountDownLatch(1);
    StreamObserver<PutAck> ackObserver =
        new StreamObserver<>() {
          @Override
          public void onNext(PutAck value) {
            System.out.println(
                "PutAck: ok="
                    + value.getOk()
                    + " checksum="
                    + value.getChecksum()
                    + " msg="
                    + value.getMsg());
          }

          @Override
          public void onError(Throwable t) {
            t.printStackTrace();
            done.countDown();
          }

          @Override
          public void onCompleted() {
            done.countDown();
          }
        };

    StreamObserver<PutRequest> requestObserver = stub.putBlock(ackObserver);

    String vc = vectorClockJson == null ? new VectorClock().toJson() : vectorClockJson;
    String client = (clientId == null || clientId.isBlank()) ? DEFAULT_CLIENT_ID : clientId;

    requestObserver.onNext(
        PutRequest.newBuilder()
            .setOpen(
                PutOpen.newBuilder()
                    .setBlockId(blockId)
                    .setVectorClock(vc)
                    .setClientId(client)
                    .setSize(Files.size(file))
                    .build())
            .build());

    int chunkSize = 1024 * 1024;
    byte[] buffer = new byte[chunkSize];
    try (var in = Files.newInputStream(file)) {
      int read;
      while ((read = in.read(buffer)) > 0) {
        requestObserver.onNext(
            PutRequest.newBuilder()
                .setChunk(
                    BlockChunk.newBuilder().setData(ByteString.copyFrom(buffer, 0, read)))
                .build());
      }
    }
    requestObserver.onCompleted();

    done.await(30, TimeUnit.SECONDS);
    channel.shutdownNow();
  }

  public static void get(String host, int port, String blockId, Path out) throws Exception {
    ManagedChannel channel =
        ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
    StorageServiceGrpc.StorageServiceBlockingStub blockingStub =
        StorageServiceGrpc.newBlockingStub(channel);

    Iterator<GetResponse> iterator =
        blockingStub.getBlock(GetHdr.newBuilder().setBlockId(blockId).setMinR(1).build());
    try (var output = Files.newOutputStream(out)) {
      while (iterator.hasNext()) {
        GetResponse response = iterator.next();
        switch (response.getPayloadCase()) {
          case META ->
              System.out.println(
                  "Meta: vc="
                      + response.getMeta().getVectorClock()
                      + " checksum="
                      + response.getMeta().getChecksum());
          case CHUNK -> output.write(response.getChunk().getData().toByteArray());
          case PAYLOAD_NOT_SET -> {
            // ignore
          }
        }
      }
    }
    channel.shutdownNow();
  }

  private static String defaultClientId() {
    try {
      return java.net.InetAddress.getLocalHost().getHostName();
    } catch (Exception e) {
      return "cli-" + java.util.UUID.randomUUID();
    }
  }
}
