package com.ds.client;

import ds.MetadataServiceGrpc;
import ds.PlanPutReq;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

public final class HealthCheck {
  private HealthCheck() {}

  public static void main(String[] args) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 7000)
        .usePlaintext()
        .build();

    try {
      MetadataServiceGrpc.MetadataServiceBlockingStub stub = MetadataServiceGrpc.newBlockingStub(channel);
      stub.planPut(PlanPutReq.newBuilder().setPath("/health-check").setSize(0).setChunkSize(0).build());
      System.out.println("Unexpected success calling PlanPut");
    } catch (StatusRuntimeException e) {
      System.out.println("MetadataService PlanPut returned status: " + e.getStatus());
    } finally {
      channel.shutdownNow();
    }
  }
}
