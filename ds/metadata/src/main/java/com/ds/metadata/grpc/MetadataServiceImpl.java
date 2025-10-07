package com.ds.metadata.grpc;

import com.ds.metadata.MetaStore;
import com.ds.metadata.PlacementService;
import com.ds.metadata.ZkCoordinator;
import ds.Ack;
import ds.BlockPlan;
import ds.CommitBlock;
import ds.CommitReq;
import ds.FilePath;
import ds.LocateResp;
import ds.MetadataServiceGrpc;
import ds.PlanPutReq;
import ds.PlanPutResp;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetadataServiceImpl extends MetadataServiceGrpc.MetadataServiceImplBase {
  private final MetaStore meta;
  private final PlacementService placement;
  private final ZkCoordinator coord;

  public MetadataServiceImpl(MetaStore meta, PlacementService placement, ZkCoordinator coord) {
    this.meta = meta;
    this.placement = placement;
    this.coord = coord;
  }

  @Override
  public void planPut(PlanPutReq req, StreamObserver<PlanPutResp> respObs) {
    if (!coord.isLeader()) {
      respObs.onError(
          io.grpc.Status.FAILED_PRECONDITION
              .withDescription("Not leader")
              .asRuntimeException());
      return;
    }
    try {
      int chunkSize = req.getChunkSize() > 0 ? req.getChunkSize() : 8 * 1024 * 1024;
      long size = req.getSize();
      int chunks = (int) Math.ceil((double) size / chunkSize);
      List<BlockPlan> plans = new ArrayList<>();
      for (int i = 0; i < chunks; i++) {
        String blockId = req.getPath() + "#b" + i;
        List<PlacementService.NodeInfo> replicas = placement.chooseReplicas(blockId);
        BlockPlan.Builder bp = BlockPlan.newBuilder().setBlockId(blockId).setSize(chunkSize);
        for (var ni : replicas) {
          bp.addReplicaUrls(ni.host + ":" + ni.port);
        }
        plans.add(bp.build());
      }
      PlanPutResp resp = PlanPutResp.newBuilder().addAllBlocks(plans).build();
      respObs.onNext(resp);
      respObs.onCompleted();
    } catch (Exception e) {
      respObs.onError(
          io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }
  }

  @Override
  public void commit(CommitReq req, StreamObserver<Ack> respObs) {
    if (!coord.isLeader()) {
      respObs.onError(
          io.grpc.Status.FAILED_PRECONDITION
              .withDescription("Not leader")
              .asRuntimeException());
      return;
    }
    try {
      MetaStore.FileEntry fe = new MetaStore.FileEntry();
      fe.size = req.getSize();
      fe.ctime = fe.mtime = System.currentTimeMillis();
      List<String> blockIds = new ArrayList<>();
      Map<String, MetaStore.BlockEntry> bes = new HashMap<>();
      for (CommitBlock cb : req.getBlocksList()) {
        blockIds.add(cb.getBlockId());
        MetaStore.BlockEntry be = new MetaStore.BlockEntry();
        long size = cb.getSize();
        be.size = size > 0 ? size : 8L * 1024 * 1024;
        if (!cb.getReplicasList().isEmpty()) {
          be.replicas.addAll(cb.getReplicasList());
        } else {
          var nodes = placement.chooseReplicas(cb.getBlockId());
          for (var ni : nodes) {
            be.replicas.add(ni.host + ":" + ni.port);
          }
        }
        bes.put(cb.getBlockId(), be);
      }
      fe.blocks = blockIds;
      meta.commit(req.getPath(), fe, bes); // atomic, idempotent
      respObs.onNext(Ack.newBuilder().setOk(true).setMsg("committed").build());
      respObs.onCompleted();
    } catch (Exception e) {
      respObs.onError(
          io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }
  }

  @Override
  public void locate(FilePath req, StreamObserver<LocateResp> respObs) {
    try {
      var fe = meta.getFile(req.getPath()).orElseThrow(() -> new Exception("file not found"));
      List<BlockPlan> bps = new ArrayList<>();
      for (String bid : fe.blocks) {
        var be = meta.getBlock(bid).orElseThrow(() -> new Exception("block not found " + bid));
        BlockPlan.Builder bp = BlockPlan.newBuilder().setBlockId(bid).setSize(be.size);
        for (String r : be.replicas) {
          bp.addReplicaUrls(r);
        }
        bps.add(bp.build());
      }
      LocateResp resp = LocateResp.newBuilder().addAllBlocks(bps).setSize(fe.size).build();
      respObs.onNext(resp);
      respObs.onCompleted();
    } catch (Exception e) {
      respObs.onError(
          io.grpc.Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException());
    }
  }
}
