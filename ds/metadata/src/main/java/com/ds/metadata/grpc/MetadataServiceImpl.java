package com.ds.metadata.grpc;

import com.ds.common.Metrics;
import com.ds.metadata.MetaStore;
import com.ds.metadata.PlacementService;
import com.ds.metadata.ZkCoordinator;
import com.ds.time.ClusterClock;
import ds.Ack;
import ds.BlockPlan;
import ds.CommitBlock;
import ds.CommitReq;
import ds.FilePath;
import ds.LocateResp;
import ds.MetadataServiceGrpc;
import ds.PlanPutReq;
import ds.PlanPutResp;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Timer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataServiceImpl extends MetadataServiceGrpc.MetadataServiceImplBase {
  private static final Logger log = LoggerFactory.getLogger(MetadataServiceImpl.class);

  private final MetaStore meta;
  private final PlacementService placement;
  private final ZkCoordinator coord;
  private final Timer commitTimer = Metrics.timer("consensus_commit_latency");

  public MetadataServiceImpl(MetaStore meta, PlacementService placement, ZkCoordinator coord) {
    this.meta = meta;
    this.placement = placement;
    this.coord = coord;
  }

  @Override
  public void planPut(PlanPutReq req, StreamObserver<PlanPutResp> respObs) {
    if (!coord.isLeader()) {
      respObs.onError(
          Status.FAILED_PRECONDITION.withDescription("Not leader").asRuntimeException());
      return;
    }
    if (!ensureEpoch(respObs, "planPut")) {
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
      respObs.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    }
  }

  @Override
  public void commit(CommitReq req, StreamObserver<Ack> respObs) {
    if (!coord.isLeader()) {
      respObs.onError(
          Status.FAILED_PRECONDITION.withDescription("Not leader").asRuntimeException());
      return;
    }
    if (!ensureEpoch(respObs, "commit")) {
      return;
    }
    Timer.Sample sample = Timer.start(Metrics.reg());
    try {
      MetaStore.FileEntry fe = new MetaStore.FileEntry();
      fe.size = req.getSize();
      long now = ClusterClock.now();
      fe.ctime = fe.mtime = now;
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
      respObs.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
    } finally {
      sample.stop(commitTimer);
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
      respObs.onError(Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException());
    }
  }

  private boolean ensureEpoch(StreamObserver<?> respObs, String operation) {
    long published = coord.fetchPublishedEpoch();
    long local = coord.getLeaderEpoch();
    if (published != 0 && local != 0 && published != local) {
      log.warn(
          "[CONSENSUS] {} rejected due to epoch mismatch (local={}, remote={})",
          operation,
          local,
          published);
      respObs.onError(
          Status.FAILED_PRECONDITION
              .withDescription("Stale leader detected, aborting write")
              .asRuntimeException());
      return false;
    }
    return true;
  }
}
