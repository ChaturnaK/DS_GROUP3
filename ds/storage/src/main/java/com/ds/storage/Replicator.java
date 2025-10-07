package com.ds.storage;

import ds.GetHdr;
import ds.GetResponse;
import ds.StorageServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Pulls blocks from another replica. */
public class Replicator {
  private static final Logger log = LoggerFactory.getLogger(Replicator.class);
  private final BlockStore store;

  public Replicator(BlockStore store) {
    this.store = store;
  }

  public boolean replicate(String blockId, String fromHost, int fromPort) {
    ManagedChannel ch = null;
    try {
      log.info("Replicating {} from {}:{}", blockId, fromHost, fromPort);
      ch = ManagedChannelBuilder.forAddress(fromHost, fromPort).usePlaintext().build();
      StorageServiceGrpc.StorageServiceBlockingStub stub = StorageServiceGrpc.newBlockingStub(ch);

      var it = stub.getBlock(GetHdr.newBuilder().setBlockId(blockId).setMinR(1).build());
      List<byte[]> chunks = new ArrayList<>();
      String checksum = "";
      String vc = "";
      while (it.hasNext()) {
        GetResponse r = it.next();
        switch (r.getPayloadCase()) {
          case META -> {
            vc = r.getMeta().getVectorClock();
            checksum = r.getMeta().getChecksum();
          }
          case CHUNK -> chunks.add(r.getChunk().getData().toByteArray());
          default -> {
            // ignore
          }
        }
      }
      BlockStore.PutResult res = store.writeStreaming(blockId, chunks);
      if (!res.checksumHex.equals(checksum)) {
        throw new IOException(
            "Checksum mismatch src=" + checksum + " dst=" + res.checksumHex);
      }
      BlockStore.Meta meta = new BlockStore.Meta();
      meta.vectorClock = vc;
      meta.checksum = checksum;
      meta.primary = "";
      store.writeMetaObj(blockId, meta);
      log.info("Replication complete {} bytes={} checksum={}", blockId, res.bytesWritten, checksum);
      return true;
    } catch (Exception e) {
      log.error("Replication failed for {}: {}", blockId, e.toString());
      return false;
    } finally {
      if (ch != null) {
        ch.shutdownNow();
      }
    }
  }
}
