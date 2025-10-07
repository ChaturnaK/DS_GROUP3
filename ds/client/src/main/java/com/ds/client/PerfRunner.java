package com.ds.client;

import com.ds.common.Metrics;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

public class PerfRunner {
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println("Usage: perf <MiB> <remotePathBase>");
      return;
    }
    int mib = Integer.parseInt(args[0]);
    String base = args[1];
    Path tmp = Files.createTempFile("perf", ".bin");
    byte[] buf = new byte[1024 * 1024];
    Random rnd = new Random(0);
    try (var out = Files.newOutputStream(tmp)) {
      for (int i = 0; i < mib; i++) {
        rnd.nextBytes(buf);
        out.write(buf);
      }
    }
    String remote = base + "/file-" + mib + "MiB.bin";

    long t0 = System.nanoTime();
    DsClient.main(new String[] {"put", tmp.toString(), remote});
    long t1 = System.nanoTime();
    Path outFile = Files.createTempFile("perfo", ".bin");
    DsClient.main(new String[] {"get", remote, outFile.toString()});
    long t2 = System.nanoTime();

    double putSec = (t1 - t0) / 1_000_000_000.0;
    double getSec = (t2 - t1) / 1_000_000_000.0;
    double mb = mib;
    System.out.printf("[PERF] put=%.2f MB/s get=%.2f MB/s%n", mb / putSec, mb / getSec);
    Metrics.counter("perf_runs").increment();
    Metrics.dumpCsv();

    Files.deleteIfExists(tmp);
    Files.deleteIfExists(outFile);
  }
}
