package com.ds.time;

import com.ds.common.Metrics;
import io.micrometer.core.instrument.Gauge;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Periodically samples NTP servers to measure clock offset. Exposes gauges so the rest of the
 * system can detect when samples go stale and fall back to logical clocks.
 */
public final class NtpSync implements Runnable {
  private static final String STATUS_GAUGE = "ntp.sync.status";
  private static final String FAILURE_GAUGE = "ntp.sync.failures";
  private static final String STALENESS_GAUGE = "ntp.sync.staleness.sec";
  private static final int HEALTH_HEALTHY = 2;
  private static final int HEALTH_DEGRADED = 1;
  private static final int HEALTH_UNRELIABLE = 0;

  /**
   * Health state derived from recent sync quality. This is deliberately simple for the assignment
   * yet covers the most common failure modes.
   */
  public enum Health {
    HEALTHY,
    DEGRADED,
    UNRELIABLE
  }

  private final String[] hosts;
  private final int port;
  private final Gauge offsetGauge;
  private final int failureThreshold;
  private final long degradedMs;
  private final long unreliableMs;
  private volatile double offsetMs = 0.0;
  private volatile long lastSuccessMs = 0L;
  private volatile int consecutiveFailures = 0;
  private volatile int currentHostIdx = 0;
  private volatile boolean degradedLogged = false;

  public NtpSync(String host, int port) {
    this(host, port, 3, 500L, 1000L);
  }

  /** Configurable constructor so assignments can tweak failure/latency thresholds. */
  public NtpSync(String host, int port, int failureThreshold, long degradedMs, long unreliableMs) {
    this.hosts =
        host == null || host.isBlank()
            ? new String[] {"pool.ntp.org", "time.google.com", "time.cloudflare.com"}
            : host.split(",");
    this.port = port;
    this.failureThreshold = Math.max(1, failureThreshold);
    this.degradedMs = Math.max(1L, degradedMs);
    this.unreliableMs = Math.max(this.degradedMs, unreliableMs);
    this.offsetGauge = Metrics.gauge("ntp.offset.ms", 0.0);
    this.lastSuccessMs = System.currentTimeMillis();
    Metrics.gauge(STATUS_GAUGE, 1.0);
    Metrics.gauge(FAILURE_GAUGE, 0.0);
    Metrics.gauge(STALENESS_GAUGE, 0.0);
    Metrics.gauge("clock.health", HEALTH_HEALTHY);
    updateHealthMetric(health());
  }

  @Override
  public void run() {
    Exception lastError = null;
    // Collect offsets so we can compute a median when more than one server answers.
    List<Double> successfulOffsets = new ArrayList<>();
    for (int attempts = 0; attempts < hosts.length; attempts++) {
      int idx = (currentHostIdx + attempts) % hosts.length;
      String target = hosts[idx].trim();
      if (target.isEmpty()) {
        continue;
      }
      try {
        double off = query(target);
        successfulOffsets.add(off);
        currentHostIdx = idx;
      } catch (Exception e) {
        lastError = e;
      }
    }
    if (successfulOffsets.isEmpty()) {
      handleFailure(lastError);
    } else {
      handleSuccess(median(successfulOffsets));
    }
  }

  private void handleSuccess(double offset) {
    offsetMs = offset;
    lastSuccessMs = System.currentTimeMillis();
    consecutiveFailures = 0;
    Metrics.gauge("ntp.offset.ms", offsetMs);
    Metrics.gauge(STATUS_GAUGE, 1.0);
    Metrics.gauge(FAILURE_GAUGE, 0.0);
    Metrics.gauge(STALENESS_GAUGE, 0.0);
    if (degradedLogged) {
      System.out.println("[NTP] synchronization recovered; resuming normal mode");
      degradedLogged = false;
    }
    Health health = health();
    updateHealthMetric(health);
    System.out.println(
        String.format("[NTP] health=%s offset=%.2f ms", health, offsetMs));
  }

  private void handleFailure(Exception error) {
    consecutiveFailures++;
    Metrics.counter("ntp.sync.failures_total").increment();
    Metrics.gauge(STATUS_GAUGE, 0.0);
    Metrics.gauge(FAILURE_GAUGE, consecutiveFailures);
    long stalenessMs =
        lastSuccessMs == 0 ? Long.MAX_VALUE : System.currentTimeMillis() - lastSuccessMs;
    double stalenessSeconds =
        stalenessMs == Long.MAX_VALUE ? Double.MAX_VALUE : stalenessMs / 1000.0;
    Metrics.gauge(STALENESS_GAUGE, stalenessSeconds);
    String msg = error == null ? "unknown error" : error.getMessage();
    System.out.println("[NTP] failed: " + msg + " (falling back to local clock)");
    Health health = health();
    updateHealthMetric(health);
    if (!degradedLogged && consecutiveFailures >= failureThreshold) {
      System.out.println(
          "[NTP] consecutive failures >= "
              + failureThreshold
              + "; operating in degraded mode until sync recovers");
      degradedLogged = true;
    }
  }

  private double query(String targetHost) throws Exception {
    byte[] buf = new byte[48];
    buf[0] = 0b0010_0011;
    long t0 = System.currentTimeMillis();
    // try-with-resources so the socket is always closed on validation failures.
    try (DatagramSocket sock = new DatagramSocket()) {
      sock.setSoTimeout(2000);
      DatagramPacket pkt =
          new DatagramPacket(buf, buf.length, InetAddress.getByName(targetHost), port);
      sock.send(pkt);
      DatagramPacket resp = new DatagramPacket(buf, buf.length);
      sock.receive(resp);
      if (resp.getLength() < 48) {
        throw new IllegalStateException("short NTP reply");
      }
    }
    long t3 = System.currentTimeMillis();
    // Basic sanity checks to reject spoofed or malformed responses.
    int mode = buf[0] & 0b111;
    if (mode != 4) {
      throw new IllegalStateException("unexpected NTP mode=" + mode);
    }
    boolean timestampZero = true;
    for (int i = 40; i < 48; i++) {
      if (buf[i] != 0) {
        timestampZero = false;
        break;
      }
    }
    if (timestampZero) {
      throw new IllegalStateException("invalid NTP transmit timestamp");
    }
    long secs = ((buf[40] & 0xffL) << 24)
        | ((buf[41] & 0xffL) << 16)
        | ((buf[42] & 0xffL) << 8)
        | (buf[43] & 0xffL);
    long fracs = ((buf[44] & 0xffL) << 24)
        | ((buf[45] & 0xffL) << 16)
        | ((buf[46] & 0xffL) << 8)
        | (buf[47] & 0xffL);
    double t2 = (secs - 2208988800L) * 1000.0 + (fracs * 1000.0 / 4294967296.0);
    return ((t2 - t0) + (t2 - t3)) / 2.0;
  }

  /** Expose the most recent offset for observers/tests. */
  public double offsetMs() {
    return offsetMs;
  }

  /** Expose last-success timestamp in epoch millis. */
  public long lastSuccessMs() {
    return lastSuccessMs;
  }

  /** Expose the consecutive failure count to help tests assert degraded behaviour. */
  public int consecutiveFailures() {
    return consecutiveFailures;
  }

  /** Simple health getter as required by the assignment. */
  public Health health() {
    long now = System.currentTimeMillis();
    long staleness = lastSuccessMs == 0 ? Long.MAX_VALUE : Math.max(0L, now - lastSuccessMs);
    double absOffset = Math.abs(offsetMs);
    if (consecutiveFailures >= failureThreshold
        || absOffset >= unreliableMs
        || staleness >= unreliableMs) {
      return Health.UNRELIABLE;
    }
    if (consecutiveFailures > 0 || absOffset >= degradedMs || staleness >= degradedMs) {
      return Health.DEGRADED;
    }
    return Health.HEALTHY;
  }

  private void updateHealthMetric(Health health) {
    int value =
        switch (health) {
          case HEALTHY -> HEALTH_HEALTHY;
          case DEGRADED -> HEALTH_DEGRADED;
          case UNRELIABLE -> HEALTH_UNRELIABLE;
        };
    Metrics.gauge("clock.health", value);
  }

  /** Median helper so the offset is more robust when multiple replies arrive. */
  private static double median(List<Double> values) {
    if (values.isEmpty()) {
      throw new IllegalArgumentException("values must not be empty");
    }
    List<Double> copy = new ArrayList<>(values);
    Collections.sort(copy);
    int mid = copy.size() / 2;
    if (copy.size() % 2 == 1) {
      return copy.get(mid);
    }
    return (copy.get(mid - 1) + copy.get(mid)) / 2.0;
  }
}
