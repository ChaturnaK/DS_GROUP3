package com.ds.time;

import com.ds.common.Metrics;
import io.micrometer.core.instrument.Gauge;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

public final class NtpSync implements Runnable {
  private volatile double offsetMs = 0.0;
  private final String host;
  private final int port;
  private final Gauge gauge;

  public NtpSync(String host, int port) {
    this.host = host;
    this.port = port;
    this.gauge = Metrics.gauge("ntp.offset.ms", 0.0);
  }

  @Override
  public void run() {
    try {
      double off = query();
      offsetMs = off;
      Metrics.gauge("ntp.offset.ms", offsetMs);
      System.out.println("[NTP] offset_ms=" + off);
    } catch (Exception e) {
      System.out.println("[NTP] failed: " + e.getMessage());
    }
  }

  private double query() throws Exception {
    byte[] buf = new byte[48];
    buf[0] = 0b0010_0011;
    long t0 = System.currentTimeMillis();
    DatagramSocket sock = new DatagramSocket();
    sock.setSoTimeout(2000);
    DatagramPacket pkt = new DatagramPacket(buf, buf.length, InetAddress.getByName(host), port);
    sock.send(pkt);
    DatagramPacket resp = new DatagramPacket(buf, buf.length);
    sock.receive(resp);
    long t3 = System.currentTimeMillis();
    long secs = ((buf[40] & 0xffL) << 24)
        | ((buf[41] & 0xffL) << 16)
        | ((buf[42] & 0xffL) << 8)
        | (buf[43] & 0xffL);
    long fracs = ((buf[44] & 0xffL) << 24)
        | ((buf[45] & 0xffL) << 16)
        | ((buf[46] & 0xffL) << 8)
        | (buf[47] & 0xffL);
    double t2 = (secs - 2208988800L) * 1000.0 + (fracs * 1000.0 / 4294967296.0);
    double offset = ((t2 - t0) + (t2 - t3)) / 2.0;
    sock.close();
    return offset;
  }
}
