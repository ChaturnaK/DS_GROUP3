package com.ds.client;

import java.nio.charset.StandardCharsets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class LeaderDiscovery implements AutoCloseable {
  private final CuratorFramework zk;
  private static final String PATH = "/ds/metadata/leader";

  public LeaderDiscovery(String zkAddr) {
    zk = CuratorFrameworkFactory.newClient(zkAddr, new ExponentialBackoffRetry(200, 10));
    zk.start();
    try {
      zk.blockUntilConnected();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public String hostPort() {
    try {
      return extractEndpoint(readPayload());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public long epoch() {
    try {
      return extractEpoch(readPayload());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    zk.close();
  }

  private String readPayload() throws Exception {
    if (zk.checkExists().forPath(PATH) == null) {
      throw new IllegalStateException("no leader znode");
    }
    byte[] data = zk.getData().forPath(PATH);
    if (data == null || data.length == 0) {
      throw new IllegalStateException("leader znode empty");
    }
    return new String(data, StandardCharsets.UTF_8);
  }

  private static String extractEndpoint(String payload) {
    int idx = payload.lastIndexOf(':');
    if (idx <= 0) {
      return payload;
    }
    return payload.substring(0, idx);
  }

  private static long extractEpoch(String payload) {
    int idx = payload.lastIndexOf(':');
    if (idx < 0 || idx == payload.length() - 1) {
      return 0L;
    }
    try {
      return Long.parseLong(payload.substring(idx + 1));
    } catch (NumberFormatException e) {
      return 0L;
    }
  }
}
