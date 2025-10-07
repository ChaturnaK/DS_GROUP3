package com.ds.client;

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
      if (zk.checkExists().forPath(PATH) == null) {
        throw new IllegalStateException("no leader znode");
      }
      byte[] data = zk.getData().forPath(PATH);
      if (data == null || data.length == 0) {
        throw new IllegalStateException("leader znode empty");
      }
      return new String(data);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    zk.close();
  }
}
