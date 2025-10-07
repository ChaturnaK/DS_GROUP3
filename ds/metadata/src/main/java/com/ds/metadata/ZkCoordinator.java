package com.ds.metadata;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkCoordinator implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(ZkCoordinator.class);

  private final CuratorFramework client;
  private final LeaderLatch leaderLatch;
  private static final String STORAGE_NODES_PATH = "/ds/nodes/storage";
  private static final String LEADER_PATH = "/ds/metadata/leader";
  private final List<Consumer<Boolean>> leaderListeners = new CopyOnWriteArrayList<>();

  public ZkCoordinator(String zk, String id) throws Exception {
    this.client =
        CuratorFrameworkFactory.newClient(zk, new ExponentialBackoffRetry(200, 10));
    this.client.start();
    this.client.blockUntilConnected();
    this.leaderLatch = new LeaderLatch(client, "/ds/metadata/leader", id);
    this.leaderLatch.addListener(
        new LeaderLatchListener() {
          @Override
          public void isLeader() {
            log.info("Leadership granted for {}", id);
            notifyLeaderListeners(true);
          }

          @Override
          public void notLeader() {
            log.info("Leadership revoked for {}", id);
            notifyLeaderListeners(false);
          }
        });
    this.leaderLatch.start();
    watchStorageNodes();
  }

  public CuratorFramework client() {
    return client;
  }

  public boolean isLeader() {
    return leaderLatch.hasLeadership();
  }

  public void publishLeaderEndpoint(String endpoint) {
    try {
      if (client.checkExists().forPath(LEADER_PATH) == null) {
        client.create().creatingParentsIfNeeded().forPath(LEADER_PATH, endpoint.getBytes());
      } else {
        client.setData().forPath(LEADER_PATH, endpoint.getBytes());
      }
    } catch (Exception e) {
      log.warn("Failed to publish leader endpoint {}: {}", endpoint, e.toString());
    }
  }

  public void clearLeaderEndpoint() {
    try {
      if (client.checkExists().forPath(LEADER_PATH) != null) {
        client.setData().forPath(LEADER_PATH, new byte[0]);
      }
    } catch (Exception e) {
      log.warn("Failed to clear leader endpoint: {}", e.toString());
    }
  }

  public void addLeadershipListener(Consumer<Boolean> listener) {
    if (listener != null) {
      leaderListeners.add(listener);
    }
  }

  @Override
  public void close() {
    try {
      leaderLatch.close();
    } catch (Exception ignore) {
      // ignore
    }
    clearLeaderEndpoint();
    client.close();
  }

  private void watchStorageNodes() {
    try {
      client
          .getChildren()
          .usingWatcher(
              (Watcher)
                  event -> {
                    log.info("Node change: {}", event);
                    watchStorageNodes();
                  })
          .forPath(STORAGE_NODES_PATH);
    } catch (Exception e) {
      log.warn("Failed to set watcher on {}: {}", STORAGE_NODES_PATH, e.toString());
    }
  }

  private void notifyLeaderListeners(boolean isLeader) {
    for (Consumer<Boolean> listener : leaderListeners) {
      try {
        listener.accept(isLeader);
      } catch (Exception e) {
        log.warn("Leader listener error: {}", e.toString());
      }
    }
  }
}
