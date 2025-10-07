package com.ds.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public final class Quorum {
  public record Result<T>(T value, List<Throwable> failures, List<String> successes) {}

  private final ExecutorService pool;
  private final Duration timeout;
  private final int quorum;

  public Quorum(int threads, Duration timeout, int quorum) {
    this.pool = Executors.newFixedThreadPool(threads);
    this.timeout = timeout;
    this.quorum = quorum;
  }

  public <T> Result<T> runRace(List<String> ids, List<Supplier<T>> tasks) throws Exception {
    if (tasks.size() != ids.size()) {
      throw new IllegalArgumentException("ids!=tasks");
    }
    List<CompletableFuture<T>> futures = new ArrayList<>();
    List<Throwable> fails = new ArrayList<>();
    List<String> oks = new ArrayList<>();
    CompletableFuture<T> winner = new CompletableFuture<>();

    for (int i = 0; i < tasks.size(); i++) {
      final int idx = i;
      CompletableFuture<T> cf =
          CompletableFuture.supplyAsync(tasks.get(idx), pool)
              .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)
              .whenComplete(
                  (v, ex) -> {
                    synchronized (this) {
                      if (ex == null) {
                        oks.add(ids.get(idx));
                        if (oks.size() >= quorum && !winner.isDone()) {
                          winner.complete(v);
                        }
                      } else {
                        fails.add(ex);
                      }
                    }
                  });
      futures.add(cf);
    }

    try {
      T v = winner.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
      futures.forEach(f -> f.cancel(true));
      return new Result<>(v, fails, oks);
    } catch (Exception e) {
      futures.forEach(
          f -> {
            try {
              f.join();
            } catch (Throwable ignored) {
              // ignored
            }
          });
      throw new TimeoutException(
          "Quorum not met: ok="
              + oks.size()
              + " needed="
              + quorum
              + " fails="
              + fails.size());
    }
  }

  public void shutdown() {
    pool.shutdownNow();
  }
}
