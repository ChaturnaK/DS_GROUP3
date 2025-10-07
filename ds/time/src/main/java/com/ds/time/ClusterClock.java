package com.ds.time;

import com.ds.time.NtpSync.Health;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Provides cluster-wide timestamps that fall back to a logical clock when NTP health degrades.
 * Other modules should call {@link #now()} instead of {@link System#currentTimeMillis()} so they
 * automatically adapt to NTP availability.
 */
public final class ClusterClock {
  private static final AtomicLong LOGICAL = new AtomicLong(System.currentTimeMillis());
  private static volatile Supplier<Health> healthSupplier = () -> Health.HEALTHY;

  private ClusterClock() {}

  /** Registers a supplier that reports the latest NTP health. */
  public static void registerHealthSupplier(Supplier<Health> supplier) {
    healthSupplier = supplier == null ? () -> Health.HEALTHY : supplier;
  }

  /** Returns a timestamp in milliseconds, falling back to logical time when needed. */
  public static long now() {
    Health health = currentHealth();
    long physical = System.currentTimeMillis();
    return switch (health) {
      case HEALTHY -> recordPhysical(physical);
      case DEGRADED -> LOGICAL.updateAndGet(prev -> Math.max(prev + 1, physical));
      case UNRELIABLE -> LOGICAL.incrementAndGet();
    };
  }

  /** Exposes the last known health so components can branch on it if needed. */
  public static Health currentHealth() {
    Supplier<Health> supplier = healthSupplier;
    try {
      return supplier == null ? Health.HEALTHY : supplier.get();
    } catch (Exception e) {
      return Health.HEALTHY;
    }
  }

  /** Returns true when consumers should avoid relying on physical wall-clock time. */
  public static boolean preferLogical() {
    return currentHealth() != Health.HEALTHY;
  }

  /** Advances and returns the logical clock without consulting wall time. */
  public static long logicalNow() {
    return LOGICAL.incrementAndGet();
  }

  private static long recordPhysical(long physical) {
    return LOGICAL.updateAndGet(prev -> Math.max(prev, physical));
  }
}
