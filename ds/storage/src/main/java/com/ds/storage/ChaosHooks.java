package com.ds.storage;

import java.util.concurrent.ThreadLocalRandom;

public final class ChaosHooks {
  private static volatile int dropPct = Integer.getInteger("CHAOS_DROP_PCT", 0);
  private static volatile int delayMs = Integer.getInteger("CHAOS_DELAY_MS", 0);

  private ChaosHooks() {}

  public static void configureFromEnv() {
    String d = System.getenv("CHAOS_DROP_PCT");
    String l = System.getenv("CHAOS_DELAY_MS");
    if (d != null) {
      try {
        dropPct = Math.max(0, Math.min(100, Integer.parseInt(d)));
      } catch (Exception ignored) {
        // ignore malformed env
      }
    }
    if (l != null) {
      try {
        delayMs = Math.max(0, Integer.parseInt(l));
      } catch (Exception ignored) {
        // ignore malformed env
      }
    }
  }

  public static void maybeDelay() {
    if (delayMs > 0) {
      try {
        Thread.sleep(delayMs);
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public static boolean shouldDrop() {
    if (dropPct <= 0) {
      return false;
    }
    int r = ThreadLocalRandom.current().nextInt(100);
    return r < dropPct;
  }
}
