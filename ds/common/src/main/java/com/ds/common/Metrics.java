package com.ds.common;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class Metrics {
  private static volatile MeterRegistry REG;
  private static final Map<String, MutableGauge> GAUGES = new ConcurrentHashMap<>();

  private Metrics() {}

  public static synchronized MeterRegistry reg() {
    if (REG == null) {
      REG = new SimpleMeterRegistry();
    }
    return REG;
  }

  public static Timer timer(String name, String... tags) {
    return Timer.builder(name).tags(tags).register(reg());
  }

  public static Counter counter(String name, String... tags) {
    return Counter.builder(name).tags(tags).register(reg());
  }

  public static Gauge gauge(String name, Number number, String... tags) {
    String key = gaugeKey(name, tags);
    MutableGauge holder =
        GAUGES.computeIfAbsent(
            key,
            k -> {
              MutableGauge mg = new MutableGauge();
              mg.gauge = Gauge.builder(name, mg, MutableGauge::getValue).tags(tags).register(reg());
              return mg;
            });
    holder.setValue(number == null ? 0.0 : number.doubleValue());
    return holder.gauge;
  }

  private static String gaugeKey(String name, String... tags) {
    StringBuilder sb = new StringBuilder(name);
    if (tags != null && tags.length > 0) {
      sb.append('|').append(String.join("|", Arrays.asList(tags)));
    }
    return sb.toString();
  }

  public static void dumpCsv() {
    try {
      Path dir = Path.of("./metrics");
      Files.createDirectories(dir);
      for (Meter meter : reg().getMeters()) {
        StringBuilder sb = new StringBuilder();
        for (Measurement measurement : meter.measure()) {
          sb.append(System.currentTimeMillis())
              .append(',')
              .append(measurement.getStatistic().name())
              .append(',')
              .append(measurement.getValue())
              .append('\n');
        }
        if (sb.length() == 0) {
          continue;
        }
        Path out = dir.resolve(meter.getId().getName() + ".csv");
        Files.writeString(
            out, sb.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
      }
    } catch (IOException ignored) {
      // Swallow errors writing metrics snapshots.
    }
  }

  private static final class MutableGauge extends Number {
    private volatile double value;
    private Gauge gauge;

    @Override
    public int intValue() {
      return (int) value;
    }

    @Override
    public long longValue() {
      return (long) value;
    }

    @Override
    public float floatValue() {
      return (float) value;
    }

    @Override
    public double doubleValue() {
      return value;
    }

    double getValue() {
      return value;
    }

    void setValue(double v) {
      this.value = v;
    }
  }
}
