package com.ds.common;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class VectorClock {
  public enum Order {
    LESS,
    GREATER,
    EQUAL,
    CONCURRENT
  }

  private final Map<String, Integer> v = new HashMap<>();

  public VectorClock() {}

  public VectorClock(Map<String, Integer> m) {
    if (m != null) {
      v.putAll(m);
    }
  }

  public static VectorClock fromJson(String json) {
    if (json == null || json.isBlank()) {
      return new VectorClock();
    }
    Map<String, Integer> m =
        JsonSerde.read(json.getBytes(), new TypeReference<Map<String, Integer>>() {});
    return new VectorClock(m == null ? Map.of() : m);
  }

  public String toJson() {
    return new String(JsonSerde.write(v));
  }

  public void increment(String id) {
    if (id == null || id.isBlank()) {
      return;
    }
    v.put(id, v.getOrDefault(id, 0) + 1);
  }

  public Order compare(VectorClock other) {
    if (other == null) {
      return Order.GREATER;
    }
    boolean less = false;
    boolean greater = false;
    Set<String> keys = new HashSet<>(v.keySet());
    keys.addAll(other.v.keySet());
    for (String k : keys) {
      int a = v.getOrDefault(k, 0);
      int b = other.v.getOrDefault(k, 0);
      if (a < b) {
        less = true;
      }
      if (a > b) {
        greater = true;
      }
    }
    if (!less && !greater) {
      return Order.EQUAL;
    }
    if (less && greater) {
      return Order.CONCURRENT;
    }
    return greater ? Order.GREATER : Order.LESS;
  }

  public VectorClock merged(VectorClock other) {
    Map<String, Integer> m = new HashMap<>(v);
    if (other != null) {
      for (var e : other.v.entrySet()) {
        m.put(e.getKey(), Math.max(m.getOrDefault(e.getKey(), 0), e.getValue()));
      }
    }
    return new VectorClock(m);
  }
}
