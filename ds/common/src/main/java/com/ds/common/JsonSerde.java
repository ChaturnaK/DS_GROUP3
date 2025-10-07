package com.ds.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public final class JsonSerde {
  private static final ObjectMapper MAPPER =
      new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

  private JsonSerde() {}

  public static byte[] write(Object o) {
    try {
      return MAPPER.writeValueAsBytes(o);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T read(byte[] bytes, Class<T> clazz) {
    try {
      return MAPPER.readValue(bytes, clazz);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T read(byte[] bytes, TypeReference<T> type) {
    try {
      return MAPPER.readValue(bytes, type);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
