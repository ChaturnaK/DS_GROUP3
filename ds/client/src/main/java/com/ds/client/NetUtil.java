package com.ds.client;

public final class NetUtil {
  private NetUtil() {}

  public static String host(String hp) {
    int c = hp.indexOf(':');
    return c < 0 ? hp : hp.substring(0, c);
  }

  public static int port(String hp) {
    int c = hp.indexOf(':');
    return c < 0 ? 0 : Integer.parseInt(hp.substring(c + 1));
  }
}
