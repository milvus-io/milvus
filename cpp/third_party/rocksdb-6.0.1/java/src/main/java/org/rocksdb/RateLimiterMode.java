// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Mode for {@link RateLimiter#RateLimiter(long, long, int, RateLimiterMode)}.
 */
public enum RateLimiterMode {
  READS_ONLY((byte)0x0),
  WRITES_ONLY((byte)0x1),
  ALL_IO((byte)0x2);

  private final byte value;

  RateLimiterMode(final byte value) {
    this.value = value;
  }

  /**
   * <p>Returns the byte value of the enumerations value.</p>
   *
   * @return byte representation
   */
  public byte getValue() {
    return value;
  }

  /**
   * <p>Get the RateLimiterMode enumeration value by
   * passing the byte identifier to this method.</p>
   *
   * @param byteIdentifier of RateLimiterMode.
   *
   * @return AccessHint instance.
   *
   * @throws IllegalArgumentException if the access hint for the byteIdentifier
   *     cannot be found
   */
  public static RateLimiterMode getRateLimiterMode(final byte byteIdentifier) {
    for (final RateLimiterMode rateLimiterMode : RateLimiterMode.values()) {
      if (rateLimiterMode.getValue() == byteIdentifier) {
        return rateLimiterMode;
      }
    }

    throw new IllegalArgumentException(
        "Illegal value provided for RateLimiterMode.");
  }
}
