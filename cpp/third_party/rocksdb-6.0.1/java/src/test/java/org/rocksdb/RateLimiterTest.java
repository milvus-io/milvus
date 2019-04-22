// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.rocksdb.RateLimiter.*;

public class RateLimiterTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Test
  public void bytesPerSecond() {
    try(final RateLimiter rateLimiter =
            new RateLimiter(1000, DEFAULT_REFILL_PERIOD_MICROS,
                DEFAULT_FAIRNESS, DEFAULT_MODE, DEFAULT_AUTOTUNE)) {
      assertThat(rateLimiter.getBytesPerSecond()).isGreaterThan(0);
      rateLimiter.setBytesPerSecond(2000);
      assertThat(rateLimiter.getBytesPerSecond()).isGreaterThan(0);
    }
  }

  @Test
  public void getSingleBurstBytes() {
    try(final RateLimiter rateLimiter =
            new RateLimiter(1000, DEFAULT_REFILL_PERIOD_MICROS,
                DEFAULT_FAIRNESS, DEFAULT_MODE, DEFAULT_AUTOTUNE)) {
      assertThat(rateLimiter.getSingleBurstBytes()).isEqualTo(100);
    }
  }

  @Test
  public void getTotalBytesThrough() {
    try(final RateLimiter rateLimiter =
            new RateLimiter(1000, DEFAULT_REFILL_PERIOD_MICROS,
                DEFAULT_FAIRNESS, DEFAULT_MODE, DEFAULT_AUTOTUNE)) {
      assertThat(rateLimiter.getTotalBytesThrough()).isEqualTo(0);
    }
  }

  @Test
  public void getTotalRequests() {
    try(final RateLimiter rateLimiter =
            new RateLimiter(1000, DEFAULT_REFILL_PERIOD_MICROS,
                DEFAULT_FAIRNESS, DEFAULT_MODE, DEFAULT_AUTOTUNE)) {
      assertThat(rateLimiter.getTotalRequests()).isEqualTo(0);
    }
  }

  @Test
  public void autoTune() {
    try(final RateLimiter rateLimiter =
            new RateLimiter(1000, DEFAULT_REFILL_PERIOD_MICROS,
                DEFAULT_FAIRNESS, DEFAULT_MODE, true)) {
      assertThat(rateLimiter.getBytesPerSecond()).isGreaterThan(0);
    }
  }
}
