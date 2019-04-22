// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class TransactionOptionsTest {

  private static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  @Test
  public void snapshot() {
    try (final TransactionOptions opt = new TransactionOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setSetSnapshot(boolValue);
      assertThat(opt.isSetSnapshot()).isEqualTo(boolValue);
    }
  }

  @Test
  public void deadlockDetect() {
    try (final TransactionOptions opt = new TransactionOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setDeadlockDetect(boolValue);
      assertThat(opt.isDeadlockDetect()).isEqualTo(boolValue);
    }
  }

  @Test
  public void lockTimeout() {
    try (final TransactionOptions opt = new TransactionOptions()) {
      final long longValue = rand.nextLong();
      opt.setLockTimeout(longValue);
      assertThat(opt.getLockTimeout()).isEqualTo(longValue);
    }
  }

  @Test
  public void expiration() {
    try (final TransactionOptions opt = new TransactionOptions()) {
      final long longValue = rand.nextLong();
      opt.setExpiration(longValue);
      assertThat(opt.getExpiration()).isEqualTo(longValue);
    }
  }

  @Test
  public void deadlockDetectDepth() {
    try (final TransactionOptions opt = new TransactionOptions()) {
      final long longValue = rand.nextLong();
      opt.setDeadlockDetectDepth(longValue);
      assertThat(opt.getDeadlockDetectDepth()).isEqualTo(longValue);
    }
  }

  @Test
  public void maxWriteBatchSize() {
    try (final TransactionOptions opt = new TransactionOptions()) {
      final long longValue = rand.nextLong();
      opt.setMaxWriteBatchSize(longValue);
      assertThat(opt.getMaxWriteBatchSize()).isEqualTo(longValue);
    }
  }
}
