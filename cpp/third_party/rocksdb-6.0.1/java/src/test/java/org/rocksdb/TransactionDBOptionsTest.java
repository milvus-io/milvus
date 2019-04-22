// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class TransactionDBOptionsTest {

  private static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  @Test
  public void maxNumLocks() {
    try (final TransactionDBOptions opt = new TransactionDBOptions()) {
      final long longValue = rand.nextLong();
      opt.setMaxNumLocks(longValue);
      assertThat(opt.getMaxNumLocks()).isEqualTo(longValue);
    }
  }

  @Test
  public void maxNumStripes() {
    try (final TransactionDBOptions opt = new TransactionDBOptions()) {
      final long longValue = rand.nextLong();
      opt.setNumStripes(longValue);
      assertThat(opt.getNumStripes()).isEqualTo(longValue);
    }
  }

  @Test
  public void transactionLockTimeout() {
    try (final TransactionDBOptions opt = new TransactionDBOptions()) {
      final long longValue = rand.nextLong();
      opt.setTransactionLockTimeout(longValue);
      assertThat(opt.getTransactionLockTimeout()).isEqualTo(longValue);
    }
  }

  @Test
  public void defaultLockTimeout() {
    try (final TransactionDBOptions opt = new TransactionDBOptions()) {
      final long longValue = rand.nextLong();
      opt.setDefaultLockTimeout(longValue);
      assertThat(opt.getDefaultLockTimeout()).isEqualTo(longValue);
    }
  }

  @Test
  public void writePolicy() {
    try (final TransactionDBOptions opt = new TransactionDBOptions()) {
      final TxnDBWritePolicy writePolicy = TxnDBWritePolicy.WRITE_UNPREPARED;  // non-default
      opt.setWritePolicy(writePolicy);
      assertThat(opt.getWritePolicy()).isEqualTo(writePolicy);
    }
  }

}
