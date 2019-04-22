// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.Test;
import org.rocksdb.util.DirectBytewiseComparator;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

public class OptimisticTransactionOptionsTest {

  private static final Random rand = PlatformRandomHelper.
      getPlatformSpecificRandomFactory();

  @Test
  public void setSnapshot() {
    try (final OptimisticTransactionOptions opt = new OptimisticTransactionOptions()) {
      final boolean boolValue = rand.nextBoolean();
      opt.setSetSnapshot(boolValue);
      assertThat(opt.isSetSnapshot()).isEqualTo(boolValue);
    }
  }

  @Test
  public void comparator() {
    try (final OptimisticTransactionOptions opt = new OptimisticTransactionOptions();
         final ComparatorOptions copt = new ComparatorOptions();
         final DirectComparator comparator = new DirectBytewiseComparator(copt)) {
      opt.setComparator(comparator);
    }
  }
}
