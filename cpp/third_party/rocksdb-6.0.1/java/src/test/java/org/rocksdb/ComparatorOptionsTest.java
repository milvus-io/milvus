// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ComparatorOptionsTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Test
  public void comparatorOptions() {
    try(final ComparatorOptions copt = new ComparatorOptions()) {

      assertThat(copt).isNotNull();
      // UseAdaptiveMutex test
      copt.setUseAdaptiveMutex(true);
      assertThat(copt.useAdaptiveMutex()).isTrue();

      copt.setUseAdaptiveMutex(false);
      assertThat(copt.useAdaptiveMutex()).isFalse();
    }
  }
}
