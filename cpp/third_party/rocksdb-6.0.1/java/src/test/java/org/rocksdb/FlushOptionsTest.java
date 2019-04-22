// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FlushOptionsTest {

  @Test
  public void waitForFlush() {
    try (final FlushOptions flushOptions = new FlushOptions()) {
      assertThat(flushOptions.waitForFlush()).isTrue();
      flushOptions.setWaitForFlush(false);
      assertThat(flushOptions.waitForFlush()).isFalse();
    }
  }

  @Test
  public void allowWriteStall() {
    try (final FlushOptions flushOptions = new FlushOptions()) {
      assertThat(flushOptions.allowWriteStall()).isFalse();
      flushOptions.setAllowWriteStall(true);
      assertThat(flushOptions.allowWriteStall()).isTrue();
    }
  }
}
