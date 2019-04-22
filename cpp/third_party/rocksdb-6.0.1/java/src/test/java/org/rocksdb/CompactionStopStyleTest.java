// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionStopStyleTest {

  @Test(expected = IllegalArgumentException.class)
  public void failIfIllegalByteValueProvided() {
    CompactionStopStyle.getCompactionStopStyle((byte) -1);
  }

  @Test
  public void getCompactionStopStyle() {
    assertThat(CompactionStopStyle.getCompactionStopStyle(
        CompactionStopStyle.CompactionStopStyleTotalSize.getValue()))
            .isEqualTo(CompactionStopStyle.CompactionStopStyleTotalSize);
  }

  @Test
  public void valueOf() {
    assertThat(CompactionStopStyle.valueOf("CompactionStopStyleSimilarSize")).
        isEqualTo(CompactionStopStyle.CompactionStopStyleSimilarSize);
  }
}
