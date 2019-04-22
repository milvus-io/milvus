// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionOptionsUniversalTest {

  static {
    RocksDB.loadLibrary();
  }

  @Test
  public void sizeRatio() {
    final int sizeRatio = 4;
    try(final CompactionOptionsUniversal opt = new CompactionOptionsUniversal()) {
      opt.setSizeRatio(sizeRatio);
      assertThat(opt.sizeRatio()).isEqualTo(sizeRatio);
    }
  }

  @Test
  public void minMergeWidth() {
    final int minMergeWidth = 3;
    try(final CompactionOptionsUniversal opt = new CompactionOptionsUniversal()) {
      opt.setMinMergeWidth(minMergeWidth);
      assertThat(opt.minMergeWidth()).isEqualTo(minMergeWidth);
    }
  }

  @Test
  public void maxMergeWidth() {
    final int maxMergeWidth = Integer.MAX_VALUE - 1234;
    try(final CompactionOptionsUniversal opt = new CompactionOptionsUniversal()) {
      opt.setMaxMergeWidth(maxMergeWidth);
      assertThat(opt.maxMergeWidth()).isEqualTo(maxMergeWidth);
    }
  }

  @Test
  public void maxSizeAmplificationPercent() {
    final int maxSizeAmplificationPercent = 150;
    try(final CompactionOptionsUniversal opt = new CompactionOptionsUniversal()) {
      opt.setMaxSizeAmplificationPercent(maxSizeAmplificationPercent);
      assertThat(opt.maxSizeAmplificationPercent()).isEqualTo(maxSizeAmplificationPercent);
    }
  }

  @Test
  public void compressionSizePercent() {
    final int compressionSizePercent = 500;
    try(final CompactionOptionsUniversal opt = new CompactionOptionsUniversal()) {
      opt.setCompressionSizePercent(compressionSizePercent);
      assertThat(opt.compressionSizePercent()).isEqualTo(compressionSizePercent);
    }
  }

  @Test
  public void stopStyle() {
    final CompactionStopStyle stopStyle = CompactionStopStyle.CompactionStopStyleSimilarSize;
    try(final CompactionOptionsUniversal opt = new CompactionOptionsUniversal()) {
      opt.setStopStyle(stopStyle);
      assertThat(opt.stopStyle()).isEqualTo(stopStyle);
    }
  }

  @Test
  public void allowTrivialMove() {
    final boolean allowTrivialMove = true;
    try(final CompactionOptionsUniversal opt = new CompactionOptionsUniversal()) {
      opt.setAllowTrivialMove(allowTrivialMove);
      assertThat(opt.allowTrivialMove()).isEqualTo(allowTrivialMove);
    }
  }
}
