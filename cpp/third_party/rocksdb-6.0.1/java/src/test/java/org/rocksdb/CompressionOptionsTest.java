// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CompressionOptionsTest {

  static {
    RocksDB.loadLibrary();
  }

  @Test
  public void windowBits() {
    final int windowBits = 7;
    try(final CompressionOptions opt = new CompressionOptions()) {
      opt.setWindowBits(windowBits);
      assertThat(opt.windowBits()).isEqualTo(windowBits);
    }
  }

  @Test
  public void level() {
    final int level = 6;
    try(final CompressionOptions opt = new CompressionOptions()) {
      opt.setLevel(level);
      assertThat(opt.level()).isEqualTo(level);
    }
  }

  @Test
  public void strategy() {
    final int strategy = 2;
    try(final CompressionOptions opt = new CompressionOptions()) {
      opt.setStrategy(strategy);
      assertThat(opt.strategy()).isEqualTo(strategy);
    }
  }

  @Test
  public void maxDictBytes() {
    final int maxDictBytes = 999;
    try(final CompressionOptions opt = new CompressionOptions()) {
      opt.setMaxDictBytes(maxDictBytes);
      assertThat(opt.maxDictBytes()).isEqualTo(maxDictBytes);
    }
  }

  @Test
  public void zstdMaxTrainBytes() {
    final int zstdMaxTrainBytes = 999;
    try(final CompressionOptions opt = new CompressionOptions()) {
      opt.setZStdMaxTrainBytes(zstdMaxTrainBytes);
      assertThat(opt.zstdMaxTrainBytes()).isEqualTo(zstdMaxTrainBytes);
    }
  }

  @Test
  public void enabled() {
    try(final CompressionOptions opt = new CompressionOptions()) {
      assertThat(opt.enabled()).isFalse();
      opt.setEnabled(true);
      assertThat(opt.enabled()).isTrue();
    }
  }
}
