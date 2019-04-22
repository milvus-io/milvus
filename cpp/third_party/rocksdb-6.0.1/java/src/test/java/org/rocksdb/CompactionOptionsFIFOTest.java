// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionOptionsFIFOTest {

  static {
    RocksDB.loadLibrary();
  }

  @Test
  public void maxTableFilesSize() {
    final long size = 500 * 1024 * 1026;
    try (final CompactionOptionsFIFO opt = new CompactionOptionsFIFO()) {
      opt.setMaxTableFilesSize(size);
      assertThat(opt.maxTableFilesSize()).isEqualTo(size);
    }
  }

  @Test
  public void allowCompaction() {
    final boolean allowCompaction = true;
    try (final CompactionOptionsFIFO opt = new CompactionOptionsFIFO()) {
      opt.setAllowCompaction(allowCompaction);
      assertThat(opt.allowCompaction()).isEqualTo(allowCompaction);
    }
  }
}
