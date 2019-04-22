// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.*;

public class SstFileManagerTest {

  @Test
  public void maxAllowedSpaceUsage() throws RocksDBException {
    try (final SstFileManager sstFileManager = new SstFileManager(Env.getDefault())) {
      sstFileManager.setMaxAllowedSpaceUsage(1024 * 1024 * 64);
      assertThat(sstFileManager.isMaxAllowedSpaceReached()).isFalse();
      assertThat(sstFileManager.isMaxAllowedSpaceReachedIncludingCompactions()).isFalse();
    }
  }

  @Test
  public void compactionBufferSize() throws RocksDBException {
    try (final SstFileManager sstFileManager = new SstFileManager(Env.getDefault())) {
      sstFileManager.setCompactionBufferSize(1024 * 1024 * 10);
      assertThat(sstFileManager.isMaxAllowedSpaceReachedIncludingCompactions()).isFalse();
    }
  }

  @Test
  public void totalSize() throws RocksDBException {
    try (final SstFileManager sstFileManager = new SstFileManager(Env.getDefault())) {
      assertThat(sstFileManager.getTotalSize()).isEqualTo(0);
    }
  }

  @Test
  public void trackedFiles() throws RocksDBException {
    try (final SstFileManager sstFileManager = new SstFileManager(Env.getDefault())) {
      assertThat(sstFileManager.getTrackedFiles()).isEqualTo(Collections.emptyMap());
    }
  }

  @Test
  public void deleteRateBytesPerSecond() throws RocksDBException {
    try (final SstFileManager sstFileManager = new SstFileManager(Env.getDefault())) {
      assertThat(sstFileManager.getDeleteRateBytesPerSecond()).isEqualTo(SstFileManager.RATE_BYTES_PER_SEC_DEFAULT);
      final long ratePerSecond = 1024 * 1024 * 52;
      sstFileManager.setDeleteRateBytesPerSecond(ratePerSecond);
      assertThat(sstFileManager.getDeleteRateBytesPerSecond()).isEqualTo(ratePerSecond);
    }
  }

  @Test
  public void maxTrashDBRatio() throws RocksDBException {
    try (final SstFileManager sstFileManager = new SstFileManager(Env.getDefault())) {
      assertThat(sstFileManager.getMaxTrashDBRatio()).isEqualTo(SstFileManager.MAX_TRASH_DB_RATION_DEFAULT);
      final double trashRatio = 0.2;
      sstFileManager.setMaxTrashDBRatio(trashRatio);
      assertThat(sstFileManager.getMaxTrashDBRatio()).isEqualTo(trashRatio);
    }
  }
}
