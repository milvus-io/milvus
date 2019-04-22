// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CompactionJobStatsTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Test
  public void reset() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      compactionJobStats.reset();
      assertThat(compactionJobStats.elapsedMicros()).isEqualTo(0);
    }
  }

  @Test
  public void add() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats();
         final CompactionJobStats otherCompactionJobStats = new CompactionJobStats()) {
      compactionJobStats.add(otherCompactionJobStats);
    }
  }

  @Test
  public void elapsedMicros() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.elapsedMicros()).isEqualTo(0);
    }
  }

  @Test
  public void numInputRecords() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.numInputRecords()).isEqualTo(0);
    }
  }

  @Test
  public void numInputFiles() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.numInputFiles()).isEqualTo(0);
    }
  }

  @Test
  public void numInputFilesAtOutputLevel() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.numInputFilesAtOutputLevel()).isEqualTo(0);
    }
  }

  @Test
  public void numOutputRecords() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.numOutputRecords()).isEqualTo(0);
    }
  }

  @Test
  public void numOutputFiles() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.numOutputFiles()).isEqualTo(0);
    }
  }

  @Test
  public void isManualCompaction() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.isManualCompaction()).isFalse();
    }
  }

  @Test
  public void totalInputBytes() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.totalInputBytes()).isEqualTo(0);
    }
  }

  @Test
  public void totalOutputBytes() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.totalOutputBytes()).isEqualTo(0);
    }
  }


  @Test
  public void numRecordsReplaced() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.numRecordsReplaced()).isEqualTo(0);
    }
  }

  @Test
  public void totalInputRawKeyBytes() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.totalInputRawKeyBytes()).isEqualTo(0);
    }
  }

  @Test
  public void totalInputRawValueBytes() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.totalInputRawValueBytes()).isEqualTo(0);
    }
  }

  @Test
  public void numInputDeletionRecords() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.numInputDeletionRecords()).isEqualTo(0);
    }
  }

  @Test
  public void numExpiredDeletionRecords() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.numExpiredDeletionRecords()).isEqualTo(0);
    }
  }

  @Test
  public void numCorruptKeys() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.numCorruptKeys()).isEqualTo(0);
    }
  }

  @Test
  public void fileWriteNanos() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.fileWriteNanos()).isEqualTo(0);
    }
  }

  @Test
  public void fileRangeSyncNanos() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.fileRangeSyncNanos()).isEqualTo(0);
    }
  }

  @Test
  public void fileFsyncNanos() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.fileFsyncNanos()).isEqualTo(0);
    }
  }

  @Test
  public void filePrepareWriteNanos() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.filePrepareWriteNanos()).isEqualTo(0);
    }
  }

  @Test
  public void smallestOutputKeyPrefix() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.smallestOutputKeyPrefix()).isEmpty();
    }
  }

  @Test
  public void largestOutputKeyPrefix() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.largestOutputKeyPrefix()).isEmpty();
    }
  }

  @Test
  public void numSingleDelFallthru() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.numSingleDelFallthru()).isEqualTo(0);
    }
  }

  @Test
  public void numSingleDelMismatch() {
    try (final CompactionJobStats compactionJobStats = new CompactionJobStats()) {
      assertThat(compactionJobStats.numSingleDelMismatch()).isEqualTo(0);
    }
  }
}
