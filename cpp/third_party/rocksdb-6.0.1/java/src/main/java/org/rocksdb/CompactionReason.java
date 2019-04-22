// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public enum CompactionReason {
  kUnknown((byte)0x0),

  /**
   * [Level] number of L0 files &gt; level0_file_num_compaction_trigger
   */
  kLevelL0FilesNum((byte)0x1),

  /**
   * [Level] total size of level &gt; MaxBytesForLevel()
   */
  kLevelMaxLevelSize((byte)0x2),

  /**
   * [Universal] Compacting for size amplification
   */
  kUniversalSizeAmplification((byte)0x3),

  /**
   * [Universal] Compacting for size ratio
   */
  kUniversalSizeRatio((byte)0x4),

  /**
   * [Universal] number of sorted runs &gt; level0_file_num_compaction_trigger
   */
  kUniversalSortedRunNum((byte)0x5),

  /**
   * [FIFO] total size &gt; max_table_files_size
   */
  kFIFOMaxSize((byte)0x6),

  /**
   * [FIFO] reduce number of files.
   */
  kFIFOReduceNumFiles((byte)0x7),

  /**
   * [FIFO] files with creation time &lt; (current_time - interval)
   */
  kFIFOTtl((byte)0x8),

  /**
   * Manual compaction
   */
  kManualCompaction((byte)0x9),

  /**
   * DB::SuggestCompactRange() marked files for compaction
   */
  kFilesMarkedForCompaction((byte)0x10),

  /**
   * [Level] Automatic compaction within bottommost level to cleanup duplicate
   * versions of same user key, usually due to a released snapshot.
   */
  kBottommostFiles((byte)0x0A),

  /**
   * Compaction based on TTL
   */
  kTtl((byte)0x0B),

  /**
   * According to the comments in flush_job.cc, RocksDB treats flush as
   * a level 0 compaction in internal stats.
   */
  kFlush((byte)0x0C),

  /**
   * Compaction caused by external sst file ingestion
   */
  kExternalSstIngestion((byte)0x0D);

  private final byte value;

  CompactionReason(final byte value) {
    this.value = value;
  }

  /**
   * Get the internal representation value.
   *
   * @return the internal representation value
   */
  byte getValue() {
    return value;
  }

  /**
   * Get the CompactionReason from the internal representation value.
   *
   * @return the compaction reason.
   *
   * @throws IllegalArgumentException if the value is unknown.
   */
  static CompactionReason fromValue(final byte value) {
    for (final CompactionReason compactionReason : CompactionReason.values()) {
      if(compactionReason.value == value) {
        return compactionReason;
      }
    }

    throw new IllegalArgumentException(
        "Illegal value provided for CompactionReason: " + value);
  }
}
