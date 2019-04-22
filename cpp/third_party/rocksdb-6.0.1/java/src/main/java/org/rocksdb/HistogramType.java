// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public enum HistogramType {

  DB_GET((byte) 0x0),

  DB_WRITE((byte) 0x1),

  COMPACTION_TIME((byte) 0x2),

  SUBCOMPACTION_SETUP_TIME((byte) 0x3),

  TABLE_SYNC_MICROS((byte) 0x4),

  COMPACTION_OUTFILE_SYNC_MICROS((byte) 0x5),

  WAL_FILE_SYNC_MICROS((byte) 0x6),

  MANIFEST_FILE_SYNC_MICROS((byte) 0x7),

  /**
   * TIME SPENT IN IO DURING TABLE OPEN.
   */
  TABLE_OPEN_IO_MICROS((byte) 0x8),

  DB_MULTIGET((byte) 0x9),

  READ_BLOCK_COMPACTION_MICROS((byte) 0xA),

  READ_BLOCK_GET_MICROS((byte) 0xB),

  WRITE_RAW_BLOCK_MICROS((byte) 0xC),

  STALL_L0_SLOWDOWN_COUNT((byte) 0xD),

  STALL_MEMTABLE_COMPACTION_COUNT((byte) 0xE),

  STALL_L0_NUM_FILES_COUNT((byte) 0xF),

  HARD_RATE_LIMIT_DELAY_COUNT((byte) 0x10),

  SOFT_RATE_LIMIT_DELAY_COUNT((byte) 0x11),

  NUM_FILES_IN_SINGLE_COMPACTION((byte) 0x12),

  DB_SEEK((byte) 0x13),

  WRITE_STALL((byte) 0x14),

  SST_READ_MICROS((byte) 0x15),

  /**
   * The number of subcompactions actually scheduled during a compaction.
   */
  NUM_SUBCOMPACTIONS_SCHEDULED((byte) 0x16),

  /**
   * Value size distribution in each operation.
   */
  BYTES_PER_READ((byte) 0x17),
  BYTES_PER_WRITE((byte) 0x18),
  BYTES_PER_MULTIGET((byte) 0x19),

  /**
   * number of bytes compressed.
   */
  BYTES_COMPRESSED((byte) 0x1A),

  /**
   * number of bytes decompressed.
   *
   * number of bytes is when uncompressed; i.e. before/after respectively
   */
  BYTES_DECOMPRESSED((byte) 0x1B),

  COMPRESSION_TIMES_NANOS((byte) 0x1C),

  DECOMPRESSION_TIMES_NANOS((byte) 0x1D),

  READ_NUM_MERGE_OPERANDS((byte) 0x1E),

  /**
   * Time spent flushing memtable to disk.
   */
  FLUSH_TIME((byte) 0x20),

  /**
   * Size of keys written to BlobDB.
   */
  BLOB_DB_KEY_SIZE((byte) 0x21),

  /**
   * Size of values written to BlobDB.
   */
  BLOB_DB_VALUE_SIZE((byte) 0x22),

  /**
   * BlobDB Put/PutWithTTL/PutUntil/Write latency.
   */
  BLOB_DB_WRITE_MICROS((byte) 0x23),

  /**
   * BlobDB Get lagency.
   */
  BLOB_DB_GET_MICROS((byte) 0x24),

  /**
   * BlobDB MultiGet latency.
   */
  BLOB_DB_MULTIGET_MICROS((byte) 0x25),

  /**
   * BlobDB Seek/SeekToFirst/SeekToLast/SeekForPrev latency.
   */
  BLOB_DB_SEEK_MICROS((byte) 0x26),

  /**
   * BlobDB Next latency.
   */
  BLOB_DB_NEXT_MICROS((byte) 0x27),

  /**
   * BlobDB Prev latency.
   */
  BLOB_DB_PREV_MICROS((byte) 0x28),

  /**
   * Blob file write latency.
   */
  BLOB_DB_BLOB_FILE_WRITE_MICROS((byte) 0x29),

  /**
   * Blob file read latency.
   */
  BLOB_DB_BLOB_FILE_READ_MICROS((byte) 0x2A),

  /**
   * Blob file sync latency.
   */
  BLOB_DB_BLOB_FILE_SYNC_MICROS((byte) 0x2B),

  /**
   * BlobDB garbage collection time.
   */
  BLOB_DB_GC_MICROS((byte) 0x2C),

  /**
   * BlobDB compression time.
   */
  BLOB_DB_COMPRESSION_MICROS((byte) 0x2D),

  /**
   * BlobDB decompression time.
   */
  BLOB_DB_DECOMPRESSION_MICROS((byte) 0x2E),

  // 0x1F for backwards compatibility on current minor version.
  HISTOGRAM_ENUM_MAX((byte) 0x1F);

  private final byte value;

  HistogramType(final byte value) {
    this.value = value;
  }

  /**
   * @deprecated
   * Exposes internal value of native enum mappings. This method will be marked private in the
   * next major release.
   */
  @Deprecated
  public byte getValue() {
    return value;
  }
}
