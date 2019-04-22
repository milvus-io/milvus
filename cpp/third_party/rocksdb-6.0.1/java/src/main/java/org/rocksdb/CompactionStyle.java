// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.List;

/**
 * Enum CompactionStyle
 *
 * RocksDB supports different styles of compaction. Available
 * compaction styles can be chosen using this enumeration.
 *
 * <ol>
 *   <li><strong>LEVEL</strong> - Level based Compaction style</li>
 *   <li><strong>UNIVERSAL</strong> - Universal Compaction Style is a
 *   compaction style, targeting the use cases requiring lower write
 *   amplification, trading off read amplification and space
 *   amplification.</li>
 *   <li><strong>FIFO</strong> - FIFO compaction style is the simplest
 *   compaction strategy. It is suited for keeping event log data with
 *   very low overhead (query log for example). It periodically deletes
 *   the old data, so it's basically a TTL compaction style.</li>
 *   <li><strong>NONE</strong> - Disable background compaction.
 *   Compaction jobs are submitted
 *   {@link RocksDB#compactFiles(CompactionOptions, ColumnFamilyHandle, List, int, int, CompactionJobInfo)} ()}.</li>
 * </ol>
 *
 * @see <a
 * href="https://github.com/facebook/rocksdb/wiki/Universal-Compaction">
 * Universal Compaction</a>
 * @see <a
 * href="https://github.com/facebook/rocksdb/wiki/FIFO-compaction-style">
 * FIFO Compaction</a>
 */
public enum CompactionStyle {
  LEVEL((byte) 0x0),
  UNIVERSAL((byte) 0x1),
  FIFO((byte) 0x2),
  NONE((byte) 0x3);

  private final byte value;

  CompactionStyle(final byte value) {
    this.value = value;
  }

  /**
   * Get the internal representation value.
   *
   * @return the internal representation value.
   */
  //TODO(AR) should be made package-private
  public byte getValue() {
    return value;
  }

  /**
   * Get the Compaction style from the internal representation value.
   *
   * @param value the internal representation value.
   *
   * @return the Compaction style
   *
   * @throws IllegalArgumentException if the value does not match a
   *     CompactionStyle
   */
  static CompactionStyle fromValue(final byte value)
      throws IllegalArgumentException {
    for (final CompactionStyle compactionStyle : CompactionStyle.values()) {
      if (compactionStyle.value == value) {
        return compactionStyle;
      }
    }
    throw new IllegalArgumentException("Unknown value for CompactionStyle: "
        + value);
  }
}
