// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Compaction Priorities
 */
public enum CompactionPriority {

  /**
   * Slightly Prioritize larger files by size compensated by #deletes
   */
  ByCompensatedSize((byte)0x0),

  /**
   * First compact files whose data's latest update time is oldest.
   * Try this if you only update some hot keys in small ranges.
   */
  OldestLargestSeqFirst((byte)0x1),

  /**
   * First compact files whose range hasn't been compacted to the next level
   * for the longest. If your updates are random across the key space,
   * write amplification is slightly better with this option.
   */
  OldestSmallestSeqFirst((byte)0x2),

  /**
   * First compact files whose ratio between overlapping size in next level
   * and its size is the smallest. It in many cases can optimize write
   * amplification.
   */
  MinOverlappingRatio((byte)0x3);


  private final byte value;

  CompactionPriority(final byte value) {
    this.value = value;
  }

  /**
   * Returns the byte value of the enumerations value
   *
   * @return byte representation
   */
  public byte getValue() {
    return value;
  }

  /**
   * Get CompactionPriority by byte value.
   *
   * @param value byte representation of CompactionPriority.
   *
   * @return {@link org.rocksdb.CompactionPriority} instance or null.
   * @throws java.lang.IllegalArgumentException if an invalid
   *     value is provided.
   */
  public static CompactionPriority getCompactionPriority(final byte value) {
    for (final CompactionPriority compactionPriority :
        CompactionPriority.values()) {
      if (compactionPriority.getValue() == value){
        return compactionPriority;
      }
    }
    throw new IllegalArgumentException(
        "Illegal value provided for CompactionPriority.");
  }
}
