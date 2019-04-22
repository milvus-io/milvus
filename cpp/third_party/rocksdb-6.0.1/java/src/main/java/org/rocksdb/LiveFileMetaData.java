// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * The full set of metadata associated with each SST file.
 */
public class LiveFileMetaData extends SstFileMetaData {
  private final byte[] columnFamilyName;
  private final int level;

  /**
   * Called from JNI C++
   */
  private LiveFileMetaData(
      final byte[] columnFamilyName,
      final int level,
      final String fileName,
      final String path,
      final long size,
      final long smallestSeqno,
      final long largestSeqno,
      final byte[] smallestKey,
      final byte[] largestKey,
      final long numReadsSampled,
      final boolean beingCompacted,
      final long numEntries,
      final long numDeletions) {
    super(fileName, path, size, smallestSeqno, largestSeqno, smallestKey,
        largestKey, numReadsSampled, beingCompacted, numEntries, numDeletions);
    this.columnFamilyName = columnFamilyName;
    this.level = level;
  }

  /**
   * Get the name of the column family.
   *
   * @return the name of the column family
   */
  public byte[] columnFamilyName() {
    return columnFamilyName;
  }

  /**
   * Get the level at which this file resides.
   *
   * @return the level at which the file resides.
   */
  public int level() {
    return level;
  }
}
