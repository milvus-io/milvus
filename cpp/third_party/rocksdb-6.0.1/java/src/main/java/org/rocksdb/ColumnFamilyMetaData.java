// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Arrays;
import java.util.List;

/**
 * The metadata that describes a column family.
 */
public class ColumnFamilyMetaData {
  private final long size;
  private final long fileCount;
  private final byte[] name;
  private final LevelMetaData[] levels;

  /**
   * Called from JNI C++
   */
  private ColumnFamilyMetaData(
      final long size,
      final long fileCount,
      final byte[] name,
      final LevelMetaData[] levels) {
    this.size = size;
    this.fileCount = fileCount;
    this.name = name;
    this.levels = levels;
  }

  /**
   * The size of this column family in bytes, which is equal to the sum of
   * the file size of its {@link #levels()}.
   *
   * @return the size of this column family
   */
  public long size() {
    return size;
  }

  /**
   * The number of files in this column family.
   *
   * @return the number of files
   */
  public long fileCount() {
    return fileCount;
  }

  /**
   * The name of the column family.
   *
   * @return the name
   */
  public byte[] name() {
    return name;
  }

  /**
   * The metadata of all levels in this column family.
   *
   * @return the levels metadata
   */
  public List<LevelMetaData> levels() {
    return Arrays.asList(levels);
  }
}
