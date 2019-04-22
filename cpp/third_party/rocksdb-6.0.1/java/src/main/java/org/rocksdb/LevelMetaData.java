// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Arrays;
import java.util.List;

/**
 * The metadata that describes a level.
 */
public class LevelMetaData {
  private final int level;
  private final long size;
  private final SstFileMetaData[] files;

  /**
   * Called from JNI C++
   */
  private LevelMetaData(final int level, final long size,
      final SstFileMetaData[] files) {
    this.level = level;
    this.size = size;
    this.files = files;
  }

  /**
   * The level which this meta data describes.
   *
   * @return the level
   */
  public int level() {
    return level;
  }

  /**
   * The size of this level in bytes, which is equal to the sum of
   * the file size of its {@link #files()}.
   *
   * @return the size
   */
  public long size() {
    return size;
  }

  /**
   * The metadata of all sst files in this level.
   *
   * @return the metadata of the files
   */
  public List<SstFileMetaData> files() {
    return Arrays.asList(files);
  }
}
