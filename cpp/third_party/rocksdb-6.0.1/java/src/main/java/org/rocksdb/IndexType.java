// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * IndexType used in conjunction with BlockBasedTable.
 */
public enum IndexType {
  /**
   * A space efficient index block that is optimized for
   * binary-search-based index.
   */
  kBinarySearch((byte) 0),
  /**
   * The hash index, if enabled, will do the hash lookup when
   * {@code Options.prefix_extractor} is provided.
   */
  kHashSearch((byte) 1),
  /**
   * A two-level index implementation. Both levels are binary search indexes.
   */
  kTwoLevelIndexSearch((byte) 2);

  /**
   * Returns the byte value of the enumerations value
   *
   * @return byte representation
   */
  public byte getValue() {
    return value_;
  }

  IndexType(byte value) {
    value_ = value;
  }

  private final byte value_;
}
