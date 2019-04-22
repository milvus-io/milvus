// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;


/**
 * DataBlockIndexType used in conjunction with BlockBasedTable.
 */
public enum DataBlockIndexType {
  /**
   * traditional block type
   */
  kDataBlockBinarySearch((byte)0x0),

  /**
   * additional hash index
   */
  kDataBlockBinaryAndHash((byte)0x1);

  private final byte value;

  DataBlockIndexType(final byte value) {
    this.value = value;
  }

  byte getValue() {
    return value;
  }
}
