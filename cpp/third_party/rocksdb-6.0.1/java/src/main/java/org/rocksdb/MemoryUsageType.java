// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * MemoryUsageType
 *
 * <p>The value will be used as a key to indicate the type of memory usage
 * described</p>
 */
public enum MemoryUsageType {
  /**
   * Memory usage of all the mem-tables.
   */
  kMemTableTotal((byte) 0),
  /**
   * Memory usage of those un-flushed mem-tables.
   */
  kMemTableUnFlushed((byte) 1),
  /**
   * Memory usage of all the table readers.
   */
  kTableReadersTotal((byte) 2),
  /**
   * Memory usage by Cache.
   */
  kCacheTotal((byte) 3),
  /**
   * Max usage types - copied to keep 1:1 with native.
   */
  kNumUsageTypes((byte) 4);

  /**
   * Returns the byte value of the enumerations value
   *
   * @return byte representation
   */
  public byte getValue() {
    return value_;
  }

  /**
   * <p>Get the MemoryUsageType enumeration value by
   * passing the byte identifier to this method.</p>
   *
   * @param byteIdentifier of MemoryUsageType.
   *
   * @return MemoryUsageType instance.
   *
   * @throws IllegalArgumentException if the usage type for the byteIdentifier
   *     cannot be found
   */
  public static MemoryUsageType getMemoryUsageType(final byte byteIdentifier) {
    for (final MemoryUsageType memoryUsageType : MemoryUsageType.values()) {
      if (memoryUsageType.getValue() == byteIdentifier) {
        return memoryUsageType;
      }
    }

    throw new IllegalArgumentException(
        "Illegal value provided for MemoryUsageType.");
  }

  MemoryUsageType(byte value) {
    value_ = value;
  }

  private final byte value_;
}
