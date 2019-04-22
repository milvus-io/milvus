// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * The type of a thread.
 */
public enum ThreadType {
  /**
   * RocksDB BG thread in high-pri thread pool.
   */
  HIGH_PRIORITY((byte)0x0),

  /**
   * RocksDB BG thread in low-pri thread pool.
   */
  LOW_PRIORITY((byte)0x1),

  /**
   * User thread (Non-RocksDB BG thread).
   */
  USER((byte)0x2),

  /**
   * RocksDB BG thread in bottom-pri thread pool
   */
  BOTTOM_PRIORITY((byte)0x3);

  private final byte value;

  ThreadType(final byte value) {
    this.value = value;
  }

  /**
   * Get the internal representation value.
   *
   * @return the internal representation value.
   */
  byte getValue() {
    return value;
  }

  /**
   * Get the Thread type from the internal representation value.
   *
   * @param value the internal representation value.
   *
   * @return the thread type
   *
   * @throws IllegalArgumentException if the value does not match a ThreadType
   */
  static ThreadType fromValue(final byte value)
      throws IllegalArgumentException {
    for (final ThreadType threadType : ThreadType.values()) {
      if (threadType.value == value) {
        return threadType;
      }
    }
    throw new IllegalArgumentException("Unknown value for ThreadType: " + value);
  }
}
