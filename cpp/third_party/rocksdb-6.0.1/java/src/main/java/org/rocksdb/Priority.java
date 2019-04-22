// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * The Thread Pool priority.
 */
public enum Priority {
  BOTTOM((byte) 0x0),
  LOW((byte) 0x1),
  HIGH((byte)0x2),
  TOTAL((byte)0x3);

  private final byte value;

  Priority(final byte value) {
    this.value = value;
  }

  /**
   * <p>Returns the byte value of the enumerations value.</p>
   *
   * @return byte representation
   */
  byte getValue() {
    return value;
  }

  /**
   * Get Priority by byte value.
   *
   * @param value byte representation of Priority.
   *
   * @return {@link org.rocksdb.Priority} instance.
   * @throws java.lang.IllegalArgumentException if an invalid
   *     value is provided.
   */
  static Priority getPriority(final byte value) {
    for (final Priority priority : Priority.values()) {
      if (priority.getValue() == value){
        return priority;
      }
    }
    throw new IllegalArgumentException("Illegal value provided for Priority.");
  }
}
