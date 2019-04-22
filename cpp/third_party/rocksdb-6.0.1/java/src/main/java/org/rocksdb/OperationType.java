// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * The type used to refer to a thread operation.
 *
 * A thread operation describes high-level action of a thread,
 * examples include compaction and flush.
 */
public enum OperationType {
  OP_UNKNOWN((byte)0x0),
  OP_COMPACTION((byte)0x1),
  OP_FLUSH((byte)0x2);

  private final byte value;

  OperationType(final byte value) {
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
   * Get the Operation type from the internal representation value.
   *
   * @param value the internal representation value.
   *
   * @return the operation type
   *
   * @throws IllegalArgumentException if the value does not match
   *     an OperationType
   */
  static OperationType fromValue(final byte value)
      throws IllegalArgumentException {
    for (final OperationType threadType : OperationType.values()) {
      if (threadType.value == value) {
        return threadType;
      }
    }
    throw new IllegalArgumentException(
        "Unknown value for OperationType: " + value);
  }
}
