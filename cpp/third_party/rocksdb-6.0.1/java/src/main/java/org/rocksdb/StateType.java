// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * The type used to refer to a thread state.
 *
 * A state describes lower-level action of a thread
 * such as reading / writing a file or waiting for a mutex.
 */
public enum StateType {
  STATE_UNKNOWN((byte)0x0),
  STATE_MUTEX_WAIT((byte)0x1);

  private final byte value;

  StateType(final byte value) {
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
   * Get the State type from the internal representation value.
   *
   * @param value the internal representation value.
   *
   * @return the state type
   *
   * @throws IllegalArgumentException if the value does not match
   *     a StateType
   */
  static StateType fromValue(final byte value)
      throws IllegalArgumentException {
    for (final StateType threadType : StateType.values()) {
      if (threadType.value == value) {
        return threadType;
      }
    }
    throw new IllegalArgumentException(
        "Unknown value for StateType: " + value);
  }
}
