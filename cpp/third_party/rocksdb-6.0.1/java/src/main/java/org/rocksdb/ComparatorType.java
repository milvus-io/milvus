// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

enum ComparatorType {
  JAVA_COMPARATOR((byte)0x0),
  JAVA_DIRECT_COMPARATOR((byte)0x1),
  JAVA_NATIVE_COMPARATOR_WRAPPER((byte)0x2);

  private final byte value;

  ComparatorType(final byte value) {
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
   * <p>Get the ComparatorType enumeration value by
   * passing the byte identifier to this method.</p>
   *
   * @param byteIdentifier of ComparatorType.
   *
   * @return ComparatorType instance.
   *
   * @throws IllegalArgumentException if the comparator type for the byteIdentifier
   *     cannot be found
   */
  static ComparatorType getComparatorType(final byte byteIdentifier) {
    for (final ComparatorType comparatorType : ComparatorType.values()) {
      if (comparatorType.getValue() == byteIdentifier) {
        return comparatorType;
      }
    }

    throw new IllegalArgumentException(
        "Illegal value provided for ComparatorType.");
  }
}
