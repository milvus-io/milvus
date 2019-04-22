// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

/**
 * The transaction db write policy.
 */
public enum TxnDBWritePolicy {
  /**
   * Write only the committed data.
   */
  WRITE_COMMITTED((byte)0x00),

  /**
   * Write data after the prepare phase of 2pc.
   */
  WRITE_PREPARED((byte)0x1),

  /**
   * Write data before the prepare phase of 2pc.
   */
  WRITE_UNPREPARED((byte)0x2);

  private byte value;

  TxnDBWritePolicy(final byte value) {
    this.value = value;
  }

  /**
   * <p>Returns the byte value of the enumerations value.</p>
   *
   * @return byte representation
   */
  public byte getValue() {
    return value;
  }

  /**
   * <p>Get the TxnDBWritePolicy enumeration value by
   * passing the byte identifier to this method.</p>
   *
   * @param byteIdentifier of TxnDBWritePolicy.
   *
   * @return TxnDBWritePolicy instance.
   *
   * @throws IllegalArgumentException If TxnDBWritePolicy cannot be found for
   *     the provided byteIdentifier
   */
  public static TxnDBWritePolicy getTxnDBWritePolicy(final byte byteIdentifier) {
    for (final TxnDBWritePolicy txnDBWritePolicy : TxnDBWritePolicy.values()) {
      if (txnDBWritePolicy.getValue() == byteIdentifier) {
        return txnDBWritePolicy;
      }
    }

    throw new IllegalArgumentException(
        "Illegal value provided for TxnDBWritePolicy.");
  }
}
