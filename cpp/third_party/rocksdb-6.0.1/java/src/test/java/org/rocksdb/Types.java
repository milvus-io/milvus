// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Simple type conversion methods
 * for use in tests
 */
public class Types {

  /**
   * Convert first 4 bytes of a byte array to an int
   *
   * @param data The byte array
   *
   * @return An integer
   */
  public static int byteToInt(final byte data[]) {
    return (data[0] & 0xff) |
        ((data[1] & 0xff) << 8) |
        ((data[2] & 0xff) << 16) |
        ((data[3] & 0xff) << 24);
  }

  /**
   * Convert an int to 4 bytes
   *
   * @param v The int
   *
   * @return A byte array containing 4 bytes
   */
  public static byte[] intToByte(final int v) {
    return new byte[] {
        (byte)((v >>> 0) & 0xff),
        (byte)((v >>> 8) & 0xff),
        (byte)((v >>> 16) & 0xff),
        (byte)((v >>> 24) & 0xff)
    };
  }
}
