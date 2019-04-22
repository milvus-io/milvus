// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public enum WalFileType {
  /**
   * Indicates that WAL file is in archive directory. WAL files are moved from
   * the main db directory to archive directory once they are not live and stay
   * there until cleaned up. Files are cleaned depending on archive size
   * (Options::WAL_size_limit_MB) and time since last cleaning
   * (Options::WAL_ttl_seconds).
   */
  kArchivedLogFile((byte)0x0),

  /**
   * Indicates that WAL file is live and resides in the main db directory
   */
  kAliveLogFile((byte)0x1);

  private final byte value;

  WalFileType(final byte value) {
    this.value = value;
  }

  /**
   * Get the internal representation value.
   *
   * @return the internal representation value
   */
  byte getValue() {
    return value;
  }

  /**
   * Get the WalFileType from the internal representation value.
   *
   * @return the wal file type.
   *
   * @throws IllegalArgumentException if the value is unknown.
   */
  static WalFileType fromValue(final byte value) {
    for (final WalFileType walFileType : WalFileType.values()) {
      if(walFileType.value == value) {
        return walFileType;
      }
    }

    throw new IllegalArgumentException(
        "Illegal value provided for WalFileType: " + value);
  }
}
