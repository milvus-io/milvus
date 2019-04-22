// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public enum WalProcessingOption {
  /**
   * Continue processing as usual.
   */
  CONTINUE_PROCESSING((byte)0x0),

  /**
   * Ignore the current record but continue processing of log(s).
   */
  IGNORE_CURRENT_RECORD((byte)0x1),

  /**
   * Stop replay of logs and discard logs.
   * Logs won't be replayed on subsequent recovery.
   */
  STOP_REPLAY((byte)0x2),

  /**
   * Corrupted record detected by filter.
   */
  CORRUPTED_RECORD((byte)0x3);

  private final byte value;

  WalProcessingOption(final byte value) {
    this.value = value;
  }

  /**
   * Get the internal representation.
   *
   * @return the internal representation.
   */
  byte getValue() {
    return value;
  }

  public static WalProcessingOption fromValue(final byte value) {
    for (final WalProcessingOption walProcessingOption : WalProcessingOption.values()) {
      if (walProcessingOption.value == value) {
        return walProcessingOption;
      }
    }
    throw new IllegalArgumentException(
        "Illegal value provided for WalProcessingOption: " + value);
  }
}
