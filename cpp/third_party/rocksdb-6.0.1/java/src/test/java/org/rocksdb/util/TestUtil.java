// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

import org.rocksdb.CompactionPriority;
import org.rocksdb.Options;
import org.rocksdb.WALRecoveryMode;

import java.util.Random;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * General test utilities.
 */
public class TestUtil {

  /**
   * Get the options for log iteration tests.
   *
   * @return the options
   */
  public static Options optionsForLogIterTest() {
    return defaultOptions()
        .setCreateIfMissing(true)
        .setWalTtlSeconds(1000);
  }

  /**
   * Get the default options.
   *
   * @return the options
   */
  public static Options defaultOptions() {
      return new Options()
          .setWriteBufferSize(4090 * 4096)
          .setTargetFileSizeBase(2 * 1024 * 1024)
          .setMaxBytesForLevelBase(10 * 1024 * 1024)
          .setMaxOpenFiles(5000)
          .setWalRecoveryMode(WALRecoveryMode.TolerateCorruptedTailRecords)
          .setCompactionPriority(CompactionPriority.ByCompensatedSize);
  }

  private static final Random random = new Random();

  /**
   * Generate a random string of bytes.
   *
   * @param len the length of the string to generate.
   *
   * @return the random string of bytes
   */
  public static byte[] dummyString(final int len) {
    final byte[] str = new byte[len];
    random.nextBytes(str);
    return str;
  }

  /**
   * Convert a UTF-8 String to a byte array.
   *
   * @param str the string
   *
   * @return the byte array.
   */
  public static byte[] u(final String str) {
    return str.getBytes(UTF_8);
  }
}
