// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Random;

/**
 * Helper class to get the appropriate Random class instance dependent
 * on the current platform architecture (32bit vs 64bit)
 */
public class PlatformRandomHelper {
    /**
     * Determine if OS is 32-Bit/64-Bit
     *
     * @return boolean value indicating if operating system is 64 Bit.
     */
    public static boolean isOs64Bit(){
      final boolean is64Bit;
      if (System.getProperty("os.name").contains("Windows")) {
        is64Bit = (System.getenv("ProgramFiles(x86)") != null);
      } else {
        is64Bit = (System.getProperty("os.arch").contains("64"));
      }
      return is64Bit;
    }

    /**
     * Factory to get a platform specific Random instance
     *
     * @return {@link java.util.Random} instance.
     */
    public static Random getPlatformSpecificRandomFactory(){
      if (isOs64Bit()) {
        return new Random();
      }
      return new Random32Bit();
    }

    /**
     * Random32Bit is a class which overrides {@code nextLong} to
     * provide random numbers which fit in size_t. This workaround
     * is necessary because there is no unsigned_int &lt; Java 8
     */
    private static class Random32Bit extends Random {
      @Override
      public long nextLong(){
      return this.nextInt(Integer.MAX_VALUE);
    }
    }

    /**
     * Utility class constructor
     */
    private PlatformRandomHelper() { }
}
