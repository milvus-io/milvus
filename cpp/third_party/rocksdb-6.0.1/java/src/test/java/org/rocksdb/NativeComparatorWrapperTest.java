// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.*;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;

public class NativeComparatorWrapperTest {

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  private static final Random random = new Random();

  @Test
  public void rountrip() throws RocksDBException {
    final String dbPath = dbFolder.getRoot().getAbsolutePath();
    final int ITERATIONS = 1_000;

    final String[] storedKeys = new String[ITERATIONS];
    try (final NativeStringComparatorWrapper comparator = new NativeStringComparatorWrapper();
        final Options opt = new Options()
        .setCreateIfMissing(true)
        .setComparator(comparator)) {

      // store random integer keys
      try (final RocksDB db = RocksDB.open(opt, dbPath)) {
        for (int i = 0; i < ITERATIONS; i++) {
          final String strKey = randomString();
          final byte key[] = strKey.getBytes();
          // does key already exist (avoid duplicates)
          if (i > 0 && db.get(key) != null) {
            i--; // generate a different key
          } else {
            db.put(key, "value".getBytes());
            storedKeys[i] = strKey;
          }
        }
      }

      // sort the stored keys into ascending alpha-numeric order
      Arrays.sort(storedKeys, new Comparator<String>() {
        @Override
        public int compare(final String o1, final String o2) {
          return o1.compareTo(o2);
        }
      });

      // re-open db and read from start to end
      // string keys should be in ascending
      // order
      try (final RocksDB db = RocksDB.open(opt, dbPath);
           final RocksIterator it = db.newIterator()) {
        int count = 0;
        for (it.seekToFirst(); it.isValid(); it.next()) {
          final String strKey = new String(it.key());
          assertEquals(storedKeys[count++], strKey);
        }
      }
    }
  }

  private String randomString() {
    final char[] chars = new char[12];
    for(int i = 0; i < 12; i++) {
      final int letterCode = random.nextInt(24);
      final char letter = (char) (((int) 'a') + letterCode);
      chars[i] = letter;
    }
    return String.copyValueOf(chars);
  }

  public static class NativeStringComparatorWrapper
      extends NativeComparatorWrapper {

    @Override
    protected long initializeNative(final long... nativeParameterHandles) {
      return newStringComparator();
    }

    private native long newStringComparator();
  }
}
