// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.rocksdb.Types.byteToInt;
import static org.rocksdb.Types.intToByte;

/**
 * Abstract tests for both Comparator and DirectComparator
 */
public abstract class AbstractComparatorTest {

  /**
   * Get a comparator which will expect Integer keys
   * and determine an ascending order
   *
   * @return An integer ascending order key comparator
   */
  public abstract AbstractComparator getAscendingIntKeyComparator();

  /**
   * Test which stores random keys into the database
   * using an @see getAscendingIntKeyComparator
   * it then checks that these keys are read back in
   * ascending order
   *
   * @param db_path A path where we can store database
   *                files temporarily
   *
   * @throws java.io.IOException if IO error happens.
   */
  public void testRoundtrip(final Path db_path) throws IOException,
      RocksDBException {
    try (final AbstractComparator comparator = getAscendingIntKeyComparator();
         final Options opt = new Options()
             .setCreateIfMissing(true)
             .setComparator(comparator)) {

      // store 10,000 random integer keys
      final int ITERATIONS = 10000;
      try (final RocksDB db = RocksDB.open(opt, db_path.toString())) {
        final Random random = new Random();
        for (int i = 0; i < ITERATIONS; i++) {
          final byte key[] = intToByte(random.nextInt());
          // does key already exist (avoid duplicates)
          if (i > 0 && db.get(key) != null) {
            i--; // generate a different key
          } else {
            db.put(key, "value".getBytes());
          }
        }
      }

      // re-open db and read from start to end
      // integer keys should be in ascending
      // order as defined by SimpleIntComparator
      try (final RocksDB db = RocksDB.open(opt, db_path.toString());
           final RocksIterator it = db.newIterator()) {
        it.seekToFirst();
        int lastKey = Integer.MIN_VALUE;
        int count = 0;
        for (it.seekToFirst(); it.isValid(); it.next()) {
          final int thisKey = byteToInt(it.key());
          assertThat(thisKey).isGreaterThan(lastKey);
          lastKey = thisKey;
          count++;
        }
        assertThat(count).isEqualTo(ITERATIONS);
      }
    }
  }

  /**
   * Test which stores random keys into a column family
   * in the database
   * using an @see getAscendingIntKeyComparator
   * it then checks that these keys are read back in
   * ascending order
   *
   * @param db_path A path where we can store database
   *                files temporarily
   *
   * @throws java.io.IOException if IO error happens.
   */
  public void testRoundtripCf(final Path db_path) throws IOException,
      RocksDBException {

    try(final AbstractComparator comparator = getAscendingIntKeyComparator()) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
          new ColumnFamilyDescriptor("new_cf".getBytes(),
              new ColumnFamilyOptions().setComparator(comparator))
      );

      final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

      try (final DBOptions opt = new DBOptions().
          setCreateIfMissing(true).
          setCreateMissingColumnFamilies(true)) {

        // store 10,000 random integer keys
        final int ITERATIONS = 10000;

        try (final RocksDB db = RocksDB.open(opt, db_path.toString(),
            cfDescriptors, cfHandles)) {
          try {
            assertThat(cfDescriptors.size()).isEqualTo(2);
            assertThat(cfHandles.size()).isEqualTo(2);

            final Random random = new Random();
            for (int i = 0; i < ITERATIONS; i++) {
              final byte key[] = intToByte(random.nextInt());
              if (i > 0 && db.get(cfHandles.get(1), key) != null) {
                // does key already exist (avoid duplicates)
                i--; // generate a different key
              } else {
                db.put(cfHandles.get(1), key, "value".getBytes());
              }
            }
          } finally {
            for (final ColumnFamilyHandle handle : cfHandles) {
              handle.close();
            }
          }
          cfHandles.clear();
        }

        // re-open db and read from start to end
        // integer keys should be in ascending
        // order as defined by SimpleIntComparator
        try (final RocksDB db = RocksDB.open(opt, db_path.toString(),
            cfDescriptors, cfHandles);
             final RocksIterator it = db.newIterator(cfHandles.get(1))) {
          try {
            assertThat(cfDescriptors.size()).isEqualTo(2);
            assertThat(cfHandles.size()).isEqualTo(2);

            it.seekToFirst();
            int lastKey = Integer.MIN_VALUE;
            int count = 0;
            for (it.seekToFirst(); it.isValid(); it.next()) {
              final int thisKey = byteToInt(it.key());
              assertThat(thisKey).isGreaterThan(lastKey);
              lastKey = thisKey;
              count++;
            }

            assertThat(count).isEqualTo(ITERATIONS);

          } finally {
            for (final ColumnFamilyHandle handle : cfHandles) {
              handle.close();
            }
          }
          cfHandles.clear();
        }
      }
    }
  }

  /**
   * Compares integer keys
   * so that they are in ascending order
   *
   * @param a 4-bytes representing an integer key
   * @param b 4-bytes representing an integer key
   *
   * @return negative if a &lt; b, 0 if a == b, positive otherwise
   */
  protected final int compareIntKeys(final byte[] a, final byte[] b) {

    final int iA = byteToInt(a);
    final int iB = byteToInt(b);

    // protect against int key calculation overflow
    final double diff = (double)iA - iB;
    final int result;
    if (diff < Integer.MIN_VALUE) {
      result = Integer.MIN_VALUE;
    } else if(diff > Integer.MAX_VALUE) {
      result = Integer.MAX_VALUE;
    } else {
      result = (int)diff;
    }

    return result;
  }
}
