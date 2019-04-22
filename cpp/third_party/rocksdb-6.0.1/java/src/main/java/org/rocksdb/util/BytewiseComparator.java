// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb.util;

import org.rocksdb.*;

import java.nio.ByteBuffer;

/**
 * This is a Java Native implementation of the C++
 * equivalent BytewiseComparatorImpl using {@link Slice}
 *
 * The performance of Comparators implemented in Java is always
 * less than their C++ counterparts due to the bridging overhead,
 * as such you likely don't want to use this apart from benchmarking
 * and you most likely instead wanted
 * {@link org.rocksdb.BuiltinComparator#BYTEWISE_COMPARATOR}
 */
public class BytewiseComparator extends Comparator {

  public BytewiseComparator(final ComparatorOptions copt) {
    super(copt);
  }

  @Override
  public String name() {
    return "rocksdb.java.BytewiseComparator";
  }

  @Override
  public int compare(final Slice a, final Slice b) {
    return compare(a.data(), b.data());
  }

  @Override
  public String findShortestSeparator(final String start,
                                      final Slice limit) {
    final byte[] startBytes = start.getBytes();
    final byte[] limitBytes = limit.data();

    // Find length of common prefix
    final int min_length = Math.min(startBytes.length, limit.size());
    int diff_index = 0;
    while ((diff_index < min_length) &&
        (startBytes[diff_index] == limitBytes[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      final byte diff_byte = startBytes[diff_index];
      if(diff_byte < 0xff && diff_byte + 1 < limitBytes[diff_index]) {
        final byte shortest[] = new byte[diff_index + 1];
        System.arraycopy(startBytes, 0, shortest, 0, diff_index + 1);
        shortest[diff_index]++;
        assert(compare(shortest, limitBytes) < 0);
        return new String(shortest);
      }
    }

    return null;
  }

  private static int compare(final byte[] a, final byte[] b) {
    return ByteBuffer.wrap(a).compareTo(ByteBuffer.wrap(b));
  }

  @Override
  public String findShortSuccessor(final String key) {
    final byte[] keyBytes = key.getBytes();

    // Find first character that can be incremented
    final int n = keyBytes.length;
    for (int i = 0; i < n; i++) {
      final byte byt = keyBytes[i];
      if (byt != 0xff) {
        final byte shortSuccessor[] = new byte[i + 1];
        System.arraycopy(keyBytes, 0, shortSuccessor, 0, i + 1);
        shortSuccessor[i]++;
        return new String(shortSuccessor);
      }
    }
    // *key is a run of 0xffs.  Leave it alone.

    return null;
  }
}
