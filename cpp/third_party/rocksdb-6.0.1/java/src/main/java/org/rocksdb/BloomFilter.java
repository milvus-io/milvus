// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Bloom filter policy that uses a bloom filter with approximately
 * the specified number of bits per key.
 *
 * <p>
 * Note: if you are using a custom comparator that ignores some parts
 * of the keys being compared, you must not use this {@code BloomFilter}
 * and must provide your own FilterPolicy that also ignores the
 * corresponding parts of the keys. For example, if the comparator
 * ignores trailing spaces, it would be incorrect to use a
 * FilterPolicy (like {@code BloomFilter}) that does not ignore
 * trailing spaces in keys.</p>
 */
public class BloomFilter extends Filter {

  private static final int DEFAULT_BITS_PER_KEY = 10;
  private static final boolean DEFAULT_MODE = true;

  /**
   * BloomFilter constructor
   *
   * <p>
   * Callers must delete the result after any database that is using the
   * result has been closed.</p>
   */
  public BloomFilter() {
    this(DEFAULT_BITS_PER_KEY, DEFAULT_MODE);
  }

  /**
   * BloomFilter constructor
   *
   * <p>
   * bits_per_key: bits per key in bloom filter. A good value for bits_per_key
   * is 10, which yields a filter with ~ 1% false positive rate.
   * </p>
   * <p>
   * Callers must delete the result after any database that is using the
   * result has been closed.</p>
   *
   * @param bitsPerKey number of bits to use
   */
  public BloomFilter(final int bitsPerKey) {
    this(bitsPerKey, DEFAULT_MODE);
  }

  /**
   * BloomFilter constructor
   *
   * <p>
   * bits_per_key: bits per key in bloom filter. A good value for bits_per_key
   * is 10, which yields a filter with ~ 1% false positive rate.
   * <p><strong>default bits_per_key</strong>: 10</p>
   *
   * <p>use_block_based_builder: use block based filter rather than full filter.
   * If you want to builder full filter, it needs to be set to false.
   * </p>
   * <p><strong>default mode: block based filter</strong></p>
   * <p>
   * Callers must delete the result after any database that is using the
   * result has been closed.</p>
   *
   * @param bitsPerKey number of bits to use
   * @param useBlockBasedMode use block based mode or full filter mode
   */
  public BloomFilter(final int bitsPerKey, final boolean useBlockBasedMode) {
    super(createNewBloomFilter(bitsPerKey, useBlockBasedMode));
  }

  private native static long createNewBloomFilter(final int bitsKeyKey,
      final boolean useBlockBasedMode);
}
