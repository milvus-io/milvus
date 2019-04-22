// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Least Recently Used Cache
 */
public class LRUCache extends Cache {

  /**
   * Create a new cache with a fixed size capacity
   *
   * @param capacity The fixed size capacity of the cache
   */
  public LRUCache(final long capacity) {
    this(capacity, -1, false, 0.0);
  }

  /**
   * Create a new cache with a fixed size capacity. The cache is sharded
   * to 2^numShardBits shards, by hash of the key. The total capacity
   * is divided and evenly assigned to each shard.
   * numShardBits = -1 means it is automatically determined: every shard
   * will be at least 512KB and number of shard bits will not exceed 6.
   *
   * @param capacity The fixed size capacity of the cache
   * @param numShardBits The cache is sharded to 2^numShardBits shards,
   *     by hash of the key
   */
  public LRUCache(final long capacity, final int numShardBits) {
    super(newLRUCache(capacity, numShardBits, false,0.0));
  }

  /**
   * Create a new cache with a fixed size capacity. The cache is sharded
   * to 2^numShardBits shards, by hash of the key. The total capacity
   * is divided and evenly assigned to each shard. If strictCapacityLimit
   * is set, insert to the cache will fail when cache is full.
   * numShardBits = -1 means it is automatically determined: every shard
   * will be at least 512KB and number of shard bits will not exceed 6.
   *
   * @param capacity The fixed size capacity of the cache
   * @param numShardBits The cache is sharded to 2^numShardBits shards,
   *     by hash of the key
   * @param strictCapacityLimit insert to the cache will fail when cache is full
   */
  public LRUCache(final long capacity, final int numShardBits,
                  final boolean strictCapacityLimit) {
    super(newLRUCache(capacity, numShardBits, strictCapacityLimit,0.0));
  }

  /**
   * Create a new cache with a fixed size capacity. The cache is sharded
   * to 2^numShardBits shards, by hash of the key. The total capacity
   * is divided and evenly assigned to each shard. If strictCapacityLimit
   * is set, insert to the cache will fail when cache is full. User can also
   * set percentage of the cache reserves for high priority entries via
   * highPriPoolRatio.
   * numShardBits = -1 means it is automatically determined: every shard
   * will be at least 512KB and number of shard bits will not exceed 6.
   *
   * @param capacity The fixed size capacity of the cache
   * @param numShardBits The cache is sharded to 2^numShardBits shards,
   *     by hash of the key
   * @param strictCapacityLimit insert to the cache will fail when cache is full
   * @param highPriPoolRatio percentage of the cache reserves for high priority
   *     entries
   */
  public LRUCache(final long capacity, final int numShardBits,
      final boolean strictCapacityLimit, final double highPriPoolRatio) {
    super(newLRUCache(capacity, numShardBits, strictCapacityLimit,
        highPriPoolRatio));
  }

  private native static long newLRUCache(final long capacity,
      final int numShardBits, final boolean strictCapacityLimit,
      final double highPriPoolRatio);
  @Override protected final native void disposeInternal(final long handle);
}
