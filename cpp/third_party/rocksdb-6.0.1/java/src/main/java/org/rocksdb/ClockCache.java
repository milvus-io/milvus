// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Similar to {@link LRUCache}, but based on the CLOCK algorithm with
 * better concurrent performance in some cases
 */
public class ClockCache extends Cache {

  /**
   * Create a new cache with a fixed size capacity.
   *
   * @param capacity The fixed size capacity of the cache
   */
  public ClockCache(final long capacity) {
    super(newClockCache(capacity, -1, false));
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
  public ClockCache(final long capacity, final int numShardBits) {
    super(newClockCache(capacity, numShardBits, false));
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
  public ClockCache(final long capacity, final int numShardBits,
      final boolean strictCapacityLimit) {
    super(newClockCache(capacity, numShardBits, strictCapacityLimit));
  }

  private native static long newClockCache(final long capacity,
      final int numShardBits, final boolean strictCapacityLimit);
  @Override protected final native void disposeInternal(final long handle);
}
