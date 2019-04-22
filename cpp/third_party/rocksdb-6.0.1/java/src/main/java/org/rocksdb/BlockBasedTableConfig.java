// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

/**
 * The config for plain table sst format.
 *
 * BlockBasedTable is a RocksDB's default SST file format.
 */
//TODO(AR) should be renamed BlockBasedTableOptions
public class BlockBasedTableConfig extends TableFormatConfig {

  public BlockBasedTableConfig() {
    //TODO(AR) flushBlockPolicyFactory
    cacheIndexAndFilterBlocks = false;
    cacheIndexAndFilterBlocksWithHighPriority = false;
    pinL0FilterAndIndexBlocksInCache = false;
    pinTopLevelIndexAndFilter = true;
    indexType = IndexType.kBinarySearch;
    dataBlockIndexType = DataBlockIndexType.kDataBlockBinarySearch;
    dataBlockHashTableUtilRatio = 0.75;
    checksumType = ChecksumType.kCRC32c;
    noBlockCache = false;
    blockCache = null;
    persistentCache = null;
    blockCacheCompressed = null;
    blockSize = 4 * 1024;
    blockSizeDeviation = 10;
    blockRestartInterval = 16;
    indexBlockRestartInterval = 1;
    metadataBlockSize = 4096;
    partitionFilters = false;
    useDeltaEncoding = true;
    filterPolicy = null;
    wholeKeyFiltering = true;
    verifyCompression = true;
    readAmpBytesPerBit = 0;
    formatVersion = 2;
    enableIndexCompression = true;
    blockAlign = false;

    // NOTE: ONLY used if blockCache == null
    blockCacheSize = 8 * 1024 * 1024;
    blockCacheNumShardBits = 0;

    // NOTE: ONLY used if blockCacheCompressed == null
    blockCacheCompressedSize = 0;
    blockCacheCompressedNumShardBits = 0;
  }

  /**
   * Indicating if we'd put index/filter blocks to the block cache.
   * If not specified, each "table reader" object will pre-load index/filter
   * block during table initialization.
   *
   * @return if index and filter blocks should be put in block cache.
   */
  public boolean cacheIndexAndFilterBlocks() {
    return cacheIndexAndFilterBlocks;
  }

  /**
   * Indicating if we'd put index/filter blocks to the block cache.
   * If not specified, each "table reader" object will pre-load index/filter
   * block during table initialization.
   *
   * @param cacheIndexAndFilterBlocks and filter blocks should be put in block cache.
   * @return the reference to the current config.
   */
  public BlockBasedTableConfig setCacheIndexAndFilterBlocks(
      final boolean cacheIndexAndFilterBlocks) {
    this.cacheIndexAndFilterBlocks = cacheIndexAndFilterBlocks;
    return this;
  }

  /**
   * Indicates if index and filter blocks will be treated as high-priority in the block cache.
   * See note below about applicability. If not specified, defaults to false.
   *
   * @return if index and filter blocks will be treated as high-priority.
   */
  public boolean cacheIndexAndFilterBlocksWithHighPriority() {
    return cacheIndexAndFilterBlocksWithHighPriority;
  }

  /**
   * If true, cache index and filter blocks with high priority. If set to true,
   * depending on implementation of block cache, index and filter blocks may be
   * less likely to be evicted than data blocks.
   *
   * @param cacheIndexAndFilterBlocksWithHighPriority if index and filter blocks
   *            will be treated as high-priority.
   * @return the reference to the current config.
   */
  public BlockBasedTableConfig setCacheIndexAndFilterBlocksWithHighPriority(
      final boolean cacheIndexAndFilterBlocksWithHighPriority) {
    this.cacheIndexAndFilterBlocksWithHighPriority = cacheIndexAndFilterBlocksWithHighPriority;
    return this;
  }

  /**
   * Indicating if we'd like to pin L0 index/filter blocks to the block cache.
   If not specified, defaults to false.
   *
   * @return if L0 index and filter blocks should be pinned to the block cache.
   */
  public boolean pinL0FilterAndIndexBlocksInCache() {
    return pinL0FilterAndIndexBlocksInCache;
  }

  /**
   * Indicating if we'd like to pin L0 index/filter blocks to the block cache.
   If not specified, defaults to false.
   *
   * @param pinL0FilterAndIndexBlocksInCache pin blocks in block cache
   * @return the reference to the current config.
   */
  public BlockBasedTableConfig setPinL0FilterAndIndexBlocksInCache(
      final boolean pinL0FilterAndIndexBlocksInCache) {
    this.pinL0FilterAndIndexBlocksInCache = pinL0FilterAndIndexBlocksInCache;
    return this;
  }

  /**
   * Indicates if top-level index and filter blocks should be pinned.
   *
   * @return if top-level index and filter blocks should be pinned.
   */
  public boolean pinTopLevelIndexAndFilter() {
    return pinTopLevelIndexAndFilter;
  }

  /**
   * If cacheIndexAndFilterBlocks is true and the below is true, then
   * the top-level index of partitioned filter and index blocks are stored in
   * the cache, but a reference is held in the "table reader" object so the
   * blocks are pinned and only evicted from cache when the table reader is
   * freed. This is not limited to l0 in LSM tree.
   *
   * @param pinTopLevelIndexAndFilter if top-level index and filter blocks should be pinned.
   * @return the reference to the current config.
   */
  public BlockBasedTableConfig setPinTopLevelIndexAndFilter(final boolean pinTopLevelIndexAndFilter) {
    this.pinTopLevelIndexAndFilter = pinTopLevelIndexAndFilter;
    return this;
  }

  /**
   * Get the index type.
   *
   * @return the currently set index type
   */
  public IndexType indexType() {
    return indexType;
  }

  /**
   * Sets the index type to used with this table.
   *
   * @param indexType {@link org.rocksdb.IndexType} value
   * @return the reference to the current option.
   */
  public BlockBasedTableConfig setIndexType(
      final IndexType indexType) {
    this.indexType = indexType;
    return this;
  }

  /**
   * Get the data block index type.
   *
   * @return the currently set data block index type
   */
  public DataBlockIndexType dataBlockIndexType() {
    return dataBlockIndexType;
  }

  /**
   * Sets the data block index type to used with this table.
   *
   * @param dataBlockIndexType {@link org.rocksdb.DataBlockIndexType} value
   * @return the reference to the current option.
   */
  public BlockBasedTableConfig setDataBlockIndexType(
      final DataBlockIndexType dataBlockIndexType) {
    this.dataBlockIndexType = dataBlockIndexType;
    return this;
  }

  /**
   * Get the #entries/#buckets. It is valid only when {@link #dataBlockIndexType()} is
   * {@link DataBlockIndexType#kDataBlockBinaryAndHash}.
   *
   * @return the #entries/#buckets.
   */
  public double dataBlockHashTableUtilRatio() {
    return dataBlockHashTableUtilRatio;
  }

  /**
   * Set the #entries/#buckets. It is valid only when {@link #dataBlockIndexType()} is
   * {@link DataBlockIndexType#kDataBlockBinaryAndHash}.
   *
   * @param dataBlockHashTableUtilRatio #entries/#buckets
   * @return the reference to the current option.
   */
  public BlockBasedTableConfig setDataBlockHashTableUtilRatio(
      final double dataBlockHashTableUtilRatio) {
    this.dataBlockHashTableUtilRatio = dataBlockHashTableUtilRatio;
    return this;
  }

  /**
   * Get the checksum type to be used with this table.
   *
   * @return the currently set checksum type
   */
  public ChecksumType checksumType() {
    return checksumType;
  }

  /**
   * Sets
   *
   * @param checksumType {@link org.rocksdb.ChecksumType} value.
   * @return the reference to the current option.
   */
  public BlockBasedTableConfig setChecksumType(
      final ChecksumType checksumType) {
    this.checksumType = checksumType;
    return this;
  }

  /**
   * Determine if the block cache is disabled.
   *
   * @return if block cache is disabled
   */
  public boolean noBlockCache() {
    return noBlockCache;
  }

  /**
   * Disable block cache. If this is set to true,
   * then no block cache should be used, and the {@link #setBlockCache(Cache)}
   * should point to a {@code null} object.
   *
   * Default: false
   *
   * @param noBlockCache if use block cache
   * @return the reference to the current config.
   */
  public BlockBasedTableConfig setNoBlockCache(final boolean noBlockCache) {
    this.noBlockCache = noBlockCache;
    return this;
  }

  /**
   * Use the specified cache for blocks.
   * When not null this take precedence even if the user sets a block cache size.
   *
   * {@link org.rocksdb.Cache} should not be disposed before options instances
   * using this cache is disposed.
   *
   * {@link org.rocksdb.Cache} instance can be re-used in multiple options
   * instances.
   *
   * @param blockCache {@link org.rocksdb.Cache} Cache java instance
   *     (e.g. LRUCache).
   *
   * @return the reference to the current config.
   */
  public BlockBasedTableConfig setBlockCache(final Cache blockCache) {
    this.blockCache = blockCache;
    return this;
  }

  /**
   * Use the specified persistent cache.
   *
   * If {@code !null} use the specified cache for pages read from device,
   * otherwise no page cache is used.
   *
   * @param persistentCache the persistent cache
   *
   * @return the reference to the current config.
   */
  public BlockBasedTableConfig setPersistentCache(
      final PersistentCache persistentCache) {
    this.persistentCache = persistentCache;
    return this;
  }

  /**
   * Use the specified cache for compressed blocks.
   *
   * If {@code null}, RocksDB will not use a compressed block cache.
   *
   * Note: though it looks similar to {@link #setBlockCache(Cache)}, RocksDB
   *     doesn't put the same type of object there.
   *
   * {@link org.rocksdb.Cache} should not be disposed before options instances
   * using this cache is disposed.
   *
   * {@link org.rocksdb.Cache} instance can be re-used in multiple options
   * instances.
   *
   * @param blockCacheCompressed {@link org.rocksdb.Cache} Cache java instance
   *     (e.g. LRUCache).
   *
   * @return the reference to the current config.
   */
  public BlockBasedTableConfig setBlockCacheCompressed(
      final Cache blockCacheCompressed) {
    this.blockCacheCompressed = blockCacheCompressed;
    return this;
  }

  /**
   * Get the approximate size of user data packed per block.
   *
   * @return block size in bytes
   */
  public long blockSize() {
    return blockSize;
  }

  /**
   * Approximate size of user data packed per block. Note that the
   * block size specified here corresponds to uncompressed data.  The
   * actual size of the unit read from disk may be smaller if
   * compression is enabled.  This parameter can be changed dynamically.
   * Default: 4K
   *
   * @param blockSize block size in bytes
   * @return the reference to the current config.
   */
  public BlockBasedTableConfig setBlockSize(final long blockSize) {
    this.blockSize = blockSize;
    return this;
  }

  /**
   * @return the hash table ratio.
   */
  public int blockSizeDeviation() {
    return blockSizeDeviation;
  }

  /**
   * This is used to close a block before it reaches the configured
   * {@link #blockSize()}. If the percentage of free space in the current block
   * is less than this specified number and adding a new record to the block
   * will exceed the configured block size, then this block will be closed and
   * the new record will be written to the next block.
   *
   * Default is 10.
   *
   * @param blockSizeDeviation the deviation to block size allowed
   * @return the reference to the current config.
   */
  public BlockBasedTableConfig setBlockSizeDeviation(
      final int blockSizeDeviation) {
    this.blockSizeDeviation = blockSizeDeviation;
    return this;
  }

  /**
   * Get the block restart interval.
   *
   * @return block restart interval
   */
  public int blockRestartInterval() {
    return blockRestartInterval;
  }

  /**
   * Set the block restart interval.
   *
   * @param restartInterval block restart interval.
   * @return the reference to the current config.
   */
  public BlockBasedTableConfig setBlockRestartInterval(
      final int restartInterval) {
    blockRestartInterval = restartInterval;
    return this;
  }

  /**
   * Get the index block restart interval.
   *
   * @return index block restart interval
   */
  public int indexBlockRestartInterval() {
    return indexBlockRestartInterval;
  }

  /**
   * Set the index block restart interval
   *
   * @param restartInterval index block restart interval.
   * @return the reference to the current config.
   */
  public BlockBasedTableConfig setIndexBlockRestartInterval(
      final int restartInterval) {
    indexBlockRestartInterval = restartInterval;
    return this;
  }

  /**
   * Get the block size for partitioned metadata.
   *
   * @return block size for partitioned metadata.
   */
  public long metadataBlockSize() {
    return metadataBlockSize;
  }

  /**
   * Set block size for partitioned metadata.
   *
   * @param metadataBlockSize Partitioned metadata block size.
   * @return the reference to the current config.
   */
  public BlockBasedTableConfig setMetadataBlockSize(
      final long metadataBlockSize) {
    this.metadataBlockSize = metadataBlockSize;
    return this;
  }

  /**
   * Indicates if we're using partitioned filters.
   *
   * @return if we're using partition filters.
   */
  public boolean partitionFilters() {
    return partitionFilters;
  }

  /**
   * Use partitioned full filters for each SST file. This option is incompatible
   * with block-based filters.
   *
   * Defaults to false.
   *
   * @param partitionFilters use partition filters.
   * @return the reference to the current config.
   */
  public BlockBasedTableConfig setPartitionFilters(final boolean partitionFilters) {
    this.partitionFilters = partitionFilters;
    return this;
  }

  /**
   * Determine if delta encoding is being used to compress block keys.
   *
   * @return true if delta encoding is enabled, false otherwise.
   */
  public boolean useDeltaEncoding() {
    return useDeltaEncoding;
  }

  /**
   * Use delta encoding to compress keys in blocks.
   *
   * NOTE: {@link ReadOptions#pinData()} requires this option to be disabled.
   *
   * Default: true
   *
   * @param useDeltaEncoding true to enable delta encoding
   *
   * @return the reference to the current config.
   */
  public BlockBasedTableConfig setUseDeltaEncoding(
      final boolean useDeltaEncoding) {
    this.useDeltaEncoding = useDeltaEncoding;
    return this;
  }

  /**
   * Use the specified filter policy to reduce disk reads.
   *
   * {@link org.rocksdb.Filter} should not be disposed before options instances
   * using this filter is disposed. If {@link Filter#dispose()} function is not
   * called, then filter object will be GC'd automatically.
   *
   * {@link org.rocksdb.Filter} instance can be re-used in multiple options
   * instances.
   *
   * @param filterPolicy {@link org.rocksdb.Filter} Filter Policy java instance.
   * @return the reference to the current config.
   */
  public BlockBasedTableConfig setFilterPolicy(
      final Filter filterPolicy) {
    this.filterPolicy = filterPolicy;
    return this;
  }

  /*
   * @deprecated Use {@link #setFilterPolicy(Filter)}
   */
  @Deprecated
  public BlockBasedTableConfig setFilter(
      final Filter filter) {
    return setFilterPolicy(filter);
  }

  /**
   * Determine if whole keys as opposed to prefixes are placed in the filter.
   *
   * @return if whole key filtering is enabled
   */
  public boolean wholeKeyFiltering() {
    return wholeKeyFiltering;
  }

  /**
   * If true, place whole keys in the filter (not just prefixes).
   * This must generally be true for gets to be efficient.
   * Default: true
   *
   * @param wholeKeyFiltering if enable whole key filtering
   * @return the reference to the current config.
   */
  public BlockBasedTableConfig setWholeKeyFiltering(
      final boolean wholeKeyFiltering) {
    this.wholeKeyFiltering = wholeKeyFiltering;
    return this;
  }

  /**
   * Returns true when compression verification is enabled.
   *
   * See {@link #setVerifyCompression(boolean)}.
   *
   * @return true if compression verification is enabled.
   */
  public boolean verifyCompression() {
    return verifyCompression;
  }

  /**
   * Verify that decompressing the compressed block gives back the input. This
   * is a verification mode that we use to detect bugs in compression
   * algorithms.
   *
   * @param verifyCompression true to enable compression verification.
   *
   * @return the reference to the current config.
   */
  public BlockBasedTableConfig setVerifyCompression(
      final boolean verifyCompression) {
    this.verifyCompression = verifyCompression;
    return this;
  }

  /**
   * Get the Read amplification bytes per-bit.
   *
   * See {@link #setReadAmpBytesPerBit(int)}.
   *
   * @return the bytes per-bit.
   */
  public int readAmpBytesPerBit() {
    return readAmpBytesPerBit;
  }

  /**
   * Set the Read amplification bytes per-bit.
   *
   * If used, For every data block we load into memory, we will create a bitmap
   * of size ((block_size / `read_amp_bytes_per_bit`) / 8) bytes. This bitmap
   * will be used to figure out the percentage we actually read of the blocks.
   *
   * When this feature is used Tickers::READ_AMP_ESTIMATE_USEFUL_BYTES and
   * Tickers::READ_AMP_TOTAL_READ_BYTES can be used to calculate the
   * read amplification using this formula
   * (READ_AMP_TOTAL_READ_BYTES / READ_AMP_ESTIMATE_USEFUL_BYTES)
   *
   * value  =&gt;  memory usage (percentage of loaded blocks memory)
   * 1      =&gt;  12.50 %
   * 2      =&gt;  06.25 %
   * 4      =&gt;  03.12 %
   * 8      =&gt;  01.56 %
   * 16     =&gt;  00.78 %
   *
   * Note: This number must be a power of 2, if not it will be sanitized
   * to be the next lowest power of 2, for example a value of 7 will be
   * treated as 4, a value of 19 will be treated as 16.
   *
   * Default: 0 (disabled)
   *
   * @param readAmpBytesPerBit the bytes per-bit
   *
   * @return the reference to the current config.
   */
  public BlockBasedTableConfig setReadAmpBytesPerBit(final int readAmpBytesPerBit) {
    this.readAmpBytesPerBit = readAmpBytesPerBit;
    return this;
  }

  /**
   * Get the format version.
   * See {@link #setFormatVersion(int)}.
   *
   * @return the currently configured format version.
   */
  public int formatVersion() {
    return formatVersion;
  }

  /**
   * <p>We currently have five versions:</p>
   *
   * <ul>
   * <li><strong>0</strong> - This version is currently written
   * out by all RocksDB's versions by default. Can be read by really old
   * RocksDB's. Doesn't support changing checksum (default is CRC32).</li>
   * <li><strong>1</strong> - Can be read by RocksDB's versions since 3.0.
   * Supports non-default checksum, like xxHash. It is written by RocksDB when
   * BlockBasedTableOptions::checksum is something other than kCRC32c. (version
   * 0 is silently upconverted)</li>
   * <li><strong>2</strong> - Can be read by RocksDB's versions since 3.10.
   * Changes the way we encode compressed blocks with LZ4, BZip2 and Zlib
   * compression. If you don't plan to run RocksDB before version 3.10,
   * you should probably use this.</li>
   * <li><strong>3</strong> - Can be read by RocksDB's versions since 5.15. Changes the way we
   * encode the keys in index blocks. If you don't plan to run RocksDB before
   * version 5.15, you should probably use this.
   * This option only affects newly written tables. When reading existing
   * tables, the information about version is read from the footer.</li>
   * <li><strong>4</strong> - Can be read by RocksDB's versions since 5.16. Changes the way we
   * encode the values in index blocks. If you don't plan to run RocksDB before
   * version 5.16 and you are using index_block_restart_interval &gt; 1, you should
   * probably use this as it would reduce the index size.</li>
   * </ul>
   * <p> This option only affects newly written tables. When reading existing
   * tables, the information about version is read from the footer.</p>
   *
   * @param formatVersion integer representing the version to be used.
   *
   * @return the reference to the current option.
   */
  public BlockBasedTableConfig setFormatVersion(
      final int formatVersion) {
    assert(formatVersion >= 0 && formatVersion <= 4);
    this.formatVersion = formatVersion;
    return this;
  }

  /**
   * Determine if index compression is enabled.
   *
   * See {@link #setEnableIndexCompression(boolean)}.
   *
   * @return true if index compression is enabled, false otherwise
   */
  public boolean enableIndexCompression() {
    return enableIndexCompression;
  }

  /**
   * Store index blocks on disk in compressed format.
   *
   * Changing this option to false  will avoid the overhead of decompression
   * if index blocks are evicted and read back.
   *
   * @param enableIndexCompression true to enable index compression,
   *     false to disable
   *
   * @return the reference to the current option.
   */
  public BlockBasedTableConfig setEnableIndexCompression(
      final boolean enableIndexCompression) {
    this.enableIndexCompression = enableIndexCompression;
    return this;
  }

  /**
   * Determines whether data blocks are aligned on the lesser of page size
   * and block size.
   *
   * @return true if data blocks are aligned on the lesser of page size
   *     and block size.
   */
  public boolean blockAlign() {
    return blockAlign;
  }

  /**
   * Set whether data blocks should be aligned on the lesser of page size
   * and block size.
   *
   * @param blockAlign true to align data blocks on the lesser of page size
   *     and block size.
   *
   * @return the reference to the current option.
   */
  public BlockBasedTableConfig setBlockAlign(final boolean blockAlign) {
    this.blockAlign = blockAlign;
    return this;
  }


  /**
   * Get the size of the cache in bytes that will be used by RocksDB.
   *
   * @return block cache size in bytes
   */
  @Deprecated
  public long blockCacheSize() {
    return blockCacheSize;
  }

  /**
   * Set the size of the cache in bytes that will be used by RocksDB.
   * If cacheSize is non-positive, then cache will not be used.
   * DEFAULT: 8M
   *
   * @param blockCacheSize block cache size in bytes
   * @return the reference to the current config.
   *
   * @deprecated Use {@link #setBlockCache(Cache)}.
   */
  @Deprecated
  public BlockBasedTableConfig setBlockCacheSize(final long blockCacheSize) {
    this.blockCacheSize = blockCacheSize;
    return this;
  }

  /**
   * Returns the number of shard bits used in the block cache.
   * The resulting number of shards would be 2 ^ (returned value).
   * Any negative number means use default settings.
   *
   * @return the number of shard bits used in the block cache.
   */
  @Deprecated
  public int cacheNumShardBits() {
    return blockCacheNumShardBits;
  }

  /**
   * Controls the number of shards for the block cache.
   * This is applied only if cacheSize is set to non-negative.
   *
   * @param blockCacheNumShardBits the number of shard bits. The resulting
   *     number of shards would be 2 ^ numShardBits.  Any negative
   *     number means use default settings."
   * @return the reference to the current option.
   *
   * @deprecated Use {@link #setBlockCache(Cache)}.
   */
  @Deprecated
  public BlockBasedTableConfig setCacheNumShardBits(
      final int blockCacheNumShardBits) {
    this.blockCacheNumShardBits = blockCacheNumShardBits;
    return this;
  }

  /**
   * Size of compressed block cache. If 0, then block_cache_compressed is set
   * to null.
   *
   * @return size of compressed block cache.
   */
  @Deprecated
  public long blockCacheCompressedSize() {
    return blockCacheCompressedSize;
  }

  /**
   * Size of compressed block cache. If 0, then block_cache_compressed is set
   * to null.
   *
   * @param blockCacheCompressedSize of compressed block cache.
   * @return the reference to the current config.
   *
   * @deprecated Use {@link #setBlockCacheCompressed(Cache)}.
   */
  @Deprecated
  public BlockBasedTableConfig setBlockCacheCompressedSize(
      final long blockCacheCompressedSize) {
    this.blockCacheCompressedSize = blockCacheCompressedSize;
    return this;
  }

  /**
   * Controls the number of shards for the block compressed cache.
   * This is applied only if blockCompressedCacheSize is set to non-negative.
   *
   * @return numShardBits the number of shard bits.  The resulting
   *     number of shards would be 2 ^ numShardBits.  Any negative
   *     number means use default settings.
   */
  @Deprecated
  public int blockCacheCompressedNumShardBits() {
    return blockCacheCompressedNumShardBits;
  }

  /**
   * Controls the number of shards for the block compressed cache.
   * This is applied only if blockCompressedCacheSize is set to non-negative.
   *
   * @param blockCacheCompressedNumShardBits the number of shard bits.  The resulting
   *     number of shards would be 2 ^ numShardBits.  Any negative
   *     number means use default settings."
   * @return the reference to the current option.
   *
   * @deprecated Use {@link #setBlockCacheCompressed(Cache)}.
   */
  @Deprecated
  public BlockBasedTableConfig setBlockCacheCompressedNumShardBits(
      final int blockCacheCompressedNumShardBits) {
    this.blockCacheCompressedNumShardBits = blockCacheCompressedNumShardBits;
    return this;
  }

  /**
   * Influence the behavior when kHashSearch is used.
   *  if false, stores a precise prefix to block range mapping
   *  if true, does not store prefix and allows prefix hash collision
   *  (less memory consumption)
   *
   * @return if hash collisions should be allowed.
   *
   * @deprecated This option is now deprecated. No matter what value it
   *     is set to, it will behave as
   *     if {@link #hashIndexAllowCollision()} == true.
   */
  @Deprecated
  public boolean hashIndexAllowCollision() {
    return true;
  }

  /**
   * Influence the behavior when kHashSearch is used.
   * if false, stores a precise prefix to block range mapping
   * if true, does not store prefix and allows prefix hash collision
   * (less memory consumption)
   *
   * @param hashIndexAllowCollision points out if hash collisions should be allowed.
   *
   * @return the reference to the current config.
   *
   * @deprecated This option is now deprecated. No matter what value it
   *     is set to, it will behave as
   *     if {@link #hashIndexAllowCollision()} == true.
   */
  @Deprecated
  public BlockBasedTableConfig setHashIndexAllowCollision(
      final boolean hashIndexAllowCollision) {
    // no-op
    return this;
  }

  @Override protected long newTableFactoryHandle() {
    final long filterPolicyHandle;
    if (filterPolicy != null) {
      filterPolicyHandle = filterPolicy.nativeHandle_;
    } else {
      filterPolicyHandle = 0;
    }

    final long blockCacheHandle;
    if (blockCache != null) {
      blockCacheHandle = blockCache.nativeHandle_;
    } else {
      blockCacheHandle = 0;
    }

    final long persistentCacheHandle;
    if (persistentCache != null) {
      persistentCacheHandle = persistentCache.nativeHandle_;
    } else {
      persistentCacheHandle = 0;
    }

    final long blockCacheCompressedHandle;
    if (blockCacheCompressed != null) {
      blockCacheCompressedHandle = blockCacheCompressed.nativeHandle_;
    } else {
      blockCacheCompressedHandle = 0;
    }

    return newTableFactoryHandle(cacheIndexAndFilterBlocks,
        cacheIndexAndFilterBlocksWithHighPriority,
        pinL0FilterAndIndexBlocksInCache, pinTopLevelIndexAndFilter,
        indexType.getValue(), dataBlockIndexType.getValue(),
        dataBlockHashTableUtilRatio, checksumType.getValue(), noBlockCache,
        blockCacheHandle, persistentCacheHandle, blockCacheCompressedHandle,
        blockSize, blockSizeDeviation, blockRestartInterval,
        indexBlockRestartInterval, metadataBlockSize, partitionFilters,
        useDeltaEncoding, filterPolicyHandle, wholeKeyFiltering,
        verifyCompression, readAmpBytesPerBit, formatVersion,
        enableIndexCompression, blockAlign,
        blockCacheSize, blockCacheNumShardBits,
        blockCacheCompressedSize, blockCacheCompressedNumShardBits);
  }

  private native long newTableFactoryHandle(
      final boolean cacheIndexAndFilterBlocks,
      final boolean cacheIndexAndFilterBlocksWithHighPriority,
      final boolean pinL0FilterAndIndexBlocksInCache,
      final boolean pinTopLevelIndexAndFilter,
      final byte indexTypeValue,
      final byte dataBlockIndexTypeValue,
      final double dataBlockHashTableUtilRatio,
      final byte checksumTypeValue,
      final boolean noBlockCache,
      final long blockCacheHandle,
      final long persistentCacheHandle,
      final long blockCacheCompressedHandle,
      final long blockSize,
      final int blockSizeDeviation,
      final int blockRestartInterval,
      final int indexBlockRestartInterval,
      final long metadataBlockSize,
      final boolean partitionFilters,
      final boolean useDeltaEncoding,
      final long filterPolicyHandle,
      final boolean wholeKeyFiltering,
      final boolean verifyCompression,
      final int readAmpBytesPerBit,
      final int formatVersion,
      final boolean enableIndexCompression,
      final boolean blockAlign,

      @Deprecated final long blockCacheSize,
      @Deprecated final int blockCacheNumShardBits,

      @Deprecated final long blockCacheCompressedSize,
      @Deprecated final int blockCacheCompressedNumShardBits
  );

  //TODO(AR) flushBlockPolicyFactory
  private boolean cacheIndexAndFilterBlocks;
  private boolean cacheIndexAndFilterBlocksWithHighPriority;
  private boolean pinL0FilterAndIndexBlocksInCache;
  private boolean pinTopLevelIndexAndFilter;
  private IndexType indexType;
  private DataBlockIndexType dataBlockIndexType;
  private double dataBlockHashTableUtilRatio;
  private ChecksumType checksumType;
  private boolean noBlockCache;
  private Cache blockCache;
  private PersistentCache persistentCache;
  private Cache blockCacheCompressed;
  private long blockSize;
  private int blockSizeDeviation;
  private int blockRestartInterval;
  private int indexBlockRestartInterval;
  private long metadataBlockSize;
  private boolean partitionFilters;
  private boolean useDeltaEncoding;
  private Filter filterPolicy;
  private boolean wholeKeyFiltering;
  private boolean verifyCompression;
  private int readAmpBytesPerBit;
  private int formatVersion;
  private boolean enableIndexCompression;
  private boolean blockAlign;

  // NOTE: ONLY used if blockCache == null
  @Deprecated private long blockCacheSize;
  @Deprecated private int blockCacheNumShardBits;

  // NOTE: ONLY used if blockCacheCompressed == null
  @Deprecated private long blockCacheCompressedSize;
  @Deprecated private int blockCacheCompressedNumShardBits;
}
