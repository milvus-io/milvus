// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

public class BlockBasedTableConfigTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void cacheIndexAndFilterBlocks() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setCacheIndexAndFilterBlocks(true);
    assertThat(blockBasedTableConfig.cacheIndexAndFilterBlocks()).
        isTrue();

  }

  @Test
  public void cacheIndexAndFilterBlocksWithHighPriority() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
    assertThat(blockBasedTableConfig.cacheIndexAndFilterBlocksWithHighPriority()).
        isTrue();
  }

  @Test
  public void pinL0FilterAndIndexBlocksInCache() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setPinL0FilterAndIndexBlocksInCache(true);
    assertThat(blockBasedTableConfig.pinL0FilterAndIndexBlocksInCache()).
        isTrue();
  }

  @Test
  public void pinTopLevelIndexAndFilter() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setPinTopLevelIndexAndFilter(false);
    assertThat(blockBasedTableConfig.pinTopLevelIndexAndFilter()).
        isFalse();
  }

  @Test
  public void indexType() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    assertThat(IndexType.values().length).isEqualTo(3);
    blockBasedTableConfig.setIndexType(IndexType.kHashSearch);
    assertThat(blockBasedTableConfig.indexType().equals(
        IndexType.kHashSearch));
    assertThat(IndexType.valueOf("kBinarySearch")).isNotNull();
    blockBasedTableConfig.setIndexType(IndexType.valueOf("kBinarySearch"));
    assertThat(blockBasedTableConfig.indexType().equals(
        IndexType.kBinarySearch));
  }

  @Test
  public void dataBlockIndexType() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setDataBlockIndexType(DataBlockIndexType.kDataBlockBinaryAndHash);
    assertThat(blockBasedTableConfig.dataBlockIndexType().equals(
        DataBlockIndexType.kDataBlockBinaryAndHash));
    blockBasedTableConfig.setDataBlockIndexType(DataBlockIndexType.kDataBlockBinarySearch);
    assertThat(blockBasedTableConfig.dataBlockIndexType().equals(
        DataBlockIndexType.kDataBlockBinarySearch));
  }

  @Test
  public void checksumType() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    assertThat(ChecksumType.values().length).isEqualTo(3);
    assertThat(ChecksumType.valueOf("kxxHash")).
        isEqualTo(ChecksumType.kxxHash);
    blockBasedTableConfig.setChecksumType(ChecksumType.kNoChecksum);
    blockBasedTableConfig.setChecksumType(ChecksumType.kxxHash);
    assertThat(blockBasedTableConfig.checksumType().equals(
        ChecksumType.kxxHash));
  }

  @Test
  public void noBlockCache() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setNoBlockCache(true);
    assertThat(blockBasedTableConfig.noBlockCache()).isTrue();
  }

  @Test
  public void blockCache() {
    try (
        final Cache cache = new LRUCache(17 * 1024 * 1024);
        final Options options = new Options().setTableFormatConfig(
            new BlockBasedTableConfig().setBlockCache(cache))) {
      assertThat(options.tableFactoryName()).isEqualTo("BlockBasedTable");
    }
  }

  @Test
  public void blockCacheIntegration() throws RocksDBException {
    try (final Cache cache = new LRUCache(8 * 1024 * 1024);
         final Statistics statistics = new Statistics()) {
      for (int shard = 0; shard < 8; shard++) {
        try (final Options options =
                 new Options()
                     .setCreateIfMissing(true)
                     .setStatistics(statistics)
                     .setTableFormatConfig(new BlockBasedTableConfig().setBlockCache(cache));
             final RocksDB db =
                 RocksDB.open(options, dbFolder.getRoot().getAbsolutePath() + "/" + shard)) {
          final byte[] key = "some-key".getBytes(StandardCharsets.UTF_8);
          final byte[] value = "some-value".getBytes(StandardCharsets.UTF_8);

          db.put(key, value);
          db.flush(new FlushOptions());
          db.get(key);

          assertThat(statistics.getTickerCount(TickerType.BLOCK_CACHE_ADD)).isEqualTo(shard + 1);
        }
      }
    }
  }

  @Test
  public void persistentCache() throws RocksDBException {
    try (final DBOptions dbOptions = new DBOptions().
        setInfoLogLevel(InfoLogLevel.INFO_LEVEL).
        setCreateIfMissing(true);
        final Logger logger = new Logger(dbOptions) {
      @Override
      protected void log(final InfoLogLevel infoLogLevel, final String logMsg) {
        System.out.println(infoLogLevel.name() + ": " + logMsg);
      }
    }) {
      try (final PersistentCache persistentCache =
               new PersistentCache(Env.getDefault(), dbFolder.getRoot().getPath(), 1024 * 1024 * 100, logger, false);
           final Options options = new Options().setTableFormatConfig(
               new BlockBasedTableConfig().setPersistentCache(persistentCache))) {
        assertThat(options.tableFactoryName()).isEqualTo("BlockBasedTable");
      }
    }
  }

  @Test
  public void blockCacheCompressed() {
    try (final Cache cache = new LRUCache(17 * 1024 * 1024);
         final Options options = new Options().setTableFormatConfig(
        new BlockBasedTableConfig().setBlockCacheCompressed(cache))) {
      assertThat(options.tableFactoryName()).isEqualTo("BlockBasedTable");
    }
  }

  @Ignore("See issue: https://github.com/facebook/rocksdb/issues/4822")
  @Test
  public void blockCacheCompressedIntegration() throws RocksDBException {
    final byte[] key1 = "some-key1".getBytes(StandardCharsets.UTF_8);
    final byte[] key2 = "some-key1".getBytes(StandardCharsets.UTF_8);
    final byte[] key3 = "some-key1".getBytes(StandardCharsets.UTF_8);
    final byte[] key4 = "some-key1".getBytes(StandardCharsets.UTF_8);
    final byte[] value = "some-value".getBytes(StandardCharsets.UTF_8);

    try (final Cache compressedCache = new LRUCache(8 * 1024 * 1024);
         final Statistics statistics = new Statistics()) {

      final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig()
          .setNoBlockCache(true)
          .setBlockCache(null)
          .setBlockCacheCompressed(compressedCache)
          .setFormatVersion(4);

      try (final Options options = new Options()
             .setCreateIfMissing(true)
             .setStatistics(statistics)
             .setTableFormatConfig(blockBasedTableConfig)) {

        for (int shard = 0; shard < 8; shard++) {
          try (final FlushOptions flushOptions = new FlushOptions();
               final WriteOptions writeOptions = new WriteOptions();
               final ReadOptions readOptions = new ReadOptions();
               final RocksDB db =
                   RocksDB.open(options, dbFolder.getRoot().getAbsolutePath() + "/" + shard)) {

            db.put(writeOptions, key1, value);
            db.put(writeOptions, key2, value);
            db.put(writeOptions, key3, value);
            db.put(writeOptions, key4, value);
            db.flush(flushOptions);

            db.get(readOptions, key1);
            db.get(readOptions, key2);
            db.get(readOptions, key3);
            db.get(readOptions, key4);

            assertThat(statistics.getTickerCount(TickerType.BLOCK_CACHE_COMPRESSED_ADD)).isEqualTo(shard + 1);
          }
        }
      }
    }
  }

  @Test
  public void blockSize() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setBlockSize(10);
    assertThat(blockBasedTableConfig.blockSize()).isEqualTo(10);
  }

  @Test
  public void blockSizeDeviation() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setBlockSizeDeviation(12);
    assertThat(blockBasedTableConfig.blockSizeDeviation()).
        isEqualTo(12);
  }

  @Test
  public void blockRestartInterval() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setBlockRestartInterval(15);
    assertThat(blockBasedTableConfig.blockRestartInterval()).
        isEqualTo(15);
  }

  @Test
  public void indexBlockRestartInterval() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setIndexBlockRestartInterval(15);
    assertThat(blockBasedTableConfig.indexBlockRestartInterval()).
        isEqualTo(15);
  }

  @Test
  public void metadataBlockSize() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setMetadataBlockSize(1024);
    assertThat(blockBasedTableConfig.metadataBlockSize()).
        isEqualTo(1024);
  }

  @Test
  public void partitionFilters() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setPartitionFilters(true);
    assertThat(blockBasedTableConfig.partitionFilters()).
        isTrue();
  }

  @Test
  public void useDeltaEncoding() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setUseDeltaEncoding(false);
    assertThat(blockBasedTableConfig.useDeltaEncoding()).
        isFalse();
  }

  @Test
  public void blockBasedTableWithFilterPolicy() {
    try(final Options options = new Options()
        .setTableFormatConfig(new BlockBasedTableConfig()
            .setFilterPolicy(new BloomFilter(10)))) {
      assertThat(options.tableFactoryName()).
          isEqualTo("BlockBasedTable");
    }
  }

  @Test
  public void blockBasedTableWithoutFilterPolicy() {
    try(final Options options = new Options().setTableFormatConfig(
        new BlockBasedTableConfig().setFilterPolicy(null))) {
      assertThat(options.tableFactoryName()).
          isEqualTo("BlockBasedTable");
    }
  }

  @Test
  public void wholeKeyFiltering() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setWholeKeyFiltering(false);
    assertThat(blockBasedTableConfig.wholeKeyFiltering()).
        isFalse();
  }

  @Test
  public void verifyCompression() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setVerifyCompression(true);
    assertThat(blockBasedTableConfig.verifyCompression()).
        isTrue();
  }

  @Test
  public void readAmpBytesPerBit() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setReadAmpBytesPerBit(2);
    assertThat(blockBasedTableConfig.readAmpBytesPerBit()).
        isEqualTo(2);
  }

  @Test
  public void formatVersion() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    for (int version = 0; version < 5; version++) {
      blockBasedTableConfig.setFormatVersion(version);
      assertThat(blockBasedTableConfig.formatVersion()).isEqualTo(version);
    }
  }

  @Test(expected = AssertionError.class)
  public void formatVersionFailNegative() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setFormatVersion(-1);
  }

  @Test(expected = AssertionError.class)
  public void formatVersionFailIllegalVersion() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setFormatVersion(99);
  }

  @Test
  public void enableIndexCompression() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setEnableIndexCompression(false);
    assertThat(blockBasedTableConfig.enableIndexCompression()).
        isFalse();
  }

  @Test
  public void blockAlign() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setBlockAlign(true);
    assertThat(blockBasedTableConfig.blockAlign()).
        isTrue();
  }

  @Deprecated
  @Test
  public void hashIndexAllowCollision() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setHashIndexAllowCollision(false);
    assertThat(blockBasedTableConfig.hashIndexAllowCollision()).
        isTrue();  // NOTE: setHashIndexAllowCollision should do nothing!
  }

  @Deprecated
  @Test
  public void blockCacheSize() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setBlockCacheSize(8 * 1024);
    assertThat(blockBasedTableConfig.blockCacheSize()).
        isEqualTo(8 * 1024);
  }

  @Deprecated
  @Test
  public void blockCacheNumShardBits() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setCacheNumShardBits(5);
    assertThat(blockBasedTableConfig.cacheNumShardBits()).
        isEqualTo(5);
  }

  @Deprecated
  @Test
  public void blockCacheCompressedSize() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setBlockCacheCompressedSize(40);
    assertThat(blockBasedTableConfig.blockCacheCompressedSize()).
        isEqualTo(40);
  }

  @Deprecated
  @Test
  public void blockCacheCompressedNumShardBits() {
    final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig();
    blockBasedTableConfig.setBlockCacheCompressedNumShardBits(4);
    assertThat(blockBasedTableConfig.blockCacheCompressedNumShardBits()).
        isEqualTo(4);
  }
}
