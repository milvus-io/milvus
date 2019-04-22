// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class OptionsUtilTest {
  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource = new RocksMemoryResource();

  @Rule public TemporaryFolder dbFolder = new TemporaryFolder();

  enum TestAPI { LOAD_LATEST_OPTIONS, LOAD_OPTIONS_FROM_FILE }

  @Test
  public void loadLatestOptions() throws RocksDBException {
    verifyOptions(TestAPI.LOAD_LATEST_OPTIONS);
  }

  @Test
  public void loadOptionsFromFile() throws RocksDBException {
    verifyOptions(TestAPI.LOAD_OPTIONS_FROM_FILE);
  }

  @Test
  public void getLatestOptionsFileName() throws RocksDBException {
    final String dbPath = dbFolder.getRoot().getAbsolutePath();
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options, dbPath)) {
      assertThat(db).isNotNull();
    }

    String fName = OptionsUtil.getLatestOptionsFileName(dbPath, Env.getDefault());
    assertThat(fName).isNotNull();
    assert(fName.startsWith("OPTIONS-") == true);
    // System.out.println("latest options fileName: " + fName);
  }

  private void verifyOptions(TestAPI apiType) throws RocksDBException {
    final String dbPath = dbFolder.getRoot().getAbsolutePath();
    final Options options = new Options()
                                .setCreateIfMissing(true)
                                .setParanoidChecks(false)
                                .setMaxOpenFiles(478)
                                .setDelayedWriteRate(1234567L);
    final ColumnFamilyOptions baseDefaultCFOpts = new ColumnFamilyOptions();
    final byte[] secondCFName = "new_cf".getBytes();
    final ColumnFamilyOptions baseSecondCFOpts =
        new ColumnFamilyOptions()
            .setWriteBufferSize(70 * 1024)
            .setMaxWriteBufferNumber(7)
            .setMaxBytesForLevelBase(53 * 1024 * 1024)
            .setLevel0FileNumCompactionTrigger(3)
            .setLevel0SlowdownWritesTrigger(51)
            .setBottommostCompressionType(CompressionType.ZSTD_COMPRESSION);

    // Create a database with a new column family
    try (final RocksDB db = RocksDB.open(options, dbPath)) {
      assertThat(db).isNotNull();

      // create column family
      try (final ColumnFamilyHandle columnFamilyHandle =
               db.createColumnFamily(new ColumnFamilyDescriptor(secondCFName, baseSecondCFOpts))) {
        assert(columnFamilyHandle != null);
      }
    }

    // Read the options back and verify
    DBOptions dbOptions = new DBOptions();
    final List<ColumnFamilyDescriptor> cfDescs = new ArrayList<>();
    String path = dbPath;
    if (apiType == TestAPI.LOAD_LATEST_OPTIONS) {
      OptionsUtil.loadLatestOptions(path, Env.getDefault(), dbOptions, cfDescs, false);
    } else if (apiType == TestAPI.LOAD_OPTIONS_FROM_FILE) {
      path = dbPath + "/" + OptionsUtil.getLatestOptionsFileName(dbPath, Env.getDefault());
      OptionsUtil.loadOptionsFromFile(path, Env.getDefault(), dbOptions, cfDescs, false);
    }

    assertThat(dbOptions.createIfMissing()).isEqualTo(options.createIfMissing());
    assertThat(dbOptions.paranoidChecks()).isEqualTo(options.paranoidChecks());
    assertThat(dbOptions.maxOpenFiles()).isEqualTo(options.maxOpenFiles());
    assertThat(dbOptions.delayedWriteRate()).isEqualTo(options.delayedWriteRate());

    assertThat(cfDescs.size()).isEqualTo(2);
    assertThat(cfDescs.get(0)).isNotNull();
    assertThat(cfDescs.get(1)).isNotNull();
    assertThat(cfDescs.get(0).columnFamilyName()).isEqualTo(RocksDB.DEFAULT_COLUMN_FAMILY);
    assertThat(cfDescs.get(1).columnFamilyName()).isEqualTo(secondCFName);

    ColumnFamilyOptions defaultCFOpts = cfDescs.get(0).columnFamilyOptions();
    assertThat(defaultCFOpts.writeBufferSize()).isEqualTo(baseDefaultCFOpts.writeBufferSize());
    assertThat(defaultCFOpts.maxWriteBufferNumber())
        .isEqualTo(baseDefaultCFOpts.maxWriteBufferNumber());
    assertThat(defaultCFOpts.maxBytesForLevelBase())
        .isEqualTo(baseDefaultCFOpts.maxBytesForLevelBase());
    assertThat(defaultCFOpts.level0FileNumCompactionTrigger())
        .isEqualTo(baseDefaultCFOpts.level0FileNumCompactionTrigger());
    assertThat(defaultCFOpts.level0SlowdownWritesTrigger())
        .isEqualTo(baseDefaultCFOpts.level0SlowdownWritesTrigger());
    assertThat(defaultCFOpts.bottommostCompressionType())
        .isEqualTo(baseDefaultCFOpts.bottommostCompressionType());

    ColumnFamilyOptions secondCFOpts = cfDescs.get(1).columnFamilyOptions();
    assertThat(secondCFOpts.writeBufferSize()).isEqualTo(baseSecondCFOpts.writeBufferSize());
    assertThat(secondCFOpts.maxWriteBufferNumber())
        .isEqualTo(baseSecondCFOpts.maxWriteBufferNumber());
    assertThat(secondCFOpts.maxBytesForLevelBase())
        .isEqualTo(baseSecondCFOpts.maxBytesForLevelBase());
    assertThat(secondCFOpts.level0FileNumCompactionTrigger())
        .isEqualTo(baseSecondCFOpts.level0FileNumCompactionTrigger());
    assertThat(secondCFOpts.level0SlowdownWritesTrigger())
        .isEqualTo(baseSecondCFOpts.level0SlowdownWritesTrigger());
    assertThat(secondCFOpts.bottommostCompressionType())
        .isEqualTo(baseSecondCFOpts.bottommostCompressionType());
  }
}
