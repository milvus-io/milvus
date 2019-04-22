// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class TtlDBTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void ttlDBOpen() throws RocksDBException, InterruptedException {
    try (final Options options = new Options().setCreateIfMissing(true).setMaxCompactionBytes(0);
         final TtlDB ttlDB = TtlDB.open(options, dbFolder.getRoot().getAbsolutePath())) {
      ttlDB.put("key".getBytes(), "value".getBytes());
      assertThat(ttlDB.get("key".getBytes())).
          isEqualTo("value".getBytes());
      assertThat(ttlDB.get("key".getBytes())).isNotNull();
    }
  }

  @Test
  public void ttlDBOpenWithTtl() throws RocksDBException, InterruptedException {
    try (final Options options = new Options().setCreateIfMissing(true).setMaxCompactionBytes(0);
         final TtlDB ttlDB = TtlDB.open(options, dbFolder.getRoot().getAbsolutePath(), 1, false);) {
      ttlDB.put("key".getBytes(), "value".getBytes());
      assertThat(ttlDB.get("key".getBytes())).
          isEqualTo("value".getBytes());
      TimeUnit.SECONDS.sleep(2);
      ttlDB.compactRange();
      assertThat(ttlDB.get("key".getBytes())).isNull();
    }
  }

  @Test
  public void ttlDbOpenWithColumnFamilies() throws RocksDBException,
      InterruptedException {
    final List<ColumnFamilyDescriptor> cfNames = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("new_cf".getBytes())
    );
    final List<Integer> ttlValues = Arrays.asList(0, 1);

    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    try (final DBOptions dbOptions = new DBOptions()
        .setCreateMissingColumnFamilies(true)
        .setCreateIfMissing(true);
         final TtlDB ttlDB = TtlDB.open(dbOptions,
             dbFolder.getRoot().getAbsolutePath(), cfNames,
             columnFamilyHandleList, ttlValues, false)) {
      try {
        ttlDB.put("key".getBytes(), "value".getBytes());
        assertThat(ttlDB.get("key".getBytes())).
            isEqualTo("value".getBytes());
        ttlDB.put(columnFamilyHandleList.get(1), "key".getBytes(),
            "value".getBytes());
        assertThat(ttlDB.get(columnFamilyHandleList.get(1),
            "key".getBytes())).isEqualTo("value".getBytes());
        TimeUnit.SECONDS.sleep(2);

        ttlDB.compactRange();
        ttlDB.compactRange(columnFamilyHandleList.get(1));

        assertThat(ttlDB.get("key".getBytes())).isNotNull();
        assertThat(ttlDB.get(columnFamilyHandleList.get(1),
            "key".getBytes())).isNull();
      } finally {
        for (final ColumnFamilyHandle columnFamilyHandle :
            columnFamilyHandleList) {
          columnFamilyHandle.close();
        }
      }
    }
  }

  @Test
  public void createTtlColumnFamily() throws RocksDBException,
      InterruptedException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final TtlDB ttlDB = TtlDB.open(options,
             dbFolder.getRoot().getAbsolutePath());
         final ColumnFamilyHandle columnFamilyHandle =
             ttlDB.createColumnFamilyWithTtl(
                 new ColumnFamilyDescriptor("new_cf".getBytes()), 1)) {
      ttlDB.put(columnFamilyHandle, "key".getBytes(),
          "value".getBytes());
      assertThat(ttlDB.get(columnFamilyHandle, "key".getBytes())).
          isEqualTo("value".getBytes());
      TimeUnit.SECONDS.sleep(2);
      ttlDB.compactRange(columnFamilyHandle);
      assertThat(ttlDB.get(columnFamilyHandle, "key".getBytes())).isNull();
    }
  }
}
