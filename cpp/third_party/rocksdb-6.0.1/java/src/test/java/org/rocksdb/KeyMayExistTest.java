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

import static org.assertj.core.api.Assertions.assertThat;

public class KeyMayExistTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void keyMayExist() throws RocksDBException {
    final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("new_cf".getBytes())
    );

    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath(),
             cfDescriptors, columnFamilyHandleList)) {
      try {
        assertThat(columnFamilyHandleList.size()).
            isEqualTo(2);
        db.put("key".getBytes(), "value".getBytes());
        // Test without column family
        StringBuilder retValue = new StringBuilder();
        boolean exists = db.keyMayExist("key".getBytes(), retValue);
        assertThat(exists).isTrue();
        assertThat(retValue.toString()).isEqualTo("value");

        // Slice key
        StringBuilder builder = new StringBuilder("prefix");
        int offset = builder.toString().length();
        builder.append("slice key 0");
        int len = builder.toString().length() - offset;
        builder.append("suffix");

        byte[] sliceKey = builder.toString().getBytes();
        byte[] sliceValue = "slice value 0".getBytes();
        db.put(sliceKey, offset, len, sliceValue, 0, sliceValue.length);

        retValue = new StringBuilder();
        exists = db.keyMayExist(sliceKey, offset, len, retValue);
        assertThat(exists).isTrue();
        assertThat(retValue.toString().getBytes()).isEqualTo(sliceValue);

        // Test without column family but with readOptions
        try (final ReadOptions readOptions = new ReadOptions()) {
          retValue = new StringBuilder();
          exists = db.keyMayExist(readOptions, "key".getBytes(), retValue);
          assertThat(exists).isTrue();
          assertThat(retValue.toString()).isEqualTo("value");

          retValue = new StringBuilder();
          exists = db.keyMayExist(readOptions, sliceKey, offset, len, retValue);
          assertThat(exists).isTrue();
          assertThat(retValue.toString().getBytes()).isEqualTo(sliceValue);
        }

        // Test with column family
        retValue = new StringBuilder();
        exists = db.keyMayExist(columnFamilyHandleList.get(0), "key".getBytes(),
            retValue);
        assertThat(exists).isTrue();
        assertThat(retValue.toString()).isEqualTo("value");

        // Test slice sky with column family
        retValue = new StringBuilder();
        exists = db.keyMayExist(columnFamilyHandleList.get(0), sliceKey, offset, len,
            retValue);
        assertThat(exists).isTrue();
        assertThat(retValue.toString().getBytes()).isEqualTo(sliceValue);

        // Test with column family and readOptions
        try (final ReadOptions readOptions = new ReadOptions()) {
          retValue = new StringBuilder();
          exists = db.keyMayExist(readOptions,
              columnFamilyHandleList.get(0), "key".getBytes(),
              retValue);
          assertThat(exists).isTrue();
          assertThat(retValue.toString()).isEqualTo("value");

          // Test slice key with column family and read options
          retValue = new StringBuilder();
          exists = db.keyMayExist(readOptions,
              columnFamilyHandleList.get(0), sliceKey, offset, len,
              retValue);
          assertThat(exists).isTrue();
          assertThat(retValue.toString().getBytes()).isEqualTo(sliceValue);
        }

        // KeyMayExist in CF1 must return false
        assertThat(db.keyMayExist(columnFamilyHandleList.get(1),
            "key".getBytes(), retValue)).isFalse();

        // slice key
        assertThat(db.keyMayExist(columnFamilyHandleList.get(1),
           sliceKey, 1, 3, retValue)).isFalse();
      } finally {
        for (final ColumnFamilyHandle columnFamilyHandle :
            columnFamilyHandleList) {
          columnFamilyHandle.close();
        }
      }
    }
  }
}
