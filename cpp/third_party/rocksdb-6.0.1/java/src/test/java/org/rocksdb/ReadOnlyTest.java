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

public class ReadOnlyTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void readOnlyOpen() throws RocksDBException {
    try (final Options options = new Options()
        .setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {
      db.put("key".getBytes(), "value".getBytes());
      try (final RocksDB db2 = RocksDB.openReadOnly(
          dbFolder.getRoot().getAbsolutePath())) {
        assertThat("value").
            isEqualTo(new String(db2.get("key".getBytes())));
      }
    }

    try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()) {
      final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
      cfDescriptors.add(new ColumnFamilyDescriptor(
          RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts));

      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath(),
          cfDescriptors, columnFamilyHandleList)) {
        try (final ColumnFamilyOptions newCfOpts = new ColumnFamilyOptions();
             final ColumnFamilyOptions newCf2Opts = new ColumnFamilyOptions()
        ) {
          columnFamilyHandleList.add(db.createColumnFamily(
              new ColumnFamilyDescriptor("new_cf".getBytes(), newCfOpts)));
          columnFamilyHandleList.add(db.createColumnFamily(
              new ColumnFamilyDescriptor("new_cf2".getBytes(), newCf2Opts)));
          db.put(columnFamilyHandleList.get(2), "key2".getBytes(),
              "value2".getBytes());

          final List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList =
              new ArrayList<>();
          try (final RocksDB db2 = RocksDB.openReadOnly(
              dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
              readOnlyColumnFamilyHandleList)) {
            try (final ColumnFamilyOptions newCfOpts2 =
                     new ColumnFamilyOptions();
                 final ColumnFamilyOptions newCf2Opts2 =
                     new ColumnFamilyOptions()
            ) {
              assertThat(db2.get("key2".getBytes())).isNull();
              assertThat(db2.get(readOnlyColumnFamilyHandleList.get(0),
                  "key2".getBytes())).
                  isNull();
              cfDescriptors.clear();
              cfDescriptors.add(
                  new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
                      newCfOpts2));
              cfDescriptors.add(new ColumnFamilyDescriptor("new_cf2".getBytes(),
                      newCf2Opts2));

              final List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList2
                  = new ArrayList<>();
              try (final RocksDB db3 = RocksDB.openReadOnly(
                  dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
                  readOnlyColumnFamilyHandleList2)) {
                try {
                  assertThat(new String(db3.get(
                      readOnlyColumnFamilyHandleList2.get(1),
                      "key2".getBytes()))).isEqualTo("value2");
                } finally {
                  for (final ColumnFamilyHandle columnFamilyHandle :
                      readOnlyColumnFamilyHandleList2) {
                    columnFamilyHandle.close();
                  }
                }
              }
            } finally {
              for (final ColumnFamilyHandle columnFamilyHandle :
                  readOnlyColumnFamilyHandleList) {
                columnFamilyHandle.close();
              }
            }
          }
        } finally {
          for (final ColumnFamilyHandle columnFamilyHandle :
              columnFamilyHandleList) {
            columnFamilyHandle.close();
          }
        }
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void failToWriteInReadOnly() throws RocksDBException {
    try (final Options options = new Options()
        .setCreateIfMissing(true)) {

      try (final RocksDB db = RocksDB.open(options,
          dbFolder.getRoot().getAbsolutePath())) {
        //no-op
      }
    }

    try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts)
      );

      final List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList =
          new ArrayList<>();
      try (final RocksDB rDb = RocksDB.openReadOnly(
          dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
          readOnlyColumnFamilyHandleList)) {
        try {
          // test that put fails in readonly mode
          rDb.put("key".getBytes(), "value".getBytes());
        } finally {
          for (final ColumnFamilyHandle columnFamilyHandle :
              readOnlyColumnFamilyHandleList) {
            columnFamilyHandle.close();
          }
        }
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void failToCFWriteInReadOnly() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {
      //no-op
    }

    try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts)
      );
      final List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList =
          new ArrayList<>();
      try (final RocksDB rDb = RocksDB.openReadOnly(
          dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
          readOnlyColumnFamilyHandleList)) {
        try {
          rDb.put(readOnlyColumnFamilyHandleList.get(0),
              "key".getBytes(), "value".getBytes());
        } finally {
          for (final ColumnFamilyHandle columnFamilyHandle :
              readOnlyColumnFamilyHandleList) {
            columnFamilyHandle.close();
          }
        }
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void failToRemoveInReadOnly() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {
      //no-op
    }

    try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts)
      );

      final List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList =
          new ArrayList<>();

      try (final RocksDB rDb = RocksDB.openReadOnly(
          dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
          readOnlyColumnFamilyHandleList)) {
        try {
          rDb.remove("key".getBytes());
        } finally {
          for (final ColumnFamilyHandle columnFamilyHandle :
              readOnlyColumnFamilyHandleList) {
            columnFamilyHandle.close();
          }
        }
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void failToCFRemoveInReadOnly() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {
      //no-op
    }

    try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts)
      );

      final List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList =
          new ArrayList<>();
      try (final RocksDB rDb = RocksDB.openReadOnly(
          dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
          readOnlyColumnFamilyHandleList)) {
        try {
          rDb.remove(readOnlyColumnFamilyHandleList.get(0),
              "key".getBytes());
        } finally {
          for (final ColumnFamilyHandle columnFamilyHandle :
              readOnlyColumnFamilyHandleList) {
            columnFamilyHandle.close();
          }
        }
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void failToWriteBatchReadOnly() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {
      //no-op
    }

    try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts)
      );

      final List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList =
          new ArrayList<>();
      try (final RocksDB rDb = RocksDB.openReadOnly(
          dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
          readOnlyColumnFamilyHandleList);
           final WriteBatch wb = new WriteBatch();
           final WriteOptions wOpts = new WriteOptions()) {
        try {
          wb.put("key".getBytes(), "value".getBytes());
          rDb.write(wOpts, wb);
        } finally {
          for (final ColumnFamilyHandle columnFamilyHandle :
              readOnlyColumnFamilyHandleList) {
            columnFamilyHandle.close();
          }
        }
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void failToCFWriteBatchReadOnly() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {
      //no-op
    }

    try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts)
      );

      final List<ColumnFamilyHandle> readOnlyColumnFamilyHandleList =
          new ArrayList<>();
      try (final RocksDB rDb = RocksDB.openReadOnly(
          dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
          readOnlyColumnFamilyHandleList);
           final WriteBatch wb = new WriteBatch();
           final WriteOptions wOpts = new WriteOptions()) {
        try {
          wb.put(readOnlyColumnFamilyHandleList.get(0), "key".getBytes(),
              "value".getBytes());
          rDb.write(wOpts, wb);
        } finally {
          for (final ColumnFamilyHandle columnFamilyHandle :
              readOnlyColumnFamilyHandleList) {
            columnFamilyHandle.close();
          }
        }
      }
    }
  }
}
