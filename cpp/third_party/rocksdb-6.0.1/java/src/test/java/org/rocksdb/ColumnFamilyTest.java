// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.*;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class ColumnFamilyTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void columnFamilyDescriptorName() throws RocksDBException {
    final byte[] cfName = "some_name".getBytes(UTF_8);

    try(final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()) {
      final ColumnFamilyDescriptor cfDescriptor =
              new ColumnFamilyDescriptor(cfName, cfOptions);
      assertThat(cfDescriptor.getName()).isEqualTo(cfName);
    }
  }

  @Test
  public void columnFamilyDescriptorOptions() throws RocksDBException {
    final byte[] cfName = "some_name".getBytes(UTF_8);

    try(final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
            .setCompressionType(CompressionType.BZLIB2_COMPRESSION)) {
      final ColumnFamilyDescriptor cfDescriptor =
          new ColumnFamilyDescriptor(cfName, cfOptions);

        assertThat(cfDescriptor.getOptions().compressionType())
            .isEqualTo(CompressionType.BZLIB2_COMPRESSION);
    }
  }

  @Test
  public void listColumnFamilies() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {
      // Test listColumnFamilies
      final List<byte[]> columnFamilyNames = RocksDB.listColumnFamilies(options,
          dbFolder.getRoot().getAbsolutePath());
      assertThat(columnFamilyNames).isNotNull();
      assertThat(columnFamilyNames.size()).isGreaterThan(0);
      assertThat(columnFamilyNames.size()).isEqualTo(1);
      assertThat(new String(columnFamilyNames.get(0))).isEqualTo("default");
    }
  }

  @Test
  public void defaultColumnFamily() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {
      final ColumnFamilyHandle cfh = db.getDefaultColumnFamily();
      try {
        assertThat(cfh).isNotNull();

        assertThat(cfh.getName()).isEqualTo("default".getBytes(UTF_8));
        assertThat(cfh.getID()).isEqualTo(0);

        final byte[] key = "key".getBytes();
        final byte[] value = "value".getBytes();

        db.put(cfh, key, value);

        final byte[] actualValue = db.get(cfh, key);

        assertThat(cfh).isNotNull();
        assertThat(actualValue).isEqualTo(value);
      } finally {
        cfh.close();
      }
    }
  }

  @Test
  public void createColumnFamily() throws RocksDBException {
    final byte[] cfName = "new_cf".getBytes(UTF_8);
    final ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(cfName,
            new ColumnFamilyOptions());

    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
                 dbFolder.getRoot().getAbsolutePath())) {

      final ColumnFamilyHandle columnFamilyHandle = db.createColumnFamily(cfDescriptor);

      try {
        assertThat(columnFamilyHandle.getName()).isEqualTo(cfName);
        assertThat(columnFamilyHandle.getID()).isEqualTo(1);

        final ColumnFamilyDescriptor latestDescriptor = columnFamilyHandle.getDescriptor();
        assertThat(latestDescriptor.getName()).isEqualTo(cfName);

        final List<byte[]> columnFamilyNames = RocksDB.listColumnFamilies(
                options, dbFolder.getRoot().getAbsolutePath());
        assertThat(columnFamilyNames).isNotNull();
        assertThat(columnFamilyNames.size()).isGreaterThan(0);
        assertThat(columnFamilyNames.size()).isEqualTo(2);
        assertThat(new String(columnFamilyNames.get(0))).isEqualTo("default");
        assertThat(new String(columnFamilyNames.get(1))).isEqualTo("new_cf");
      } finally {
        columnFamilyHandle.close();
      }
    }
  }

  @Test
  public void openWithColumnFamilies() throws RocksDBException {
    final List<ColumnFamilyDescriptor> cfNames = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("new_cf".getBytes())
    );

    final List<ColumnFamilyHandle> columnFamilyHandleList =
        new ArrayList<>();

    // Test open database with column family names
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath(), cfNames,
             columnFamilyHandleList)) {

      try {
        assertThat(columnFamilyHandleList.size()).isEqualTo(2);
        db.put("dfkey1".getBytes(), "dfvalue".getBytes());
        db.put(columnFamilyHandleList.get(0), "dfkey2".getBytes(),
            "dfvalue".getBytes());
        db.put(columnFamilyHandleList.get(1), "newcfkey1".getBytes(),
            "newcfvalue".getBytes());

        String retVal = new String(db.get(columnFamilyHandleList.get(1),
            "newcfkey1".getBytes()));
        assertThat(retVal).isEqualTo("newcfvalue");
        assertThat((db.get(columnFamilyHandleList.get(1),
            "dfkey1".getBytes()))).isNull();
        db.remove(columnFamilyHandleList.get(1), "newcfkey1".getBytes());
        assertThat((db.get(columnFamilyHandleList.get(1),
            "newcfkey1".getBytes()))).isNull();
        db.remove(columnFamilyHandleList.get(0), new WriteOptions(),
            "dfkey2".getBytes());
        assertThat(db.get(columnFamilyHandleList.get(0), new ReadOptions(),
            "dfkey2".getBytes())).isNull();
      } finally {
        for (final ColumnFamilyHandle columnFamilyHandle :
            columnFamilyHandleList) {
          columnFamilyHandle.close();
        }
      }
    }
  }

  @Test
  public void getWithOutValueAndCf() throws RocksDBException {
    final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

    // Test open database with column family names
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
             columnFamilyHandleList)) {
      try {
        db.put(columnFamilyHandleList.get(0), new WriteOptions(),
            "key1".getBytes(), "value".getBytes());
        db.put("key2".getBytes(), "12345678".getBytes());
        final byte[] outValue = new byte[5];
        // not found value
        int getResult = db.get("keyNotFound".getBytes(), outValue);
        assertThat(getResult).isEqualTo(RocksDB.NOT_FOUND);
        // found value which fits in outValue
        getResult = db.get(columnFamilyHandleList.get(0), "key1".getBytes(),
            outValue);
        assertThat(getResult).isNotEqualTo(RocksDB.NOT_FOUND);
        assertThat(outValue).isEqualTo("value".getBytes());
        // found value which fits partially
        getResult = db.get(columnFamilyHandleList.get(0), new ReadOptions(),
            "key2".getBytes(), outValue);
        assertThat(getResult).isNotEqualTo(RocksDB.NOT_FOUND);
        assertThat(outValue).isEqualTo("12345".getBytes());
      } finally {
        for (final ColumnFamilyHandle columnFamilyHandle :
            columnFamilyHandleList) {
          columnFamilyHandle.close();
        }
      }
    }
  }

  @Test
  public void createWriteDropColumnFamily() throws RocksDBException {
    final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("new_cf".getBytes()));
    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
             columnFamilyHandleList)) {
      ColumnFamilyHandle tmpColumnFamilyHandle = null;
      try {
        tmpColumnFamilyHandle = db.createColumnFamily(
            new ColumnFamilyDescriptor("tmpCF".getBytes(),
                new ColumnFamilyOptions()));
        db.put(tmpColumnFamilyHandle, "key".getBytes(), "value".getBytes());
        db.dropColumnFamily(tmpColumnFamilyHandle);
        assertThat(tmpColumnFamilyHandle.isOwningHandle()).isTrue();
      } finally {
        if (tmpColumnFamilyHandle != null) {
          tmpColumnFamilyHandle.close();
        }
        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
          columnFamilyHandle.close();
        }
      }
    }
  }

  @Test
  public void createWriteDropColumnFamilies() throws RocksDBException {
    final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("new_cf".getBytes()));
    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
             columnFamilyHandleList)) {
      ColumnFamilyHandle tmpColumnFamilyHandle = null;
      ColumnFamilyHandle tmpColumnFamilyHandle2 = null;
      try {
        tmpColumnFamilyHandle = db.createColumnFamily(
            new ColumnFamilyDescriptor("tmpCF".getBytes(),
                new ColumnFamilyOptions()));
        tmpColumnFamilyHandle2 = db.createColumnFamily(
            new ColumnFamilyDescriptor("tmpCF2".getBytes(),
                new ColumnFamilyOptions()));
        db.put(tmpColumnFamilyHandle, "key".getBytes(), "value".getBytes());
        db.put(tmpColumnFamilyHandle2, "key".getBytes(), "value".getBytes());
        db.dropColumnFamilies(Arrays.asList(tmpColumnFamilyHandle, tmpColumnFamilyHandle2));
        assertThat(tmpColumnFamilyHandle.isOwningHandle()).isTrue();
        assertThat(tmpColumnFamilyHandle2.isOwningHandle()).isTrue();
      } finally {
        if (tmpColumnFamilyHandle != null) {
          tmpColumnFamilyHandle.close();
        }
        if (tmpColumnFamilyHandle2 != null) {
          tmpColumnFamilyHandle2.close();
        }
        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
          columnFamilyHandle.close();
        }
      }
    }
  }

  @Test
  public void writeBatch() throws RocksDBException {
    try (final StringAppendOperator stringAppendOperator = new StringAppendOperator();
         final ColumnFamilyOptions defaultCfOptions = new ColumnFamilyOptions()
             .setMergeOperator(stringAppendOperator)) {
      final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
              defaultCfOptions),
          new ColumnFamilyDescriptor("new_cf".getBytes()));
      final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
      try (final DBOptions options = new DBOptions()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true);
           final RocksDB db = RocksDB.open(options,
               dbFolder.getRoot().getAbsolutePath(),
               cfDescriptors, columnFamilyHandleList);
           final WriteBatch writeBatch = new WriteBatch();
           final WriteOptions writeOpt = new WriteOptions()) {
        try {
          writeBatch.put("key".getBytes(), "value".getBytes());
          writeBatch.put(db.getDefaultColumnFamily(),
              "mergeKey".getBytes(), "merge".getBytes());
          writeBatch.merge(db.getDefaultColumnFamily(), "mergeKey".getBytes(),
              "merge".getBytes());
          writeBatch.put(columnFamilyHandleList.get(1), "newcfkey".getBytes(),
              "value".getBytes());
          writeBatch.put(columnFamilyHandleList.get(1), "newcfkey2".getBytes(),
              "value2".getBytes());
          writeBatch.remove("xyz".getBytes());
          writeBatch.remove(columnFamilyHandleList.get(1), "xyz".getBytes());
          db.write(writeOpt, writeBatch);

          assertThat(db.get(columnFamilyHandleList.get(1),
              "xyz".getBytes()) == null);
          assertThat(new String(db.get(columnFamilyHandleList.get(1),
              "newcfkey".getBytes()))).isEqualTo("value");
          assertThat(new String(db.get(columnFamilyHandleList.get(1),
              "newcfkey2".getBytes()))).isEqualTo("value2");
          assertThat(new String(db.get("key".getBytes()))).isEqualTo("value");
          // check if key is merged
          assertThat(new String(db.get(db.getDefaultColumnFamily(),
              "mergeKey".getBytes()))).isEqualTo("merge,merge");
        } finally {
          for (final ColumnFamilyHandle columnFamilyHandle :
              columnFamilyHandleList) {
            columnFamilyHandle.close();
          }
        }
      }
    }
  }

  @Test
  public void iteratorOnColumnFamily() throws RocksDBException {
    final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("new_cf".getBytes()));
    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath(),
             cfDescriptors, columnFamilyHandleList)) {
      try {

        db.put(columnFamilyHandleList.get(1), "newcfkey".getBytes(),
            "value".getBytes());
        db.put(columnFamilyHandleList.get(1), "newcfkey2".getBytes(),
            "value2".getBytes());
        try (final RocksIterator rocksIterator =
                 db.newIterator(columnFamilyHandleList.get(1))) {
          rocksIterator.seekToFirst();
          Map<String, String> refMap = new HashMap<>();
          refMap.put("newcfkey", "value");
          refMap.put("newcfkey2", "value2");
          int i = 0;
          while (rocksIterator.isValid()) {
            i++;
            assertThat(refMap.get(new String(rocksIterator.key()))).
                isEqualTo(new String(rocksIterator.value()));
            rocksIterator.next();
          }
          assertThat(i).isEqualTo(2);
        }
      } finally {
        for (final ColumnFamilyHandle columnFamilyHandle :
            columnFamilyHandleList) {
          columnFamilyHandle.close();
        }
      }
    }
  }

  @Test
  public void multiGet() throws RocksDBException {
    final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("new_cf".getBytes()));
    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath(),
             cfDescriptors, columnFamilyHandleList)) {
      try {
        db.put(columnFamilyHandleList.get(0), "key".getBytes(),
            "value".getBytes());
        db.put(columnFamilyHandleList.get(1), "newcfkey".getBytes(),
            "value".getBytes());

        final List<byte[]> keys = Arrays.asList(new byte[][]{
            "key".getBytes(), "newcfkey".getBytes()
        });
        Map<byte[], byte[]> retValues = db.multiGet(columnFamilyHandleList,
            keys);
        assertThat(retValues.size()).isEqualTo(2);
        assertThat(new String(retValues.get(keys.get(0))))
            .isEqualTo("value");
        assertThat(new String(retValues.get(keys.get(1))))
            .isEqualTo("value");
        retValues = db.multiGet(new ReadOptions(), columnFamilyHandleList,
            keys);
        assertThat(retValues.size()).isEqualTo(2);
        assertThat(new String(retValues.get(keys.get(0))))
            .isEqualTo("value");
        assertThat(new String(retValues.get(keys.get(1))))
            .isEqualTo("value");
      } finally {
        for (final ColumnFamilyHandle columnFamilyHandle :
            columnFamilyHandleList) {
          columnFamilyHandle.close();
        }
      }
    }
  }

  @Test
  public void multiGetAsList() throws RocksDBException {
    final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("new_cf".getBytes()));
    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath(),
             cfDescriptors, columnFamilyHandleList)) {
      try {
        db.put(columnFamilyHandleList.get(0), "key".getBytes(),
            "value".getBytes());
        db.put(columnFamilyHandleList.get(1), "newcfkey".getBytes(),
            "value".getBytes());

        final List<byte[]> keys = Arrays.asList(new byte[][]{
            "key".getBytes(), "newcfkey".getBytes()
        });
        List<byte[]> retValues = db.multiGetAsList(columnFamilyHandleList,
            keys);
        assertThat(retValues.size()).isEqualTo(2);
        assertThat(new String(retValues.get(0)))
            .isEqualTo("value");
        assertThat(new String(retValues.get(1)))
            .isEqualTo("value");
        retValues = db.multiGetAsList(new ReadOptions(), columnFamilyHandleList,
            keys);
        assertThat(retValues.size()).isEqualTo(2);
        assertThat(new String(retValues.get(0)))
            .isEqualTo("value");
        assertThat(new String(retValues.get(1)))
            .isEqualTo("value");
      } finally {
        for (final ColumnFamilyHandle columnFamilyHandle :
            columnFamilyHandleList) {
          columnFamilyHandle.close();
        }
      }
    }
  }

  @Test
  public void properties() throws RocksDBException {
    final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("new_cf".getBytes()));
    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath(),
             cfDescriptors, columnFamilyHandleList)) {
      try {
        assertThat(db.getProperty("rocksdb.estimate-num-keys")).
            isNotNull();
        assertThat(db.getLongProperty(columnFamilyHandleList.get(0),
            "rocksdb.estimate-num-keys")).isGreaterThanOrEqualTo(0);
        assertThat(db.getProperty("rocksdb.stats")).isNotNull();
        assertThat(db.getProperty(columnFamilyHandleList.get(0),
            "rocksdb.sstables")).isNotNull();
        assertThat(db.getProperty(columnFamilyHandleList.get(1),
            "rocksdb.estimate-num-keys")).isNotNull();
        assertThat(db.getProperty(columnFamilyHandleList.get(1),
            "rocksdb.stats")).isNotNull();
        assertThat(db.getProperty(columnFamilyHandleList.get(1),
            "rocksdb.sstables")).isNotNull();
        assertThat(db.getAggregatedLongProperty("rocksdb.estimate-num-keys")).
            isNotNull();
        assertThat(db.getAggregatedLongProperty("rocksdb.estimate-num-keys")).
            isGreaterThanOrEqualTo(0);
      } finally {
        for (final ColumnFamilyHandle columnFamilyHandle :
            columnFamilyHandleList) {
          columnFamilyHandle.close();
        }
      }
    }
  }


  @Test
  public void iterators() throws RocksDBException {
    final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("new_cf".getBytes()));
    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
             columnFamilyHandleList)) {
      List<RocksIterator> iterators = null;
      try {
        iterators = db.newIterators(columnFamilyHandleList);
        assertThat(iterators.size()).isEqualTo(2);
        RocksIterator iter = iterators.get(0);
        iter.seekToFirst();
        final Map<String, String> defRefMap = new HashMap<>();
        defRefMap.put("dfkey1", "dfvalue");
        defRefMap.put("key", "value");
        while (iter.isValid()) {
          assertThat(defRefMap.get(new String(iter.key()))).
              isEqualTo(new String(iter.value()));
          iter.next();
        }
        // iterate over new_cf key/value pairs
        final Map<String, String> cfRefMap = new HashMap<>();
        cfRefMap.put("newcfkey", "value");
        cfRefMap.put("newcfkey2", "value2");
        iter = iterators.get(1);
        iter.seekToFirst();
        while (iter.isValid()) {
          assertThat(cfRefMap.get(new String(iter.key()))).
              isEqualTo(new String(iter.value()));
          iter.next();
        }
      } finally {
        if (iterators != null) {
          for (final RocksIterator rocksIterator : iterators) {
            rocksIterator.close();
          }
        }
        for (final ColumnFamilyHandle columnFamilyHandle :
            columnFamilyHandleList) {
          columnFamilyHandle.close();
        }
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void failPutDisposedCF() throws RocksDBException {
    final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("new_cf".getBytes()));
    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath(),
             cfDescriptors, columnFamilyHandleList)) {
      try {
        db.dropColumnFamily(columnFamilyHandleList.get(1));
        db.put(columnFamilyHandleList.get(1), "key".getBytes(),
            "value".getBytes());
      } finally {
        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
          columnFamilyHandle.close();
        }
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void failRemoveDisposedCF() throws RocksDBException {
    final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("new_cf".getBytes()));
    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath(),
             cfDescriptors, columnFamilyHandleList)) {
      try {
        db.dropColumnFamily(columnFamilyHandleList.get(1));
        db.remove(columnFamilyHandleList.get(1), "key".getBytes());
      } finally {
        for (final ColumnFamilyHandle columnFamilyHandle :
            columnFamilyHandleList) {
          columnFamilyHandle.close();
        }
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void failGetDisposedCF() throws RocksDBException {
    final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("new_cf".getBytes()));
    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
             columnFamilyHandleList)) {
      try {
        db.dropColumnFamily(columnFamilyHandleList.get(1));
        db.get(columnFamilyHandleList.get(1), "key".getBytes());
      } finally {
        for (final ColumnFamilyHandle columnFamilyHandle :
            columnFamilyHandleList) {
          columnFamilyHandle.close();
        }
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void failMultiGetWithoutCorrectNumberOfCF() throws RocksDBException {
    final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
        new ColumnFamilyDescriptor("new_cf".getBytes()));
    final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();
    try (final DBOptions options = new DBOptions()
        .setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath(), cfDescriptors,
             columnFamilyHandleList)) {
      try {
        final List<byte[]> keys = new ArrayList<>();
        keys.add("key".getBytes());
        keys.add("newcfkey".getBytes());
        final List<ColumnFamilyHandle> cfCustomList = new ArrayList<>();
        db.multiGet(cfCustomList, keys);

      } finally {
        for (final ColumnFamilyHandle columnFamilyHandle :
            columnFamilyHandleList) {
          columnFamilyHandle.close();
        }
      }
    }
  }

  @Test
  public void testByteCreateFolumnFamily() throws RocksDBException {

    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())
    ) {
      final byte[] b0 = new byte[]{(byte) 0x00};
      final byte[] b1 = new byte[]{(byte) 0x01};
      final byte[] b2 = new byte[]{(byte) 0x02};
      ColumnFamilyHandle cf1 = null, cf2 = null, cf3 = null;
      try {
        cf1 = db.createColumnFamily(new ColumnFamilyDescriptor(b0));
        cf2 = db.createColumnFamily(new ColumnFamilyDescriptor(b1));
        final List<byte[]> families = RocksDB.listColumnFamilies(options,
            dbFolder.getRoot().getAbsolutePath());
        assertThat(families).contains("default".getBytes(), b0, b1);
        cf3 = db.createColumnFamily(new ColumnFamilyDescriptor(b2));
      } finally {
        if (cf1 != null) {
          cf1.close();
        }
        if (cf2 != null) {
          cf2.close();
        }
        if (cf3 != null) {
          cf3.close();
        }
      }
    }
  }

  @Test
  public void testCFNamesWithZeroBytes() throws RocksDBException {
    ColumnFamilyHandle cf1 = null, cf2 = null;
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath());
    ) {
      try {
        final byte[] b0 = new byte[]{0, 0};
        final byte[] b1 = new byte[]{0, 1};
        cf1 = db.createColumnFamily(new ColumnFamilyDescriptor(b0));
        cf2 = db.createColumnFamily(new ColumnFamilyDescriptor(b1));
        final List<byte[]> families = RocksDB.listColumnFamilies(options,
            dbFolder.getRoot().getAbsolutePath());
        assertThat(families).contains("default".getBytes(), b0, b1);
      } finally {
        if (cf1 != null) {
          cf1.close();
        }
        if (cf2 != null) {
          cf2.close();
        }
      }
    }
  }

  @Test
  public void testCFNameSimplifiedChinese() throws RocksDBException {
    ColumnFamilyHandle columnFamilyHandle = null;
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath());
    ) {
      try {
        final String simplifiedChinese = "\u7b80\u4f53\u5b57";
        columnFamilyHandle = db.createColumnFamily(
            new ColumnFamilyDescriptor(simplifiedChinese.getBytes()));

        final List<byte[]> families = RocksDB.listColumnFamilies(options,
            dbFolder.getRoot().getAbsolutePath());
        assertThat(families).contains("default".getBytes(),
            simplifiedChinese.getBytes());
      } finally {
        if (columnFamilyHandle != null) {
          columnFamilyHandle.close();
        }
      }
    }
  }
}
