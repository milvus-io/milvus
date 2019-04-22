//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;


public class WriteBatchWithIndexTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void readYourOwnWrites() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {

      final byte[] k1 = "key1".getBytes();
      final byte[] v1 = "value1".getBytes();
      final byte[] k2 = "key2".getBytes();
      final byte[] v2 = "value2".getBytes();

      db.put(k1, v1);
      db.put(k2, v2);

      try (final WriteBatchWithIndex wbwi = new WriteBatchWithIndex(true);
           final RocksIterator base = db.newIterator();
           final RocksIterator it = wbwi.newIteratorWithBase(base)) {
        it.seek(k1);
        assertThat(it.isValid()).isTrue();
        assertThat(it.key()).isEqualTo(k1);
        assertThat(it.value()).isEqualTo(v1);

        it.seek(k2);
        assertThat(it.isValid()).isTrue();
        assertThat(it.key()).isEqualTo(k2);
        assertThat(it.value()).isEqualTo(v2);

        //put data to the write batch and make sure we can read it.
        final byte[] k3 = "key3".getBytes();
        final byte[] v3 = "value3".getBytes();
        wbwi.put(k3, v3);
        it.seek(k3);
        assertThat(it.isValid()).isTrue();
        assertThat(it.key()).isEqualTo(k3);
        assertThat(it.value()).isEqualTo(v3);

        //update k2 in the write batch and check the value
        final byte[] v2Other = "otherValue2".getBytes();
        wbwi.put(k2, v2Other);
        it.seek(k2);
        assertThat(it.isValid()).isTrue();
        assertThat(it.key()).isEqualTo(k2);
        assertThat(it.value()).isEqualTo(v2Other);

        //delete k1 and make sure we can read back the write
        wbwi.delete(k1);
        it.seek(k1);
        assertThat(it.key()).isNotEqualTo(k1);

        //reinsert k1 and make sure we see the new value
        final byte[] v1Other = "otherValue1".getBytes();
        wbwi.put(k1, v1Other);
        it.seek(k1);
        assertThat(it.isValid()).isTrue();
        assertThat(it.key()).isEqualTo(k1);
        assertThat(it.value()).isEqualTo(v1Other);

        //single remove k3 and make sure we can read back the write
        wbwi.singleDelete(k3);
        it.seek(k3);
        assertThat(it.isValid()).isEqualTo(false);

        //reinsert k3 and make sure we see the new value
        final byte[] v3Other = "otherValue3".getBytes();
        wbwi.put(k3, v3Other);
        it.seek(k3);
        assertThat(it.isValid()).isTrue();
        assertThat(it.key()).isEqualTo(k3);
        assertThat(it.value()).isEqualTo(v3Other);
      }
    }
  }

  @Test
  public void write_writeBatchWithIndex() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {

      final byte[] k1 = "key1".getBytes();
      final byte[] v1 = "value1".getBytes();
      final byte[] k2 = "key2".getBytes();
      final byte[] v2 = "value2".getBytes();

      try (final WriteBatchWithIndex wbwi = new WriteBatchWithIndex()) {
        wbwi.put(k1, v1);
        wbwi.put(k2, v2);

        db.write(new WriteOptions(), wbwi);
      }

      assertThat(db.get(k1)).isEqualTo(v1);
      assertThat(db.get(k2)).isEqualTo(v2);
    }
  }

  @Test
  public void iterator() throws RocksDBException {
    try (final WriteBatchWithIndex wbwi = new WriteBatchWithIndex(true)) {

      final String k1 = "key1";
      final String v1 = "value1";
      final String k2 = "key2";
      final String v2 = "value2";
      final String k3 = "key3";
      final String v3 = "value3";
      final String k4 = "key4";
      final String k5 = "key5";
      final String k6 = "key6";
      final String k7 = "key7";
      final String v8 = "value8";
      final byte[] k1b = k1.getBytes(UTF_8);
      final byte[] v1b = v1.getBytes(UTF_8);
      final byte[] k2b = k2.getBytes(UTF_8);
      final byte[] v2b = v2.getBytes(UTF_8);
      final byte[] k3b = k3.getBytes(UTF_8);
      final byte[] v3b = v3.getBytes(UTF_8);
      final byte[] k4b = k4.getBytes(UTF_8);
      final byte[] k5b = k5.getBytes(UTF_8);
      final byte[] k6b = k6.getBytes(UTF_8);
      final byte[] k7b = k7.getBytes(UTF_8);
      final byte[] v8b = v8.getBytes(UTF_8);

      // add put records
      wbwi.put(k1b, v1b);
      wbwi.put(k2b, v2b);
      wbwi.put(k3b, v3b);

      // add a deletion record
      wbwi.delete(k4b);

      // add a single deletion record
      wbwi.singleDelete(k5b);

      // add a delete range record
      wbwi.deleteRange(k6b, k7b);

      // add a log record
      wbwi.putLogData(v8b);

      final WBWIRocksIterator.WriteEntry[] expected = {
          new WBWIRocksIterator.WriteEntry(WBWIRocksIterator.WriteType.PUT,
              new DirectSlice(k1), new DirectSlice(v1)),
          new WBWIRocksIterator.WriteEntry(WBWIRocksIterator.WriteType.PUT,
              new DirectSlice(k2), new DirectSlice(v2)),
          new WBWIRocksIterator.WriteEntry(WBWIRocksIterator.WriteType.PUT,
              new DirectSlice(k3), new DirectSlice(v3)),
          new WBWIRocksIterator.WriteEntry(WBWIRocksIterator.WriteType.DELETE,
              new DirectSlice(k4), DirectSlice.NONE),
          new WBWIRocksIterator.WriteEntry(WBWIRocksIterator.WriteType.SINGLE_DELETE,
              new DirectSlice(k5), DirectSlice.NONE),
          new WBWIRocksIterator.WriteEntry(WBWIRocksIterator.WriteType.DELETE_RANGE,
              new DirectSlice(k6), new DirectSlice(k7)),
      };

      try (final WBWIRocksIterator it = wbwi.newIterator()) {
        //direct access - seek to key offsets
        final int[] testOffsets = {2, 0, 3, 4, 1, 5};

        for (int i = 0; i < testOffsets.length; i++) {
          final int testOffset = testOffsets[i];
          final byte[] key = toArray(expected[testOffset].getKey().data());

          it.seek(key);
          assertThat(it.isValid()).isTrue();

          final WBWIRocksIterator.WriteEntry entry = it.entry();
          assertThat(entry).isEqualTo(expected[testOffset]);
        }

        //forward iterative access
        int i = 0;
        for (it.seekToFirst(); it.isValid(); it.next()) {
          assertThat(it.entry()).isEqualTo(expected[i++]);
        }

        //reverse iterative access
        i = expected.length - 1;
        for (it.seekToLast(); it.isValid(); it.prev()) {
          assertThat(it.entry()).isEqualTo(expected[i--]);
        }
      }
    }
  }

  @Test
  public void zeroByteTests() throws RocksDBException {
    try (final WriteBatchWithIndex wbwi = new WriteBatchWithIndex(true)) {
      final byte[] zeroByteValue = new byte[]{0, 0};
      //add zero byte value
      wbwi.put(zeroByteValue, zeroByteValue);

      final ByteBuffer buffer = ByteBuffer.allocateDirect(zeroByteValue.length);
      buffer.put(zeroByteValue);

      final WBWIRocksIterator.WriteEntry expected =
          new WBWIRocksIterator.WriteEntry(WBWIRocksIterator.WriteType.PUT,
              new DirectSlice(buffer, zeroByteValue.length),
              new DirectSlice(buffer, zeroByteValue.length));

      try (final WBWIRocksIterator it = wbwi.newIterator()) {
        it.seekToFirst();
        final WBWIRocksIterator.WriteEntry actual = it.entry();
        assertThat(actual.equals(expected)).isTrue();
        assertThat(it.entry().hashCode() == expected.hashCode()).isTrue();
      }
    }
  }

  @Test
  public void savePoints() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {
      try (final WriteBatchWithIndex wbwi = new WriteBatchWithIndex(true);
           final ReadOptions readOptions = new ReadOptions()) {
        wbwi.put("k1".getBytes(), "v1".getBytes());
        wbwi.put("k2".getBytes(), "v2".getBytes());
        wbwi.put("k3".getBytes(), "v3".getBytes());

        assertThat(getFromWriteBatchWithIndex(db, readOptions, wbwi, "k1"))
            .isEqualTo("v1");
        assertThat(getFromWriteBatchWithIndex(db, readOptions, wbwi, "k2"))
            .isEqualTo("v2");
        assertThat(getFromWriteBatchWithIndex(db, readOptions, wbwi, "k3"))
            .isEqualTo("v3");


        wbwi.setSavePoint();

        wbwi.delete("k2".getBytes());
        wbwi.put("k3".getBytes(), "v3-2".getBytes());

        assertThat(getFromWriteBatchWithIndex(db, readOptions, wbwi, "k2"))
            .isNull();
        assertThat(getFromWriteBatchWithIndex(db, readOptions, wbwi, "k3"))
            .isEqualTo("v3-2");


        wbwi.setSavePoint();

        wbwi.put("k3".getBytes(), "v3-3".getBytes());
        wbwi.put("k4".getBytes(), "v4".getBytes());

        assertThat(getFromWriteBatchWithIndex(db, readOptions, wbwi, "k3"))
            .isEqualTo("v3-3");
        assertThat(getFromWriteBatchWithIndex(db, readOptions, wbwi, "k4"))
            .isEqualTo("v4");


        wbwi.rollbackToSavePoint();

        assertThat(getFromWriteBatchWithIndex(db, readOptions, wbwi, "k2"))
            .isNull();
        assertThat(getFromWriteBatchWithIndex(db, readOptions, wbwi, "k3"))
            .isEqualTo("v3-2");
        assertThat(getFromWriteBatchWithIndex(db, readOptions, wbwi, "k4"))
            .isNull();


        wbwi.rollbackToSavePoint();

        assertThat(getFromWriteBatchWithIndex(db, readOptions, wbwi, "k1"))
            .isEqualTo("v1");
        assertThat(getFromWriteBatchWithIndex(db, readOptions, wbwi, "k2"))
            .isEqualTo("v2");
        assertThat(getFromWriteBatchWithIndex(db, readOptions, wbwi, "k3"))
            .isEqualTo("v3");
        assertThat(getFromWriteBatchWithIndex(db, readOptions, wbwi, "k4"))
            .isNull();
      }
    }
  }

  @Test
  public void restorePoints() throws RocksDBException {
    try (final WriteBatchWithIndex wbwi = new WriteBatchWithIndex()) {

      wbwi.put("k1".getBytes(UTF_8), "v1".getBytes(UTF_8));
      wbwi.put("k2".getBytes(UTF_8), "v2".getBytes(UTF_8));

      wbwi.setSavePoint();

      wbwi.put("k1".getBytes(UTF_8), "123456789".getBytes(UTF_8));
      wbwi.delete("k2".getBytes(UTF_8));

      wbwi.rollbackToSavePoint();

      try(final DBOptions options = new DBOptions()) {
        assertThat(wbwi.getFromBatch(options,"k1".getBytes(UTF_8))).isEqualTo("v1".getBytes());
        assertThat(wbwi.getFromBatch(options,"k2".getBytes(UTF_8))).isEqualTo("v2".getBytes());
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void restorePoints_withoutSavePoints() throws RocksDBException {
    try (final WriteBatchWithIndex wbwi = new WriteBatchWithIndex()) {
      wbwi.rollbackToSavePoint();
    }
  }

  @Test(expected = RocksDBException.class)
  public void restorePoints_withoutSavePoints_nested() throws RocksDBException {
    try (final WriteBatchWithIndex wbwi = new WriteBatchWithIndex()) {

      wbwi.setSavePoint();
      wbwi.rollbackToSavePoint();

      // without previous corresponding setSavePoint
      wbwi.rollbackToSavePoint();
    }
  }

  @Test
  public void popSavePoint() throws RocksDBException {
    try (final WriteBatchWithIndex wbwi = new WriteBatchWithIndex()) {

      wbwi.put("k1".getBytes(), "v1".getBytes());
      wbwi.put("k2".getBytes(), "v2".getBytes());

      wbwi.setSavePoint();

      wbwi.put("k1".getBytes(), "123456789".getBytes());
      wbwi.delete("k2".getBytes());

      wbwi.setSavePoint();

      wbwi.popSavePoint();

      wbwi.rollbackToSavePoint();

      try(final DBOptions options = new DBOptions()) {
        assertThat(wbwi.getFromBatch(options,"k1".getBytes(UTF_8))).isEqualTo("v1".getBytes());
        assertThat(wbwi.getFromBatch(options,"k2".getBytes(UTF_8))).isEqualTo("v2".getBytes());
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void popSavePoint_withoutSavePoints() throws RocksDBException {
    try (final WriteBatchWithIndex wbwi = new WriteBatchWithIndex()) {
      wbwi.popSavePoint();
    }
  }

  @Test(expected = RocksDBException.class)
  public void popSavePoint_withoutSavePoints_nested() throws RocksDBException {
    try (final WriteBatchWithIndex wbwi = new WriteBatchWithIndex()) {

      wbwi.setSavePoint();
      wbwi.popSavePoint();

      // without previous corresponding setSavePoint
      wbwi.popSavePoint();
    }
  }

  @Test
  public void maxBytes() throws RocksDBException {
    try (final WriteBatchWithIndex wbwi = new WriteBatchWithIndex()) {
      wbwi.setMaxBytes(19);

      wbwi.put("k1".getBytes(), "v1".getBytes());
    }
  }

  @Test(expected = RocksDBException.class)
  public void maxBytes_over() throws RocksDBException {
    try (final WriteBatchWithIndex wbwi = new WriteBatchWithIndex()) {
      wbwi.setMaxBytes(1);

      wbwi.put("k1".getBytes(), "v1".getBytes());
    }
  }

  @Test
  public void getWriteBatch() {
    try (final WriteBatchWithIndex wbwi = new WriteBatchWithIndex()) {

      final WriteBatch wb = wbwi.getWriteBatch();
      assertThat(wb).isNotNull();
      assertThat(wb.isOwningHandle()).isFalse();
    }
  }

  private static String getFromWriteBatchWithIndex(final RocksDB db,
      final ReadOptions readOptions, final WriteBatchWithIndex wbwi,
      final String skey) {
    final byte[] key = skey.getBytes();
    try (final RocksIterator baseIterator = db.newIterator(readOptions);
         final RocksIterator iterator = wbwi.newIteratorWithBase(baseIterator)) {
      iterator.seek(key);

      // Arrays.equals(key, iterator.key()) ensures an exact match in Rocks,
      // instead of a nearest match
      return iterator.isValid() &&
          Arrays.equals(key, iterator.key()) ?
          new String(iterator.value()) : null;
    }
  }

  @Test
  public void getFromBatch() throws RocksDBException {
    final byte[] k1 = "k1".getBytes();
    final byte[] k2 = "k2".getBytes();
    final byte[] k3 = "k3".getBytes();
    final byte[] k4 = "k4".getBytes();

    final byte[] v1 = "v1".getBytes();
    final byte[] v2 = "v2".getBytes();
    final byte[] v3 = "v3".getBytes();

    try (final WriteBatchWithIndex wbwi = new WriteBatchWithIndex(true);
         final DBOptions dbOptions = new DBOptions()) {
      wbwi.put(k1, v1);
      wbwi.put(k2, v2);
      wbwi.put(k3, v3);

      assertThat(wbwi.getFromBatch(dbOptions, k1)).isEqualTo(v1);
      assertThat(wbwi.getFromBatch(dbOptions, k2)).isEqualTo(v2);
      assertThat(wbwi.getFromBatch(dbOptions, k3)).isEqualTo(v3);
      assertThat(wbwi.getFromBatch(dbOptions, k4)).isNull();

      wbwi.delete(k2);

      assertThat(wbwi.getFromBatch(dbOptions, k2)).isNull();
    }
  }

  @Test
  public void getFromBatchAndDB() throws RocksDBException {
    final byte[] k1 = "k1".getBytes();
    final byte[] k2 = "k2".getBytes();
    final byte[] k3 = "k3".getBytes();
    final byte[] k4 = "k4".getBytes();

    final byte[] v1 = "v1".getBytes();
    final byte[] v2 = "v2".getBytes();
    final byte[] v3 = "v3".getBytes();
    final byte[] v4 = "v4".getBytes();

    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {

      db.put(k1, v1);
      db.put(k2, v2);
      db.put(k4, v4);

      try (final WriteBatchWithIndex wbwi = new WriteBatchWithIndex(true);
           final DBOptions dbOptions = new DBOptions();
           final ReadOptions readOptions = new ReadOptions()) {

        assertThat(wbwi.getFromBatch(dbOptions, k1)).isNull();
        assertThat(wbwi.getFromBatch(dbOptions, k2)).isNull();
        assertThat(wbwi.getFromBatch(dbOptions, k4)).isNull();

        wbwi.put(k3, v3);

        assertThat(wbwi.getFromBatch(dbOptions, k3)).isEqualTo(v3);

        assertThat(wbwi.getFromBatchAndDB(db, readOptions, k1)).isEqualTo(v1);
        assertThat(wbwi.getFromBatchAndDB(db, readOptions, k2)).isEqualTo(v2);
        assertThat(wbwi.getFromBatchAndDB(db, readOptions, k3)).isEqualTo(v3);
        assertThat(wbwi.getFromBatchAndDB(db, readOptions, k4)).isEqualTo(v4);

        wbwi.delete(k4);

        assertThat(wbwi.getFromBatchAndDB(db, readOptions, k4)).isNull();
      }
    }
  }
  private byte[] toArray(final ByteBuffer buf) {
    final byte[] ary = new byte[buf.remaining()];
    buf.get(ary);
    return ary;
  }

  @Test
  public void deleteRange() throws RocksDBException {
    try (final RocksDB db = RocksDB.open(dbFolder.getRoot().getAbsolutePath());
         final WriteOptions wOpt = new WriteOptions()) {
      db.put("key1".getBytes(), "value".getBytes());
      db.put("key2".getBytes(), "12345678".getBytes());
      db.put("key3".getBytes(), "abcdefg".getBytes());
      db.put("key4".getBytes(), "xyz".getBytes());
      assertThat(db.get("key1".getBytes())).isEqualTo("value".getBytes());
      assertThat(db.get("key2".getBytes())).isEqualTo("12345678".getBytes());
      assertThat(db.get("key3".getBytes())).isEqualTo("abcdefg".getBytes());
      assertThat(db.get("key4".getBytes())).isEqualTo("xyz".getBytes());

      WriteBatch batch = new WriteBatch();
      batch.deleteRange("key2".getBytes(), "key4".getBytes());
      db.write(new WriteOptions(), batch);

      assertThat(db.get("key1".getBytes())).isEqualTo("value".getBytes());
      assertThat(db.get("key2".getBytes())).isNull();
      assertThat(db.get("key3".getBytes())).isNull();
      assertThat(db.get("key4".getBytes())).isEqualTo("xyz".getBytes());
    }
  }
}
