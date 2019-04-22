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
import org.rocksdb.util.CapturingWriteBatchHandler;
import org.rocksdb.util.CapturingWriteBatchHandler.Event;
import org.rocksdb.util.WriteBatchGetter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.rocksdb.util.CapturingWriteBatchHandler.Action.*;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This class mimics the db/write_batch_test.cc
 * in the c++ rocksdb library.
 */
public class WriteBatchTest {
  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void emptyWriteBatch() {
    try (final WriteBatch batch = new WriteBatch()) {
      assertThat(batch.count()).isEqualTo(0);
    }
  }

  @Test
  public void multipleBatchOperations()
      throws RocksDBException {

    final byte[] foo = "foo".getBytes(UTF_8);
    final byte[] bar = "bar".getBytes(UTF_8);
    final byte[] box = "box".getBytes(UTF_8);
    final byte[] baz = "baz".getBytes(UTF_8);
    final byte[] boo = "boo".getBytes(UTF_8);
    final byte[] hoo = "hoo".getBytes(UTF_8);
    final byte[] hello = "hello".getBytes(UTF_8);

    try (final WriteBatch batch = new WriteBatch()) {
      batch.put(foo, bar);
      batch.delete(box);
      batch.put(baz, boo);
      batch.merge(baz, hoo);
      batch.singleDelete(foo);
      batch.deleteRange(baz, foo);
      batch.putLogData(hello);

      try(final CapturingWriteBatchHandler handler =
              new CapturingWriteBatchHandler()) {
        batch.iterate(handler);

        assertThat(handler.getEvents().size()).isEqualTo(7);

        assertThat(handler.getEvents().get(0)).isEqualTo(new Event(PUT, foo, bar));
        assertThat(handler.getEvents().get(1)).isEqualTo(new Event(DELETE, box, null));
        assertThat(handler.getEvents().get(2)).isEqualTo(new Event(PUT, baz, boo));
        assertThat(handler.getEvents().get(3)).isEqualTo(new Event(MERGE, baz, hoo));
        assertThat(handler.getEvents().get(4)).isEqualTo(new Event(SINGLE_DELETE, foo, null));
        assertThat(handler.getEvents().get(5)).isEqualTo(new Event(DELETE_RANGE, baz, foo));
        assertThat(handler.getEvents().get(6)).isEqualTo(new Event(LOG, null, hello));
      }
    }
  }

  @Test
  public void testAppendOperation()
      throws RocksDBException {
    try (final WriteBatch b1 = new WriteBatch();
         final WriteBatch b2 = new WriteBatch()) {
      WriteBatchTestInternalHelper.setSequence(b1, 200);
      WriteBatchTestInternalHelper.setSequence(b2, 300);
      WriteBatchTestInternalHelper.append(b1, b2);
      assertThat(getContents(b1).length).isEqualTo(0);
      assertThat(b1.count()).isEqualTo(0);
      b2.put("a".getBytes(UTF_8), "va".getBytes(UTF_8));
      WriteBatchTestInternalHelper.append(b1, b2);
      assertThat("Put(a, va)@200".equals(new String(getContents(b1),
          UTF_8)));
      assertThat(b1.count()).isEqualTo(1);
      b2.clear();
      b2.put("b".getBytes(UTF_8), "vb".getBytes(UTF_8));
      WriteBatchTestInternalHelper.append(b1, b2);
      assertThat(("Put(a, va)@200" +
          "Put(b, vb)@201")
          .equals(new String(getContents(b1), UTF_8)));
      assertThat(b1.count()).isEqualTo(2);
      b2.delete("foo".getBytes(UTF_8));
      WriteBatchTestInternalHelper.append(b1, b2);
      assertThat(("Put(a, va)@200" +
          "Put(b, vb)@202" +
          "Put(b, vb)@201" +
          "Delete(foo)@203")
          .equals(new String(getContents(b1), UTF_8)));
      assertThat(b1.count()).isEqualTo(4);
    }
  }

  @Test
  public void blobOperation()
      throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {
      batch.put("k1".getBytes(UTF_8), "v1".getBytes(UTF_8));
      batch.put("k2".getBytes(UTF_8), "v2".getBytes(UTF_8));
      batch.put("k3".getBytes(UTF_8), "v3".getBytes(UTF_8));
      batch.putLogData("blob1".getBytes(UTF_8));
      batch.delete("k2".getBytes(UTF_8));
      batch.putLogData("blob2".getBytes(UTF_8));
      batch.merge("foo".getBytes(UTF_8), "bar".getBytes(UTF_8));
      assertThat(batch.count()).isEqualTo(5);
      assertThat(("Merge(foo, bar)@4" +
          "Put(k1, v1)@0" +
          "Delete(k2)@3" +
          "Put(k2, v2)@1" +
          "Put(k3, v3)@2")
          .equals(new String(getContents(batch), UTF_8)));
    }
  }

  @Test
  public void savePoints()
      throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {
      batch.put("k1".getBytes(UTF_8), "v1".getBytes(UTF_8));
      batch.put("k2".getBytes(UTF_8), "v2".getBytes(UTF_8));
      batch.put("k3".getBytes(UTF_8), "v3".getBytes(UTF_8));

      assertThat(getFromWriteBatch(batch, "k1")).isEqualTo("v1");
      assertThat(getFromWriteBatch(batch, "k2")).isEqualTo("v2");
      assertThat(getFromWriteBatch(batch, "k3")).isEqualTo("v3");

      batch.setSavePoint();

      batch.delete("k2".getBytes(UTF_8));
      batch.put("k3".getBytes(UTF_8), "v3-2".getBytes(UTF_8));

      assertThat(getFromWriteBatch(batch, "k2")).isNull();
      assertThat(getFromWriteBatch(batch, "k3")).isEqualTo("v3-2");


      batch.setSavePoint();

      batch.put("k3".getBytes(UTF_8), "v3-3".getBytes(UTF_8));
      batch.put("k4".getBytes(UTF_8), "v4".getBytes(UTF_8));

      assertThat(getFromWriteBatch(batch, "k3")).isEqualTo("v3-3");
      assertThat(getFromWriteBatch(batch, "k4")).isEqualTo("v4");


      batch.rollbackToSavePoint();

      assertThat(getFromWriteBatch(batch, "k2")).isNull();
      assertThat(getFromWriteBatch(batch, "k3")).isEqualTo("v3-2");
      assertThat(getFromWriteBatch(batch, "k4")).isNull();


      batch.rollbackToSavePoint();

      assertThat(getFromWriteBatch(batch, "k1")).isEqualTo("v1");
      assertThat(getFromWriteBatch(batch, "k2")).isEqualTo("v2");
      assertThat(getFromWriteBatch(batch, "k3")).isEqualTo("v3");
      assertThat(getFromWriteBatch(batch, "k4")).isNull();
    }
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

  @Test
  public void restorePoints() throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {

      batch.put("k1".getBytes(), "v1".getBytes());
      batch.put("k2".getBytes(), "v2".getBytes());

      batch.setSavePoint();

      batch.put("k1".getBytes(), "123456789".getBytes());
      batch.delete("k2".getBytes());

      batch.rollbackToSavePoint();

      try(final CapturingWriteBatchHandler handler = new CapturingWriteBatchHandler()) {
        batch.iterate(handler);

        assertThat(handler.getEvents().size()).isEqualTo(2);
        assertThat(handler.getEvents().get(0)).isEqualTo(new Event(PUT, "k1".getBytes(), "v1".getBytes()));
        assertThat(handler.getEvents().get(1)).isEqualTo(new Event(PUT, "k2".getBytes(), "v2".getBytes()));
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void restorePoints_withoutSavePoints() throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {
      batch.rollbackToSavePoint();
    }
  }

  @Test(expected = RocksDBException.class)
  public void restorePoints_withoutSavePoints_nested() throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {

      batch.setSavePoint();
      batch.rollbackToSavePoint();

      // without previous corresponding setSavePoint
      batch.rollbackToSavePoint();
    }
  }

  @Test
  public void popSavePoint() throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {

      batch.put("k1".getBytes(), "v1".getBytes());
      batch.put("k2".getBytes(), "v2".getBytes());

      batch.setSavePoint();

      batch.put("k1".getBytes(), "123456789".getBytes());
      batch.delete("k2".getBytes());

      batch.setSavePoint();

      batch.popSavePoint();

      batch.rollbackToSavePoint();

      try(final CapturingWriteBatchHandler handler = new CapturingWriteBatchHandler()) {
        batch.iterate(handler);

        assertThat(handler.getEvents().size()).isEqualTo(2);
        assertThat(handler.getEvents().get(0)).isEqualTo(new Event(PUT, "k1".getBytes(), "v1".getBytes()));
        assertThat(handler.getEvents().get(1)).isEqualTo(new Event(PUT, "k2".getBytes(), "v2".getBytes()));
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void popSavePoint_withoutSavePoints() throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {
      batch.popSavePoint();
    }
  }

  @Test(expected = RocksDBException.class)
  public void popSavePoint_withoutSavePoints_nested() throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {

      batch.setSavePoint();
      batch.popSavePoint();

      // without previous corresponding setSavePoint
      batch.popSavePoint();
    }
  }

  @Test
  public void maxBytes() throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {
      batch.setMaxBytes(19);

      batch.put("k1".getBytes(), "v1".getBytes());
    }
  }

  @Test(expected = RocksDBException.class)
  public void maxBytes_over() throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {
      batch.setMaxBytes(1);

      batch.put("k1".getBytes(), "v1".getBytes());
    }
  }

  @Test
  public void data() throws RocksDBException {
    try (final WriteBatch batch1 = new WriteBatch()) {
      batch1.delete("k0".getBytes());
      batch1.put("k1".getBytes(), "v1".getBytes());
      batch1.put("k2".getBytes(), "v2".getBytes());
      batch1.put("k3".getBytes(), "v3".getBytes());
      batch1.putLogData("log1".getBytes());
      batch1.merge("k2".getBytes(), "v22".getBytes());
      batch1.delete("k3".getBytes());

      final byte[] serialized = batch1.data();

      try(final WriteBatch batch2 = new WriteBatch(serialized)) {
        assertThat(batch2.count()).isEqualTo(batch1.count());

        try(final CapturingWriteBatchHandler handler1 = new CapturingWriteBatchHandler()) {
          batch1.iterate(handler1);

          try (final CapturingWriteBatchHandler handler2 = new CapturingWriteBatchHandler()) {
            batch2.iterate(handler2);

            assertThat(handler1.getEvents().equals(handler2.getEvents())).isTrue();
          }
        }
      }
    }
  }

  @Test
  public void dataSize() throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {
      batch.put("k1".getBytes(), "v1".getBytes());

      assertThat(batch.getDataSize()).isEqualTo(19);
    }
  }

  @Test
  public void hasPut() throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {
      assertThat(batch.hasPut()).isFalse();

      batch.put("k1".getBytes(), "v1".getBytes());

      assertThat(batch.hasPut()).isTrue();
    }
  }

  @Test
  public void hasDelete() throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {
      assertThat(batch.hasDelete()).isFalse();

      batch.delete("k1".getBytes());

      assertThat(batch.hasDelete()).isTrue();
    }
  }

  @Test
  public void hasSingleDelete() throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {
      assertThat(batch.hasSingleDelete()).isFalse();

      batch.singleDelete("k1".getBytes());

      assertThat(batch.hasSingleDelete()).isTrue();
    }
  }

  @Test
  public void hasDeleteRange() throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {
      assertThat(batch.hasDeleteRange()).isFalse();

      batch.deleteRange("k1".getBytes(), "k2".getBytes());

      assertThat(batch.hasDeleteRange()).isTrue();
    }
  }

  @Test
  public void hasBeginPrepareRange() throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {
      assertThat(batch.hasBeginPrepare()).isFalse();
    }
  }

  @Test
  public void hasEndrepareRange() throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {
      assertThat(batch.hasEndPrepare()).isFalse();
    }
  }

  @Test
  public void hasCommit() throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {
      assertThat(batch.hasCommit()).isFalse();
    }
  }

  @Test
  public void hasRollback() throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {
      assertThat(batch.hasRollback()).isFalse();
    }
  }

  @Test
  public void walTerminationPoint() throws RocksDBException {
    try (final WriteBatch batch = new WriteBatch()) {
      WriteBatch.SavePoint walTerminationPoint = batch.getWalTerminationPoint();
      assertThat(walTerminationPoint.isCleared()).isTrue();

      batch.put("k1".getBytes(UTF_8), "v1".getBytes(UTF_8));

      batch.markWalTerminationPoint();

      walTerminationPoint = batch.getWalTerminationPoint();
      assertThat(walTerminationPoint.getSize()).isEqualTo(19);
      assertThat(walTerminationPoint.getCount()).isEqualTo(1);
      assertThat(walTerminationPoint.getContentFlags()).isEqualTo(2);
    }
  }

  @Test
  public void getWriteBatch() {
    try (final WriteBatch batch = new WriteBatch()) {
      assertThat(batch.getWriteBatch()).isEqualTo(batch);
    }
  }

  static byte[] getContents(final WriteBatch wb) {
    return getContents(wb.nativeHandle_);
  }

  static String getFromWriteBatch(final WriteBatch wb, final String key)
      throws RocksDBException {
    final WriteBatchGetter getter =
        new WriteBatchGetter(key.getBytes(UTF_8));
    wb.iterate(getter);
    if(getter.getValue() != null) {
      return new String(getter.getValue(), UTF_8);
    } else {
      return null;
    }
  }

  private static native byte[] getContents(final long writeBatchHandle);
}

/**
 * Package-private class which provides java api to access
 * c++ WriteBatchInternal.
 */
class WriteBatchTestInternalHelper {
  static void setSequence(final WriteBatch wb, final long sn) {
    setSequence(wb.nativeHandle_, sn);
  }

  static long sequence(final WriteBatch wb) {
    return sequence(wb.nativeHandle_);
  }

  static void append(final WriteBatch wb1, final WriteBatch wb2) {
    append(wb1.nativeHandle_, wb2.nativeHandle_);
  }

  private static native void setSequence(final long writeBatchHandle,
      final long sn);

  private static native long sequence(final long writeBatchHandle);

  private static native void append(final long writeBatchHandle1,
      final long writeBatchHandle2);
}
