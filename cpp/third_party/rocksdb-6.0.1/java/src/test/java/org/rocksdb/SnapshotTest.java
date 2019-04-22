// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;

public class SnapshotTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void snapshots() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {
      db.put("key".getBytes(), "value".getBytes());
      // Get new Snapshot of database
      try (final Snapshot snapshot = db.getSnapshot()) {
        assertThat(snapshot.getSequenceNumber()).isGreaterThan(0);
        assertThat(snapshot.getSequenceNumber()).isEqualTo(1);
        try (final ReadOptions readOptions = new ReadOptions()) {
          // set snapshot in ReadOptions
          readOptions.setSnapshot(snapshot);

          // retrieve key value pair
          assertThat(new String(db.get("key".getBytes()))).
              isEqualTo("value");
          // retrieve key value pair created before
          // the snapshot was made
          assertThat(new String(db.get(readOptions,
              "key".getBytes()))).isEqualTo("value");
          // add new key/value pair
          db.put("newkey".getBytes(), "newvalue".getBytes());
          // using no snapshot the latest db entries
          // will be taken into account
          assertThat(new String(db.get("newkey".getBytes()))).
              isEqualTo("newvalue");
          // snapshopot was created before newkey
          assertThat(db.get(readOptions, "newkey".getBytes())).
              isNull();
          // Retrieve snapshot from read options
          try (final Snapshot sameSnapshot = readOptions.snapshot()) {
            readOptions.setSnapshot(sameSnapshot);
            // results must be the same with new Snapshot
            // instance using the same native pointer
            assertThat(new String(db.get(readOptions,
                "key".getBytes()))).isEqualTo("value");
            // update key value pair to newvalue
            db.put("key".getBytes(), "newvalue".getBytes());
            // read with previously created snapshot will
            // read previous version of key value pair
            assertThat(new String(db.get(readOptions,
                "key".getBytes()))).isEqualTo("value");
            // read for newkey using the snapshot must be
            // null
            assertThat(db.get(readOptions, "newkey".getBytes())).
                isNull();
            // setting null to snapshot in ReadOptions leads
            // to no Snapshot being used.
            readOptions.setSnapshot(null);
            assertThat(new String(db.get(readOptions,
                "newkey".getBytes()))).isEqualTo("newvalue");
            // release Snapshot
            db.releaseSnapshot(snapshot);
          }
        }
      }
    }
  }

  @Test
  public void iteratorWithSnapshot() throws RocksDBException {
    try (final Options options = new Options().setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {
      db.put("key".getBytes(), "value".getBytes());

      // Get new Snapshot of database
      // set snapshot in ReadOptions
      try (final Snapshot snapshot = db.getSnapshot();
           final ReadOptions readOptions =
               new ReadOptions().setSnapshot(snapshot)) {
        db.put("key2".getBytes(), "value2".getBytes());

        // iterate over current state of db
        try (final RocksIterator iterator = db.newIterator()) {
          iterator.seekToFirst();
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.key()).isEqualTo("key".getBytes());
          iterator.next();
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.key()).isEqualTo("key2".getBytes());
          iterator.next();
          assertThat(iterator.isValid()).isFalse();
        }

        // iterate using a snapshot
        try (final RocksIterator snapshotIterator =
                 db.newIterator(readOptions)) {
          snapshotIterator.seekToFirst();
          assertThat(snapshotIterator.isValid()).isTrue();
          assertThat(snapshotIterator.key()).isEqualTo("key".getBytes());
          snapshotIterator.next();
          assertThat(snapshotIterator.isValid()).isFalse();
        }

        // release Snapshot
        db.releaseSnapshot(snapshot);
      }
    }
  }

  @Test
  public void iteratorWithSnapshotOnColumnFamily() throws RocksDBException {
    try (final Options options = new Options()
        .setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())) {

      db.put("key".getBytes(), "value".getBytes());

      // Get new Snapshot of database
      // set snapshot in ReadOptions
      try (final Snapshot snapshot = db.getSnapshot();
           final ReadOptions readOptions = new ReadOptions()
               .setSnapshot(snapshot)) {
        db.put("key2".getBytes(), "value2".getBytes());

        // iterate over current state of column family
        try (final RocksIterator iterator = db.newIterator(
            db.getDefaultColumnFamily())) {
          iterator.seekToFirst();
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.key()).isEqualTo("key".getBytes());
          iterator.next();
          assertThat(iterator.isValid()).isTrue();
          assertThat(iterator.key()).isEqualTo("key2".getBytes());
          iterator.next();
          assertThat(iterator.isValid()).isFalse();
        }

        // iterate using a snapshot on default column family
        try (final RocksIterator snapshotIterator = db.newIterator(
            db.getDefaultColumnFamily(), readOptions)) {
          snapshotIterator.seekToFirst();
          assertThat(snapshotIterator.isValid()).isTrue();
          assertThat(snapshotIterator.key()).isEqualTo("key".getBytes());
          snapshotIterator.next();
          assertThat(snapshotIterator.isValid()).isFalse();

          // release Snapshot
          db.releaseSnapshot(snapshot);
        }
      }
    }
  }
}
