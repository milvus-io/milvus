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

public class RocksMemEnvTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Test
  public void memEnvFillAndReopen() throws RocksDBException {

    final byte[][] keys = {
        "aaa".getBytes(),
        "bbb".getBytes(),
        "ccc".getBytes()
    };

    final byte[][] values = {
        "foo".getBytes(),
        "bar".getBytes(),
        "baz".getBytes()
    };

    try (final Env env = new RocksMemEnv(Env.getDefault());
         final Options options = new Options()
             .setCreateIfMissing(true)
             .setEnv(env);
         final FlushOptions flushOptions = new FlushOptions()
             .setWaitForFlush(true);
    ) {
      try (final RocksDB db = RocksDB.open(options, "dir/db")) {
        // write key/value pairs using MemEnv
        for (int i = 0; i < keys.length; i++) {
          db.put(keys[i], values[i]);
        }

        // read key/value pairs using MemEnv
        for (int i = 0; i < keys.length; i++) {
          assertThat(db.get(keys[i])).isEqualTo(values[i]);
        }

        // Check iterator access
        try (final RocksIterator iterator = db.newIterator()) {
          iterator.seekToFirst();
          for (int i = 0; i < keys.length; i++) {
            assertThat(iterator.isValid()).isTrue();
            assertThat(iterator.key()).isEqualTo(keys[i]);
            assertThat(iterator.value()).isEqualTo(values[i]);
            iterator.next();
          }
          // reached end of database
          assertThat(iterator.isValid()).isFalse();
        }

        // flush
        db.flush(flushOptions);

        // read key/value pairs after flush using MemEnv
        for (int i = 0; i < keys.length; i++) {
          assertThat(db.get(keys[i])).isEqualTo(values[i]);
        }
      }

      options.setCreateIfMissing(false);

      // After reopen the values shall still be in the mem env.
      // as long as the env is not freed.
      try (final RocksDB db = RocksDB.open(options, "dir/db")) {
        // read key/value pairs using MemEnv
        for (int i = 0; i < keys.length; i++) {
          assertThat(db.get(keys[i])).isEqualTo(values[i]);
        }
      }
    }
  }

  @Test
  public void multipleDatabaseInstances() throws RocksDBException {
    // db - keys
    final byte[][] keys = {
        "aaa".getBytes(),
        "bbb".getBytes(),
        "ccc".getBytes()
    };
    // otherDb - keys
    final byte[][] otherKeys = {
        "111".getBytes(),
        "222".getBytes(),
        "333".getBytes()
    };
    // values
    final byte[][] values = {
        "foo".getBytes(),
        "bar".getBytes(),
        "baz".getBytes()
    };

    try (final Env env = new RocksMemEnv(Env.getDefault());
         final Options options = new Options()
             .setCreateIfMissing(true)
             .setEnv(env);
         final RocksDB db = RocksDB.open(options, "dir/db");
         final RocksDB otherDb = RocksDB.open(options, "dir/otherDb")
    ) {
      // write key/value pairs using MemEnv
      // to db and to otherDb.
      for (int i = 0; i < keys.length; i++) {
        db.put(keys[i], values[i]);
        otherDb.put(otherKeys[i], values[i]);
      }

      // verify key/value pairs after flush using MemEnv
      for (int i = 0; i < keys.length; i++) {
        // verify db
        assertThat(db.get(otherKeys[i])).isNull();
        assertThat(db.get(keys[i])).isEqualTo(values[i]);

        // verify otherDb
        assertThat(otherDb.get(keys[i])).isNull();
        assertThat(otherDb.get(otherKeys[i])).isEqualTo(values[i]);
      }
    }
  }

  @Test(expected = RocksDBException.class)
  public void createIfMissingFalse() throws RocksDBException {
    try (final Env env = new RocksMemEnv(Env.getDefault());
         final Options options = new Options()
             .setCreateIfMissing(false)
             .setEnv(env);
         final RocksDB db = RocksDB.open(options, "db/dir")) {
      // shall throw an exception because db dir does not
      // exist.
    }
  }
}
