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

public class FlushTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void flush() throws RocksDBException {
    try(final Options options = new Options()
        .setCreateIfMissing(true)
        .setMaxWriteBufferNumber(10)
        .setMinWriteBufferNumberToMerge(10);
        final WriteOptions wOpt = new WriteOptions()
            .setDisableWAL(true);
        final FlushOptions flushOptions = new FlushOptions()
            .setWaitForFlush(true)) {
      assertThat(flushOptions.waitForFlush()).isTrue();

      try(final RocksDB db = RocksDB.open(options,
          dbFolder.getRoot().getAbsolutePath())) {
        db.put(wOpt, "key1".getBytes(), "value1".getBytes());
        db.put(wOpt, "key2".getBytes(), "value2".getBytes());
        db.put(wOpt, "key3".getBytes(), "value3".getBytes());
        db.put(wOpt, "key4".getBytes(), "value4".getBytes());
        assertThat(db.getProperty("rocksdb.num-entries-active-mem-table"))
            .isEqualTo("4");
        db.flush(flushOptions);
        assertThat(db.getProperty("rocksdb.num-entries-active-mem-table"))
            .isEqualTo("0");
      }
    }
  }
}
