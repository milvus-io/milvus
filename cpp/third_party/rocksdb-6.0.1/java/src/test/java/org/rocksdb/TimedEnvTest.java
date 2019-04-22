// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static java.nio.charset.StandardCharsets.UTF_8;

public class TimedEnvTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void construct() throws RocksDBException {
    try (final Env env = new TimedEnv(Env.getDefault())) {
      // no-op
    }
  }

  @Test
  public void construct_integration() throws RocksDBException {
    try (final Env env = new TimedEnv(Env.getDefault());
         final Options options = new Options()
             .setCreateIfMissing(true)
             .setEnv(env);
    ) {
      try (final RocksDB db = RocksDB.open(options, dbFolder.getRoot().getPath())) {
        db.put("key1".getBytes(UTF_8), "value1".getBytes(UTF_8));
      }
    }
  }
}
