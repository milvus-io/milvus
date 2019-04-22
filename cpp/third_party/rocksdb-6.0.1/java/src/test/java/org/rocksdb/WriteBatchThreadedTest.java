// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

@RunWith(Parameterized.class)
public class WriteBatchThreadedTest {

  @Parameters(name = "WriteBatchThreadedTest(threadCount={0})")
  public static Iterable<Integer> data() {
    return Arrays.asList(new Integer[]{1, 10, 50, 100});
  }

  @Parameter
  public int threadCount;

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  RocksDB db;

  @Before
  public void setUp() throws Exception {
    RocksDB.loadLibrary();
    final Options options = new Options()
        .setCreateIfMissing(true)
        .setIncreaseParallelism(32);
    db = RocksDB.open(options, dbFolder.getRoot().getAbsolutePath());
    assert (db != null);
  }

  @After
  public void tearDown() throws Exception {
    if (db != null) {
      db.close();
    }
  }

  @Test
  public void threadedWrites() throws InterruptedException, ExecutionException {
    final List<Callable<Void>> callables = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      final int offset = i * 100;
      callables.add(new Callable<Void>() {
        @Override
        public Void call() throws RocksDBException {
          try (final WriteBatch wb = new WriteBatch();
               final WriteOptions w_opt = new WriteOptions()) {
            for (int i = offset; i < offset + 100; i++) {
              wb.put(ByteBuffer.allocate(4).putInt(i).array(), "parallel rocks test".getBytes());
            }
            db.write(w_opt, wb);
          }
          return null;
        }
      });
    }

    //submit the callables
    final ExecutorService executorService =
        Executors.newFixedThreadPool(threadCount);
    try {
      final ExecutorCompletionService<Void> completionService =
          new ExecutorCompletionService<>(executorService);
      final Set<Future<Void>> futures = new HashSet<>();
      for (final Callable<Void> callable : callables) {
        futures.add(completionService.submit(callable));
      }

      while (futures.size() > 0) {
        final Future<Void> future = completionService.take();
        futures.remove(future);

        try {
          future.get();
        } catch (final ExecutionException e) {
          for (final Future<Void> f : futures) {
            f.cancel(true);
          }

          throw e;
        }
      }
    } finally {
      executorService.shutdown();
      executorService.awaitTermination(10, TimeUnit.SECONDS);
    }
  }
}
