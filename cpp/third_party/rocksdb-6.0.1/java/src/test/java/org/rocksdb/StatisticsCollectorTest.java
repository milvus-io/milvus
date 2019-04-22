// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Collections;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.assertj.core.api.Assertions.assertThat;

public class StatisticsCollectorTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void statisticsCollector()
      throws InterruptedException, RocksDBException {
    try (final Statistics statistics = new Statistics();
            final Options opt = new Options()
        .setStatistics(statistics)
        .setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())) {

      try(final Statistics stats = opt.statistics()) {

        final StatsCallbackMock callback = new StatsCallbackMock();
        final StatsCollectorInput statsInput =
                new StatsCollectorInput(stats, callback);

        final StatisticsCollector statsCollector = new StatisticsCollector(
                Collections.singletonList(statsInput), 100);
        statsCollector.start();

        Thread.sleep(1000);

        assertThat(callback.tickerCallbackCount).isGreaterThan(0);
        assertThat(callback.histCallbackCount).isGreaterThan(0);

        statsCollector.shutDown(1000);
      }
    }
  }
}
