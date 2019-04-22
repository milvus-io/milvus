// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

public class StatisticsTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void statsLevel() throws RocksDBException {
    final Statistics statistics = new Statistics();
    statistics.setStatsLevel(StatsLevel.ALL);
    assertThat(statistics.statsLevel()).isEqualTo(StatsLevel.ALL);
  }

  @Test
  public void getTickerCount() throws RocksDBException {
    try (final Statistics statistics = new Statistics();
         final Options opt = new Options()
             .setStatistics(statistics)
             .setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())) {

      final byte[] key = "some-key".getBytes(StandardCharsets.UTF_8);
      final byte[] value = "some-value".getBytes(StandardCharsets.UTF_8);

      db.put(key, value);
      for(int i = 0; i < 10; i++) {
        db.get(key);
      }

      assertThat(statistics.getTickerCount(TickerType.BYTES_READ)).isGreaterThan(0);
    }
  }

  @Test
  public void getAndResetTickerCount() throws RocksDBException {
    try (final Statistics statistics = new Statistics();
         final Options opt = new Options()
             .setStatistics(statistics)
             .setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())) {

      final byte[] key = "some-key".getBytes(StandardCharsets.UTF_8);
      final byte[] value = "some-value".getBytes(StandardCharsets.UTF_8);

      db.put(key, value);
      for(int i = 0; i < 10; i++) {
        db.get(key);
      }

      final long read = statistics.getAndResetTickerCount(TickerType.BYTES_READ);
      assertThat(read).isGreaterThan(0);

      final long readAfterReset = statistics.getTickerCount(TickerType.BYTES_READ);
      assertThat(readAfterReset).isLessThan(read);
    }
  }

  @Test
  public void getHistogramData() throws RocksDBException {
    try (final Statistics statistics = new Statistics();
         final Options opt = new Options()
             .setStatistics(statistics)
             .setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())) {

      final byte[] key = "some-key".getBytes(StandardCharsets.UTF_8);
      final byte[] value = "some-value".getBytes(StandardCharsets.UTF_8);

      db.put(key, value);
      for(int i = 0; i < 10; i++) {
        db.get(key);
      }

      final HistogramData histogramData = statistics.getHistogramData(HistogramType.BYTES_PER_READ);
      assertThat(histogramData).isNotNull();
      assertThat(histogramData.getAverage()).isGreaterThan(0);
      assertThat(histogramData.getMedian()).isGreaterThan(0);
      assertThat(histogramData.getPercentile95()).isGreaterThan(0);
      assertThat(histogramData.getPercentile99()).isGreaterThan(0);
      assertThat(histogramData.getStandardDeviation()).isEqualTo(0.00);
      assertThat(histogramData.getMax()).isGreaterThan(0);
      assertThat(histogramData.getCount()).isGreaterThan(0);
      assertThat(histogramData.getSum()).isGreaterThan(0);
      assertThat(histogramData.getMin()).isGreaterThan(0);
    }
  }

  @Test
  public void getHistogramString() throws RocksDBException {
    try (final Statistics statistics = new Statistics();
         final Options opt = new Options()
             .setStatistics(statistics)
             .setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())) {

      final byte[] key = "some-key".getBytes(StandardCharsets.UTF_8);
      final byte[] value = "some-value".getBytes(StandardCharsets.UTF_8);

      for(int i = 0; i < 10; i++) {
        db.put(key, value);
      }

      assertThat(statistics.getHistogramString(HistogramType.BYTES_PER_WRITE)).isNotNull();
    }
  }

  @Test
  public void reset() throws RocksDBException {
    try (final Statistics statistics = new Statistics();
         final Options opt = new Options()
             .setStatistics(statistics)
             .setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())) {

      final byte[] key = "some-key".getBytes(StandardCharsets.UTF_8);
      final byte[] value = "some-value".getBytes(StandardCharsets.UTF_8);

      db.put(key, value);
      for(int i = 0; i < 10; i++) {
        db.get(key);
      }

      final long read = statistics.getTickerCount(TickerType.BYTES_READ);
      assertThat(read).isGreaterThan(0);

      statistics.reset();

      final long readAfterReset = statistics.getTickerCount(TickerType.BYTES_READ);
      assertThat(readAfterReset).isLessThan(read);
    }
  }

  @Test
  public void ToString() throws RocksDBException {
    try (final Statistics statistics = new Statistics();
         final Options opt = new Options()
             .setStatistics(statistics)
             .setCreateIfMissing(true);
         final RocksDB db = RocksDB.open(opt,
             dbFolder.getRoot().getAbsolutePath())) {
      assertThat(statistics.toString()).isNotNull();
    }
  }
}
