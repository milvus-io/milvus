// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Arrays;
import java.util.Random;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.assertj.core.api.Assertions.assertThat;

public class ReadOptionsTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void altConstructor() {
    try (final ReadOptions opt = new ReadOptions(true, true)) {
      assertThat(opt.verifyChecksums()).isTrue();
      assertThat(opt.fillCache()).isTrue();
    }
  }

  @Test
  public void copyConstructor() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setVerifyChecksums(false);
      opt.setFillCache(false);
      opt.setIterateUpperBound(buildRandomSlice());
      opt.setIterateLowerBound(buildRandomSlice());
      try (final ReadOptions other = new ReadOptions(opt)) {
        assertThat(opt.verifyChecksums()).isEqualTo(other.verifyChecksums());
        assertThat(opt.fillCache()).isEqualTo(other.fillCache());
        assertThat(Arrays.equals(opt.iterateUpperBound().data(), other.iterateUpperBound().data())).isTrue();
        assertThat(Arrays.equals(opt.iterateLowerBound().data(), other.iterateLowerBound().data())).isTrue();
      }
    }
  }

  @Test
  public void verifyChecksum() {
    try (final ReadOptions opt = new ReadOptions()) {
      final Random rand = new Random();
      final boolean boolValue = rand.nextBoolean();
      opt.setVerifyChecksums(boolValue);
      assertThat(opt.verifyChecksums()).isEqualTo(boolValue);
    }
  }

  @Test
  public void fillCache() {
    try (final ReadOptions opt = new ReadOptions()) {
      final Random rand = new Random();
      final boolean boolValue = rand.nextBoolean();
      opt.setFillCache(boolValue);
      assertThat(opt.fillCache()).isEqualTo(boolValue);
    }
  }

  @Test
  public void tailing() {
    try (final ReadOptions opt = new ReadOptions()) {
      final Random rand = new Random();
      final boolean boolValue = rand.nextBoolean();
      opt.setTailing(boolValue);
      assertThat(opt.tailing()).isEqualTo(boolValue);
    }
  }

  @Test
  public void snapshot() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setSnapshot(null);
      assertThat(opt.snapshot()).isNull();
    }
  }

  @Test
  public void readTier() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setReadTier(ReadTier.BLOCK_CACHE_TIER);
      assertThat(opt.readTier()).isEqualTo(ReadTier.BLOCK_CACHE_TIER);
    }
  }

  @Test
  public void managed() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setManaged(true);
      assertThat(opt.managed()).isTrue();
    }
  }

  @Test
  public void totalOrderSeek() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setTotalOrderSeek(true);
      assertThat(opt.totalOrderSeek()).isTrue();
    }
  }

  @Test
  public void prefixSameAsStart() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setPrefixSameAsStart(true);
      assertThat(opt.prefixSameAsStart()).isTrue();
    }
  }

  @Test
  public void pinData() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setPinData(true);
      assertThat(opt.pinData()).isTrue();
    }
  }

  @Test
  public void backgroundPurgeOnIteratorCleanup() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setBackgroundPurgeOnIteratorCleanup(true);
      assertThat(opt.backgroundPurgeOnIteratorCleanup()).isTrue();
    }
  }

  @Test
  public void readaheadSize() {
    try (final ReadOptions opt = new ReadOptions()) {
      final Random rand = new Random();
      final long longValue = rand.nextLong();
      opt.setReadaheadSize(longValue);
      assertThat(opt.readaheadSize()).isEqualTo(longValue);
    }
  }

  @Test
  public void ignoreRangeDeletions() {
    try (final ReadOptions opt = new ReadOptions()) {
      opt.setIgnoreRangeDeletions(true);
      assertThat(opt.ignoreRangeDeletions()).isTrue();
    }
  }

  @Test
  public void iterateUpperBound() {
    try (final ReadOptions opt = new ReadOptions()) {
      Slice upperBound = buildRandomSlice();
      opt.setIterateUpperBound(upperBound);
      assertThat(Arrays.equals(upperBound.data(), opt.iterateUpperBound().data())).isTrue();
    }
  }

  @Test
  public void iterateUpperBoundNull() {
    try (final ReadOptions opt = new ReadOptions()) {
      assertThat(opt.iterateUpperBound()).isNull();
    }
  }

  @Test
  public void iterateLowerBound() {
    try (final ReadOptions opt = new ReadOptions()) {
      Slice lowerBound = buildRandomSlice();
      opt.setIterateLowerBound(lowerBound);
      assertThat(Arrays.equals(lowerBound.data(), opt.iterateLowerBound().data())).isTrue();
    }
  }

  @Test
  public void iterateLowerBoundNull() {
    try (final ReadOptions opt = new ReadOptions()) {
      assertThat(opt.iterateLowerBound()).isNull();
    }
  }

  @Test
  public void tableFilter() {
    try (final ReadOptions opt = new ReadOptions();
         final AbstractTableFilter allTablesFilter = new AllTablesFilter()) {
      opt.setTableFilter(allTablesFilter);
    }
  }

  @Test
  public void iterStartSeqnum() {
    try (final ReadOptions opt = new ReadOptions()) {
      assertThat(opt.iterStartSeqnum()).isEqualTo(0);

      opt.setIterStartSeqnum(10);
      assertThat(opt.iterStartSeqnum()).isEqualTo(10);
    }
  }

  @Test
  public void failSetVerifyChecksumUninitialized() {
    try (final ReadOptions readOptions =
             setupUninitializedReadOptions(exception)) {
      readOptions.setVerifyChecksums(true);
    }
  }

  @Test
  public void failVerifyChecksumUninitialized() {
    try (final ReadOptions readOptions =
             setupUninitializedReadOptions(exception)) {
      readOptions.verifyChecksums();
    }
  }

  @Test
  public void failSetFillCacheUninitialized() {
    try (final ReadOptions readOptions =
             setupUninitializedReadOptions(exception)) {
      readOptions.setFillCache(true);
    }
  }

  @Test
  public void failFillCacheUninitialized() {
    try (final ReadOptions readOptions =
             setupUninitializedReadOptions(exception)) {
      readOptions.fillCache();
    }
  }

  @Test
  public void failSetTailingUninitialized() {
    try (final ReadOptions readOptions =
             setupUninitializedReadOptions(exception)) {
      readOptions.setTailing(true);
    }
  }

  @Test
  public void failTailingUninitialized() {
    try (final ReadOptions readOptions =
             setupUninitializedReadOptions(exception)) {
      readOptions.tailing();
    }
  }

  @Test
  public void failSetSnapshotUninitialized() {
    try (final ReadOptions readOptions =
             setupUninitializedReadOptions(exception)) {
      readOptions.setSnapshot(null);
    }
  }

  @Test
  public void failSnapshotUninitialized() {
    try (final ReadOptions readOptions =
             setupUninitializedReadOptions(exception)) {
      readOptions.snapshot();
    }
  }

  @Test
  public void failSetIterateUpperBoundUninitialized() {
    try (final ReadOptions readOptions =
             setupUninitializedReadOptions(exception)) {
      readOptions.setIterateUpperBound(null);
    }
  }

  @Test
  public void failIterateUpperBoundUninitialized() {
    try (final ReadOptions readOptions =
             setupUninitializedReadOptions(exception)) {
      readOptions.iterateUpperBound();
    }
  }

  @Test
  public void failSetIterateLowerBoundUninitialized() {
    try (final ReadOptions readOptions =
             setupUninitializedReadOptions(exception)) {
      readOptions.setIterateLowerBound(null);
    }
  }

  @Test
  public void failIterateLowerBoundUninitialized() {
    try (final ReadOptions readOptions =
             setupUninitializedReadOptions(exception)) {
      readOptions.iterateLowerBound();
    }
  }

  private ReadOptions setupUninitializedReadOptions(
      ExpectedException exception) {
    final ReadOptions readOptions = new ReadOptions();
    readOptions.close();
    exception.expect(AssertionError.class);
    return readOptions;
  }

  private Slice buildRandomSlice() {
    final Random rand = new Random();
    byte[] sliceBytes = new byte[rand.nextInt(100) + 1];
    rand.nextBytes(sliceBytes);
    return new Slice(sliceBytes);
  }

  private static class AllTablesFilter extends AbstractTableFilter {
    @Override
    public boolean filter(final TableProperties tableProperties) {
      return true;
    }
  }
}
