// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.FileSystems;

import static org.assertj.core.api.Assertions.assertThat;

public class ComparatorTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
     public void javaComparator() throws IOException, RocksDBException {

    final AbstractComparatorTest comparatorTest = new AbstractComparatorTest() {
      @Override
      public AbstractComparator getAscendingIntKeyComparator() {
        return new Comparator(new ComparatorOptions()) {

          @Override
          public String name() {
            return "test.AscendingIntKeyComparator";
          }

          @Override
          public int compare(final Slice a, final Slice b) {
            return compareIntKeys(a.data(), b.data());
          }
        };
      }
    };

    // test the round-tripability of keys written and read with the Comparator
    comparatorTest.testRoundtrip(FileSystems.getDefault().getPath(
        dbFolder.getRoot().getAbsolutePath()));
  }

  @Test
  public void javaComparatorCf() throws IOException, RocksDBException {

    final AbstractComparatorTest comparatorTest = new AbstractComparatorTest() {
      @Override
      public AbstractComparator getAscendingIntKeyComparator() {
        return new Comparator(new ComparatorOptions()) {

          @Override
          public String name() {
            return "test.AscendingIntKeyComparator";
          }

          @Override
          public int compare(final Slice a, final Slice b) {
            return compareIntKeys(a.data(), b.data());
          }
        };
      }
    };

    // test the round-tripability of keys written and read with the Comparator
    comparatorTest.testRoundtripCf(FileSystems.getDefault().getPath(
        dbFolder.getRoot().getAbsolutePath()));
  }

  @Test
  public void builtinForwardComparator()
      throws RocksDBException {
    try (final Options options = new Options()
          .setCreateIfMissing(true)
          .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
         final RocksDB rocksDb = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())
    ) {
      rocksDb.put("abc1".getBytes(), "abc1".getBytes());
      rocksDb.put("abc2".getBytes(), "abc2".getBytes());
      rocksDb.put("abc3".getBytes(), "abc3".getBytes());

      try(final RocksIterator rocksIterator = rocksDb.newIterator()) {
        // Iterate over keys using a iterator
        rocksIterator.seekToFirst();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc1".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc1".getBytes());
        rocksIterator.next();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc2".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc2".getBytes());
        rocksIterator.next();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc3".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc3".getBytes());
        rocksIterator.next();
        assertThat(rocksIterator.isValid()).isFalse();
        // Get last one
        rocksIterator.seekToLast();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc3".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc3".getBytes());
        // Seek for abc
        rocksIterator.seek("abc".getBytes());
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc1".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc1".getBytes());
      }
    }
  }

  @Test
  public void builtinReverseComparator()
      throws RocksDBException {
    try (final Options options = new Options()
      .setCreateIfMissing(true)
      .setComparator(BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR);
         final RocksDB rocksDb = RocksDB.open(options,
             dbFolder.getRoot().getAbsolutePath())
    ) {

      rocksDb.put("abc1".getBytes(), "abc1".getBytes());
      rocksDb.put("abc2".getBytes(), "abc2".getBytes());
      rocksDb.put("abc3".getBytes(), "abc3".getBytes());

      try (final RocksIterator rocksIterator = rocksDb.newIterator()) {
        // Iterate over keys using a iterator
        rocksIterator.seekToFirst();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc3".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc3".getBytes());
        rocksIterator.next();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc2".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc2".getBytes());
        rocksIterator.next();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc1".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc1".getBytes());
        rocksIterator.next();
        assertThat(rocksIterator.isValid()).isFalse();
        // Get last one
        rocksIterator.seekToLast();
        assertThat(rocksIterator.isValid()).isTrue();
        assertThat(rocksIterator.key()).isEqualTo(
            "abc1".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc1".getBytes());
        // Will be invalid because abc is after abc1
        rocksIterator.seek("abc".getBytes());
        assertThat(rocksIterator.isValid()).isFalse();
        // Will be abc3 because the next one after abc999
        // is abc3
        rocksIterator.seek("abc999".getBytes());
        assertThat(rocksIterator.key()).isEqualTo(
            "abc3".getBytes());
        assertThat(rocksIterator.value()).isEqualTo(
            "abc3".getBytes());
      }
    }
  }

  @Test
  public void builtinComparatorEnum(){
    assertThat(BuiltinComparator.BYTEWISE_COMPARATOR.ordinal())
        .isEqualTo(0);
    assertThat(
        BuiltinComparator.REVERSE_BYTEWISE_COMPARATOR.ordinal())
        .isEqualTo(1);
    assertThat(BuiltinComparator.values().length).isEqualTo(2);
    assertThat(BuiltinComparator.valueOf("BYTEWISE_COMPARATOR")).
        isEqualTo(BuiltinComparator.BYTEWISE_COMPARATOR);
  }
}
