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

public class DirectComparatorTest {
  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Test
  public void directComparator() throws IOException, RocksDBException {

    final AbstractComparatorTest comparatorTest = new AbstractComparatorTest() {
      @Override
      public AbstractComparator getAscendingIntKeyComparator() {
        return new DirectComparator(new ComparatorOptions()) {

          @Override
          public String name() {
            return "test.AscendingIntKeyDirectComparator";
          }

          @Override
          public int compare(final DirectSlice a, final DirectSlice b) {
            final byte ax[] = new byte[4], bx[] = new byte[4];
            a.data().get(ax);
            b.data().get(bx);
            return compareIntKeys(ax, bx);
          }
        };
      }
    };

    // test the round-tripability of keys written and read with the DirectComparator
    comparatorTest.testRoundtrip(FileSystems.getDefault().getPath(
        dbFolder.getRoot().getAbsolutePath()));
  }
}
