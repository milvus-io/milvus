// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SliceTest {

  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Test
  public void slice() {
    try (final Slice slice = new Slice("testSlice")) {
      assertThat(slice.empty()).isFalse();
      assertThat(slice.size()).isEqualTo(9);
      assertThat(slice.data()).isEqualTo("testSlice".getBytes());
    }

    try (final Slice otherSlice = new Slice("otherSlice".getBytes())) {
      assertThat(otherSlice.data()).isEqualTo("otherSlice".getBytes());
    }

    try (final Slice thirdSlice = new Slice("otherSlice".getBytes(), 5)) {
      assertThat(thirdSlice.data()).isEqualTo("Slice".getBytes());
    }
  }

  @Test
  public void sliceClear() {
    try (final Slice slice = new Slice("abc")) {
      assertThat(slice.toString()).isEqualTo("abc");
      slice.clear();
      assertThat(slice.toString()).isEmpty();
      slice.clear();  // make sure we don't double-free
    }
  }

  @Test
  public void sliceRemovePrefix() {
    try (final Slice slice = new Slice("abc")) {
      assertThat(slice.toString()).isEqualTo("abc");
      slice.removePrefix(1);
      assertThat(slice.toString()).isEqualTo("bc");
    }
  }

  @Test
  public void sliceEquals() {
    try (final Slice slice = new Slice("abc");
         final Slice slice2 = new Slice("abc")) {
      assertThat(slice.equals(slice2)).isTrue();
      assertThat(slice.hashCode() == slice2.hashCode()).isTrue();
    }
  }

  @Test
  public void sliceStartWith() {
    try (final Slice slice = new Slice("matchpoint");
         final Slice match = new Slice("mat");
         final Slice noMatch = new Slice("nomatch")) {
      assertThat(slice.startsWith(match)).isTrue();
      assertThat(slice.startsWith(noMatch)).isFalse();
    }
  }

  @Test
  public void sliceToString() {
    try (final Slice slice = new Slice("stringTest")) {
      assertThat(slice.toString()).isEqualTo("stringTest");
      assertThat(slice.toString(true)).isNotEqualTo("");
    }
  }
}
