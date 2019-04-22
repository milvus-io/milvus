// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

import org.junit.ClassRule;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class DirectSliceTest {
  @ClassRule
  public static final RocksMemoryResource rocksMemoryResource =
      new RocksMemoryResource();

  @Test
  public void directSlice() {
    try(final DirectSlice directSlice = new DirectSlice("abc");
        final DirectSlice otherSlice = new DirectSlice("abc")) {
      assertThat(directSlice.toString()).isEqualTo("abc");
      // clear first slice
      directSlice.clear();
      assertThat(directSlice.toString()).isEmpty();
      // get first char in otherslice
      assertThat(otherSlice.get(0)).isEqualTo("a".getBytes()[0]);
      // remove prefix
      otherSlice.removePrefix(1);
      assertThat(otherSlice.toString()).isEqualTo("bc");
    }
  }

  @Test
  public void directSliceWithByteBuffer() {
    final byte[] data = "Some text".getBytes();
    final ByteBuffer buffer = ByteBuffer.allocateDirect(data.length + 1);
    buffer.put(data);
    buffer.put(data.length, (byte)0);

    try(final DirectSlice directSlice = new DirectSlice(buffer)) {
      assertThat(directSlice.toString()).isEqualTo("Some text");
    }
  }

  @Test
  public void directSliceWithByteBufferAndLength() {
    final byte[] data = "Some text".getBytes();
    final ByteBuffer buffer = ByteBuffer.allocateDirect(data.length);
    buffer.put(data);
    try(final DirectSlice directSlice = new DirectSlice(buffer, 4)) {
      assertThat(directSlice.toString()).isEqualTo("Some");
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void directSliceInitWithoutDirectAllocation() {
    final byte[] data = "Some text".getBytes();
    final ByteBuffer buffer = ByteBuffer.wrap(data);
    try(final DirectSlice directSlice = new DirectSlice(buffer)) {
      //no-op
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void directSlicePrefixInitWithoutDirectAllocation() {
    final byte[] data = "Some text".getBytes();
    final ByteBuffer buffer = ByteBuffer.wrap(data);
    try(final DirectSlice directSlice = new DirectSlice(buffer, 4)) {
      //no-op
    }
  }

  @Test
  public void directSliceClear() {
    try(final DirectSlice directSlice = new DirectSlice("abc")) {
      assertThat(directSlice.toString()).isEqualTo("abc");
      directSlice.clear();
      assertThat(directSlice.toString()).isEmpty();
      directSlice.clear();  // make sure we don't double-free
    }
  }

  @Test
  public void directSliceRemovePrefix() {
    try(final DirectSlice directSlice = new DirectSlice("abc")) {
      assertThat(directSlice.toString()).isEqualTo("abc");
      directSlice.removePrefix(1);
      assertThat(directSlice.toString()).isEqualTo("bc");
    }
  }
}
