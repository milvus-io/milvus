// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.nio.ByteBuffer;

/**
 * Base class for slices which will receive direct
 * ByteBuffer based access to the underlying data.
 *
 * ByteBuffer backed slices typically perform better with
 * larger keys and values. When using smaller keys and
 * values consider using @see org.rocksdb.Slice
 */
public class DirectSlice extends AbstractSlice<ByteBuffer> {
  public final static DirectSlice NONE = new DirectSlice();

  /**
   * Indicates whether we have to free the memory pointed to by the Slice
   */
  private final boolean internalBuffer;
  private volatile boolean cleared = false;
  private volatile long internalBufferOffset = 0;

  /**
   * Called from JNI to construct a new Java DirectSlice
   * without an underlying C++ object set
   * at creation time.
   *
   * Note: You should be aware that it is intentionally marked as
   * package-private. This is so that developers cannot construct their own
   * default DirectSlice objects (at present). As developers cannot construct
   * their own DirectSlice objects through this, they are not creating
   * underlying C++ DirectSlice objects, and so there is nothing to free
   * (dispose) from Java.
   */
  DirectSlice() {
    super();
    this.internalBuffer = false;
  }

  /**
   * Constructs a slice
   * where the data is taken from
   * a String.
   *
   * @param str The string
   */
  public DirectSlice(final String str) {
    super(createNewSliceFromString(str));
    this.internalBuffer = true;
  }

  /**
   * Constructs a slice where the data is
   * read from the provided
   * ByteBuffer up to a certain length
   *
   * @param data The buffer containing the data
   * @param length The length of the data to use for the slice
   */
  public DirectSlice(final ByteBuffer data, final int length) {
    super(createNewDirectSlice0(ensureDirect(data), length));
    this.internalBuffer = false;
  }

  /**
   * Constructs a slice where the data is
   * read from the provided
   * ByteBuffer
   *
   * @param data The bugger containing the data
   */
  public DirectSlice(final ByteBuffer data) {
    super(createNewDirectSlice1(ensureDirect(data)));
    this.internalBuffer = false;
  }

  private static ByteBuffer ensureDirect(final ByteBuffer data) {
    if(!data.isDirect()) {
      throw new IllegalArgumentException("The ByteBuffer must be direct");
    }
    return data;
  }

  /**
   * Retrieves the byte at a specific offset
   * from the underlying data
   *
   * @param offset The (zero-based) offset of the byte to retrieve
   *
   * @return the requested byte
   */
  public byte get(final int offset) {
    return get0(getNativeHandle(), offset);
  }

  @Override
  public void clear() {
    clear0(getNativeHandle(), !cleared && internalBuffer, internalBufferOffset);
    cleared = true;
  }

  @Override
  public void removePrefix(final int n) {
    removePrefix0(getNativeHandle(), n);
    this.internalBufferOffset += n;
  }

  @Override
  protected void disposeInternal() {
    final long nativeHandle = getNativeHandle();
    if(!cleared && internalBuffer) {
      disposeInternalBuf(nativeHandle, internalBufferOffset);
    }
    disposeInternal(nativeHandle);
  }

  private native static long createNewDirectSlice0(final ByteBuffer data,
      final int length);
  private native static long createNewDirectSlice1(final ByteBuffer data);
  @Override protected final native ByteBuffer data0(long handle);
  private native byte get0(long handle, int offset);
  private native void clear0(long handle, boolean internalBuffer,
      long internalBufferOffset);
  private native void removePrefix0(long handle, int length);
  private native void disposeInternalBuf(final long handle,
      long internalBufferOffset);
}
