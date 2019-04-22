// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * <p>Base class for slices which will receive
 * byte[] based access to the underlying data.</p>
 *
 * <p>byte[] backed slices typically perform better with
 * small keys and values. When using larger keys and
 * values consider using {@link org.rocksdb.DirectSlice}</p>
 */
public class Slice extends AbstractSlice<byte[]> {

  /**
   * Indicates whether we have to free the memory pointed to by the Slice
   */
  private volatile boolean cleared;
  private volatile long internalBufferOffset = 0;

  /**
   * <p>Called from JNI to construct a new Java Slice
   * without an underlying C++ object set
   * at creation time.</p>
   *
   * <p>Note: You should be aware that
   * {@see org.rocksdb.RocksObject#disOwnNativeHandle()} is intentionally
   * called from the default Slice constructor, and that it is marked as
   * private. This is so that developers cannot construct their own default
   * Slice objects (at present). As developers cannot construct their own
   * Slice objects through this, they are not creating underlying C++ Slice
   * objects, and so there is nothing to free (dispose) from Java.</p>
   */
  @SuppressWarnings("unused")
  private Slice() {
    super();
  }

  /**
   * <p>Package-private Slice constructor which is used to construct
   * Slice instances from C++ side. As the reference to this
   * object is also managed from C++ side the handle will be disowned.</p>
   *
   * @param nativeHandle address of native instance.
   */
  Slice(final long nativeHandle) {
    this(nativeHandle, false);
  }

  /**
   * <p>Package-private Slice constructor which is used to construct
   * Slice instances using a handle. </p>
   *
   * @param nativeHandle address of native instance.
   * @param owningNativeHandle true if the Java side owns the memory pointed to
   *     by this reference, false if ownership belongs to the C++ side
   */
  Slice(final long nativeHandle, final boolean owningNativeHandle) {
    super();
    setNativeHandle(nativeHandle, owningNativeHandle);
  }

  /**
   * <p>Constructs a slice where the data is taken from
   * a String.</p>
   *
   * @param str String value.
   */
  public Slice(final String str) {
    super(createNewSliceFromString(str));
  }

  /**
   * <p>Constructs a slice where the data is a copy of
   * the byte array from a specific offset.</p>
   *
   * @param data byte array.
   * @param offset offset within the byte array.
   */
  public Slice(final byte[] data, final int offset) {
    super(createNewSlice0(data, offset));
  }

  /**
   * <p>Constructs a slice where the data is a copy of
   * the byte array.</p>
   *
   * @param data byte array.
   */
  public Slice(final byte[] data) {
    super(createNewSlice1(data));
  }

  @Override
  public void clear() {
    clear0(getNativeHandle(), !cleared, internalBufferOffset);
    cleared = true;
  }

  @Override
  public void removePrefix(final int n) {
    removePrefix0(getNativeHandle(), n);
    this.internalBufferOffset += n;
  }

  /**
   * <p>Deletes underlying C++ slice pointer
   * and any buffered data.</p>
   *
   * <p>
   * Note that this function should be called only after all
   * RocksDB instances referencing the slice are closed.
   * Otherwise an undefined behavior will occur.</p>
   */
  @Override
  protected void disposeInternal() {
    final long nativeHandle = getNativeHandle();
    if(!cleared) {
      disposeInternalBuf(nativeHandle, internalBufferOffset);
    }
    super.disposeInternal(nativeHandle);
  }

  @Override protected final native byte[] data0(long handle);
  private native static long createNewSlice0(final byte[] data,
      final int length);
  private native static long createNewSlice1(final byte[] data);
  private native void clear0(long handle, boolean internalBuffer,
      long internalBufferOffset);
  private native void removePrefix0(long handle, int length);
  private native void disposeInternalBuf(final long handle,
      long internalBufferOffset);
}
