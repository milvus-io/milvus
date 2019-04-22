// Copyright (c) 2016, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * RocksMutableObject is an implementation of {@link AbstractNativeReference}
 * whose reference to the underlying native C++ object can change.
 *
 * <p>The use of {@code RocksMutableObject} should be kept to a minimum, as it
 * has synchronization overheads and introduces complexity. Instead it is
 * recommended to use {@link RocksObject} where possible.</p>
 */
public abstract class RocksMutableObject extends AbstractNativeReference {

  /**
   * An mutable reference to the value of the C++ pointer pointing to some
   * underlying native RocksDB C++ object.
   */
  private long nativeHandle_;
  private boolean owningHandle_;

  protected RocksMutableObject() {
  }

  protected RocksMutableObject(final long nativeHandle) {
    this.nativeHandle_ = nativeHandle;
    this.owningHandle_ = true;
  }

  /**
   * Closes the existing handle, and changes the handle to the new handle
   *
   * @param newNativeHandle The C++ pointer to the new native object
   * @param owningNativeHandle true if we own the new native object
   */
  public synchronized void resetNativeHandle(final long newNativeHandle,
      final boolean owningNativeHandle) {
    close();
    setNativeHandle(newNativeHandle, owningNativeHandle);
  }

  /**
   * Sets the handle (C++ pointer) of the underlying C++ native object
   *
   * @param nativeHandle The C++ pointer to the native object
   * @param owningNativeHandle true if we own the native object
   */
  public synchronized void setNativeHandle(final long nativeHandle,
      final boolean owningNativeHandle) {
    this.nativeHandle_ = nativeHandle;
    this.owningHandle_ = owningNativeHandle;
  }

  @Override
  protected synchronized boolean isOwningHandle() {
    return this.owningHandle_;
  }

  /**
   * Gets the value of the C++ pointer pointing to the underlying
   * native C++ object
   *
   * @return the pointer value for the native object
   */
  protected synchronized long getNativeHandle() {
    assert (this.nativeHandle_ != 0);
    return this.nativeHandle_;
  }

  @Override
  public synchronized final void close() {
    if (isOwningHandle()) {
      disposeInternal();
      this.owningHandle_ = false;
      this.nativeHandle_ = 0;
    }
  }

  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }

  protected abstract void disposeInternal(final long handle);
}
