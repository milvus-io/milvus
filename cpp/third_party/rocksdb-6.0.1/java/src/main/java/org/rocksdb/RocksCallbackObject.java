// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * RocksCallbackObject is similar to {@link RocksObject} but varies
 * in its construction as it is designed for Java objects which have functions
 * which are called from C++ via JNI.
 *
 * RocksCallbackObject is the base-class any RocksDB classes that acts as a
 * callback from some underlying underlying native C++ {@code rocksdb} object.
 *
 * The use of {@code RocksObject} should always be preferred over
 * {@link RocksCallbackObject} if callbacks are not required.
 */
public abstract class RocksCallbackObject extends
    AbstractImmutableNativeReference {

  protected final long nativeHandle_;

  protected RocksCallbackObject(final long... nativeParameterHandles) {
    super(true);
    this.nativeHandle_ = initializeNative(nativeParameterHandles);
  }

  /**
   * Construct the Native C++ object which will callback
   * to our object methods
   *
   * @param nativeParameterHandles An array of native handles for any parameter
   *     objects that are needed during construction
   *
   * @return The native handle of the C++ object which will callback to us
   */
  protected abstract long initializeNative(
      final long... nativeParameterHandles);

  /**
   * Deletes underlying C++ native callback object pointer
   */
  @Override
  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }

  private native void disposeInternal(final long handle);
}
