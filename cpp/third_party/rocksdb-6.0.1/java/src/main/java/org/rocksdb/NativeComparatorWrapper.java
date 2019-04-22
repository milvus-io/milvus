// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * A simple abstraction to allow a Java class to wrap a custom comparator
 * implemented in C++.
 *
 * The native comparator must directly extend rocksdb::Comparator.
 */
public abstract class NativeComparatorWrapper
    extends AbstractComparator<Slice> {

  @Override
  final ComparatorType getComparatorType() {
    return ComparatorType.JAVA_NATIVE_COMPARATOR_WRAPPER;
  }

  @Override
  public final String name() {
    throw new IllegalStateException("This should not be called. " +
        "Implementation is in Native code");
  }

  @Override
  public final int compare(final Slice s1, final Slice s2) {
    throw new IllegalStateException("This should not be called. " +
        "Implementation is in Native code");
  }

  @Override
  public final String findShortestSeparator(final String start, final Slice limit) {
    throw new IllegalStateException("This should not be called. " +
        "Implementation is in Native code");
  }

  @Override
  public final String findShortSuccessor(final String key) {
    throw new IllegalStateException("This should not be called. " +
        "Implementation is in Native code");
  }

  /**
   * We override {@link RocksCallbackObject#disposeInternal()}
   * as disposing of a native rocksd::Comparator extension requires
   * a slightly different approach as it is not really a RocksCallbackObject
   */
  @Override
  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }

  private native void disposeInternal(final long handle);
}
