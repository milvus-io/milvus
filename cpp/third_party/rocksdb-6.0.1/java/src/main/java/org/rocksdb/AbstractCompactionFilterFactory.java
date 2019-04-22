// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Each compaction will create a new {@link AbstractCompactionFilter}
 * allowing the application to know about different compactions
 *
 * @param <T> The concrete type of the compaction filter
 */
public abstract class AbstractCompactionFilterFactory<T extends AbstractCompactionFilter<?>>
    extends RocksCallbackObject {

  public AbstractCompactionFilterFactory() {
    super(null);
  }

  @Override
  protected long initializeNative(final long... nativeParameterHandles) {
    return createNewCompactionFilterFactory0();
  }

  /**
   * Called from JNI, see compaction_filter_factory_jnicallback.cc
   *
   * @param fullCompaction {@link AbstractCompactionFilter.Context#fullCompaction}
   * @param manualCompaction {@link AbstractCompactionFilter.Context#manualCompaction}
   *
   * @return native handle of the CompactionFilter
   */
  private long createCompactionFilter(final boolean fullCompaction,
      final boolean manualCompaction) {
    final T filter = createCompactionFilter(
        new AbstractCompactionFilter.Context(fullCompaction, manualCompaction));

    // CompactionFilterFactory::CreateCompactionFilter returns a std::unique_ptr
    // which therefore has ownership of the underlying native object
    filter.disOwnNativeHandle();

    return filter.nativeHandle_;
  }

  /**
   * Create a new compaction filter
   *
   * @param context The context describing the need for a new compaction filter
   *
   * @return A new instance of {@link AbstractCompactionFilter}
   */
  public abstract T createCompactionFilter(
      final AbstractCompactionFilter.Context context);

  /**
   * A name which identifies this compaction filter
   *
   * The name will be printed to the LOG file on start up for diagnosis
   *
   * @return name which identifies this compaction filter.
   */
  public abstract String name();

  /**
   * We override {@link RocksCallbackObject#disposeInternal()}
   * as disposing of a rocksdb::AbstractCompactionFilterFactory requires
   * a slightly different approach as it is a std::shared_ptr
   */
  @Override
  protected void disposeInternal() {
    disposeInternal(nativeHandle_);
  }

  private native long createNewCompactionFilterFactory0();
  private native void disposeInternal(final long handle);
}
