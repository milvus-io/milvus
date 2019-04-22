// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public class OptimisticTransactionOptions extends RocksObject
    implements TransactionalOptions {

  public OptimisticTransactionOptions() {
    super(newOptimisticTransactionOptions());
  }

  @Override
  public boolean isSetSnapshot() {
    assert(isOwningHandle());
    return isSetSnapshot(nativeHandle_);
  }

  @Override
  public OptimisticTransactionOptions setSetSnapshot(
      final boolean setSnapshot) {
    assert(isOwningHandle());
    setSetSnapshot(nativeHandle_, setSnapshot);
    return this;
  }

  /**
   * Should be set if the DB has a non-default comparator.
   * See comment in
   * {@link WriteBatchWithIndex#WriteBatchWithIndex(AbstractComparator, int, boolean)}
   * constructor.
   *
   * @param comparator The comparator to use for the transaction.
   *
   * @return this OptimisticTransactionOptions instance
   */
  public OptimisticTransactionOptions setComparator(
      final AbstractComparator<? extends AbstractSlice<?>> comparator) {
    assert(isOwningHandle());
    setComparator(nativeHandle_, comparator.nativeHandle_);
    return this;
  }

  private native static long newOptimisticTransactionOptions();
  private native boolean isSetSnapshot(final long handle);
  private native void setSetSnapshot(final long handle,
      final boolean setSnapshot);
  private native void setComparator(final long handle,
      final long comparatorHandle);
  @Override protected final native void disposeInternal(final long handle);
}
