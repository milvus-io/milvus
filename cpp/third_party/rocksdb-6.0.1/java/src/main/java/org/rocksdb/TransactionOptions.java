// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public class TransactionOptions extends RocksObject
    implements TransactionalOptions {

  public TransactionOptions() {
    super(newTransactionOptions());
  }

  @Override
  public boolean isSetSnapshot() {
    assert(isOwningHandle());
    return isSetSnapshot(nativeHandle_);
  }

  @Override
  public TransactionOptions setSetSnapshot(final boolean setSnapshot) {
    assert(isOwningHandle());
    setSetSnapshot(nativeHandle_, setSnapshot);
    return this;
  }

  /**
   * True means that before acquiring locks, this transaction will
   * check if doing so will cause a deadlock. If so, it will return with
   * {@link Status.Code#Busy}. The user should retry their transaction.
   *
   * @return true if a deadlock is detected.
   */
  public boolean isDeadlockDetect() {
    assert(isOwningHandle());
    return isDeadlockDetect(nativeHandle_);
  }

  /**
   * Setting to true means that before acquiring locks, this transaction will
   * check if doing so will cause a deadlock. If so, it will return with
   * {@link Status.Code#Busy}. The user should retry their transaction.
   *
   * @param deadlockDetect true if we should detect deadlocks.
   *
   * @return this TransactionOptions instance
   */
  public TransactionOptions setDeadlockDetect(final boolean deadlockDetect) {
    assert(isOwningHandle());
    setDeadlockDetect(nativeHandle_, deadlockDetect);
    return this;
  }

  /**
   * The wait timeout in milliseconds when a transaction attempts to lock a key.
   *
   * If 0, no waiting is done if a lock cannot instantly be acquired.
   * If negative, {@link TransactionDBOptions#getTransactionLockTimeout(long)}
   * will be used
   *
   * @return the lock timeout in milliseconds
   */
  public long getLockTimeout() {
    assert(isOwningHandle());
    return getLockTimeout(nativeHandle_);
  }

  /**
   * If positive, specifies the wait timeout in milliseconds when
   * a transaction attempts to lock a key.
   *
   * If 0, no waiting is done if a lock cannot instantly be acquired.
   * If negative, {@link TransactionDBOptions#getTransactionLockTimeout(long)}
   * will be used
   *
   * Default: -1
   *
   * @param lockTimeout the lock timeout in milliseconds
   *
   * @return this TransactionOptions instance
   */
  public TransactionOptions setLockTimeout(final long lockTimeout) {
    assert(isOwningHandle());
    setLockTimeout(nativeHandle_, lockTimeout);
    return this;
  }

  /**
   * Expiration duration in milliseconds.
   *
   * If non-negative, transactions that last longer than this many milliseconds
   * will fail to commit. If not set, a forgotten transaction that is never
   * committed, rolled back, or deleted will never relinquish any locks it
   * holds. This could prevent keys from being written by other writers.
   *
   * @return expiration the expiration duration in milliseconds
   */
  public long getExpiration() {
    assert(isOwningHandle());
    return getExpiration(nativeHandle_);
  }

  /**
   * Expiration duration in milliseconds.
   *
   * If non-negative, transactions that last longer than this many milliseconds
   * will fail to commit. If not set, a forgotten transaction that is never
   * committed, rolled back, or deleted will never relinquish any locks it
   * holds. This could prevent keys from being written by other writers.
   *
   * Default: -1
   *
   * @param expiration the expiration duration in milliseconds
   *
   * @return this TransactionOptions instance
   */
  public TransactionOptions setExpiration(final long expiration) {
    assert(isOwningHandle());
    setExpiration(nativeHandle_, expiration);
    return this;
  }

  /**
   * Gets the number of traversals to make during deadlock detection.
   *
   * @return the number of traversals to make during
   *     deadlock detection
   */
  public long getDeadlockDetectDepth() {
    return getDeadlockDetectDepth(nativeHandle_);
  }

  /**
   * Sets the number of traversals to make during deadlock detection.
   *
   * Default: 50
   *
   * @param deadlockDetectDepth the number of traversals to make during
   *     deadlock detection
   *
   * @return this TransactionOptions instance
   */
  public TransactionOptions setDeadlockDetectDepth(
      final long deadlockDetectDepth) {
    setDeadlockDetectDepth(nativeHandle_, deadlockDetectDepth);
    return this;
  }

  /**
   * Get the maximum number of bytes that may be used for the write batch.
   *
   * @return the maximum number of bytes, 0 means no limit.
   */
  public long getMaxWriteBatchSize() {
    return getMaxWriteBatchSize(nativeHandle_);
  }

  /**
   * Set the maximum number of bytes that may be used for the write batch.
   *
   * @param maxWriteBatchSize the maximum number of bytes, 0 means no limit.
   *
   * @return this TransactionOptions instance
   */
  public TransactionOptions setMaxWriteBatchSize(final long maxWriteBatchSize) {
    setMaxWriteBatchSize(nativeHandle_, maxWriteBatchSize);
    return this;
  }

  private native static long newTransactionOptions();
  private native boolean isSetSnapshot(final long handle);
  private native void setSetSnapshot(final long handle,
      final boolean setSnapshot);
  private native boolean isDeadlockDetect(final long handle);
  private native void setDeadlockDetect(final long handle,
      final boolean deadlockDetect);
  private native long getLockTimeout(final long handle);
  private native void setLockTimeout(final long handle, final long lockTimeout);
  private native long getExpiration(final long handle);
  private native void setExpiration(final long handle, final long expiration);
  private native long getDeadlockDetectDepth(final long handle);
  private native void setDeadlockDetectDepth(final long handle,
      final long deadlockDetectDepth);
  private native long getMaxWriteBatchSize(final long handle);
  private native void setMaxWriteBatchSize(final long handle,
      final long maxWriteBatchSize);
  @Override protected final native void disposeInternal(final long handle);
}
