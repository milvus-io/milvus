// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public class TransactionDBOptions extends RocksObject {

  public TransactionDBOptions() {
    super(newTransactionDBOptions());
  }

  /**
   * Specifies the maximum number of keys that can be locked at the same time
   * per column family.
   *
   * If the number of locked keys is greater than {@link #getMaxNumLocks()},
   * transaction writes (or GetForUpdate) will return an error.
   *
   * @return The maximum number of keys that can be locked
   */
  public long getMaxNumLocks() {
    assert(isOwningHandle());
    return getMaxNumLocks(nativeHandle_);
  }

  /**
   * Specifies the maximum number of keys that can be locked at the same time
   * per column family.
   *
   * If the number of locked keys is greater than {@link #getMaxNumLocks()},
   * transaction writes (or GetForUpdate) will return an error.
   *
   * @param maxNumLocks The maximum number of keys that can be locked;
   *     If this value is not positive, no limit will be enforced.
   *
   * @return this TransactionDBOptions instance
   */
  public TransactionDBOptions setMaxNumLocks(final long maxNumLocks) {
    assert(isOwningHandle());
    setMaxNumLocks(nativeHandle_, maxNumLocks);
    return this;
  }

  /**
   * The number of sub-tables per lock table (per column family)
   *
   * @return The number of sub-tables
   */
  public long getNumStripes() {
    assert(isOwningHandle());
    return getNumStripes(nativeHandle_);
  }

  /**
   * Increasing this value will increase the concurrency by dividing the lock
   * table (per column family) into more sub-tables, each with their own
   * separate mutex.
   *
   * Default: 16
   *
   * @param numStripes The number of sub-tables
   *
   * @return this TransactionDBOptions instance
   */
  public TransactionDBOptions setNumStripes(final long numStripes) {
    assert(isOwningHandle());
    setNumStripes(nativeHandle_, numStripes);
    return this;
  }

  /**
   * The default wait timeout in milliseconds when
   * a transaction attempts to lock a key if not specified by
   * {@link TransactionOptions#setLockTimeout(long)}
   *
   * If 0, no waiting is done if a lock cannot instantly be acquired.
   * If negative, there is no timeout.
   *
   * @return the default wait timeout in milliseconds
   */
  public long getTransactionLockTimeout() {
    assert(isOwningHandle());
    return getTransactionLockTimeout(nativeHandle_);
  }

  /**
   * If positive, specifies the default wait timeout in milliseconds when
   * a transaction attempts to lock a key if not specified by
   * {@link TransactionOptions#setLockTimeout(long)}
   *
   * If 0, no waiting is done if a lock cannot instantly be acquired.
   * If negative, there is no timeout. Not using a timeout is not recommended
   * as it can lead to deadlocks.  Currently, there is no deadlock-detection to
   * recover from a deadlock.
   *
   * Default: 1000
   *
   * @param transactionLockTimeout the default wait timeout in milliseconds
   *
   * @return this TransactionDBOptions instance
   */
  public TransactionDBOptions setTransactionLockTimeout(
      final long transactionLockTimeout) {
    assert(isOwningHandle());
    setTransactionLockTimeout(nativeHandle_, transactionLockTimeout);
    return this;
  }

  /**
   * The wait timeout in milliseconds when writing a key
   * OUTSIDE of a transaction (ie by calling {@link RocksDB#put},
   * {@link RocksDB#merge}, {@link RocksDB#remove} or {@link RocksDB#write}
   * directly).
   *
   * If 0, no waiting is done if a lock cannot instantly be acquired.
   * If negative, there is no timeout and will block indefinitely when acquiring
   * a lock.
   *
   * @return the timeout in milliseconds when writing a key OUTSIDE of a
   *     transaction
   */
  public long getDefaultLockTimeout() {
    assert(isOwningHandle());
    return getDefaultLockTimeout(nativeHandle_);
  }

  /**
   * If positive, specifies the wait timeout in milliseconds when writing a key
   * OUTSIDE of a transaction (ie by calling {@link RocksDB#put},
   * {@link RocksDB#merge}, {@link RocksDB#remove} or {@link RocksDB#write}
   * directly).
   *
   * If 0, no waiting is done if a lock cannot instantly be acquired.
   * If negative, there is no timeout and will block indefinitely when acquiring
   * a lock.
   *
   * Not using a timeout can lead to deadlocks. Currently, there
   * is no deadlock-detection to recover from a deadlock.  While DB writes
   * cannot deadlock with other DB writes, they can deadlock with a transaction.
   * A negative timeout should only be used if all transactions have a small
   * expiration set.
   *
   * Default: 1000
   *
   * @param defaultLockTimeout the timeout in milliseconds when writing a key
   *     OUTSIDE of a transaction
   * @return this TransactionDBOptions instance
   */
   public TransactionDBOptions setDefaultLockTimeout(
       final long defaultLockTimeout) {
     assert(isOwningHandle());
     setDefaultLockTimeout(nativeHandle_, defaultLockTimeout);
     return this;
   }

//  /**
//   * If set, the {@link TransactionDB} will use this implementation of a mutex
//   * and condition variable for all transaction locking instead of the default
//   * mutex/condvar implementation.
//   *
//   * @param transactionDbMutexFactory the mutex factory for the transactions
//   *
//   * @return this TransactionDBOptions instance
//   */
//  public TransactionDBOptions setCustomMutexFactory(
//      final TransactionDBMutexFactory transactionDbMutexFactory) {
//
//  }

  /**
   * The policy for when to write the data into the DB. The default policy is to
   * write only the committed data {@link TxnDBWritePolicy#WRITE_COMMITTED}.
   * The data could be written before the commit phase. The DB then needs to
   * provide the mechanisms to tell apart committed from uncommitted data.
   *
   * @return The write policy.
   */
  public TxnDBWritePolicy getWritePolicy() {
    assert(isOwningHandle());
    return TxnDBWritePolicy.getTxnDBWritePolicy(getWritePolicy(nativeHandle_));
  }

  /**
   * The policy for when to write the data into the DB. The default policy is to
   * write only the committed data {@link TxnDBWritePolicy#WRITE_COMMITTED}.
   * The data could be written before the commit phase. The DB then needs to
   * provide the mechanisms to tell apart committed from uncommitted data.
   *
   * @param writePolicy The write policy.
   *
   * @return this TransactionDBOptions instance
   */
  public TransactionDBOptions setWritePolicy(
      final TxnDBWritePolicy writePolicy) {
    assert(isOwningHandle());
    setWritePolicy(nativeHandle_, writePolicy.getValue());
    return this;
  }

  private native static long newTransactionDBOptions();
  private native long getMaxNumLocks(final long handle);
  private native void setMaxNumLocks(final long handle,
      final long maxNumLocks);
  private native long getNumStripes(final long handle);
  private native void setNumStripes(final long handle, final long numStripes);
  private native long getTransactionLockTimeout(final long handle);
  private native void setTransactionLockTimeout(final long handle,
      final long transactionLockTimeout);
  private native long getDefaultLockTimeout(final long handle);
  private native void setDefaultLockTimeout(final long handle,
      final long transactionLockTimeout);
  private native byte getWritePolicy(final long handle);
  private native void setWritePolicy(final long handle, final byte writePolicy);
  @Override protected final native void disposeInternal(final long handle);
}
