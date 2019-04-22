// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Similar to {@link org.rocksdb.WriteBatch} but with a binary searchable
 * index built for all the keys inserted.
 *
 * Calling put, merge, remove or putLogData calls the same function
 * as with {@link org.rocksdb.WriteBatch} whilst also building an index.
 *
 * A user can call {@link org.rocksdb.WriteBatchWithIndex#newIterator()} to
 * create an iterator over the write batch or
 * {@link org.rocksdb.WriteBatchWithIndex#newIteratorWithBase(org.rocksdb.RocksIterator)}
 * to get an iterator for the database with Read-Your-Own-Writes like capability
 */
public class WriteBatchWithIndex extends AbstractWriteBatch {
  /**
   * Creates a WriteBatchWithIndex where no bytes
   * are reserved up-front, bytewise comparison is
   * used for fallback key comparisons,
   * and duplicate keys operations are retained
   */
  public WriteBatchWithIndex() {
    super(newWriteBatchWithIndex());
  }


  /**
   * Creates a WriteBatchWithIndex where no bytes
   * are reserved up-front, bytewise comparison is
   * used for fallback key comparisons, and duplicate key
   * assignment is determined by the constructor argument
   *
   * @param overwriteKey if true, overwrite the key in the index when
   *   inserting a duplicate key, in this way an iterator will never
   *   show two entries with the same key.
   */
  public WriteBatchWithIndex(final boolean overwriteKey) {
    super(newWriteBatchWithIndex(overwriteKey));
  }

  /**
   * Creates a WriteBatchWithIndex
   *
   * @param fallbackIndexComparator We fallback to this comparator
   *  to compare keys within a column family if we cannot determine
   *  the column family and so look up it's comparator.
   *
   * @param reservedBytes reserved bytes in underlying WriteBatch
   *
   * @param overwriteKey if true, overwrite the key in the index when
   *   inserting a duplicate key, in this way an iterator will never
   *   show two entries with the same key.
   */
  public WriteBatchWithIndex(
      final AbstractComparator<? extends AbstractSlice<?>>
          fallbackIndexComparator, final int reservedBytes,
      final boolean overwriteKey) {
    super(newWriteBatchWithIndex(fallbackIndexComparator.nativeHandle_,
        fallbackIndexComparator.getComparatorType().getValue(), reservedBytes,
        overwriteKey));
  }

  /**
   * <p>Private WriteBatchWithIndex constructor which is used to construct
   * WriteBatchWithIndex instances from C++ side. As the reference to this
   * object is also managed from C++ side the handle will be disowned.</p>
   *
   * @param nativeHandle address of native instance.
   */
  WriteBatchWithIndex(final long nativeHandle) {
    super(nativeHandle);
    disOwnNativeHandle();
  }

  /**
   * Create an iterator of a column family. User can call
   * {@link org.rocksdb.RocksIteratorInterface#seek(byte[])} to
   * search to the next entry of or after a key. Keys will be iterated in the
   * order given by index_comparator. For multiple updates on the same key,
   * each update will be returned as a separate entry, in the order of update
   * time.
   *
   * @param columnFamilyHandle The column family to iterate over
   * @return An iterator for the Write Batch contents, restricted to the column
   * family
   */
  public WBWIRocksIterator newIterator(
      final ColumnFamilyHandle columnFamilyHandle) {
    return new WBWIRocksIterator(this, iterator1(nativeHandle_,
            columnFamilyHandle.nativeHandle_));
  }

  /**
   * Create an iterator of the default column family. User can call
   * {@link org.rocksdb.RocksIteratorInterface#seek(byte[])} to
   * search to the next entry of or after a key. Keys will be iterated in the
   * order given by index_comparator. For multiple updates on the same key,
   * each update will be returned as a separate entry, in the order of update
   * time.
   *
   * @return An iterator for the Write Batch contents
   */
  public WBWIRocksIterator newIterator() {
    return new WBWIRocksIterator(this, iterator0(nativeHandle_));
  }

  /**
   * Provides Read-Your-Own-Writes like functionality by
   * creating a new Iterator that will use {@link org.rocksdb.WBWIRocksIterator}
   * as a delta and baseIterator as a base
   *
   * Updating write batch with the current key of the iterator is not safe.
   * We strongly recommand users not to do it. It will invalidate the current
   * key() and value() of the iterator. This invalidation happens even before
   * the write batch update finishes. The state may recover after Next() is
   * called.
   *
   * @param columnFamilyHandle The column family to iterate over
   * @param baseIterator The base iterator,
   *   e.g. {@link org.rocksdb.RocksDB#newIterator()}
   * @return An iterator which shows a view comprised of both the database
   * point-in-time from baseIterator and modifications made in this write batch.
   */
  public RocksIterator newIteratorWithBase(
      final ColumnFamilyHandle columnFamilyHandle,
      final RocksIterator baseIterator) {
    RocksIterator iterator = new RocksIterator(baseIterator.parent_,
        iteratorWithBase(
            nativeHandle_, columnFamilyHandle.nativeHandle_, baseIterator.nativeHandle_));
    // when the iterator is deleted it will also delete the baseIterator
    baseIterator.disOwnNativeHandle();
    return iterator;
  }

  /**
   * Provides Read-Your-Own-Writes like functionality by
   * creating a new Iterator that will use {@link org.rocksdb.WBWIRocksIterator}
   * as a delta and baseIterator as a base. Operates on the default column
   * family.
   *
   * @param baseIterator The base iterator,
   *   e.g. {@link org.rocksdb.RocksDB#newIterator()}
   * @return An iterator which shows a view comprised of both the database
   * point-in-timefrom baseIterator and modifications made in this write batch.
   */
  public RocksIterator newIteratorWithBase(final RocksIterator baseIterator) {
    return newIteratorWithBase(baseIterator.parent_.getDefaultColumnFamily(), baseIterator);
  }

  /**
   * Similar to {@link RocksDB#get(ColumnFamilyHandle, byte[])} but will only
   * read the key from this batch.
   *
   * @param columnFamilyHandle The column family to retrieve the value from
   * @param options The database options to use
   * @param key The key to read the value for
   *
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException if the batch does not have enough data to resolve
   * Merge operations, MergeInProgress status may be returned.
   */
  public byte[] getFromBatch(final ColumnFamilyHandle columnFamilyHandle,
      final DBOptions options, final byte[] key) throws RocksDBException {
    return getFromBatch(nativeHandle_, options.nativeHandle_,
        key, key.length, columnFamilyHandle.nativeHandle_);
  }

  /**
   * Similar to {@link RocksDB#get(byte[])} but will only
   * read the key from this batch.
   *
   * @param options The database options to use
   * @param key The key to read the value for
   *
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException if the batch does not have enough data to resolve
   * Merge operations, MergeInProgress status may be returned.
   */
  public byte[] getFromBatch(final DBOptions options, final byte[] key)
      throws RocksDBException {
    return getFromBatch(nativeHandle_, options.nativeHandle_, key, key.length);
  }

  /**
   * Similar to {@link RocksDB#get(ColumnFamilyHandle, byte[])} but will also
   * read writes from this batch.
   *
   * This function will query both this batch and the DB and then merge
   * the results using the DB's merge operator (if the batch contains any
   * merge requests).
   *
   * Setting {@link ReadOptions#setSnapshot(long, long)} will affect what is
   * read from the DB but will NOT change which keys are read from the batch
   * (the keys in this batch do not yet belong to any snapshot and will be
   * fetched regardless).
   *
   * @param db The Rocks database
   * @param columnFamilyHandle The column family to retrieve the value from
   * @param options The read options to use
   * @param key The key to read the value for
   *
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException if the value for the key cannot be read
   */
  public byte[] getFromBatchAndDB(final RocksDB db, final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions options, final byte[] key) throws RocksDBException {
    return getFromBatchAndDB(nativeHandle_, db.nativeHandle_,
        options.nativeHandle_, key, key.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Similar to {@link RocksDB#get(byte[])} but will also
   * read writes from this batch.
   *
   * This function will query both this batch and the DB and then merge
   * the results using the DB's merge operator (if the batch contains any
   * merge requests).
   *
   * Setting {@link ReadOptions#setSnapshot(long, long)} will affect what is
   * read from the DB but will NOT change which keys are read from the batch
   * (the keys in this batch do not yet belong to any snapshot and will be
   * fetched regardless).
   *
   * @param db The Rocks database
   * @param options The read options to use
   * @param key The key to read the value for
   *
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException if the value for the key cannot be read
   */
  public byte[] getFromBatchAndDB(final RocksDB db, final ReadOptions options,
      final byte[] key) throws RocksDBException {
    return getFromBatchAndDB(nativeHandle_, db.nativeHandle_,
        options.nativeHandle_, key, key.length);
  }

  @Override protected final native void disposeInternal(final long handle);
  @Override final native int count0(final long handle);
  @Override final native void put(final long handle, final byte[] key,
      final int keyLen, final byte[] value, final int valueLen);
  @Override final native void put(final long handle, final byte[] key,
      final int keyLen, final byte[] value, final int valueLen,
      final long cfHandle);
  @Override final native void merge(final long handle, final byte[] key,
      final int keyLen, final byte[] value, final int valueLen);
  @Override final native void merge(final long handle, final byte[] key,
      final int keyLen, final byte[] value, final int valueLen,
      final long cfHandle);
  @Override final native void delete(final long handle, final byte[] key,
      final int keyLen) throws RocksDBException;
  @Override final native void delete(final long handle, final byte[] key,
      final int keyLen, final long cfHandle) throws RocksDBException;
  @Override final native void singleDelete(final long handle, final byte[] key,
      final int keyLen) throws RocksDBException;
  @Override final native void singleDelete(final long handle, final byte[] key,
      final int keyLen, final long cfHandle) throws RocksDBException;
  @Override
  final native void deleteRange(final long handle, final byte[] beginKey, final int beginKeyLen,
      final byte[] endKey, final int endKeyLen);
  @Override
  final native void deleteRange(final long handle, final byte[] beginKey, final int beginKeyLen,
      final byte[] endKey, final int endKeyLen, final long cfHandle);
  @Override final native void putLogData(final long handle, final byte[] blob,
      final int blobLen) throws RocksDBException;
  @Override final native void clear0(final long handle);
  @Override final native void setSavePoint0(final long handle);
  @Override final native void rollbackToSavePoint0(final long handle);
  @Override final native void popSavePoint(final long handle) throws RocksDBException;
  @Override final native void setMaxBytes(final long nativeHandle,
      final long maxBytes);
  @Override final native WriteBatch getWriteBatch(final long handle);

  private native static long newWriteBatchWithIndex();
  private native static long newWriteBatchWithIndex(final boolean overwriteKey);
  private native static long newWriteBatchWithIndex(
      final long fallbackIndexComparatorHandle,
      final byte comparatorType, final int reservedBytes,
      final boolean overwriteKey);
  private native long iterator0(final long handle);
  private native long iterator1(final long handle, final long cfHandle);
  private native long iteratorWithBase(
      final long handle, final long baseIteratorHandle, final long cfHandle);
  private native byte[] getFromBatch(final long handle, final long optHandle,
      final byte[] key, final int keyLen);
  private native byte[] getFromBatch(final long handle, final long optHandle,
      final byte[] key, final int keyLen, final long cfHandle);
  private native byte[] getFromBatchAndDB(final long handle,
      final long dbHandle,  final long readOptHandle, final byte[] key,
      final int keyLen);
  private native byte[] getFromBatchAndDB(final long handle,
      final long dbHandle, final long readOptHandle, final byte[] key,
      final int keyLen, final long cfHandle);
}
