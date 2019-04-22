// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.List;

/**
 * Database with TTL support.
 *
 * <p><strong>Use case</strong></p>
 * <p>This API should be used to open the db when key-values inserted are
 * meant to be removed from the db in a non-strict 'ttl' amount of time
 * Therefore, this guarantees that key-values inserted will remain in the
 * db for &gt;= ttl amount of time and the db will make efforts to remove the
 * key-values as soon as possible after ttl seconds of their insertion.
 * </p>
 *
 * <p><strong>Behaviour</strong></p>
 * <p>TTL is accepted in seconds
 * (int32_t)Timestamp(creation) is suffixed to values in Put internally
 * Expired TTL values deleted in compaction only:(Timestamp+ttl&lt;time_now)
 * Get/Iterator may return expired entries(compaction not run on them yet)
 * Different TTL may be used during different Opens
 * </p>
 *
 * <p><strong>Example</strong></p>
 * <ul>
 * <li>Open1 at t=0 with ttl=4 and insert k1,k2, close at t=2</li>
 * <li>Open2 at t=3 with ttl=5. Now k1,k2 should be deleted at t&gt;=5</li>
 * </ul>
 *
 * <p>
 * read_only=true opens in the usual read-only mode. Compactions will not be
 *  triggered(neither manual nor automatic), so no expired entries removed
 * </p>
 *
 * <p><strong>Constraints</strong></p>
 * <p>Not specifying/passing or non-positive TTL behaves
 * like TTL = infinity</p>
 *
 * <p><strong>!!!WARNING!!!</strong></p>
 * <p>Calling DB::Open directly to re-open a db created by this API will get
 * corrupt values(timestamp suffixed) and no ttl effect will be there
 * during the second Open, so use this API consistently to open the db
 * Be careful when passing ttl with a small positive value because the
 * whole database may be deleted in a small amount of time.</p>
 */
public class TtlDB extends RocksDB {

  /**
   * <p>Opens a TtlDB.</p>
   *
   * <p>Database is opened in read-write mode without default TTL.</p>
   *
   * @param options {@link org.rocksdb.Options} instance.
   * @param db_path path to database.
   *
   * @return TtlDB instance.
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public static TtlDB open(final Options options, final String db_path)
      throws RocksDBException {
    return open(options, db_path, 0, false);
  }

  /**
   * <p>Opens a TtlDB.</p>
   *
   * @param options {@link org.rocksdb.Options} instance.
   * @param db_path path to database.
   * @param ttl time to live for new entries.
   * @param readOnly boolean value indicating if database if db is
   *     opened read-only.
   *
   * @return TtlDB instance.
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public static TtlDB open(final Options options, final String db_path,
      final int ttl, final boolean readOnly) throws RocksDBException {
    return new TtlDB(open(options.nativeHandle_, db_path, ttl, readOnly));
  }

  /**
   * <p>Opens a TtlDB.</p>
   *
   * @param options {@link org.rocksdb.Options} instance.
   * @param db_path path to database.
   * @param columnFamilyDescriptors list of column family descriptors
   * @param columnFamilyHandles will be filled with ColumnFamilyHandle instances
   *     on open.
   * @param ttlValues time to live values per column family handle
   * @param readOnly boolean value indicating if database if db is
   *     opened read-only.
   *
   * @return TtlDB instance.
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   * @throws java.lang.IllegalArgumentException when there is not a ttl value
   *     per given column family handle.
   */
  public static TtlDB open(final DBOptions options, final String db_path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles,
      final List<Integer> ttlValues, final boolean readOnly)
      throws RocksDBException {
    if (columnFamilyDescriptors.size() != ttlValues.size()) {
      throw new IllegalArgumentException("There must be a ttl value per column"
          + "family handle.");
    }

    final byte[][] cfNames = new byte[columnFamilyDescriptors.size()][];
    final long[] cfOptionHandles = new long[columnFamilyDescriptors.size()];
    for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
      final ColumnFamilyDescriptor cfDescriptor =
          columnFamilyDescriptors.get(i);
      cfNames[i] = cfDescriptor.columnFamilyName();
      cfOptionHandles[i] = cfDescriptor.columnFamilyOptions().nativeHandle_;
    }

    final int ttlVals[] = new int[ttlValues.size()];
    for(int i = 0; i < ttlValues.size(); i++) {
      ttlVals[i] = ttlValues.get(i);
    }
    final long[] handles = openCF(options.nativeHandle_, db_path,
            cfNames, cfOptionHandles, ttlVals, readOnly);

    final TtlDB ttlDB = new TtlDB(handles[0]);
    for (int i = 1; i < handles.length; i++) {
      columnFamilyHandles.add(new ColumnFamilyHandle(ttlDB, handles[i]));
    }
    return ttlDB;
  }

  /**
   * <p>Close the TtlDB instance and release resource.</p>
   *
   * This is similar to {@link #close()} except that it
   * throws an exception if any error occurs.
   *
   * This will not fsync the WAL files.
   * If syncing is required, the caller must first call {@link #syncWal()}
   * or {@link #write(WriteOptions, WriteBatch)} using an empty write batch
   * with {@link WriteOptions#setSync(boolean)} set to true.
   *
   * See also {@link #close()}.
   *
   * @throws RocksDBException if an error occurs whilst closing.
   */
  public void closeE() throws RocksDBException {
    if (owningHandle_.compareAndSet(true, false)) {
      try {
        closeDatabase(nativeHandle_);
      } finally {
        disposeInternal();
      }
    }
  }

  /**
   * <p>Close the TtlDB instance and release resource.</p>
   *
   *
   * This will not fsync the WAL files.
   * If syncing is required, the caller must first call {@link #syncWal()}
   * or {@link #write(WriteOptions, WriteBatch)} using an empty write batch
   * with {@link WriteOptions#setSync(boolean)} set to true.
   *
   * See also {@link #close()}.
   */
  @Override
  public void close() {
    if (owningHandle_.compareAndSet(true, false)) {
      try {
        closeDatabase(nativeHandle_);
      } catch (final RocksDBException e) {
        // silently ignore the error report
      } finally {
        disposeInternal();
      }
    }
  }

  /**
   * <p>Creates a new ttl based column family with a name defined
   * in given ColumnFamilyDescriptor and allocates a
   * ColumnFamilyHandle within an internal structure.</p>
   *
   * <p>The ColumnFamilyHandle is automatically disposed with DB
   * disposal.</p>
   *
   * @param columnFamilyDescriptor column family to be created.
   * @param ttl TTL to set for this column family.
   *
   * @return {@link org.rocksdb.ColumnFamilyHandle} instance.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public ColumnFamilyHandle createColumnFamilyWithTtl(
      final ColumnFamilyDescriptor columnFamilyDescriptor,
      final int ttl) throws RocksDBException {
    return new ColumnFamilyHandle(this,
        createColumnFamilyWithTtl(nativeHandle_,
            columnFamilyDescriptor.getName(),
            columnFamilyDescriptor.getOptions().nativeHandle_, ttl));
  }

  /**
   * <p>A protected constructor that will be used in the static
   * factory method
   * {@link #open(Options, String, int, boolean)}
   * and
   * {@link #open(DBOptions, String, java.util.List, java.util.List,
   * java.util.List, boolean)}.
   * </p>
   *
   * @param nativeHandle The native handle of the C++ TtlDB object
   */
  protected TtlDB(final long nativeHandle) {
    super(nativeHandle);
  }

  @Override protected native void disposeInternal(final long handle);

  private native static long open(final long optionsHandle,
      final String db_path, final int ttl, final boolean readOnly)
      throws RocksDBException;
  private native static long[] openCF(final long optionsHandle,
      final String db_path, final byte[][] columnFamilyNames,
      final long[] columnFamilyOptions, final int[] ttlValues,
      final boolean readOnly) throws RocksDBException;
  private native long createColumnFamilyWithTtl(final long handle,
      final byte[] columnFamilyName, final long columnFamilyOptions, int ttl)
      throws RocksDBException;
  private native static void closeDatabase(final long handle)
      throws RocksDBException;
}
