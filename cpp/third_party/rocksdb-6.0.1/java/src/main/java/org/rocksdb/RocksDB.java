// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.*;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.rocksdb.util.Environment;

/**
 * A RocksDB is a persistent ordered map from keys to values.  It is safe for
 * concurrent access from multiple threads without any external synchronization.
 * All methods of this class could potentially throw RocksDBException, which
 * indicates sth wrong at the RocksDB library side and the call failed.
 */
public class RocksDB extends RocksObject {
  public static final byte[] DEFAULT_COLUMN_FAMILY = "default".getBytes();
  public static final int NOT_FOUND = -1;

  private enum LibraryState {
    NOT_LOADED,
    LOADING,
    LOADED
  }

  private static AtomicReference<LibraryState> libraryLoaded
      = new AtomicReference<>(LibraryState.NOT_LOADED);

  static {
    RocksDB.loadLibrary();
  }

  /**
   * Loads the necessary library files.
   * Calling this method twice will have no effect.
   * By default the method extracts the shared library for loading at
   * java.io.tmpdir, however, you can override this temporary location by
   * setting the environment variable ROCKSDB_SHAREDLIB_DIR.
   */
  public static void loadLibrary() {
    if (libraryLoaded.get() == LibraryState.LOADED) {
      return;
    }

    if (libraryLoaded.compareAndSet(LibraryState.NOT_LOADED,
        LibraryState.LOADING)) {
      final String tmpDir = System.getenv("ROCKSDB_SHAREDLIB_DIR");
      // loading possibly necessary libraries.
      for (final CompressionType compressionType : CompressionType.values()) {
        try {
          if (compressionType.getLibraryName() != null) {
            System.loadLibrary(compressionType.getLibraryName());
          }
        } catch (UnsatisfiedLinkError e) {
          // since it may be optional, we ignore its loading failure here.
        }
      }
      try {
        NativeLibraryLoader.getInstance().loadLibrary(tmpDir);
      } catch (IOException e) {
        libraryLoaded.set(LibraryState.NOT_LOADED);
        throw new RuntimeException("Unable to load the RocksDB shared library",
            e);
      }

      libraryLoaded.set(LibraryState.LOADED);
      return;
    }

    while (libraryLoaded.get() == LibraryState.LOADING) {
      try {
        Thread.sleep(10);
      } catch(final InterruptedException e) {
        //ignore
      }
    }
  }

  /**
   * Tries to load the necessary library files from the given list of
   * directories.
   *
   * @param paths a list of strings where each describes a directory
   *     of a library.
   */
  public static void loadLibrary(final List<String> paths) {
    if (libraryLoaded.get() == LibraryState.LOADED) {
      return;
    }

    if (libraryLoaded.compareAndSet(LibraryState.NOT_LOADED,
        LibraryState.LOADING)) {
      for (final CompressionType compressionType : CompressionType.values()) {
        if (compressionType.equals(CompressionType.NO_COMPRESSION)) {
          continue;
        }
        for (final String path : paths) {
          try {
            System.load(path + "/" + Environment.getSharedLibraryFileName(
                compressionType.getLibraryName()));
            break;
          } catch (UnsatisfiedLinkError e) {
            // since they are optional, we ignore loading fails.
          }
        }
      }
      boolean success = false;
      UnsatisfiedLinkError err = null;
      for (final String path : paths) {
        try {
          System.load(path + "/" +
              Environment.getJniLibraryFileName("rocksdbjni"));
          success = true;
          break;
        } catch (UnsatisfiedLinkError e) {
          err = e;
        }
      }
      if (!success) {
        libraryLoaded.set(LibraryState.NOT_LOADED);
        throw err;
      }

      libraryLoaded.set(LibraryState.LOADED);
      return;
    }

    while (libraryLoaded.get() == LibraryState.LOADING) {
      try {
        Thread.sleep(10);
      } catch(final InterruptedException e) {
        //ignore
      }
    }
  }

  /**
   * Private constructor.
   *
   * @param nativeHandle The native handle of the C++ RocksDB object
   */
  protected RocksDB(final long nativeHandle) {
    super(nativeHandle);
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance given
   * the path to the database using the default options w/ createIfMissing
   * set to true.
   *
   * @param path the path to the rocksdb.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @see Options#setCreateIfMissing(boolean)
   */
  public static RocksDB open(final String path) throws RocksDBException {
    final Options options = new Options();
    options.setCreateIfMissing(true);
    return open(options, path);
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance given
   * the path to the database using the specified options and db path and a list
   * of column family names.
   * <p>
   * If opened in read write mode every existing column family name must be
   * passed within the list to this method.</p>
   * <p>
   * If opened in read-only mode only a subset of existing column families must
   * be passed to this method.</p>
   * <p>
   * Options instance *should* not be disposed before all DBs using this options
   * instance have been closed. If user doesn't call options dispose explicitly,
   * then this options instance will be GC'd automatically</p>
   * <p>
   * ColumnFamily handles are disposed when the RocksDB instance is disposed.
   * </p>
   *
   * @param path the path to the rocksdb.
   * @param columnFamilyDescriptors list of column family descriptors
   * @param columnFamilyHandles will be filled with ColumnFamilyHandle instances
   *     on open.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @see DBOptions#setCreateIfMissing(boolean)
   */
  public static RocksDB open(final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    final DBOptions options = new DBOptions();
    return open(options, path, columnFamilyDescriptors, columnFamilyHandles);
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance given
   * the path to the database using the specified options and db path.
   *
   * <p>
   * Options instance *should* not be disposed before all DBs using this options
   * instance have been closed. If user doesn't call options dispose explicitly,
   * then this options instance will be GC'd automatically.</p>
   * <p>
   * Options instance can be re-used to open multiple DBs if DB statistics is
   * not used. If DB statistics are required, then its recommended to open DB
   * with new Options instance as underlying native statistics instance does not
   * use any locks to prevent concurrent updates.</p>
   *
   * @param options {@link org.rocksdb.Options} instance.
   * @param path the path to the rocksdb.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   *
   * @see Options#setCreateIfMissing(boolean)
   */
  public static RocksDB open(final Options options, final String path)
      throws RocksDBException {
    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.
    final RocksDB db = new RocksDB(open(options.nativeHandle_, path));
    db.storeOptionsInstance(options);
    return db;
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance given
   * the path to the database using the specified options and db path and a list
   * of column family names.
   * <p>
   * If opened in read write mode every existing column family name must be
   * passed within the list to this method.</p>
   * <p>
   * If opened in read-only mode only a subset of existing column families must
   * be passed to this method.</p>
   * <p>
   * Options instance *should* not be disposed before all DBs using this options
   * instance have been closed. If user doesn't call options dispose explicitly,
   * then this options instance will be GC'd automatically.</p>
   * <p>
   * Options instance can be re-used to open multiple DBs if DB statistics is
   * not used. If DB statistics are required, then its recommended to open DB
   * with new Options instance as underlying native statistics instance does not
   * use any locks to prevent concurrent updates.</p>
   * <p>
   * ColumnFamily handles are disposed when the RocksDB instance is disposed.
   * </p>
   *
   * @param options {@link org.rocksdb.DBOptions} instance.
   * @param path the path to the rocksdb.
   * @param columnFamilyDescriptors list of column family descriptors
   * @param columnFamilyHandles will be filled with ColumnFamilyHandle instances
   *     on open.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   *
   * @see DBOptions#setCreateIfMissing(boolean)
   */
  public static RocksDB open(final DBOptions options, final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {

    final byte[][] cfNames = new byte[columnFamilyDescriptors.size()][];
    final long[] cfOptionHandles = new long[columnFamilyDescriptors.size()];
    for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
      final ColumnFamilyDescriptor cfDescriptor = columnFamilyDescriptors
          .get(i);
      cfNames[i] = cfDescriptor.columnFamilyName();
      cfOptionHandles[i] = cfDescriptor.columnFamilyOptions().nativeHandle_;
    }

    final long[] handles = open(options.nativeHandle_, path, cfNames,
        cfOptionHandles);
    final RocksDB db = new RocksDB(handles[0]);
    db.storeOptionsInstance(options);

    for (int i = 1; i < handles.length; i++) {
      columnFamilyHandles.add(new ColumnFamilyHandle(db, handles[i]));
    }

    return db;
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance in
   * Read-Only mode given the path to the database using the default
   * options.
   *
   * @param path the path to the RocksDB.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static RocksDB openReadOnly(final String path)
      throws RocksDBException {
    // This allows to use the rocksjni default Options instead of
    // the c++ one.
    Options options = new Options();
    return openReadOnly(options, path);
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance in
   * Read-Only mode given the path to the database using the default
   * options.
   *
   * @param path the path to the RocksDB.
   * @param columnFamilyDescriptors list of column family descriptors
   * @param columnFamilyHandles will be filled with ColumnFamilyHandle instances
   *     on open.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static RocksDB openReadOnly(final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    // This allows to use the rocksjni default Options instead of
    // the c++ one.
    final DBOptions options = new DBOptions();
    return openReadOnly(options, path, columnFamilyDescriptors,
        columnFamilyHandles);
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance in
   * Read-Only mode given the path to the database using the specified
   * options and db path.
   *
   * Options instance *should* not be disposed before all DBs using this options
   * instance have been closed. If user doesn't call options dispose explicitly,
   * then this options instance will be GC'd automatically.
   *
   * @param options {@link Options} instance.
   * @param path the path to the RocksDB.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static RocksDB openReadOnly(final Options options, final String path)
      throws RocksDBException {
    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.
    final RocksDB db = new RocksDB(openROnly(options.nativeHandle_, path));
    db.storeOptionsInstance(options);
    return db;
  }

  /**
   * The factory constructor of RocksDB that opens a RocksDB instance in
   * Read-Only mode given the path to the database using the specified
   * options and db path.
   *
   * <p>This open method allows to open RocksDB using a subset of available
   * column families</p>
   * <p>Options instance *should* not be disposed before all DBs using this
   * options instance have been closed. If user doesn't call options dispose
   * explicitly,then this options instance will be GC'd automatically.</p>
   *
   * @param options {@link DBOptions} instance.
   * @param path the path to the RocksDB.
   * @param columnFamilyDescriptors list of column family descriptors
   * @param columnFamilyHandles will be filled with ColumnFamilyHandle instances
   *     on open.
   * @return a {@link RocksDB} instance on success, null if the specified
   *     {@link RocksDB} can not be opened.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static RocksDB openReadOnly(final DBOptions options, final String path,
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    // when non-default Options is used, keeping an Options reference
    // in RocksDB can prevent Java to GC during the life-time of
    // the currently-created RocksDB.

    final byte[][] cfNames = new byte[columnFamilyDescriptors.size()][];
    final long[] cfOptionHandles = new long[columnFamilyDescriptors.size()];
    for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
      final ColumnFamilyDescriptor cfDescriptor = columnFamilyDescriptors
          .get(i);
      cfNames[i] = cfDescriptor.columnFamilyName();
      cfOptionHandles[i] = cfDescriptor.columnFamilyOptions().nativeHandle_;
    }

    final long[] handles = openROnly(options.nativeHandle_, path, cfNames,
        cfOptionHandles);
    final RocksDB db = new RocksDB(handles[0]);
    db.storeOptionsInstance(options);

    for (int i = 1; i < handles.length; i++) {
      columnFamilyHandles.add(new ColumnFamilyHandle(db, handles[i]));
    }

    return db;
  }

  /**
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
   * This is similar to {@link #closeE()} except that it
   * silently ignores any errors.
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
   * Static method to determine all available column families for a
   * rocksdb database identified by path
   *
   * @param options Options for opening the database
   * @param path Absolute path to rocksdb database
   * @return List&lt;byte[]&gt; List containing the column family names
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static List<byte[]> listColumnFamilies(final Options options,
      final String path) throws RocksDBException {
    return Arrays.asList(RocksDB.listColumnFamilies(options.nativeHandle_,
        path));
  }

  /**
   * Creates a new column family with the name columnFamilyName and
   * allocates a ColumnFamilyHandle within an internal structure.
   * The ColumnFamilyHandle is automatically disposed with DB disposal.
   *
   * @param columnFamilyDescriptor column family to be created.
   * @return {@link org.rocksdb.ColumnFamilyHandle} instance.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public ColumnFamilyHandle createColumnFamily(
      final ColumnFamilyDescriptor columnFamilyDescriptor)
      throws RocksDBException {
    return new ColumnFamilyHandle(this, createColumnFamily(nativeHandle_,
        columnFamilyDescriptor.getName(),
        columnFamilyDescriptor.getName().length,
        columnFamilyDescriptor.getOptions().nativeHandle_));
  }

  /**
   * Bulk create column families with the same column family options.
   *
   * @param columnFamilyOptions the options for the column families.
   * @param columnFamilyNames the names of the column families.
   *
   * @return the handles to the newly created column families.
   */
  public List<ColumnFamilyHandle> createColumnFamilies(
      final ColumnFamilyOptions columnFamilyOptions,
      final List<byte[]> columnFamilyNames) throws RocksDBException {
    final byte[][] cfNames = columnFamilyNames.toArray(
        new byte[0][]);
    final long[] cfHandles = createColumnFamilies(nativeHandle_,
        columnFamilyOptions.nativeHandle_, cfNames);
    final List<ColumnFamilyHandle> columnFamilyHandles =
        new ArrayList<>(cfHandles.length);
    for (int i = 0; i < cfHandles.length; i++) {
      columnFamilyHandles.add(new ColumnFamilyHandle(this, cfHandles[i]));
    }
    return columnFamilyHandles;
  }

  /**
   * Bulk create column families with the same column family options.
   *
   * @param columnFamilyDescriptors the descriptions of the column families.
   *
   * @return the handles to the newly created column families.
   */
  public List<ColumnFamilyHandle> createColumnFamilies(
      final List<ColumnFamilyDescriptor> columnFamilyDescriptors)
      throws RocksDBException {
    final long[] cfOptsHandles = new long[columnFamilyDescriptors.size()];
    final byte[][] cfNames = new byte[columnFamilyDescriptors.size()][];
    for (int i = 0; i < columnFamilyDescriptors.size(); i++) {
      final ColumnFamilyDescriptor columnFamilyDescriptor
          = columnFamilyDescriptors.get(i);
      cfOptsHandles[i] = columnFamilyDescriptor.getOptions().nativeHandle_;
      cfNames[i] = columnFamilyDescriptor.getName();
    }
    final long[] cfHandles = createColumnFamilies(nativeHandle_,
        cfOptsHandles, cfNames);
    final List<ColumnFamilyHandle> columnFamilyHandles =
        new ArrayList<>(cfHandles.length);
    for (int i = 0; i < cfHandles.length; i++) {
      columnFamilyHandles.add(new ColumnFamilyHandle(this, cfHandles[i]));
    }
    return columnFamilyHandles;
  }

  /**
   * Drops the column family specified by {@code columnFamilyHandle}. This call
   * only records a drop record in the manifest and prevents the column
   * family from flushing and compacting.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void dropColumnFamily(final ColumnFamilyHandle columnFamilyHandle)
      throws RocksDBException {
    dropColumnFamily(nativeHandle_, columnFamilyHandle.nativeHandle_);
  }

  // Bulk drop column families. This call only records drop records in the
  // manifest and prevents the column families from flushing and compacting.
  // In case of error, the request may succeed partially. User may call
  // ListColumnFamilies to check the result.
  public void dropColumnFamilies(
      final List<ColumnFamilyHandle> columnFamilies) throws RocksDBException {
    final long[] cfHandles = new long[columnFamilies.size()];
    for (int i = 0; i < columnFamilies.size(); i++) {
      cfHandles[i] = columnFamilies.get(i).nativeHandle_;
    }
    dropColumnFamilies(nativeHandle_, cfHandles);
  }

  //TODO(AR) what about DestroyColumnFamilyHandle

  /**
   * Set the database entry for "key" to "value".
   *
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void put(final byte[] key, final byte[] value)
      throws RocksDBException {
    put(nativeHandle_, key, 0, key.length, value, 0, value.length);
  }

  /**
   * Set the database entry for "key" to "value".
   *
   * @param key The specified key to be inserted
   * @param offset the offset of the "key" array to be used, must be
   *    non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param value the value associated with the specified key
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, must be
   *     non-negative and no larger than ("value".length -  offset)
   *
   * @throws RocksDBException thrown if errors happens in underlying native
   *     library.
   * @throws IndexOutOfBoundsException if an offset or length is out of bounds
   */
  public void put(final byte[] key, final int offset, final int len,
      final byte[] value, final int vOffset, final int vLen)
      throws RocksDBException {
    checkBounds(offset, len, key.length);
    checkBounds(vOffset, vLen, value.length);
    put(nativeHandle_, key, offset, len, value, vOffset, vLen);
  }

  /**
   * Set the database entry for "key" to "value" in the specified
   * column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * throws IllegalArgumentException if column family is not present
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void put(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final byte[] value) throws RocksDBException {
    put(nativeHandle_, key, 0, key.length, value, 0, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Set the database entry for "key" to "value" in the specified
   * column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key The specified key to be inserted
   * @param offset the offset of the "key" array to be used, must
   *     be non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param value the value associated with the specified key
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, must be
   *     non-negative and no larger than ("value".length - offset)
   *
   * @throws RocksDBException thrown if errors happens in underlying native
   *     library.
   * @throws IndexOutOfBoundsException if an offset or length is out of bounds
   */
  public void put(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final int offset, final int len,
      final byte[] value, final int vOffset, final int vLen)
      throws RocksDBException {
    checkBounds(offset, len, key.length);
    checkBounds(vOffset, vLen, value.length);
    put(nativeHandle_, key, offset, len, value, vOffset, vLen,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Set the database entry for "key" to "value".
   *
   * @param writeOpts {@link org.rocksdb.WriteOptions} instance.
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void put(final WriteOptions writeOpts, final byte[] key,
      final byte[] value) throws RocksDBException {
    put(nativeHandle_, writeOpts.nativeHandle_,
        key, 0, key.length, value, 0, value.length);
  }

  /**
   * Set the database entry for "key" to "value".
   *
   * @param writeOpts {@link org.rocksdb.WriteOptions} instance.
   * @param key The specified key to be inserted
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param value the value associated with the specified key
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, must be
   *     non-negative and no larger than ("value".length -  offset)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IndexOutOfBoundsException if an offset or length is out of bounds
   */
  public void put(final WriteOptions writeOpts,
      final byte[] key, final int offset, final int len,
      final byte[] value, final int vOffset, final int vLen)
      throws RocksDBException {
    checkBounds(offset, len, key.length);
    checkBounds(vOffset, vLen, value.length);
    put(nativeHandle_, writeOpts.nativeHandle_,
        key, offset, len, value, vOffset, vLen);
  }

  /**
   * Set the database entry for "key" to "value" for the specified
   * column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param writeOpts {@link org.rocksdb.WriteOptions} instance.
   * @param key the specified key to be inserted.
   * @param value the value associated with the specified key.
   *
   * throws IllegalArgumentException if column family is not present
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @see IllegalArgumentException
   */
  public void put(final ColumnFamilyHandle columnFamilyHandle,
      final WriteOptions writeOpts, final byte[] key,
      final byte[] value) throws RocksDBException {
    put(nativeHandle_, writeOpts.nativeHandle_, key, 0, key.length, value,
        0, value.length, columnFamilyHandle.nativeHandle_);
  }

  /**
   * Set the database entry for "key" to "value" for the specified
   * column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param writeOpts {@link org.rocksdb.WriteOptions} instance.
   * @param key The specified key to be inserted
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param value the value associated with the specified key
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, must be
   *     non-negative and no larger than ("value".length -  offset)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IndexOutOfBoundsException if an offset or length is out of bounds
   */
  public void put(final ColumnFamilyHandle columnFamilyHandle,
      final WriteOptions writeOpts,
      final byte[] key, final int offset, final int len,
      final byte[] value, final int vOffset, final int vLen)
      throws RocksDBException {
    checkBounds(offset, len, key.length);
    checkBounds(vOffset, vLen, value.length);
    put(nativeHandle_, writeOpts.nativeHandle_, key, offset, len, value,
        vOffset, vLen, columnFamilyHandle.nativeHandle_);
  }

  /**
   * Remove the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   *
   * @deprecated Use {@link #delete(byte[])}
   */
  @Deprecated
  public void remove(final byte[] key) throws RocksDBException {
    delete(key);
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final byte[] key) throws RocksDBException {
    delete(nativeHandle_, key, 0, key.length);
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param key Key to delete within database
   * @param offset the offset of the "key" array to be used, must be
   *      non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be
   *      non-negative and no larger than ("key".length - offset)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final byte[] key, final int offset, final int len)
      throws RocksDBException {
    delete(nativeHandle_, key, offset, len);
  }

  /**
   * Remove the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   *
   * @deprecated Use {@link #delete(ColumnFamilyHandle, byte[])}
   */
  @Deprecated
  public void remove(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key) throws RocksDBException {
    delete(columnFamilyHandle, key);
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key) throws RocksDBException {
    delete(nativeHandle_, key, 0, key.length, columnFamilyHandle.nativeHandle_);
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key Key to delete within database
   * @param offset the offset of the "key" array to be used,
   *     must be non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("value".length - offset)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final int offset, final int len)
      throws RocksDBException {
    delete(nativeHandle_, key, offset, len, columnFamilyHandle.nativeHandle_);
  }

  /**
   * Remove the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param writeOpt WriteOptions to be used with delete operation
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   *
   * @deprecated Use {@link #delete(WriteOptions, byte[])}
   */
  @Deprecated
  public void remove(final WriteOptions writeOpt, final byte[] key)
      throws RocksDBException {
    delete(writeOpt, key);
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param writeOpt WriteOptions to be used with delete operation
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final WriteOptions writeOpt, final byte[] key)
      throws RocksDBException {
    delete(nativeHandle_, writeOpt.nativeHandle_, key, 0, key.length);
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param writeOpt WriteOptions to be used with delete operation
   * @param key Key to delete within database
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be
   *     non-negative and no larger than ("key".length -  offset)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final WriteOptions writeOpt, final byte[] key,
      final int offset, final int len) throws RocksDBException {
    delete(nativeHandle_, writeOpt.nativeHandle_, key, offset, len);
  }

  /**
   * Remove the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param writeOpt WriteOptions to be used with delete operation
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   *
   * @deprecated Use {@link #delete(ColumnFamilyHandle, WriteOptions, byte[])}
   */
  @Deprecated
  public void remove(final ColumnFamilyHandle columnFamilyHandle,
      final WriteOptions writeOpt, final byte[] key) throws RocksDBException {
    delete(columnFamilyHandle, writeOpt, key);
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param writeOpt WriteOptions to be used with delete operation
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final ColumnFamilyHandle columnFamilyHandle,
      final WriteOptions writeOpt, final byte[] key)
      throws RocksDBException {
    delete(nativeHandle_, writeOpt.nativeHandle_, key, 0, key.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Delete the database entry (if any) for "key".  Returns OK on
   * success, and a non-OK status on error.  It is not an error if "key"
   * did not exist in the database.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param writeOpt WriteOptions to be used with delete operation
   * @param key Key to delete within database
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be
   *     non-negative and no larger than ("key".length -  offset)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void delete(final ColumnFamilyHandle columnFamilyHandle,
      final WriteOptions writeOpt, final byte[] key, final int offset,
      final int len)  throws RocksDBException {
    delete(nativeHandle_, writeOpt.nativeHandle_, key, offset, len,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Remove the database entry for {@code key}. Requires that the key exists
   * and was not overwritten. It is not an error if the key did not exist
   * in the database.
   *
   * If a key is overwritten (by calling {@link #put(byte[], byte[])} multiple
   * times), then the result of calling SingleDelete() on this key is undefined.
   * SingleDelete() only behaves correctly if there has been only one Put()
   * for this key since the previous call to SingleDelete() for this key.
   *
   * This feature is currently an experimental performance optimization
   * for a very specific workload. It is up to the caller to ensure that
   * SingleDelete is only used for a key that is not deleted using Delete() or
   * written using Merge(). Mixing SingleDelete operations with Deletes and
   * Merges can result in undefined behavior.
   *
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  @Experimental("Performance optimization for a very specific workload")
  public void singleDelete(final byte[] key) throws RocksDBException {
    singleDelete(nativeHandle_, key, key.length);
  }

  /**
   * Remove the database entry for {@code key}. Requires that the key exists
   * and was not overwritten. It is not an error if the key did not exist
   * in the database.
   *
   * If a key is overwritten (by calling {@link #put(byte[], byte[])} multiple
   * times), then the result of calling SingleDelete() on this key is undefined.
   * SingleDelete() only behaves correctly if there has been only one Put()
   * for this key since the previous call to SingleDelete() for this key.
   *
   * This feature is currently an experimental performance optimization
   * for a very specific workload. It is up to the caller to ensure that
   * SingleDelete is only used for a key that is not deleted using Delete() or
   * written using Merge(). Mixing SingleDelete operations with Deletes and
   * Merges can result in undefined behavior.
   *
   * @param columnFamilyHandle The column family to delete the key from
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  @Experimental("Performance optimization for a very specific workload")
  public void singleDelete(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key) throws RocksDBException {
    singleDelete(nativeHandle_, key, key.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Remove the database entry for {@code key}. Requires that the key exists
   * and was not overwritten. It is not an error if the key did not exist
   * in the database.
   *
   * If a key is overwritten (by calling {@link #put(byte[], byte[])} multiple
   * times), then the result of calling SingleDelete() on this key is undefined.
   * SingleDelete() only behaves correctly if there has been only one Put()
   * for this key since the previous call to SingleDelete() for this key.
   *
   * This feature is currently an experimental performance optimization
   * for a very specific workload. It is up to the caller to ensure that
   * SingleDelete is only used for a key that is not deleted using Delete() or
   * written using Merge(). Mixing SingleDelete operations with Deletes and
   * Merges can result in undefined behavior.
   *
   * Note: consider setting {@link WriteOptions#setSync(boolean)} true.
   *
   * @param writeOpt Write options for the delete
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  @Experimental("Performance optimization for a very specific workload")
  public void singleDelete(final WriteOptions writeOpt, final byte[] key)
      throws RocksDBException {
    singleDelete(nativeHandle_, writeOpt.nativeHandle_, key, key.length);
  }

  /**
   * Remove the database entry for {@code key}. Requires that the key exists
   * and was not overwritten. It is not an error if the key did not exist
   * in the database.
   *
   * If a key is overwritten (by calling {@link #put(byte[], byte[])} multiple
   * times), then the result of calling SingleDelete() on this key is undefined.
   * SingleDelete() only behaves correctly if there has been only one Put()
   * for this key since the previous call to SingleDelete() for this key.
   *
   * This feature is currently an experimental performance optimization
   * for a very specific workload. It is up to the caller to ensure that
   * SingleDelete is only used for a key that is not deleted using Delete() or
   * written using Merge(). Mixing SingleDelete operations with Deletes and
   * Merges can result in undefined behavior.
   *
   * Note: consider setting {@link WriteOptions#setSync(boolean)} true.
   *
   * @param columnFamilyHandle The column family to delete the key from
   * @param writeOpt Write options for the delete
   * @param key Key to delete within database
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  @Experimental("Performance optimization for a very specific workload")
  public void singleDelete(final ColumnFamilyHandle columnFamilyHandle,
      final WriteOptions writeOpt, final byte[] key) throws RocksDBException {
    singleDelete(nativeHandle_, writeOpt.nativeHandle_, key, key.length,
        columnFamilyHandle.nativeHandle_);
  }


  /**
   * Removes the database entries in the range ["beginKey", "endKey"), i.e.,
   * including "beginKey" and excluding "endKey". a non-OK status on error. It
   * is not an error if no keys exist in the range ["beginKey", "endKey").
   *
   * Delete the database entry (if any) for "key". Returns OK on success, and a
   * non-OK status on error. It is not an error if "key" did not exist in the
   * database.
   *
   * @param beginKey First key to delete within database (inclusive)
   * @param endKey Last key to delete within database (exclusive)
   *
   * @throws RocksDBException thrown if error happens in underlying native
   *     library.
   */
  public void deleteRange(final byte[] beginKey, final byte[] endKey)
      throws RocksDBException {
    deleteRange(nativeHandle_, beginKey, 0, beginKey.length, endKey, 0,
        endKey.length);
  }

  /**
   * Removes the database entries in the range ["beginKey", "endKey"), i.e.,
   * including "beginKey" and excluding "endKey". a non-OK status on error. It
   * is not an error if no keys exist in the range ["beginKey", "endKey").
   *
   * Delete the database entry (if any) for "key". Returns OK on success, and a
   * non-OK status on error. It is not an error if "key" did not exist in the
   * database.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle} instance
   * @param beginKey First key to delete within database (inclusive)
   * @param endKey Last key to delete within database (exclusive)
   *
   * @throws RocksDBException thrown if error happens in underlying native
   *     library.
   */
  public void deleteRange(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] beginKey, final byte[] endKey) throws RocksDBException {
    deleteRange(nativeHandle_, beginKey, 0, beginKey.length, endKey, 0,
        endKey.length, columnFamilyHandle.nativeHandle_);
  }

  /**
   * Removes the database entries in the range ["beginKey", "endKey"), i.e.,
   * including "beginKey" and excluding "endKey". a non-OK status on error. It
   * is not an error if no keys exist in the range ["beginKey", "endKey").
   *
   * Delete the database entry (if any) for "key". Returns OK on success, and a
   * non-OK status on error. It is not an error if "key" did not exist in the
   * database.
   *
   * @param writeOpt WriteOptions to be used with delete operation
   * @param beginKey First key to delete within database (inclusive)
   * @param endKey Last key to delete within database (exclusive)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  public void deleteRange(final WriteOptions writeOpt, final byte[] beginKey,
      final byte[] endKey) throws RocksDBException {
    deleteRange(nativeHandle_, writeOpt.nativeHandle_, beginKey, 0,
        beginKey.length, endKey, 0, endKey.length);
  }

  /**
   * Removes the database entries in the range ["beginKey", "endKey"), i.e.,
   * including "beginKey" and excluding "endKey". a non-OK status on error. It
   * is not an error if no keys exist in the range ["beginKey", "endKey").
   *
   * Delete the database entry (if any) for "key". Returns OK on success, and a
   * non-OK status on error. It is not an error if "key" did not exist in the
   * database.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle} instance
   * @param writeOpt WriteOptions to be used with delete operation
   * @param beginKey First key to delete within database (included)
   * @param endKey Last key to delete within database (excluded)
   *
   * @throws RocksDBException thrown if error happens in underlying native
   *     library.
   */
  public void deleteRange(final ColumnFamilyHandle columnFamilyHandle,
      final WriteOptions writeOpt, final byte[] beginKey, final byte[] endKey)
      throws RocksDBException {
    deleteRange(nativeHandle_, writeOpt.nativeHandle_, beginKey, 0,
        beginKey.length, endKey, 0, endKey.length,
        columnFamilyHandle.nativeHandle_);
  }


  /**
   * Add merge operand for key/value pair.
   *
   * @param key the specified key to be merged.
   * @param value the value to be merged with the current value for the
   *     specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void merge(final byte[] key, final byte[] value)
      throws RocksDBException {
    merge(nativeHandle_, key, 0, key.length, value, 0, value.length);
  }

  /**
   * Add merge operand for key/value pair.
   *
   * @param key the specified key to be merged.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param value the value to be merged with the current value for the
   *     specified key.
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, must be
   *     non-negative and must be non-negative and no larger than
   *     ("value".length -  offset)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IndexOutOfBoundsException if an offset or length is out of bounds
   */
  public void merge(final byte[] key, int offset, int len, final byte[] value,
      final int vOffset, final int vLen) throws RocksDBException {
    checkBounds(offset, len, key.length);
    checkBounds(vOffset, vLen, value.length);
    merge(nativeHandle_, key, offset, len, value, vOffset, vLen);
  }

  /**
   * Add merge operand for key/value pair in a ColumnFamily.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param key the specified key to be merged.
   * @param value the value to be merged with the current value for
   * the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void merge(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final byte[] value) throws RocksDBException {
    merge(nativeHandle_, key, 0, key.length, value, 0, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Add merge operand for key/value pair in a ColumnFamily.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param key the specified key to be merged.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param value the value to be merged with the current value for
   *     the specified key.
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, must be
   *     must be non-negative and no larger than ("value".length -  offset)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IndexOutOfBoundsException if an offset or length is out of bounds
   */
  public void merge(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final int offset, final int len, final byte[] value,
      final int vOffset, final int vLen) throws RocksDBException {
    checkBounds(offset, len, key.length);
    checkBounds(vOffset, vLen, value.length);
    merge(nativeHandle_, key, offset, len, value, vOffset, vLen,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Add merge operand for key/value pair.
   *
   * @param writeOpts {@link WriteOptions} for this write.
   * @param key the specified key to be merged.
   * @param value the value to be merged with the current value for
   * the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void merge(final WriteOptions writeOpts, final byte[] key,
      final byte[] value) throws RocksDBException {
    merge(nativeHandle_, writeOpts.nativeHandle_,
        key, 0, key.length, value, 0, value.length);
  }

  /**
   * Add merge operand for key/value pair.
   *
   * @param writeOpts {@link WriteOptions} for this write.
   * @param key the specified key to be merged.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("value".length -  offset)
   * @param value the value to be merged with the current value for
   *     the specified key.
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, must be
   *     non-negative and no larger than ("value".length -  offset)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IndexOutOfBoundsException if an offset or length is out of bounds
   */
  public void merge(final WriteOptions writeOpts,
      final byte[] key,  final int offset, final int len,
      final byte[] value, final int vOffset, final int vLen)
      throws RocksDBException {
    checkBounds(offset, len, key.length);
    checkBounds(vOffset, vLen, value.length);
    merge(nativeHandle_, writeOpts.nativeHandle_,
        key, offset, len, value, vOffset, vLen);
  }

  /**
   * Add merge operand for key/value pair.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param writeOpts {@link WriteOptions} for this write.
   * @param key the specified key to be merged.
   * @param value the value to be merged with the current value for the
   *     specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void merge(final ColumnFamilyHandle columnFamilyHandle,
      final WriteOptions writeOpts, final byte[] key, final byte[] value)
      throws RocksDBException {
    merge(nativeHandle_, writeOpts.nativeHandle_,
        key, 0, key.length, value, 0, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Add merge operand for key/value pair.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param writeOpts {@link WriteOptions} for this write.
   * @param key the specified key to be merged.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param value the value to be merged with the current value for
   *     the specified key.
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, must be
   *     non-negative and no larger than ("value".length -  offset)
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IndexOutOfBoundsException if an offset or length is out of bounds
   */
  public void merge(
      final ColumnFamilyHandle columnFamilyHandle, final WriteOptions writeOpts,
      final byte[] key, final int offset, final int len,
      final byte[] value, final int vOffset, final int vLen)
      throws RocksDBException {
    checkBounds(offset, len, key.length);
    checkBounds(vOffset, vLen, value.length);
    merge(nativeHandle_, writeOpts.nativeHandle_,
        key, offset, len, value, vOffset, vLen,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Apply the specified updates to the database.
   *
   * @param writeOpts WriteOptions instance
   * @param updates WriteBatch instance
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void write(final WriteOptions writeOpts, final WriteBatch updates)
      throws RocksDBException {
    write0(nativeHandle_, writeOpts.nativeHandle_, updates.nativeHandle_);
  }

  /**
   * Apply the specified updates to the database.
   *
   * @param writeOpts WriteOptions instance
   * @param updates WriteBatchWithIndex instance
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public void write(final WriteOptions writeOpts,
      final WriteBatchWithIndex updates) throws RocksDBException {
    write1(nativeHandle_, writeOpts.nativeHandle_, updates.nativeHandle_);
  }

  // TODO(AR) we should improve the #get() API, returning -1 (RocksDB.NOT_FOUND) is not very nice
  // when we could communicate better status into, also the C++ code show that -2 could be returned

  /**
   * Get the value associated with the specified key within column family*
   *
   * @param key the key to retrieve the value.
   * @param value the out-value to receive the retrieved value.
   *
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final byte[] key, final byte[] value) throws RocksDBException {
    return get(nativeHandle_, key, 0, key.length, value, 0, value.length);
  }

  /**
   * Get the value associated with the specified key within column family*
   *
   * @param key the key to retrieve the value.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param value the out-value to receive the retrieved value.
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "value".length
   * @param vLen the length of the "value" array to be used, must be
   *     non-negative and and no larger than ("value".length -  offset)
   *
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final byte[] key, final int offset, final int len,
      final byte[] value, final int vOffset, final int vLen)
      throws RocksDBException {
    checkBounds(offset, len, key.length);
    checkBounds(vOffset, vLen, value.length);
    return get(nativeHandle_, key, offset, len, value, vOffset, vLen);
  }

  /**
   * Get the value associated with the specified key within column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the key to retrieve the value.
   * @param value the out-value to receive the retrieved value.
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final ColumnFamilyHandle columnFamilyHandle, final byte[] key,
      final byte[] value) throws RocksDBException, IllegalArgumentException {
    return get(nativeHandle_, key, 0, key.length, value, 0, value.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Get the value associated with the specified key within column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the key to retrieve the value.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     an no larger than ("key".length -  offset)
   * @param value the out-value to receive the retrieved value.
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, must be
   *     non-negative and no larger than ("value".length -  offset)
   *
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final ColumnFamilyHandle columnFamilyHandle, final byte[] key,
      final int offset, final int len, final byte[] value, final int vOffset,
      final int vLen) throws RocksDBException, IllegalArgumentException {
    checkBounds(offset, len, key.length);
    checkBounds(vOffset, vLen, value.length);
    return get(nativeHandle_, key, offset, len, value, vOffset, vLen,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Get the value associated with the specified key.
   *
   * @param opt {@link org.rocksdb.ReadOptions} instance.
   * @param key the key to retrieve the value.
   * @param value the out-value to receive the retrieved value.
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final ReadOptions opt, final byte[] key,
      final byte[] value) throws RocksDBException {
    return get(nativeHandle_, opt.nativeHandle_,
               key, 0, key.length, value, 0, value.length);
  }

  /**
   * Get the value associated with the specified key.
   *
   * @param opt {@link org.rocksdb.ReadOptions} instance.
   * @param key the key to retrieve the value.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param value the out-value to receive the retrieved value.
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, must be
   *     non-negative and no larger than ("value".length -  offset)
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final ReadOptions opt, final byte[] key, final int offset,
      final int len, final byte[] value, final int vOffset, final int vLen)
      throws RocksDBException {
    checkBounds(offset, len, key.length);
    checkBounds(vOffset, vLen, value.length);
    return get(nativeHandle_, opt.nativeHandle_,
        key, offset, len, value, vOffset, vLen);
  }

  /**
   * Get the value associated with the specified key within column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param opt {@link org.rocksdb.ReadOptions} instance.
   * @param key the key to retrieve the value.
   * @param value the out-value to receive the retrieved value.
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions opt, final byte[] key, final byte[] value)
      throws RocksDBException {
    return get(nativeHandle_, opt.nativeHandle_, key, 0, key.length, value,
        0, value.length, columnFamilyHandle.nativeHandle_);
  }

  /**
   * Get the value associated with the specified key within column family.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param opt {@link org.rocksdb.ReadOptions} instance.
   * @param key the key to retrieve the value.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be
   *     non-negative and and no larger than ("key".length -  offset)
   * @param value the out-value to receive the retrieved value.
   * @param vOffset the offset of the "value" array to be used, must be
   *     non-negative and no longer than "key".length
   * @param vLen the length of the "value" array to be used, and must be
   *     non-negative and no larger than ("value".length -  offset)
   * @return The size of the actual value that matches the specified
   *     {@code key} in byte.  If the return value is greater than the
   *     length of {@code value}, then it indicates that the size of the
   *     input buffer {@code value} is insufficient and partial result will
   *     be returned.  RocksDB.NOT_FOUND will be returned if the value not
   *     found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public int get(final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions opt, final byte[] key, final int offset, final int len,
      final byte[] value, final int vOffset, final int vLen)
      throws RocksDBException {
    checkBounds(offset, len, key.length);
    checkBounds(vOffset, vLen, value.length);
    return get(nativeHandle_, opt.nativeHandle_, key, offset, len, value,
        vOffset, vLen, columnFamilyHandle.nativeHandle_);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param key the key retrieve the value.
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] get(final byte[] key) throws RocksDBException {
    return get(nativeHandle_, key, 0, key.length);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param key the key retrieve the value.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] get(final byte[] key, final int offset,
      final int len) throws RocksDBException {
    checkBounds(offset, len, key.length);
    return get(nativeHandle_, key, offset, len);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the key retrieve the value.
   * @return a byte array storing the value associated with the input key if
   *     any.  null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] get(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key) throws RocksDBException {
    return get(nativeHandle_, key, 0, key.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the key retrieve the value.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] get(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final int offset, final int len)
      throws RocksDBException {
    checkBounds(offset, len, key.length);
    return get(nativeHandle_, key, offset, len,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param key the key retrieve the value.
   * @param opt Read options.
   * @return a byte array storing the value associated with the input key if
   *     any.  null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] get(final ReadOptions opt, final byte[] key)
      throws RocksDBException {
    return get(nativeHandle_, opt.nativeHandle_, key, 0, key.length);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param key the key retrieve the value.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param opt Read options.
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] get(final ReadOptions opt, final byte[] key, final int offset,
      final int len) throws RocksDBException {
    checkBounds(offset, len, key.length);
    return get(nativeHandle_, opt.nativeHandle_, key, offset, len);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the key retrieve the value.
   * @param opt Read options.
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] get(final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions opt, final byte[] key) throws RocksDBException {
    return get(nativeHandle_, opt.nativeHandle_, key, 0, key.length,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * The simplified version of get which returns a new byte array storing
   * the value associated with the specified input key if any.  null will be
   * returned if the specified key is not found.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param key the key retrieve the value.
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than ("key".length -  offset)
   * @param opt Read options.
   * @return a byte array storing the value associated with the input key if
   *     any. null if it does not find the specified key.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public byte[] get(final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions opt, final byte[] key, final int offset, final int len)
      throws RocksDBException {
    checkBounds(offset, len, key.length);
    return get(nativeHandle_, opt.nativeHandle_, key, offset, len,
        columnFamilyHandle.nativeHandle_);
  }

  /**
   * Returns a map of keys for which values were found in DB.
   *
   * @param keys List of keys for which values need to be retrieved.
   * @return Map where key of map is the key passed by user and value for map
   * entry is the corresponding value in DB.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   *
   * @deprecated Consider {@link #multiGetAsList(List)} instead.
   */
  @Deprecated
  public Map<byte[], byte[]> multiGet(final List<byte[]> keys)
      throws RocksDBException {
    assert(keys.size() != 0);

    final byte[][] keysArray = keys.toArray(new byte[0][]);
    final int keyOffsets[] = new int[keysArray.length];
    final int keyLengths[] = new int[keysArray.length];
    for(int i = 0; i < keyLengths.length; i++) {
      keyLengths[i] = keysArray[i].length;
    }

    final byte[][] values = multiGet(nativeHandle_, keysArray, keyOffsets,
        keyLengths);

    final Map<byte[], byte[]> keyValueMap =
        new HashMap<>(computeCapacityHint(values.length));
    for(int i = 0; i < values.length; i++) {
      if(values[i] == null) {
        continue;
      }

      keyValueMap.put(keys.get(i), values[i]);
    }

    return keyValueMap;
  }

  /**
   * Returns a map of keys for which values were found in DB.
   * <p>
   * Note: Every key needs to have a related column family name in
   * {@code columnFamilyHandleList}.
   * </p>
   *
   * @param columnFamilyHandleList {@link java.util.List} containing
   *     {@link org.rocksdb.ColumnFamilyHandle} instances.
   * @param keys List of keys for which values need to be retrieved.
   * @return Map where key of map is the key passed by user and value for map
   *     entry is the corresponding value in DB.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IllegalArgumentException thrown if the size of passed keys is not
   *    equal to the amount of passed column family handles.
   *
   * @deprecated Consider {@link #multiGetAsList(List, List)} instead.
   */
  @Deprecated
  public Map<byte[], byte[]> multiGet(
      final List<ColumnFamilyHandle> columnFamilyHandleList,
      final List<byte[]> keys) throws RocksDBException,
      IllegalArgumentException {
    assert(keys.size() != 0);
    // Check if key size equals cfList size. If not a exception must be
    // thrown. If not a Segmentation fault happens.
    if (keys.size() != columnFamilyHandleList.size()) {
      throw new IllegalArgumentException(
          "For each key there must be a ColumnFamilyHandle.");
    }
    final long[] cfHandles = new long[columnFamilyHandleList.size()];
    for (int i = 0; i < columnFamilyHandleList.size(); i++) {
      cfHandles[i] = columnFamilyHandleList.get(i).nativeHandle_;
    }

    final byte[][] keysArray = keys.toArray(new byte[0][]);
    final int keyOffsets[] = new int[keysArray.length];
    final int keyLengths[] = new int[keysArray.length];
    for(int i = 0; i < keyLengths.length; i++) {
      keyLengths[i] = keysArray[i].length;
    }

    final byte[][] values = multiGet(nativeHandle_, keysArray, keyOffsets,
        keyLengths, cfHandles);

    final Map<byte[], byte[]> keyValueMap =
        new HashMap<>(computeCapacityHint(values.length));
    for(int i = 0; i < values.length; i++) {
      if (values[i] == null) {
        continue;
      }
      keyValueMap.put(keys.get(i), values[i]);
    }
    return keyValueMap;
  }

  /**
   * Returns a map of keys for which values were found in DB.
   *
   * @param opt Read options.
   * @param keys of keys for which values need to be retrieved.
   * @return Map where key of map is the key passed by user and value for map
   *     entry is the corresponding value in DB.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   *
   * @deprecated Consider {@link #multiGetAsList(ReadOptions, List)} instead.
   */
  @Deprecated
  public Map<byte[], byte[]> multiGet(final ReadOptions opt,
      final List<byte[]> keys) throws RocksDBException {
    assert(keys.size() != 0);

    final byte[][] keysArray = keys.toArray(new byte[0][]);
    final int keyOffsets[] = new int[keysArray.length];
    final int keyLengths[] = new int[keysArray.length];
    for(int i = 0; i < keyLengths.length; i++) {
      keyLengths[i] = keysArray[i].length;
    }

    final byte[][] values = multiGet(nativeHandle_, opt.nativeHandle_,
        keysArray, keyOffsets, keyLengths);

    final Map<byte[], byte[]> keyValueMap =
        new HashMap<>(computeCapacityHint(values.length));
    for(int i = 0; i < values.length; i++) {
      if(values[i] == null) {
        continue;
      }

      keyValueMap.put(keys.get(i), values[i]);
    }

    return keyValueMap;
  }

  /**
   * Returns a map of keys for which values were found in DB.
   * <p>
   * Note: Every key needs to have a related column family name in
   * {@code columnFamilyHandleList}.
   * </p>
   *
   * @param opt Read options.
   * @param columnFamilyHandleList {@link java.util.List} containing
   *     {@link org.rocksdb.ColumnFamilyHandle} instances.
   * @param keys of keys for which values need to be retrieved.
   * @return Map where key of map is the key passed by user and value for map
   *     entry is the corresponding value in DB.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IllegalArgumentException thrown if the size of passed keys is not
   *    equal to the amount of passed column family handles.
   *
   * @deprecated Consider {@link #multiGetAsList(ReadOptions, List, List)}
   *     instead.
   */
  @Deprecated
  public Map<byte[], byte[]> multiGet(final ReadOptions opt,
      final List<ColumnFamilyHandle> columnFamilyHandleList,
      final List<byte[]> keys) throws RocksDBException {
    assert(keys.size() != 0);
    // Check if key size equals cfList size. If not a exception must be
    // thrown. If not a Segmentation fault happens.
    if (keys.size()!=columnFamilyHandleList.size()){
      throw new IllegalArgumentException(
          "For each key there must be a ColumnFamilyHandle.");
    }
    final long[] cfHandles = new long[columnFamilyHandleList.size()];
    for (int i = 0; i < columnFamilyHandleList.size(); i++) {
      cfHandles[i] = columnFamilyHandleList.get(i).nativeHandle_;
    }

    final byte[][] keysArray = keys.toArray(new byte[0][]);
    final int keyOffsets[] = new int[keysArray.length];
    final int keyLengths[] = new int[keysArray.length];
    for(int i = 0; i < keyLengths.length; i++) {
      keyLengths[i] = keysArray[i].length;
    }

    final byte[][] values = multiGet(nativeHandle_, opt.nativeHandle_,
        keysArray, keyOffsets, keyLengths, cfHandles);

    final Map<byte[], byte[]> keyValueMap
        = new HashMap<>(computeCapacityHint(values.length));
    for(int i = 0; i < values.length; i++) {
      if(values[i] == null) {
        continue;
      }
      keyValueMap.put(keys.get(i), values[i]);
    }

    return keyValueMap;
  }

  /**
   * Takes a list of keys, and returns a list of values for the given list of
   * keys. List will contain null for keys which could not be found.
   *
   * @param keys List of keys for which values need to be retrieved.
   * @return List of values for the given list of keys. List will contain
   * null for keys which could not be found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public List<byte[]> multiGetAsList(final List<byte[]> keys)
      throws RocksDBException {
    assert(keys.size() != 0);

    final byte[][] keysArray = keys.toArray(new byte[keys.size()][]);
    final int keyOffsets[] = new int[keysArray.length];
    final int keyLengths[] = new int[keysArray.length];
    for(int i = 0; i < keyLengths.length; i++) {
      keyLengths[i] = keysArray[i].length;
    }

    return Arrays.asList(multiGet(nativeHandle_, keysArray, keyOffsets,
        keyLengths));
  }

  /**
   * Returns a list of values for the given list of keys. List will contain
   * null for keys which could not be found.
   * <p>
   * Note: Every key needs to have a related column family name in
   * {@code columnFamilyHandleList}.
   * </p>
   *
   * @param columnFamilyHandleList {@link java.util.List} containing
   *     {@link org.rocksdb.ColumnFamilyHandle} instances.
   * @param keys List of keys for which values need to be retrieved.
   * @return List of values for the given list of keys. List will contain
   * null for keys which could not be found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IllegalArgumentException thrown if the size of passed keys is not
   *    equal to the amount of passed column family handles.
   */
  public List<byte[]> multiGetAsList(
      final List<ColumnFamilyHandle> columnFamilyHandleList,
      final List<byte[]> keys) throws RocksDBException,
      IllegalArgumentException {
    assert(keys.size() != 0);
    // Check if key size equals cfList size. If not a exception must be
    // thrown. If not a Segmentation fault happens.
    if (keys.size() != columnFamilyHandleList.size()) {
        throw new IllegalArgumentException(
            "For each key there must be a ColumnFamilyHandle.");
    }
    final long[] cfHandles = new long[columnFamilyHandleList.size()];
    for (int i = 0; i < columnFamilyHandleList.size(); i++) {
      cfHandles[i] = columnFamilyHandleList.get(i).nativeHandle_;
    }

    final byte[][] keysArray = keys.toArray(new byte[keys.size()][]);
    final int keyOffsets[] = new int[keysArray.length];
    final int keyLengths[] = new int[keysArray.length];
    for(int i = 0; i < keyLengths.length; i++) {
      keyLengths[i] = keysArray[i].length;
    }

    return Arrays.asList(multiGet(nativeHandle_, keysArray, keyOffsets,
        keyLengths, cfHandles));
  }

  /**
   * Returns a list of values for the given list of keys. List will contain
   * null for keys which could not be found.
   *
   * @param opt Read options.
   * @param keys of keys for which values need to be retrieved.
   * @return List of values for the given list of keys. List will contain
   * null for keys which could not be found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public List<byte[]> multiGetAsList(final ReadOptions opt,
      final List<byte[]> keys) throws RocksDBException {
    assert(keys.size() != 0);

    final byte[][] keysArray = keys.toArray(new byte[keys.size()][]);
    final int keyOffsets[] = new int[keysArray.length];
    final int keyLengths[] = new int[keysArray.length];
    for(int i = 0; i < keyLengths.length; i++) {
      keyLengths[i] = keysArray[i].length;
    }

    return Arrays.asList(multiGet(nativeHandle_, opt.nativeHandle_,
        keysArray, keyOffsets, keyLengths));
  }

  /**
   * Returns a list of values for the given list of keys. List will contain
   * null for keys which could not be found.
   * <p>
   * Note: Every key needs to have a related column family name in
   * {@code columnFamilyHandleList}.
   * </p>
   *
   * @param opt Read options.
   * @param columnFamilyHandleList {@link java.util.List} containing
   *     {@link org.rocksdb.ColumnFamilyHandle} instances.
   * @param keys of keys for which values need to be retrieved.
   * @return List of values for the given list of keys. List will contain
   * null for keys which could not be found.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   * @throws IllegalArgumentException thrown if the size of passed keys is not
   *    equal to the amount of passed column family handles.
   */
  public List<byte[]> multiGetAsList(final ReadOptions opt,
      final List<ColumnFamilyHandle> columnFamilyHandleList,
      final List<byte[]> keys) throws RocksDBException {
    assert(keys.size() != 0);
    // Check if key size equals cfList size. If not a exception must be
    // thrown. If not a Segmentation fault happens.
    if (keys.size()!=columnFamilyHandleList.size()){
      throw new IllegalArgumentException(
          "For each key there must be a ColumnFamilyHandle.");
    }
    final long[] cfHandles = new long[columnFamilyHandleList.size()];
    for (int i = 0; i < columnFamilyHandleList.size(); i++) {
      cfHandles[i] = columnFamilyHandleList.get(i).nativeHandle_;
    }

    final byte[][] keysArray = keys.toArray(new byte[keys.size()][]);
    final int keyOffsets[] = new int[keysArray.length];
    final int keyLengths[] = new int[keysArray.length];
    for(int i = 0; i < keyLengths.length; i++) {
      keyLengths[i] = keysArray[i].length;
    }

    return Arrays.asList(multiGet(nativeHandle_, opt.nativeHandle_,
        keysArray, keyOffsets, keyLengths, cfHandles));
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, else true.
   *
   * This check is potentially lighter-weight than invoking DB::Get(). One way
   * to make this lighter weight is to avoid doing any IOs.
   *
   * @param key byte array of a key to search for
   * @param value StringBuilder instance which is a out parameter if a value is
   *    found in block-cache.
   * @return boolean value indicating if key does not exist or might exist.
   */
  public boolean keyMayExist(final byte[] key, final StringBuilder value) {
    return keyMayExist(nativeHandle_, key, 0, key.length, value);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, else true.
   *
   * This check is potentially lighter-weight than invoking DB::Get(). One way
   * to make this lighter weight is to avoid doing any IOs.
   *
   * @param key byte array of a key to search for
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than "key".length
   * @param value StringBuilder instance which is a out parameter if a value is
   *    found in block-cache.
   *
   * @return boolean value indicating if key does not exist or might exist.
   */
  public boolean keyMayExist(final byte[] key, final int offset, final int len,
      final StringBuilder value) {
    checkBounds(offset, len, key.length);
    return keyMayExist(nativeHandle_, key, offset, len, value);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, else true.
   *
   * This check is potentially lighter-weight than invoking DB::Get(). One way
   * to make this lighter weight is to avoid doing any IOs.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param key byte array of a key to search for
   * @param value StringBuilder instance which is a out parameter if a value is
   *    found in block-cache.
   * @return boolean value indicating if key does not exist or might exist.
   */
  public boolean keyMayExist(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, final StringBuilder value) {
    return keyMayExist(nativeHandle_, key, 0, key.length,
        columnFamilyHandle.nativeHandle_, value);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, else true.
   *
   * This check is potentially lighter-weight than invoking DB::Get(). One way
   * to make this lighter weight is to avoid doing any IOs.
   *
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param key byte array of a key to search for
   * @param offset the offset of the "key" array to be used, must be
   *    non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *    and no larger than "key".length
   * @param value StringBuilder instance which is a out parameter if a value is
   *    found in block-cache.
   * @return boolean value indicating if key does not exist or might exist.
   */
  public boolean keyMayExist(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] key, int offset, int len, final StringBuilder value) {
    checkBounds(offset, len, key.length);
    return keyMayExist(nativeHandle_, key, offset, len,
        columnFamilyHandle.nativeHandle_, value);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, else true.
   *
   * This check is potentially lighter-weight than invoking DB::Get(). One way
   * to make this lighter weight is to avoid doing any IOs.
   *
   * @param readOptions {@link ReadOptions} instance
   * @param key byte array of a key to search for
   * @param value StringBuilder instance which is a out parameter if a value is
   *    found in block-cache.
   * @return boolean value indicating if key does not exist or might exist.
   */
  public boolean keyMayExist(final ReadOptions readOptions,
      final byte[] key, final StringBuilder value) {
    return keyMayExist(nativeHandle_, readOptions.nativeHandle_,
        key, 0, key.length, value);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, else true.
   *
   * This check is potentially lighter-weight than invoking DB::Get(). One way
   * to make this lighter weight is to avoid doing any IOs.
   *
   * @param readOptions {@link ReadOptions} instance
   * @param key byte array of a key to search for
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than "key".length
   * @param value StringBuilder instance which is a out parameter if a value is
   *    found in block-cache.
   * @return boolean value indicating if key does not exist or might exist.
   */
  public boolean keyMayExist(final ReadOptions readOptions,
      final byte[] key, final int offset, final int len,
      final StringBuilder value) {
    checkBounds(offset, len, key.length);
    return keyMayExist(nativeHandle_, readOptions.nativeHandle_,
        key, offset, len, value);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, else true.
   *
   * This check is potentially lighter-weight than invoking DB::Get(). One way
   * to make this lighter weight is to avoid doing any IOs.
   *
   * @param readOptions {@link ReadOptions} instance
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param key byte array of a key to search for
   * @param value StringBuilder instance which is a out parameter if a value is
   *    found in block-cache.
   * @return boolean value indicating if key does not exist or might exist.
   */
  public boolean keyMayExist(final ReadOptions readOptions,
      final ColumnFamilyHandle columnFamilyHandle, final byte[] key,
      final StringBuilder value) {
    return keyMayExist(nativeHandle_, readOptions.nativeHandle_,
        key, 0, key.length, columnFamilyHandle.nativeHandle_,
        value);
  }

  /**
   * If the key definitely does not exist in the database, then this method
   * returns false, else true.
   *
   * This check is potentially lighter-weight than invoking DB::Get(). One way
   * to make this lighter weight is to avoid doing any IOs.
   *
   * @param readOptions {@link ReadOptions} instance
   * @param columnFamilyHandle {@link ColumnFamilyHandle} instance
   * @param key byte array of a key to search for
   * @param offset the offset of the "key" array to be used, must be
   *     non-negative and no larger than "key".length
   * @param len the length of the "key" array to be used, must be non-negative
   *     and no larger than "key".length
   * @param value StringBuilder instance which is a out parameter if a value is
   *    found in block-cache.
   * @return boolean value indicating if key does not exist or might exist.
   */
  public boolean keyMayExist(final ReadOptions readOptions,
      final ColumnFamilyHandle columnFamilyHandle, final byte[] key,
      final int offset, final int len, final StringBuilder value) {
    checkBounds(offset, len, key.length);
    return keyMayExist(nativeHandle_, readOptions.nativeHandle_,
        key, offset, len, columnFamilyHandle.nativeHandle_,
        value);
  }

  /**
   * <p>Return a heap-allocated iterator over the contents of the
   * database. The result of newIterator() is initially invalid
   * (caller must call one of the Seek methods on the iterator
   * before using it).</p>
   *
   * <p>Caller should close the iterator when it is no longer needed.
   * The returned iterator should be closed before this db is closed.
   * </p>
   *
   * @return instance of iterator object.
   */
  public RocksIterator newIterator() {
    return new RocksIterator(this, iterator(nativeHandle_));
  }

  /**
   * <p>Return a heap-allocated iterator over the contents of the
   * database. The result of newIterator() is initially invalid
   * (caller must call one of the Seek methods on the iterator
   * before using it).</p>
   *
   * <p>Caller should close the iterator when it is no longer needed.
   * The returned iterator should be closed before this db is closed.
   * </p>
   *
   * @param readOptions {@link ReadOptions} instance.
   * @return instance of iterator object.
   */
  public RocksIterator newIterator(final ReadOptions readOptions) {
    return new RocksIterator(this, iterator(nativeHandle_,
        readOptions.nativeHandle_));
  }

  /**
   * <p>Return a heap-allocated iterator over the contents of the
   * database. The result of newIterator() is initially invalid
   * (caller must call one of the Seek methods on the iterator
   * before using it).</p>
   *
   * <p>Caller should close the iterator when it is no longer needed.
   * The returned iterator should be closed before this db is closed.
   * </p>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @return instance of iterator object.
   */
  public RocksIterator newIterator(
      final ColumnFamilyHandle columnFamilyHandle) {
    return new RocksIterator(this, iteratorCF(nativeHandle_,
        columnFamilyHandle.nativeHandle_));
  }

  /**
   * <p>Return a heap-allocated iterator over the contents of the
   * database. The result of newIterator() is initially invalid
   * (caller must call one of the Seek methods on the iterator
   * before using it).</p>
   *
   * <p>Caller should close the iterator when it is no longer needed.
   * The returned iterator should be closed before this db is closed.
   * </p>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance
   * @param readOptions {@link ReadOptions} instance.
   * @return instance of iterator object.
   */
  public RocksIterator newIterator(final ColumnFamilyHandle columnFamilyHandle,
      final ReadOptions readOptions) {
    return new RocksIterator(this, iteratorCF(nativeHandle_,
        columnFamilyHandle.nativeHandle_, readOptions.nativeHandle_));
  }

  /**
   * Returns iterators from a consistent database state across multiple
   * column families. Iterators are heap allocated and need to be deleted
   * before the db is deleted
   *
   * @param columnFamilyHandleList {@link java.util.List} containing
   *     {@link org.rocksdb.ColumnFamilyHandle} instances.
   * @return {@link java.util.List} containing {@link org.rocksdb.RocksIterator}
   *     instances
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public List<RocksIterator> newIterators(
      final List<ColumnFamilyHandle> columnFamilyHandleList)
      throws RocksDBException {
    return newIterators(columnFamilyHandleList, new ReadOptions());
  }

  /**
   * Returns iterators from a consistent database state across multiple
   * column families. Iterators are heap allocated and need to be deleted
   * before the db is deleted
   *
   * @param columnFamilyHandleList {@link java.util.List} containing
   *     {@link org.rocksdb.ColumnFamilyHandle} instances.
   * @param readOptions {@link ReadOptions} instance.
   * @return {@link java.util.List} containing {@link org.rocksdb.RocksIterator}
   *     instances
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public List<RocksIterator> newIterators(
      final List<ColumnFamilyHandle> columnFamilyHandleList,
      final ReadOptions readOptions) throws RocksDBException {

    final long[] columnFamilyHandles = new long[columnFamilyHandleList.size()];
    for (int i = 0; i < columnFamilyHandleList.size(); i++) {
      columnFamilyHandles[i] = columnFamilyHandleList.get(i).nativeHandle_;
    }

    final long[] iteratorRefs = iterators(nativeHandle_, columnFamilyHandles,
        readOptions.nativeHandle_);

    final List<RocksIterator> iterators = new ArrayList<>(
        columnFamilyHandleList.size());
    for (int i=0; i<columnFamilyHandleList.size(); i++){
      iterators.add(new RocksIterator(this, iteratorRefs[i]));
    }
    return iterators;
  }


  /**
   * <p>Return a handle to the current DB state. Iterators created with
   * this handle will all observe a stable snapshot of the current DB
   * state. The caller must call ReleaseSnapshot(result) when the
   * snapshot is no longer needed.</p>
   *
   * <p>nullptr will be returned if the DB fails to take a snapshot or does
   * not support snapshot.</p>
   *
   * @return Snapshot {@link Snapshot} instance
   */
  public Snapshot getSnapshot() {
    long snapshotHandle = getSnapshot(nativeHandle_);
    if (snapshotHandle != 0) {
      return new Snapshot(snapshotHandle);
    }
    return null;
  }

  /**
   * Release a previously acquired snapshot.
   *
   * The caller must not use "snapshot" after this call.
   *
   * @param snapshot {@link Snapshot} instance
   */
  public void releaseSnapshot(final Snapshot snapshot) {
    if (snapshot != null) {
      releaseSnapshot(nativeHandle_, snapshot.nativeHandle_);
    }
  }

  /**
   * DB implements can export properties about their state
   * via this method on a per column family level.
   *
   * <p>If {@code property} is a valid property understood by this DB
   * implementation, fills {@code value} with its current value and
   * returns true. Otherwise returns false.</p>
   *
   * <p>Valid property names include:
   * <ul>
   * <li>"rocksdb.num-files-at-level&lt;N&gt;" - return the number of files at
   * level &lt;N&gt;, where &lt;N&gt; is an ASCII representation of a level
   * number (e.g. "0").</li>
   * <li>"rocksdb.stats" - returns a multi-line string that describes statistics
   *     about the internal operation of the DB.</li>
   * <li>"rocksdb.sstables" - returns a multi-line string that describes all
   *    of the sstables that make up the db contents.</li>
   * </ul>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance, or null for the default column family.
   * @param property to be fetched. See above for examples
   * @return property value
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public String getProperty(
      /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle,
      final String property) throws RocksDBException {
    return getProperty(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
        property, property.length());
  }

  /**
   * DB implementations can export properties about their state
   * via this method.  If "property" is a valid property understood by this
   * DB implementation, fills "*value" with its current value and returns
   * true.  Otherwise returns false.
   *
   * <p>Valid property names include:
   * <ul>
   * <li>"rocksdb.num-files-at-level&lt;N&gt;" - return the number of files at
   * level &lt;N&gt;, where &lt;N&gt; is an ASCII representation of a level
   * number (e.g. "0").</li>
   * <li>"rocksdb.stats" - returns a multi-line string that describes statistics
   *     about the internal operation of the DB.</li>
   * <li>"rocksdb.sstables" - returns a multi-line string that describes all
   *    of the sstables that make up the db contents.</li>
   *</ul>
   *
   * @param property to be fetched. See above for examples
   * @return property value
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public String getProperty(final String property) throws RocksDBException {
    return getProperty(null, property);
  }


  /**
   * Gets a property map.
   *
   * @param property to be fetched.
   *
   * @return the property map
   *
   * @throws RocksDBException if an error happens in the underlying native code.
   */
  public Map<String, String> getMapProperty(final String property)
      throws RocksDBException {
    return getMapProperty(null, property);
  }

  /**
   * Gets a property map.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance, or null for the default column family.
   * @param property to be fetched.
   *
   * @return the property map
   *
   * @throws RocksDBException if an error happens in the underlying native code.
   */
  public Map<String, String> getMapProperty(
      /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle,
                      final String property) throws RocksDBException {
    return getMapProperty(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
        property, property.length());
  }

  /**
   * <p> Similar to GetProperty(), but only works for a subset of properties
   * whose return value is a numerical value. Return the value as long.</p>
   *
   * <p><strong>Note</strong>: As the returned property is of type
   * {@code uint64_t} on C++ side the returning value can be negative
   * because Java supports in Java 7 only signed long values.</p>
   *
   * <p><strong>Java 7</strong>: To mitigate the problem of the non
   * existent unsigned long tpye, values should be encapsulated using
   * {@link java.math.BigInteger} to reflect the correct value. The correct
   * behavior is guaranteed if {@code 2^64} is added to negative values.</p>
   *
   * <p><strong>Java 8</strong>: In Java 8 the value should be treated as
   * unsigned long using provided methods of type {@link Long}.</p>
   *
   * @param property to be fetched.
   *
   * @return numerical property value.
   *
   * @throws RocksDBException if an error happens in the underlying native code.
   */
  public long getLongProperty(final String property) throws RocksDBException {
    return getLongProperty(null, property);
  }

  /**
   * <p> Similar to GetProperty(), but only works for a subset of properties
   * whose return value is a numerical value. Return the value as long.</p>
   *
   * <p><strong>Note</strong>: As the returned property is of type
   * {@code uint64_t} on C++ side the returning value can be negative
   * because Java supports in Java 7 only signed long values.</p>
   *
   * <p><strong>Java 7</strong>: To mitigate the problem of the non
   * existent unsigned long tpye, values should be encapsulated using
   * {@link java.math.BigInteger} to reflect the correct value. The correct
   * behavior is guaranteed if {@code 2^64} is added to negative values.</p>
   *
   * <p><strong>Java 8</strong>: In Java 8 the value should be treated as
   * unsigned long using provided methods of type {@link Long}.</p>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance, or null for the default column family
   * @param property to be fetched.
   *
   * @return numerical property value
   *
   * @throws RocksDBException if an error happens in the underlying native code.
   */
  public long getLongProperty(
      /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle,
      final String property) throws RocksDBException {
    return getLongProperty(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
        property, property.length());
  }

  /**
   * Reset internal stats for DB and all column families.
   *
   * Note this doesn't reset {@link Options#statistics()} as it is not
   * owned by DB.
   */
  public void resetStats() throws RocksDBException {
    resetStats(nativeHandle_);
  }

  /**
   * <p> Return sum of the getLongProperty of all the column families</p>
   *
   * <p><strong>Note</strong>: As the returned property is of type
   * {@code uint64_t} on C++ side the returning value can be negative
   * because Java supports in Java 7 only signed long values.</p>
   *
   * <p><strong>Java 7</strong>: To mitigate the problem of the non
   * existent unsigned long tpye, values should be encapsulated using
   * {@link java.math.BigInteger} to reflect the correct value. The correct
   * behavior is guaranteed if {@code 2^64} is added to negative values.</p>
   *
   * <p><strong>Java 8</strong>: In Java 8 the value should be treated as
   * unsigned long using provided methods of type {@link Long}.</p>
   *
   * @param property to be fetched.
   *
   * @return numerical property value
   *
   * @throws RocksDBException if an error happens in the underlying native code.
   */
  public long getAggregatedLongProperty(final String property)
      throws RocksDBException {
    return getAggregatedLongProperty(nativeHandle_, property,
        property.length());
  }

  /**
   * Get the approximate file system space used by keys in each range.
   *
   * Note that the returned sizes measure file system space usage, so
   * if the user data compresses by a factor of ten, the returned
   * sizes will be one-tenth the size of the corresponding user data size.
   *
   * If {@code sizeApproximationFlags} defines whether the returned size
   * should include the recently written data in the mem-tables (if
   * the mem-table type supports it), data serialized to disk, or both.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance, or null for the default column family
   * @param ranges the ranges over which to approximate sizes
   * @param sizeApproximationFlags flags to determine what to include in the
   *     approximation.
   *
   * @return the sizes
   */
  public long[] getApproximateSizes(
      /*@Nullable*/ final ColumnFamilyHandle columnFamilyHandle,
      final List<Range> ranges,
      final SizeApproximationFlag... sizeApproximationFlags) {

    byte flags = 0x0;
    for (final SizeApproximationFlag sizeApproximationFlag
        : sizeApproximationFlags) {
      flags |= sizeApproximationFlag.getValue();
    }

    return getApproximateSizes(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
        toRangeSliceHandles(ranges), flags);
  }

  /**
   * Get the approximate file system space used by keys in each range for
   * the default column family.
   *
   * Note that the returned sizes measure file system space usage, so
   * if the user data compresses by a factor of ten, the returned
   * sizes will be one-tenth the size of the corresponding user data size.
   *
   * If {@code sizeApproximationFlags} defines whether the returned size
   * should include the recently written data in the mem-tables (if
   * the mem-table type supports it), data serialized to disk, or both.
   *
   * @param ranges the ranges over which to approximate sizes
   * @param sizeApproximationFlags flags to determine what to include in the
   *     approximation.
   *
   * @return the sizes.
   */
  public long[] getApproximateSizes(final List<Range> ranges,
      final SizeApproximationFlag... sizeApproximationFlags) {
    return getApproximateSizes(null, ranges, sizeApproximationFlags);
  }

  public static class CountAndSize {
    public final long count;
    public final long size;

    public CountAndSize(final long count, final long size) {
      this.count = count;
      this.size = size;
    }
  }

  /**
   * This method is similar to
   * {@link #getApproximateSizes(ColumnFamilyHandle, List, SizeApproximationFlag...)},
   * except that it returns approximate number of records and size in memtables.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance, or null for the default column family
   * @param range the ranges over which to get the memtable stats
   *
   * @return the count and size for the range
   */
  public CountAndSize getApproximateMemTableStats(
      /*@Nullable*/ final ColumnFamilyHandle columnFamilyHandle,
      final Range range) {
    final long[] result = getApproximateMemTableStats(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
        range.start.getNativeHandle(),
        range.limit.getNativeHandle());
    return new CountAndSize(result[0], result[1]);
  }

  /**
   * This method is similar to
   * {@link #getApproximateSizes(ColumnFamilyHandle, List, SizeApproximationFlag...)},
   * except that it returns approximate number of records and size in memtables.
   *
   * @param range the ranges over which to get the memtable stats
   *
   * @return the count and size for the range
   */
  public CountAndSize getApproximateMemTableStats(
    final Range range) {
    return getApproximateMemTableStats(null, range);
  }

  /**
   * <p>Range compaction of database.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * <p><strong>See also</strong></p>
   * <ul>
   * <li>{@link #compactRange(boolean, int, int)}</li>
   * <li>{@link #compactRange(byte[], byte[])}</li>
   * <li>{@link #compactRange(byte[], byte[], boolean, int, int)}</li>
   * </ul>
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void compactRange() throws RocksDBException {
    compactRange(null);
  }

  /**
   * <p>Range compaction of column family.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * <p><strong>See also</strong></p>
   * <ul>
   * <li>
   *   {@link #compactRange(ColumnFamilyHandle, boolean, int, int)}
   * </li>
   * <li>
   *   {@link #compactRange(ColumnFamilyHandle, byte[], byte[])}
   * </li>
   * <li>
   *   {@link #compactRange(ColumnFamilyHandle, byte[], byte[],
   *   boolean, int, int)}
   * </li>
   * </ul>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance, or null for the default column family.
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void compactRange(
      /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle)
      throws RocksDBException {
    compactRange(nativeHandle_, null, -1, null, -1, 0,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_);
  }

  /**
   * <p>Range compaction of database.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * <p><strong>See also</strong></p>
   * <ul>
   * <li>{@link #compactRange()}</li>
   * <li>{@link #compactRange(boolean, int, int)}</li>
   * <li>{@link #compactRange(byte[], byte[], boolean, int, int)}</li>
   * </ul>
   *
   * @param begin start of key range (included in range)
   * @param end end of key range (excluded from range)
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void compactRange(final byte[] begin, final byte[] end)
      throws RocksDBException {
    compactRange(null, begin, end);
  }

  /**
   * <p>Range compaction of column family.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * <p><strong>See also</strong></p>
   * <ul>
   * <li>{@link #compactRange(ColumnFamilyHandle)}</li>
   * <li>
   *   {@link #compactRange(ColumnFamilyHandle, boolean, int, int)}
   * </li>
   * <li>
   *   {@link #compactRange(ColumnFamilyHandle, byte[], byte[],
   *   boolean, int, int)}
   * </li>
   * </ul>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance, or null for the default column family.
   * @param begin start of key range (included in range)
   * @param end end of key range (excluded from range)
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void compactRange(
      /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle,
      final byte[] begin, final byte[] end) throws RocksDBException {
    compactRange(nativeHandle_,
        begin, begin == null ? -1 : begin.length,
        end, end == null ? -1 : end.length,
        0, columnFamilyHandle == null ? 0: columnFamilyHandle.nativeHandle_);
  }

  /**
   * <p>Range compaction of database.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * <p>Compaction outputs should be placed in options.db_paths
   * [target_path_id]. Behavior is undefined if target_path_id is
   * out of range.</p>
   *
   * <p><strong>See also</strong></p>
   * <ul>
   * <li>{@link #compactRange()}</li>
   * <li>{@link #compactRange(byte[], byte[])}</li>
   * <li>{@link #compactRange(byte[], byte[], boolean, int, int)}</li>
   * </ul>
   *
   * @deprecated Use {@link #compactRange(ColumnFamilyHandle, byte[], byte[], CompactRangeOptions)} instead
   *
   * @param changeLevel reduce level after compaction
   * @param targetLevel target level to compact to
   * @param targetPathId the target path id of output path
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  @Deprecated
  public void compactRange(final boolean changeLevel, final int targetLevel,
      final int targetPathId) throws RocksDBException {
    compactRange(null, changeLevel, targetLevel, targetPathId);
  }

  /**
   * <p>Range compaction of column family.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * <p>Compaction outputs should be placed in options.db_paths
   * [target_path_id]. Behavior is undefined if target_path_id is
   * out of range.</p>
   *
   * <p><strong>See also</strong></p>
   * <ul>
   * <li>{@link #compactRange(ColumnFamilyHandle)}</li>
   * <li>
   *   {@link #compactRange(ColumnFamilyHandle, byte[], byte[])}
   * </li>
   * <li>
   *   {@link #compactRange(ColumnFamilyHandle, byte[], byte[],
   *   boolean, int, int)}
   * </li>
   * </ul>
   *
   * @deprecated Use {@link #compactRange(ColumnFamilyHandle, byte[], byte[], CompactRangeOptions)} instead
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance, or null for the default column family.
   * @param changeLevel reduce level after compaction
   * @param targetLevel target level to compact to
   * @param targetPathId the target path id of output path
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  @Deprecated
  public void compactRange(
    /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle,
    final boolean changeLevel, final int targetLevel, final int targetPathId)
      throws RocksDBException {
    final CompactRangeOptions options = new CompactRangeOptions();
    options.setChangeLevel(changeLevel);
    options.setTargetLevel(targetLevel);
    options.setTargetPathId(targetPathId);
    compactRange(nativeHandle_,
        null, -1,
        null, -1,
        options.nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_);
  }

  /**
   * <p>Range compaction of database.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * <p>Compaction outputs should be placed in options.db_paths
   * [target_path_id]. Behavior is undefined if target_path_id is
   * out of range.</p>
   *
   * <p><strong>See also</strong></p>
   * <ul>
   * <li>{@link #compactRange()}</li>
   * <li>{@link #compactRange(boolean, int, int)}</li>
   * <li>{@link #compactRange(byte[], byte[])}</li>
   * </ul>
   *
   * @deprecated Use {@link #compactRange(ColumnFamilyHandle, byte[], byte[], CompactRangeOptions)}
   *     instead
   *
   * @param begin start of key range (included in range)
   * @param end end of key range (excluded from range)
   * @param changeLevel reduce level after compaction
   * @param targetLevel target level to compact to
   * @param targetPathId the target path id of output path
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  @Deprecated
  public void compactRange(final byte[] begin, final byte[] end,
      final boolean changeLevel, final int targetLevel,
      final int targetPathId) throws RocksDBException {
    compactRange(null, begin, end, changeLevel, targetLevel, targetPathId);
  }

  /**
   * <p>Range compaction of column family.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * <p>Compaction outputs should be placed in options.db_paths
   * [target_path_id]. Behavior is undefined if target_path_id is
   * out of range.</p>
   *
   * <p><strong>See also</strong></p>
   * <ul>
   * <li>{@link #compactRange(ColumnFamilyHandle)}</li>
   * <li>
   *   {@link #compactRange(ColumnFamilyHandle, boolean, int, int)}
   * </li>
   * <li>
   *   {@link #compactRange(ColumnFamilyHandle, byte[], byte[])}
   * </li>
   * </ul>
   *
   * @deprecated Use {@link #compactRange(ColumnFamilyHandle, byte[], byte[], CompactRangeOptions)} instead
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance.
   * @param begin start of key range (included in range)
   * @param end end of key range (excluded from range)
   * @param changeLevel reduce level after compaction
   * @param targetLevel target level to compact to
   * @param targetPathId the target path id of output path
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  @Deprecated
  public void compactRange(
      /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle,
      final byte[] begin, final byte[] end, final boolean changeLevel,
      final int targetLevel, final int targetPathId)
      throws RocksDBException {
    final CompactRangeOptions options = new CompactRangeOptions();
    options.setChangeLevel(changeLevel);
    options.setTargetLevel(targetLevel);
    options.setTargetPathId(targetPathId);
    compactRange(nativeHandle_,
        begin, begin == null ? -1 : begin.length,
        end, end == null ? -1 : end.length,
        options.nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_);
  }

  /**
   * <p>Range compaction of column family.</p>
   * <p><strong>Note</strong>: After the database has been compacted,
   * all data will have been pushed down to the last level containing
   * any data.</p>
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle} instance.
   * @param begin start of key range (included in range)
   * @param end end of key range (excluded from range)
   * @param compactRangeOptions options for the compaction
   *
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void compactRange(final ColumnFamilyHandle columnFamilyHandle,
      final byte[] begin, final byte[] end,
      final CompactRangeOptions compactRangeOptions) throws RocksDBException {
    compactRange(nativeHandle_,
        begin, begin == null ? -1 : begin.length,
        end, end == null ? -1 : end.length,
        compactRangeOptions.nativeHandle_, columnFamilyHandle.nativeHandle_);
  }

  /**
   * Change the options for the column family handle.
   *
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle}
   *     instance, or null for the default column family.
   * @param mutableColumnFamilyOptions the options.
   */
  public void setOptions(
      /* @Nullable */final ColumnFamilyHandle columnFamilyHandle,
      final MutableColumnFamilyOptions mutableColumnFamilyOptions)
      throws RocksDBException {
    setOptions(nativeHandle_, columnFamilyHandle.nativeHandle_,
        mutableColumnFamilyOptions.getKeys(),
        mutableColumnFamilyOptions.getValues());
  }

  /**
   * Change the options for the default column family handle.
   *
   * @param mutableColumnFamilyOptions the options.
   */
  public void setOptions(
      final MutableColumnFamilyOptions mutableColumnFamilyOptions)
      throws RocksDBException {
    setOptions(null, mutableColumnFamilyOptions);
  }

  /**
   * Set the options for the column family handle.
   *
   * @param mutableDBoptions the options.
   */
  public void setDBOptions(final MutableDBOptions mutableDBoptions)
      throws RocksDBException {
    setDBOptions(nativeHandle_,
        mutableDBoptions.getKeys(),
        mutableDBoptions.getValues());
  }

  /**
   * Takes nputs a list of files specified by file names and
   * compacts them to the specified level.
   *
   * Note that the behavior is different from
   * {@link #compactRange(ColumnFamilyHandle, byte[], byte[])}
   * in that CompactFiles() performs the compaction job using the CURRENT
   * thread.
   *
   * @param compactionOptions compaction options
   * @param inputFileNames the name of the files to compact
   * @param outputLevel the level to which they should be compacted
   * @param outputPathId the id of the output path, or -1
   * @param compactionJobInfo the compaction job info, this parameter
   *     will be updated with the info from compacting the files,
   *     can just be null if you don't need it.
   */
  public List<String> compactFiles(
      final CompactionOptions compactionOptions,
      final List<String> inputFileNames,
      final int outputLevel,
      final int outputPathId,
      /* @Nullable */ final CompactionJobInfo compactionJobInfo)
      throws RocksDBException {
    return compactFiles(compactionOptions, null, inputFileNames, outputLevel,
        outputPathId, compactionJobInfo);
  }

  /**
   * Takes a list of files specified by file names and
   * compacts them to the specified level.
   *
   * Note that the behavior is different from
   * {@link #compactRange(ColumnFamilyHandle, byte[], byte[])}
   * in that CompactFiles() performs the compaction job using the CURRENT
   * thread.
   *
   * @param compactionOptions compaction options
   * @param columnFamilyHandle columnFamilyHandle, or null for the
   *     default column family
   * @param inputFileNames the name of the files to compact
   * @param outputLevel the level to which they should be compacted
   * @param outputPathId the id of the output path, or -1
   * @param compactionJobInfo the compaction job info, this parameter
   *     will be updated with the info from compacting the files,
   *     can just be null if you don't need it.
   */
  public List<String> compactFiles(
      final CompactionOptions compactionOptions,
      /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle,
      final List<String> inputFileNames,
      final int outputLevel,
      final int outputPathId,
      /* @Nullable */ final CompactionJobInfo compactionJobInfo)
      throws RocksDBException {
    return Arrays.asList(compactFiles(nativeHandle_, compactionOptions.nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
        inputFileNames.toArray(new String[0]),
        outputLevel,
        outputPathId,
        compactionJobInfo == null ? 0 : compactionJobInfo.nativeHandle_));
  }

  /**
   * This function will wait until all currently running background processes
   * finish. After it returns, no background process will be run until
   * {@link #continueBackgroundWork()} is called
   *
   * @throws RocksDBException If an error occurs when pausing background work
   */
  public void pauseBackgroundWork() throws RocksDBException {
    pauseBackgroundWork(nativeHandle_);
  }

  /**
   * Resumes background work which was suspended by
   * previously calling {@link #pauseBackgroundWork()}
   *
   * @throws RocksDBException If an error occurs when resuming background work
   */
  public void continueBackgroundWork() throws RocksDBException {
    continueBackgroundWork(nativeHandle_);
  }

  /**
   * Enable automatic compactions for the given column
   * families if they were previously disabled.
   *
   * The function will first set the
   * {@link ColumnFamilyOptions#disableAutoCompactions()} option for each
   * column family to false, after which it will schedule a flush/compaction.
   *
   * NOTE: Setting disableAutoCompactions to 'false' through
   * {@link #setOptions(ColumnFamilyHandle, MutableColumnFamilyOptions)}
   * does NOT schedule a flush/compaction afterwards, and only changes the
   * parameter itself within the column family option.
   *
   * @param columnFamilyHandles the column family handles
   */
  public void enableAutoCompaction(
      final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    enableAutoCompaction(nativeHandle_,
        toNativeHandleList(columnFamilyHandles));
  }

  /**
   * Number of levels used for this DB.
   *
   * @return the number of levels
   */
  public int numberLevels() {
    return numberLevels(null);
  }

  /**
   * Number of levels used for a column family in this DB.
   *
   * @param columnFamilyHandle the column family handle, or null
   *     for the default column family
   *
   * @return the number of levels
   */
  public int numberLevels(/* @Nullable */final ColumnFamilyHandle columnFamilyHandle) {
    return numberLevels(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_);
  }

  /**
   * Maximum level to which a new compacted memtable is pushed if it
   * does not create overlap.
   */
  public int maxMemCompactionLevel() {
    return maxMemCompactionLevel(null);
  }

  /**
   * Maximum level to which a new compacted memtable is pushed if it
   * does not create overlap.
   *
   * @param columnFamilyHandle the column family handle
   */
  public int maxMemCompactionLevel(
      /* @Nullable */final ColumnFamilyHandle columnFamilyHandle) {
      return maxMemCompactionLevel(nativeHandle_,
          columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_);
  }

  /**
   * Number of files in level-0 that would stop writes.
   */
  public int level0StopWriteTrigger() {
    return level0StopWriteTrigger(null);
  }

  /**
   * Number of files in level-0 that would stop writes.
   *
   * @param columnFamilyHandle the column family handle
   */
  public int level0StopWriteTrigger(
      /* @Nullable */final ColumnFamilyHandle columnFamilyHandle) {
    return level0StopWriteTrigger(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_);
  }

  /**
   * Get DB name -- the exact same name that was provided as an argument to
   * as path to {@link #open(Options, String)}.
   *
   * @return the DB name
   */
  public String getName() {
    return getName(nativeHandle_);
  }

  /**
   * Get the Env object from the DB
   *
   * @return the env
   */
  public Env getEnv() {
    final long envHandle = getEnv(nativeHandle_);
    if (envHandle == Env.getDefault().nativeHandle_) {
      return Env.getDefault();
    } else {
      final Env env = new RocksEnv(envHandle);
      env.disOwnNativeHandle();  // we do not own the Env!
      return env;
    }
  }

  /**
   * <p>Flush all memory table data.</p>
   *
   * <p>Note: it must be ensured that the FlushOptions instance
   * is not GC'ed before this method finishes. If the wait parameter is
   * set to false, flush processing is asynchronous.</p>
   *
   * @param flushOptions {@link org.rocksdb.FlushOptions} instance.
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void flush(final FlushOptions flushOptions)
      throws RocksDBException {
    flush(flushOptions, (List<ColumnFamilyHandle>) null);
  }

  /**
   * <p>Flush all memory table data.</p>
   *
   * <p>Note: it must be ensured that the FlushOptions instance
   * is not GC'ed before this method finishes. If the wait parameter is
   * set to false, flush processing is asynchronous.</p>
   *
   * @param flushOptions {@link org.rocksdb.FlushOptions} instance.
   * @param columnFamilyHandle {@link org.rocksdb.ColumnFamilyHandle} instance.
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void flush(final FlushOptions flushOptions,
      /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle)
      throws RocksDBException {
    flush(flushOptions,
        columnFamilyHandle == null ? null : Arrays.asList(columnFamilyHandle));
  }

  /**
   * Flushes multiple column families.
   *
   * If atomic flush is not enabled, this is equivalent to calling
   * {@link #flush(FlushOptions, ColumnFamilyHandle)} multiple times.
   *
   * If atomic flush is enabled, this will flush all column families
   * specified up to the latest sequence number at the time when flush is
   * requested.
   *
   * @param flushOptions {@link org.rocksdb.FlushOptions} instance.
   * @param columnFamilyHandles column family handles.
   * @throws RocksDBException thrown if an error occurs within the native
   *     part of the library.
   */
  public void flush(final FlushOptions flushOptions,
      /* @Nullable */ final List<ColumnFamilyHandle> columnFamilyHandles)
      throws RocksDBException {
    flush(nativeHandle_, flushOptions.nativeHandle_,
        toNativeHandleList(columnFamilyHandles));
  }

  /**
   * Flush the WAL memory buffer to the file. If {@code sync} is true,
   * it calls {@link #syncWal()} afterwards.
   *
   * @param sync true to also fsync to disk.
   */
  public void flushWal(final boolean sync) throws RocksDBException {
    flushWal(nativeHandle_, sync);
  }

  /**
   * Sync the WAL.
   *
   * Note that {@link #write(WriteOptions, WriteBatch)} followed by
   * {@link #syncWal()} is not exactly the same as
   * {@link #write(WriteOptions, WriteBatch)} with
   * {@link WriteOptions#sync()} set to true; In the latter case the changes
   * won't be visible until the sync is done.
   *
   * Currently only works if {@link Options#allowMmapWrites()} is set to false.
   */
  public void syncWal() throws RocksDBException {
    syncWal(nativeHandle_);
  }

  /**
   * <p>The sequence number of the most recent transaction.</p>
   *
   * @return sequence number of the most
   *     recent transaction.
   */
  public long getLatestSequenceNumber() {
    return getLatestSequenceNumber(nativeHandle_);
  }

  /**
   * Instructs DB to preserve deletes with sequence numbers &gt;= sequenceNumber.
   *
   * Has no effect if DBOptions#preserveDeletes() is set to false.
   *
   * This function assumes that user calls this function with monotonically
   * increasing seqnums (otherwise we can't guarantee that a particular delete
   * hasn't been already processed).
   *
   * @param sequenceNumber the minimum sequence number to preserve
   *
   * @return true if the value was successfully updated,
   *     false if user attempted to call if with
   *     sequenceNumber &lt;= current value.
   */
  public boolean setPreserveDeletesSequenceNumber(final long sequenceNumber) {
    return setPreserveDeletesSequenceNumber(nativeHandle_, sequenceNumber);
  }

  /**
   * <p>Prevent file deletions. Compactions will continue to occur,
   * but no obsolete files will be deleted. Calling this multiple
   * times have the same effect as calling it once.</p>
   *
   * @throws RocksDBException thrown if operation was not performed
   *     successfully.
   */
  public void disableFileDeletions() throws RocksDBException {
    disableFileDeletions(nativeHandle_);
  }

  /**
   * <p>Allow compactions to delete obsolete files.
   * If force == true, the call to EnableFileDeletions()
   * will guarantee that file deletions are enabled after
   * the call, even if DisableFileDeletions() was called
   * multiple times before.</p>
   *
   * <p>If force == false, EnableFileDeletions will only
   * enable file deletion after it's been called at least
   * as many times as DisableFileDeletions(), enabling
   * the two methods to be called by two threads
   * concurrently without synchronization
   * -- i.e., file deletions will be enabled only after both
   * threads call EnableFileDeletions()</p>
   *
   * @param force boolean value described above.
   *
   * @throws RocksDBException thrown if operation was not performed
   *     successfully.
   */
  public void enableFileDeletions(final boolean force)
      throws RocksDBException {
    enableFileDeletions(nativeHandle_, force);
  }

  public static class LiveFiles {
    /**
     * The valid size of the manifest file. The manifest file is an ever growing
     * file, but only the portion specified here is valid for this snapshot.
     */
    public final long manifestFileSize;

    /**
     * The files are relative to the {@link #getName()} and are not
     * absolute paths. Despite being relative paths, the file names begin
     * with "/".
     */
    public final List<String> files;

    LiveFiles(final long manifestFileSize, final List<String> files) {
      this.manifestFileSize = manifestFileSize;
      this.files = files;
    }
  }

  /**
   * Retrieve the list of all files in the database after flushing the memtable.
   *
   * See {@link #getLiveFiles(boolean)}.
   *
   * @return the live files
   */
  public LiveFiles getLiveFiles() throws RocksDBException {
    return getLiveFiles(true);
  }

  /**
   * Retrieve the list of all files in the database.
   *
   * In case you have multiple column families, even if {@code flushMemtable}
   * is true, you still need to call {@link #getSortedWalFiles()}
   * after {@link #getLiveFiles(boolean)} to compensate for new data that
   * arrived to already-flushed column families while other column families
   * were flushing.
   *
   * NOTE: Calling {@link #getLiveFiles(boolean)} followed by
   *     {@link #getSortedWalFiles()} can generate a lossless backup.
   *
   * @param flushMemtable set to true to flush before recoding the live
   *     files. Setting to false is useful when we don't want to wait for flush
   *     which may have to wait for compaction to complete taking an
   *     indeterminate time.
   *
   * @return the live files
   */
  public LiveFiles getLiveFiles(final boolean flushMemtable)
      throws RocksDBException {
     final String[] result = getLiveFiles(nativeHandle_, flushMemtable);
     if (result == null) {
       return null;
     }
     final String[] files = Arrays.copyOf(result, result.length - 1);
     final long manifestFileSize = Long.parseLong(result[result.length - 1]);

     return new LiveFiles(manifestFileSize, Arrays.asList(files));
  }

  /**
   * Retrieve the sorted list of all wal files with earliest file first.
   *
   * @return the log files
   */
  public List<LogFile> getSortedWalFiles() throws RocksDBException {
    final LogFile[] logFiles = getSortedWalFiles(nativeHandle_);
    return Arrays.asList(logFiles);
  }

  /**
   * <p>Returns an iterator that is positioned at a write-batch containing
   * seq_number. If the sequence number is non existent, it returns an iterator
   * at the first available seq_no after the requested seq_no.</p>
   *
   * <p>Must set WAL_ttl_seconds or WAL_size_limit_MB to large values to
   * use this api, else the WAL files will get
   * cleared aggressively and the iterator might keep getting invalid before
   * an update is read.</p>
   *
   * @param sequenceNumber sequence number offset
   *
   * @return {@link org.rocksdb.TransactionLogIterator} instance.
   *
   * @throws org.rocksdb.RocksDBException if iterator cannot be retrieved
   *     from native-side.
   */
  public TransactionLogIterator getUpdatesSince(final long sequenceNumber)
      throws RocksDBException {
    return new TransactionLogIterator(
        getUpdatesSince(nativeHandle_, sequenceNumber));
  }

  /**
   * Delete the file name from the db directory and update the internal state to
   * reflect that. Supports deletion of sst and log files only. 'name' must be
   * path relative to the db directory. eg. 000001.sst, /archive/000003.log
   *
   * @param name the file name
   */
  public void deleteFile(final String name) throws RocksDBException {
    deleteFile(nativeHandle_, name);
  }

  /**
   * Gets a list of all table files metadata.
   *
   * @return table files metadata.
   */
  public List<LiveFileMetaData> getLiveFilesMetaData() {
    return Arrays.asList(getLiveFilesMetaData(nativeHandle_));
  }

  /**
   * Obtains the meta data of the specified column family of the DB.
   *
   * @param columnFamilyHandle the column family
   *
   * @return the column family metadata
   */
  public ColumnFamilyMetaData getColumnFamilyMetaData(
      /* @Nullable */ final ColumnFamilyHandle columnFamilyHandle) {
    return getColumnFamilyMetaData(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_);
  }

  /**
   * Obtains the meta data of the default column family of the DB.
   *
   * @return the column family metadata
   */
  public ColumnFamilyMetaData GetColumnFamilyMetaData() {
    return getColumnFamilyMetaData(null);
  }

  /**
   * ingestExternalFile will load a list of external SST files (1) into the DB
   * We will try to find the lowest possible level that the file can fit in, and
   * ingest the file into this level (2). A file that have a key range that
   * overlap with the memtable key range will require us to Flush the memtable
   * first before ingesting the file.
   *
   * (1) External SST files can be created using {@link SstFileWriter}
   * (2) We will try to ingest the files to the lowest possible level
   * even if the file compression doesn't match the level compression
   *
   * @param filePathList The list of files to ingest
   * @param ingestExternalFileOptions the options for the ingestion
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  public void ingestExternalFile(final List<String> filePathList,
      final IngestExternalFileOptions ingestExternalFileOptions)
      throws RocksDBException {
    ingestExternalFile(nativeHandle_, getDefaultColumnFamily().nativeHandle_,
        filePathList.toArray(new String[0]),
        filePathList.size(), ingestExternalFileOptions.nativeHandle_);
  }

  /**
   * ingestExternalFile will load a list of external SST files (1) into the DB
   * We will try to find the lowest possible level that the file can fit in, and
   * ingest the file into this level (2). A file that have a key range that
   * overlap with the memtable key range will require us to Flush the memtable
   * first before ingesting the file.
   *
   * (1) External SST files can be created using {@link SstFileWriter}
   * (2) We will try to ingest the files to the lowest possible level
   * even if the file compression doesn't match the level compression
   *
   * @param columnFamilyHandle The column family for the ingested files
   * @param filePathList The list of files to ingest
   * @param ingestExternalFileOptions the options for the ingestion
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  public void ingestExternalFile(final ColumnFamilyHandle columnFamilyHandle,
      final List<String> filePathList,
      final IngestExternalFileOptions ingestExternalFileOptions)
      throws RocksDBException {
    ingestExternalFile(nativeHandle_, columnFamilyHandle.nativeHandle_,
        filePathList.toArray(new String[0]),
        filePathList.size(), ingestExternalFileOptions.nativeHandle_);
  }

  /**
   * Verify checksum
   *
   * @throws RocksDBException if the checksum is not valid
   */
  public void verifyChecksum() throws RocksDBException {
    verifyChecksum(nativeHandle_);
  }

  /**
   * Gets the handle for the default column family
   *
   * @return The handle of the default column family
   */
  public ColumnFamilyHandle getDefaultColumnFamily() {
    final ColumnFamilyHandle cfHandle = new ColumnFamilyHandle(this,
        getDefaultColumnFamily(nativeHandle_));
    cfHandle.disOwnNativeHandle();
    return cfHandle;
  }

  /**
   * Get the properties of all tables.
   *
   * @param columnFamilyHandle the column family handle, or null for the default
   *     column family.
   *
   * @return the properties
   */
  public Map<String, TableProperties> getPropertiesOfAllTables(
      /* @Nullable */final ColumnFamilyHandle columnFamilyHandle)
      throws RocksDBException {
    return getPropertiesOfAllTables(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_);
  }

  /**
   * Get the properties of all tables in the default column family.
   *
   * @return the properties
   */
  public Map<String, TableProperties> getPropertiesOfAllTables()
      throws RocksDBException {
    return getPropertiesOfAllTables(null);
  }

  /**
   * Get the properties of tables in range.
   *
   * @param columnFamilyHandle the column family handle, or null for the default
   *     column family.
   * @param ranges the ranges over which to get the table properties
   *
   * @return the properties
   */
  public Map<String, TableProperties> getPropertiesOfTablesInRange(
      /* @Nullable */final ColumnFamilyHandle columnFamilyHandle,
      final List<Range> ranges) throws RocksDBException {
    return getPropertiesOfTablesInRange(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
        toRangeSliceHandles(ranges));
  }

  /**
   * Get the properties of tables in range for the default column family.
   *
   * @param ranges the ranges over which to get the table properties
   *
   * @return the properties
   */
  public Map<String, TableProperties> getPropertiesOfTablesInRange(
      final List<Range> ranges) throws RocksDBException {
    return getPropertiesOfTablesInRange(null, ranges);
  }

  /**
   * Suggest the range to compact.
   *
   * @param columnFamilyHandle the column family handle, or null for the default
   *     column family.
   *
   * @return the suggested range.
   */
  public Range suggestCompactRange(
      /* @Nullable */final ColumnFamilyHandle columnFamilyHandle)
      throws RocksDBException {
    final long[] rangeSliceHandles = suggestCompactRange(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_);
    return new Range(new Slice(rangeSliceHandles[0]),
        new Slice(rangeSliceHandles[1]));
  }

  /**
   * Suggest the range to compact for the default column family.
   *
   * @return the suggested range.
   */
  public Range suggestCompactRange()
      throws RocksDBException {
    return suggestCompactRange(null);
  }

  /**
   * Promote L0.
   *
   * @param columnFamilyHandle the column family handle,
   *     or null for the default column family.
   */
  public void promoteL0(
      /* @Nullable */final ColumnFamilyHandle columnFamilyHandle,
      final int targetLevel) throws RocksDBException {
    promoteL0(nativeHandle_,
        columnFamilyHandle == null ? 0 : columnFamilyHandle.nativeHandle_,
        targetLevel);
  }

  /**
   * Promote L0 for the default column family.
   */
  public void promoteL0(final int targetLevel)
      throws RocksDBException {
    promoteL0(null, targetLevel);
  }

  /**
   * Trace DB operations.
   *
   * Use {@link #endTrace()} to stop tracing.
   *
   * @param traceOptions the options
   * @param traceWriter the trace writer
   */
  public void startTrace(final TraceOptions traceOptions,
      final AbstractTraceWriter traceWriter) throws RocksDBException {
    startTrace(nativeHandle_, traceOptions.getMaxTraceFileSize(),
        traceWriter.nativeHandle_);
    /**
     * NOTE: {@link #startTrace(long, long, long) transfers the ownership
     * from Java to C++, so we must disown the native handle here.
     */
    traceWriter.disOwnNativeHandle();
  }

  /**
   * Stop tracing DB operations.
   *
   * See {@link #startTrace(TraceOptions, AbstractTraceWriter)}
   */
  public void endTrace() throws RocksDBException {
    endTrace(nativeHandle_);
  }

  /**
   * Static method to destroy the contents of the specified database.
   * Be very careful using this method.
   *
   * @param path the path to the Rocksdb database.
   * @param options {@link org.rocksdb.Options} instance.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *    native library.
   */
  public static void destroyDB(final String path, final Options options)
      throws RocksDBException {
    destroyDB(path, options.nativeHandle_);
  }

  private /* @Nullable */ long[] toNativeHandleList(
      /* @Nullable */ final List<? extends RocksObject> objectList) {
    if (objectList == null) {
      return null;
    }
    final int len = objectList.size();
    final long[] handleList = new long[len];
    for (int i = 0; i < len; i++) {
      handleList[i] = objectList.get(i).nativeHandle_;
    }
    return handleList;
  }

  private static long[] toRangeSliceHandles(final List<Range> ranges) {
    final long rangeSliceHandles[] = new long [ranges.size() * 2];
    for (int i = 0, j = 0; i < ranges.size(); i++) {
      final Range range = ranges.get(i);
      rangeSliceHandles[j++] = range.start.getNativeHandle();
      rangeSliceHandles[j++] = range.limit.getNativeHandle();
    }
    return rangeSliceHandles;
  }

  protected void storeOptionsInstance(DBOptionsInterface options) {
    options_ = options;
  }

  private static void checkBounds(int offset, int len, int size) {
    if ((offset | len | (offset + len) | (size - (offset + len))) < 0) {
      throw new IndexOutOfBoundsException(String.format("offset(%d), len(%d), size(%d)", offset, len, size));
    }
  }

  private static int computeCapacityHint(final int estimatedNumberOfItems) {
    // Default load factor for HashMap is 0.75, so N * 1.5 will be at the load
    // limit. We add +1 for a buffer.
    return (int)Math.ceil(estimatedNumberOfItems * 1.5 + 1.0);
  }

  // native methods
  private native static long open(final long optionsHandle,
      final String path) throws RocksDBException;

  /**
   * @param optionsHandle Native handle pointing to an Options object
   * @param path The directory path for the database files
   * @param columnFamilyNames An array of column family names
   * @param columnFamilyOptions An array of native handles pointing to
   *                            ColumnFamilyOptions objects
   *
   * @return An array of native handles, [0] is the handle of the RocksDB object
   *   [1..1+n] are handles of the ColumnFamilyReferences
   *
   * @throws RocksDBException thrown if the database could not be opened
   */
  private native static long[] open(final long optionsHandle,
      final String path, final byte[][] columnFamilyNames,
      final long[] columnFamilyOptions) throws RocksDBException;

  private native static long openROnly(final long optionsHandle,
      final String path) throws RocksDBException;

  /**
   * @param optionsHandle Native handle pointing to an Options object
   * @param path The directory path for the database files
   * @param columnFamilyNames An array of column family names
   * @param columnFamilyOptions An array of native handles pointing to
   *                            ColumnFamilyOptions objects
   *
   * @return An array of native handles, [0] is the handle of the RocksDB object
   *   [1..1+n] are handles of the ColumnFamilyReferences
   *
   * @throws RocksDBException thrown if the database could not be opened
   */
  private native static long[] openROnly(final long optionsHandle,
      final String path, final byte[][] columnFamilyNames,
      final long[] columnFamilyOptions
  ) throws RocksDBException;

  @Override protected native void disposeInternal(final long handle);

  private native static void closeDatabase(final long handle)
      throws RocksDBException;
  private native static byte[][] listColumnFamilies(final long optionsHandle,
      final String path) throws RocksDBException;
  private native long createColumnFamily(final long handle,
      final byte[] columnFamilyName, final int columnFamilyNamelen,
      final long columnFamilyOptions) throws RocksDBException;
  private native long[] createColumnFamilies(final long handle,
      final long columnFamilyOptionsHandle, final byte[][] columnFamilyNames)
      throws RocksDBException;
  private native long[] createColumnFamilies(final long handle,
      final long columnFamilyOptionsHandles[], final byte[][] columnFamilyNames)
      throws RocksDBException;
  private native void dropColumnFamily(
      final long handle, final long cfHandle) throws RocksDBException;
  private native void dropColumnFamilies(final long handle,
      final long[] cfHandles) throws RocksDBException;
  //TODO(AR) best way to express DestroyColumnFamilyHandle? ...maybe in ColumnFamilyHandle?
  private native void put(final long handle, final byte[] key,
      final int keyOffset, final int keyLength, final byte[] value,
      final int valueOffset, int valueLength) throws RocksDBException;
  private native void put(final long handle, final byte[] key, final int keyOffset,
      final int keyLength, final byte[] value, final int valueOffset,
      final int valueLength, final long cfHandle) throws RocksDBException;
  private native void put(final long handle, final long writeOptHandle,
      final byte[] key,  final int keyOffset, final int keyLength,
      final byte[] value, final int valueOffset, final int valueLength)
      throws RocksDBException;
  private native void put(final long handle, final long writeOptHandle,
      final byte[] key, final int keyOffset, final int keyLength,
      final byte[] value, final int valueOffset, final int valueLength,
      final long cfHandle) throws RocksDBException;
  private native void delete(final long handle, final byte[] key,
      final int keyOffset, final int keyLength) throws RocksDBException;
  private native void delete(final long handle, final byte[] key,
      final int keyOffset, final int keyLength, final long cfHandle)
      throws RocksDBException;
  private native void delete(final long handle, final long writeOptHandle,
      final byte[] key, final int keyOffset, final int keyLength)
      throws RocksDBException;
  private native void delete(final long handle, final long writeOptHandle,
      final byte[] key, final int keyOffset, final int keyLength,
      final long cfHandle) throws RocksDBException;
  private native void singleDelete(
      final long handle, final byte[] key, final int keyLen)
      throws RocksDBException;
  private native void singleDelete(
      final long handle, final byte[] key, final int keyLen,
      final long cfHandle) throws RocksDBException;
  private native void singleDelete(
      final long handle, final long writeOptHandle, final byte[] key,
      final int keyLen) throws RocksDBException;
  private native void singleDelete(
      final long handle, final long writeOptHandle,
      final byte[] key, final int keyLen, final long cfHandle)
      throws RocksDBException;
  private native void deleteRange(final long handle, final byte[] beginKey,
      final int beginKeyOffset, final int beginKeyLength, final byte[] endKey,
      final int endKeyOffset, final int endKeyLength) throws RocksDBException;
  private native void deleteRange(final long handle, final byte[] beginKey,
      final int beginKeyOffset, final int beginKeyLength, final byte[] endKey,
      final int endKeyOffset, final int endKeyLength, final long cfHandle)
      throws RocksDBException;
  private native void deleteRange(final long handle, final long writeOptHandle,
      final byte[] beginKey, final int beginKeyOffset, final int beginKeyLength,
      final byte[] endKey, final int endKeyOffset, final int endKeyLength)
      throws RocksDBException;
  private native void deleteRange(
      final long handle, final long writeOptHandle, final byte[] beginKey,
      final int beginKeyOffset, final int beginKeyLength, final byte[] endKey,
      final int endKeyOffset, final int endKeyLength, final long cfHandle)
      throws RocksDBException;
  private native void merge(final long handle, final byte[] key,
      final int keyOffset, final int keyLength, final byte[] value,
      final int valueOffset, final int valueLength) throws RocksDBException;
  private native void merge(final long handle, final byte[] key,
      final int keyOffset, final int keyLength, final byte[] value,
      final int valueOffset, final int valueLength, final long cfHandle)
      throws RocksDBException;
  private native void merge(final long handle, final long writeOptHandle,
      final byte[] key, final int keyOffset, final int keyLength,
      final byte[] value, final int valueOffset, final int valueLength)
      throws RocksDBException;
  private native void merge(final long handle, final long writeOptHandle,
      final byte[] key, final int keyOffset, final int keyLength,
      final byte[] value, final int valueOffset, final int valueLength,
      final long cfHandle) throws RocksDBException;
  private native void write0(final long handle, final long writeOptHandle,
      final long wbHandle) throws RocksDBException;
  private native void write1(final long handle, final long writeOptHandle,
      final long wbwiHandle) throws RocksDBException;
  private native int get(final long handle, final byte[] key,
      final int keyOffset, final int keyLength, final byte[] value,
      final int valueOffset, final int valueLength) throws RocksDBException;
  private native int get(final long handle, final byte[] key,
      final int keyOffset, final int keyLength, byte[] value,
      final int valueOffset, final int valueLength, final long cfHandle)
      throws RocksDBException;
  private native int get(final long handle, final long readOptHandle,
      final byte[] key, final int keyOffset, final int keyLength,
      final byte[] value, final int valueOffset, final int valueLength)
      throws RocksDBException;
  private native int get(final long handle, final long readOptHandle,
      final byte[] key, final int keyOffset, final int keyLength,
      final byte[] value, final int valueOffset, final int valueLength,
      final long cfHandle) throws RocksDBException;
  private native byte[] get(final long handle, byte[] key, final int keyOffset,
      final int keyLength) throws RocksDBException;
  private native byte[] get(final long handle, final byte[] key,
      final int keyOffset, final int keyLength, final long cfHandle)
      throws RocksDBException;
  private native byte[] get(final long handle, final long readOptHandle,
      final byte[] key, final int keyOffset, final int keyLength)
      throws RocksDBException;
  private native byte[] get(final long handle,
      final long readOptHandle, final byte[] key, final int keyOffset,
      final int keyLength, final long cfHandle) throws RocksDBException;
  private native byte[][] multiGet(final long dbHandle, final byte[][] keys,
      final int[] keyOffsets, final int[] keyLengths);
  private native byte[][] multiGet(final long dbHandle, final byte[][] keys,
      final int[] keyOffsets, final int[] keyLengths,
      final long[] columnFamilyHandles);
  private native byte[][] multiGet(final long dbHandle, final long rOptHandle,
      final byte[][] keys, final int[] keyOffsets, final int[] keyLengths);
  private native byte[][] multiGet(final long dbHandle, final long rOptHandle,
      final byte[][] keys, final int[] keyOffsets, final int[] keyLengths,
      final long[] columnFamilyHandles);
  private native boolean keyMayExist(final long handle, final byte[] key,
      final int keyOffset, final int keyLength,
      final StringBuilder stringBuilder);
  private native boolean keyMayExist(final long handle, final byte[] key,
      final int keyOffset, final int keyLength, final long cfHandle,
      final StringBuilder stringBuilder);
  private native boolean keyMayExist(final long handle,
      final long optionsHandle, final byte[] key, final int keyOffset,
      final int keyLength, final StringBuilder stringBuilder);
  private native boolean keyMayExist(final long handle,
      final long optionsHandle, final byte[] key, final int keyOffset,
      final int keyLength, final long cfHandle,
      final StringBuilder stringBuilder);
  private native long iterator(final long handle);
  private native long iterator(final long handle, final long readOptHandle);
  private native long iteratorCF(final long handle, final long cfHandle);
  private native long iteratorCF(final long handle, final long cfHandle,
      final long readOptHandle);
  private native long[] iterators(final long handle,
      final long[] columnFamilyHandles, final long readOptHandle)
      throws RocksDBException;
  private native long getSnapshot(final long nativeHandle);
  private native void releaseSnapshot(
      final long nativeHandle, final long snapshotHandle);
  private native String getProperty(final long nativeHandle,
      final long cfHandle, final String property, final int propertyLength)
      throws RocksDBException;
  private native Map<String, String> getMapProperty(final long nativeHandle,
      final long cfHandle, final String property, final int propertyLength)
      throws RocksDBException;
  private native long getLongProperty(final long nativeHandle,
      final long cfHandle, final String property, final int propertyLength)
      throws RocksDBException;
  private native void resetStats(final long nativeHandle)
      throws RocksDBException;
  private native long getAggregatedLongProperty(final long nativeHandle,
      final String property, int propertyLength) throws RocksDBException;
  private native long[] getApproximateSizes(final long nativeHandle,
      final long columnFamilyHandle, final long[] rangeSliceHandles,
      final byte includeFlags);
  private final native long[] getApproximateMemTableStats(
      final long nativeHandle, final long columnFamilyHandle,
      final long rangeStartSliceHandle, final long rangeLimitSliceHandle);
  private native void compactRange(final long handle,
      /* @Nullable */ final byte[] begin, final int beginLen,
      /* @Nullable */ final byte[] end, final int endLen,
      final long compactRangeOptHandle, final long cfHandle)
      throws RocksDBException;
  private native void setOptions(final long handle, final long cfHandle,
      final String[] keys, final String[] values) throws RocksDBException;
  private native void setDBOptions(final long handle,
      final String[] keys, final String[] values) throws RocksDBException;
  private native String[] compactFiles(final long handle,
      final long compactionOptionsHandle,
      final long columnFamilyHandle,
      final String[] inputFileNames,
      final int outputLevel,
      final int outputPathId,
      final long compactionJobInfoHandle) throws RocksDBException;
  private native void pauseBackgroundWork(final long handle)
      throws RocksDBException;
  private native void continueBackgroundWork(final long handle)
      throws RocksDBException;
  private native void enableAutoCompaction(final long handle,
      final long[] columnFamilyHandles) throws RocksDBException;
  private native int numberLevels(final long handle,
      final long columnFamilyHandle);
  private native int maxMemCompactionLevel(final long handle,
      final long columnFamilyHandle);
  private native int level0StopWriteTrigger(final long handle,
      final long columnFamilyHandle);
  private native String getName(final long handle);
  private native long getEnv(final long handle);
  private native void flush(final long handle, final long flushOptHandle,
      /* @Nullable */ final long[] cfHandles) throws RocksDBException;
  private native void flushWal(final long handle, final boolean sync)
      throws RocksDBException;
  private native void syncWal(final long handle) throws RocksDBException;
  private native long getLatestSequenceNumber(final long handle);
  private native boolean setPreserveDeletesSequenceNumber(final long handle,
      final long sequenceNumber);
  private native void disableFileDeletions(long handle) throws RocksDBException;
  private native void enableFileDeletions(long handle, boolean force)
      throws RocksDBException;
  private native String[] getLiveFiles(final long handle,
      final boolean flushMemtable) throws RocksDBException;
  private native LogFile[] getSortedWalFiles(final long handle)
      throws RocksDBException;
  private native long getUpdatesSince(final long handle,
      final long sequenceNumber) throws RocksDBException;
  private native void deleteFile(final long handle, final String name)
      throws RocksDBException;
  private native LiveFileMetaData[] getLiveFilesMetaData(final long handle);
  private native ColumnFamilyMetaData getColumnFamilyMetaData(
      final long handle, final long columnFamilyHandle);
  private native void ingestExternalFile(final long handle,
      final long columnFamilyHandle,  final String[] filePathList,
      final int filePathListLen, final long ingestExternalFileOptionsHandle)
      throws RocksDBException;
  private native void verifyChecksum(final long handle) throws RocksDBException;
  private native long getDefaultColumnFamily(final long handle);
  private native Map<String, TableProperties> getPropertiesOfAllTables(
      final long handle, final long columnFamilyHandle) throws RocksDBException;
  private native Map<String, TableProperties> getPropertiesOfTablesInRange(
      final long handle, final long columnFamilyHandle,
      final long[] rangeSliceHandles);
  private native long[] suggestCompactRange(final long handle,
      final long columnFamilyHandle) throws RocksDBException;
  private native void promoteL0(final long handle,
      final long columnFamilyHandle, final int tragetLevel)
      throws RocksDBException;
  private native void startTrace(final long handle, final long maxTraceFileSize,
      final long traceWriterHandle) throws RocksDBException;
  private native void endTrace(final long handle) throws RocksDBException;


  private native static void destroyDB(final String path,
      final long optionsHandle) throws RocksDBException;

  protected DBOptionsInterface options_;
}
