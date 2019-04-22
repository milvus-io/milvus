// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.ArrayList;
import java.util.List;

public class OptionsUtil {
  /**
   * A static method to construct the DBOptions and ColumnFamilyDescriptors by
   * loading the latest RocksDB options file stored in the specified rocksdb
   * database.
   *
   * Note that the all the pointer options (except table_factory, which will
   * be described in more details below) will be initialized with the default
   * values.  Developers can further initialize them after this function call.
   * Below is an example list of pointer options which will be initialized.
   *
   * - env
   * - memtable_factory
   * - compaction_filter_factory
   * - prefix_extractor
   * - comparator
   * - merge_operator
   * - compaction_filter
   *
   * For table_factory, this function further supports deserializing
   * BlockBasedTableFactory and its BlockBasedTableOptions except the
   * pointer options of BlockBasedTableOptions (flush_block_policy_factory,
   * block_cache, and block_cache_compressed), which will be initialized with
   * default values.  Developers can further specify these three options by
   * casting the return value of TableFactoroy::GetOptions() to
   * BlockBasedTableOptions and making necessary changes.
   *
   * @param dbPath the path to the RocksDB.
   * @param env {@link org.rocksdb.Env} instance.
   * @param dbOptions {@link org.rocksdb.DBOptions} instance. This will be
   *     filled and returned.
   * @param cfDescs A list of {@link org.rocksdb.ColumnFamilyDescriptor}'s be
   *    returned.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */

  public static void loadLatestOptions(String dbPath, Env env, DBOptions dbOptions,
      List<ColumnFamilyDescriptor> cfDescs) throws RocksDBException {
    loadLatestOptions(dbPath, env, dbOptions, cfDescs, false);
  }

  /**
   * @param dbPath the path to the RocksDB.
   * @param env {@link org.rocksdb.Env} instance.
   * @param dbOptions {@link org.rocksdb.DBOptions} instance. This will be
   *     filled and returned.
   * @param cfDescs A list of {@link org.rocksdb.ColumnFamilyDescriptor}'s be
   *     returned.
   * @param ignoreUnknownOptions this flag can be set to true if you want to
   *     ignore options that are from a newer version of the db, esentially for
   *     forward compatibility.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  public static void loadLatestOptions(String dbPath, Env env, DBOptions dbOptions,
      List<ColumnFamilyDescriptor> cfDescs, boolean ignoreUnknownOptions) throws RocksDBException {
    loadLatestOptions(
        dbPath, env.nativeHandle_, dbOptions.nativeHandle_, cfDescs, ignoreUnknownOptions);
  }

  /**
   * Similar to LoadLatestOptions, this function constructs the DBOptions
   * and ColumnFamilyDescriptors based on the specified RocksDB Options file.
   * See LoadLatestOptions above.
   *
   * @param optionsFileName the RocksDB options file path.
   * @param env {@link org.rocksdb.Env} instance.
   * @param dbOptions {@link org.rocksdb.DBOptions} instance. This will be
   *     filled and returned.
   * @param cfDescs A list of {@link org.rocksdb.ColumnFamilyDescriptor}'s be
   *     returned.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  public static void loadOptionsFromFile(String optionsFileName, Env env, DBOptions dbOptions,
      List<ColumnFamilyDescriptor> cfDescs) throws RocksDBException {
    loadOptionsFromFile(optionsFileName, env, dbOptions, cfDescs, false);
  }

  /**
   * @param optionsFileName the RocksDB options file path.
   * @param env {@link org.rocksdb.Env} instance.
   * @param dbOptions {@link org.rocksdb.DBOptions} instance. This will be
   *     filled and returned.
   * @param cfDescs A list of {@link org.rocksdb.ColumnFamilyDescriptor}'s be
   *     returned.
   * @param ignoreUnknownOptions this flag can be set to true if you want to
   *     ignore options that are from a newer version of the db, esentially for
   *     forward compatibility.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  public static void loadOptionsFromFile(String optionsFileName, Env env, DBOptions dbOptions,
      List<ColumnFamilyDescriptor> cfDescs, boolean ignoreUnknownOptions) throws RocksDBException {
    loadOptionsFromFile(
        optionsFileName, env.nativeHandle_, dbOptions.nativeHandle_, cfDescs, ignoreUnknownOptions);
  }

  /**
   * Returns the latest options file name under the specified RocksDB path.
   *
   * @param dbPath the path to the RocksDB.
   * @param env {@link org.rocksdb.Env} instance.
   * @return the latest options file name under the db path.
   *
   * @throws RocksDBException thrown if error happens in underlying
   *     native library.
   */
  public static String getLatestOptionsFileName(String dbPath, Env env) throws RocksDBException {
    return getLatestOptionsFileName(dbPath, env.nativeHandle_);
  }

  /**
   * Private constructor.
   * This class has only static methods and shouldn't be instantiated.
   */
  private OptionsUtil() {}

  // native methods
  private native static void loadLatestOptions(String dbPath, long envHandle, long dbOptionsHandle,
      List<ColumnFamilyDescriptor> cfDescs, boolean ignoreUnknownOptions) throws RocksDBException;
  private native static void loadOptionsFromFile(String optionsFileName, long envHandle,
      long dbOptionsHandle, List<ColumnFamilyDescriptor> cfDescs, boolean ignoreUnknownOptions)
      throws RocksDBException;
  private native static String getLatestOptionsFileName(String dbPath, long envHandle)
      throws RocksDBException;
}
