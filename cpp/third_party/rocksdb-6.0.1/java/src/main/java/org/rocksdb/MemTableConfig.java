// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
package org.rocksdb;

/**
 * MemTableConfig is used to config the internal mem-table of a RocksDB.
 * It is required for each memtable to have one such sub-class to allow
 * Java developers to use it.
 *
 * To make a RocksDB to use a specific MemTable format, its associated
 * MemTableConfig should be properly set and passed into Options
 * via Options.setMemTableFactory() and open the db using that Options.
 *
 * @see Options
 */
public abstract class MemTableConfig {
  /**
   * This function should only be called by Options.setMemTableConfig(),
   * which will create a c++ shared-pointer to the c++ MemTableRepFactory
   * that associated with the Java MemTableConfig.
   *
   * @see Options#setMemTableConfig(MemTableConfig)
   *
   * @return native handle address to native memory table instance.
   */
  abstract protected long newMemTableFactoryHandle();
}
