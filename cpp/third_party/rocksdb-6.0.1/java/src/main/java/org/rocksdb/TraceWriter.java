// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * TraceWriter allows exporting RocksDB traces to any system,
 * one operation at a time.
 */
public interface TraceWriter {

  /**
   * Write the data.
   *
   * @param data the data
   *
   * @throws RocksDBException if an error occurs whilst writing.
   */
  void write(final Slice data) throws RocksDBException;

  /**
   * Close the writer.
   *
   * @throws RocksDBException if an error occurs whilst closing the writer.
   */
  void closeWriter() throws RocksDBException;

  /**
   * Get the size of the file that this writer is writing to.
   *
   * @return the file size
   */
  long getFileSize();
}
