// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * TraceOptions is used for
 * {@link RocksDB#startTrace(TraceOptions, AbstractTraceWriter)}.
 */
public class TraceOptions {
  private final long maxTraceFileSize;

  public TraceOptions() {
    this.maxTraceFileSize = 64 * 1024 * 1024 * 1024;  // 64 GB
  }

  public TraceOptions(final long maxTraceFileSize) {
    this.maxTraceFileSize = maxTraceFileSize;
  }

  /**
   * To avoid the trace file size grows large than the storage space,
   * user can set the max trace file size in Bytes. Default is 64GB
   *
   * @return the max trace size
   */
  public long getMaxTraceFileSize() {
    return maxTraceFileSize;
  }
}
