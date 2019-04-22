// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Base class for WAL Filters.
 */
public abstract class AbstractWalFilter
    extends RocksCallbackObject implements WalFilter {

  @Override
  protected long initializeNative(final long... nativeParameterHandles) {
    return createNewWalFilter();
  }

  /**
   * Called from JNI, proxy for
   *     {@link WalFilter#logRecordFound(long, String, WriteBatch, WriteBatch)}.
   *
   * @param logNumber the log handle.
   * @param logFileName the log file name
   * @param batchHandle the native handle of a WriteBatch (which we do not own)
   * @param newBatchHandle the native handle of a
   *     new WriteBatch (which we do not own)
   *
   * @return short (2 bytes) where the first byte is the
   *     {@link WalFilter.LogRecordFoundResult#walProcessingOption}
   *     {@link WalFilter.LogRecordFoundResult#batchChanged}.
   */
  private short logRecordFoundProxy(final long logNumber,
      final String logFileName, final long batchHandle,
      final long newBatchHandle) {
    final LogRecordFoundResult logRecordFoundResult = logRecordFound(
        logNumber, logFileName, new WriteBatch(batchHandle),
        new WriteBatch(newBatchHandle));
    return logRecordFoundResultToShort(logRecordFoundResult);
  }

  private static short logRecordFoundResultToShort(
      final LogRecordFoundResult logRecordFoundResult) {
    short result = (short)(logRecordFoundResult.walProcessingOption.getValue() << 8);
    return (short)(result | (logRecordFoundResult.batchChanged ? 1 : 0));
  }

  private native long createNewWalFilter();
}
