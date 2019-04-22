// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.Map;

/**
 * WALFilter allows an application to inspect write-ahead-log (WAL)
 * records or modify their processing on recovery.
 */
public interface WalFilter {

  /**
   * Provide ColumnFamily-&gt;LogNumber map to filter
   * so that filter can determine whether a log number applies to a given
   * column family (i.e. that log hasn't been flushed to SST already for the
   * column family).
   *
   * We also pass in name&gt;id map as only name is known during
   * recovery (as handles are opened post-recovery).
   * while write batch callbacks happen in terms of column family id.
   *
   * @param cfLognumber column_family_id to lognumber map
   * @param cfNameId column_family_name to column_family_id map
   */
  void columnFamilyLogNumberMap(final Map<Integer, Long> cfLognumber,
      final Map<String, Integer> cfNameId);

  /**
   * LogRecord is invoked for each log record encountered for all the logs
   * during replay on logs on recovery. This method can be used to:
   *     * inspect the record (using the batch parameter)
   *     * ignoring current record
   *         (by returning WalProcessingOption::kIgnoreCurrentRecord)
   *     * reporting corrupted record
   *         (by returning WalProcessingOption::kCorruptedRecord)
   *     * stop log replay
   *         (by returning kStop replay) - please note that this implies
   *         discarding the logs from current record onwards.
   *
   * @param logNumber log number of the current log.
   *     Filter might use this to determine if the log
   *     record is applicable to a certain column family.
   * @param logFileName log file name - only for informational purposes
   * @param batch batch encountered in the log during recovery
   * @param newBatch new batch to populate if filter wants to change
   *     the batch (for example to filter some records out, or alter some
   *     records). Please note that the new batch MUST NOT contain
   *     more records than original, else recovery would be failed.
   *
   * @return Processing option for the current record.
   */
  LogRecordFoundResult logRecordFound(final long logNumber,
      final String logFileName, final WriteBatch batch,
      final WriteBatch newBatch);

  class LogRecordFoundResult {
    public static LogRecordFoundResult CONTINUE_UNCHANGED =
        new LogRecordFoundResult(WalProcessingOption.CONTINUE_PROCESSING, false);

    final WalProcessingOption walProcessingOption;
    final boolean batchChanged;

    /**
     * @param walProcessingOption the processing option
     * @param batchChanged Whether batch was changed by the filter.
     *     It must be set to true if newBatch was populated,
     *     else newBatch has no effect.
     */
    public LogRecordFoundResult(final WalProcessingOption walProcessingOption,
        final boolean batchChanged) {
      this.walProcessingOption = walProcessingOption;
      this.batchChanged = batchChanged;
    }
  }

  /**
   * Returns a name that identifies this WAL filter.
   * The name will be printed to LOG file on start up for diagnosis.
   *
   * @return the name
   */
  String name();
}
