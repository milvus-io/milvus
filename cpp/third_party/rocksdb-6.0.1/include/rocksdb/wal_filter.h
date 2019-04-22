// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <string>
#include <map>

namespace rocksdb {

class WriteBatch;

// WALFilter allows an application to inspect write-ahead-log (WAL)
// records or modify their processing on recovery.
// Please see the details below.
class WalFilter {
 public:
  enum class WalProcessingOption {
    // Continue processing as usual
    kContinueProcessing = 0,
    // Ignore the current record but continue processing of log(s)
    kIgnoreCurrentRecord = 1,
    // Stop replay of logs and discard logs
    // Logs won't be replayed on subsequent recovery
    kStopReplay = 2,
    // Corrupted record detected by filter
    kCorruptedRecord = 3,
    // Marker for enum count
    kWalProcessingOptionMax = 4
  };

  virtual ~WalFilter() {}

  // Provide ColumnFamily->LogNumber map to filter
  // so that filter can determine whether a log number applies to a given 
  // column family (i.e. that log hasn't been flushed to SST already for the
  // column family).
  // We also pass in name->id map as only name is known during
  // recovery (as handles are opened post-recovery).
  // while write batch callbacks happen in terms of column family id.
  //
  // @params cf_lognumber_map column_family_id to lognumber map
  // @params cf_name_id_map   column_family_name to column_family_id map

  virtual void ColumnFamilyLogNumberMap(
      const std::map<uint32_t, uint64_t>& /*cf_lognumber_map*/,
      const std::map<std::string, uint32_t>& /*cf_name_id_map*/) {}

  // LogRecord is invoked for each log record encountered for all the logs
  // during replay on logs on recovery. This method can be used to:
  //  * inspect the record (using the batch parameter)
  //  * ignoring current record
  //    (by returning WalProcessingOption::kIgnoreCurrentRecord)
  //  * reporting corrupted record
  //    (by returning WalProcessingOption::kCorruptedRecord)
  //  * stop log replay
  //    (by returning kStop replay) - please note that this implies
  //    discarding the logs from current record onwards.
  //
  // @params log_number     log_number of the current log.
  //                        Filter might use this to determine if the log
  //                        record is applicable to a certain column family.
  // @params log_file_name  log file name - only for informational purposes
  // @params batch          batch encountered in the log during recovery
  // @params new_batch      new_batch to populate if filter wants to change
  //                        the batch (for example to filter some records out,
  //                        or alter some records).
  //                        Please note that the new batch MUST NOT contain
  //                        more records than original, else recovery would
  //                        be failed.
  // @params batch_changed  Whether batch was changed by the filter.
  //                        It must be set to true if new_batch was populated,
  //                        else new_batch has no effect.
  // @returns               Processing option for the current record.
  //                        Please see WalProcessingOption enum above for
  //                        details.
  virtual WalProcessingOption LogRecordFound(
      unsigned long long /*log_number*/, const std::string& /*log_file_name*/,
      const WriteBatch& batch, WriteBatch* new_batch, bool* batch_changed) {
    // Default implementation falls back to older function for compatibility
    return LogRecord(batch, new_batch, batch_changed);
  }

  // Please see the comments for LogRecord above. This function is for 
  // compatibility only and contains a subset of parameters. 
  // New code should use the function above.
  virtual WalProcessingOption LogRecord(const WriteBatch& /*batch*/,
                                        WriteBatch* /*new_batch*/,
                                        bool* /*batch_changed*/) const {
    return WalProcessingOption::kContinueProcessing;
  }

  // Returns a name that identifies this WAL filter.
  // The name will be printed to LOG file on start up for diagnosis.
  virtual const char* Name() const = 0;
};

}  // namespace rocksdb
