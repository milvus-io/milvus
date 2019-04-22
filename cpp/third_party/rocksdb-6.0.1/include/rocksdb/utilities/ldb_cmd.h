//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once

#ifndef ROCKSDB_LITE

#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <functional>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/ldb_tool.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/db_ttl.h"
#include "rocksdb/utilities/ldb_cmd_execute_result.h"

namespace rocksdb {

class LDBCommand {
 public:
  // Command-line arguments
  static const std::string ARG_DB;
  static const std::string ARG_PATH;
  static const std::string ARG_HEX;
  static const std::string ARG_KEY_HEX;
  static const std::string ARG_VALUE_HEX;
  static const std::string ARG_CF_NAME;
  static const std::string ARG_TTL;
  static const std::string ARG_TTL_START;
  static const std::string ARG_TTL_END;
  static const std::string ARG_TIMESTAMP;
  static const std::string ARG_TRY_LOAD_OPTIONS;
  static const std::string ARG_IGNORE_UNKNOWN_OPTIONS;
  static const std::string ARG_FROM;
  static const std::string ARG_TO;
  static const std::string ARG_MAX_KEYS;
  static const std::string ARG_BLOOM_BITS;
  static const std::string ARG_FIX_PREFIX_LEN;
  static const std::string ARG_COMPRESSION_TYPE;
  static const std::string ARG_COMPRESSION_MAX_DICT_BYTES;
  static const std::string ARG_BLOCK_SIZE;
  static const std::string ARG_AUTO_COMPACTION;
  static const std::string ARG_DB_WRITE_BUFFER_SIZE;
  static const std::string ARG_WRITE_BUFFER_SIZE;
  static const std::string ARG_FILE_SIZE;
  static const std::string ARG_CREATE_IF_MISSING;
  static const std::string ARG_NO_VALUE;

  struct ParsedParams {
    std::string cmd;
    std::vector<std::string> cmd_params;
    std::map<std::string, std::string> option_map;
    std::vector<std::string> flags;
  };

  static LDBCommand* SelectCommand(const ParsedParams& parsed_parms);

  static LDBCommand* InitFromCmdLineArgs(
      const std::vector<std::string>& args, const Options& options,
      const LDBOptions& ldb_options,
      const std::vector<ColumnFamilyDescriptor>* column_families,
      const std::function<LDBCommand*(const ParsedParams&)>& selector =
          SelectCommand);

  static LDBCommand* InitFromCmdLineArgs(
      int argc, char** argv, const Options& options,
      const LDBOptions& ldb_options,
      const std::vector<ColumnFamilyDescriptor>* column_families);

  bool ValidateCmdLineOptions();

  virtual Options PrepareOptionsForOpenDB();

  virtual void SetDBOptions(Options options) { options_ = options; }

  virtual void SetColumnFamilies(
      const std::vector<ColumnFamilyDescriptor>* column_families) {
    if (column_families != nullptr) {
      column_families_ = *column_families;
    } else {
      column_families_.clear();
    }
  }

  void SetLDBOptions(const LDBOptions& ldb_options) {
    ldb_options_ = ldb_options;
  }

  virtual bool NoDBOpen() { return false; }

  virtual ~LDBCommand() { CloseDB(); }

  /* Run the command, and return the execute result. */
  void Run();

  virtual void DoCommand() = 0;

  LDBCommandExecuteResult GetExecuteState() { return exec_state_; }

  void ClearPreviousRunState() { exec_state_.Reset(); }

  // Consider using Slice::DecodeHex directly instead if you don't need the
  // 0x prefix
  static std::string HexToString(const std::string& str);

  // Consider using Slice::ToString(true) directly instead if
  // you don't need the 0x prefix
  static std::string StringToHex(const std::string& str);

  static const char* DELIM;

 protected:
  LDBCommandExecuteResult exec_state_;
  std::string db_path_;
  std::string column_family_name_;
  DB* db_;
  DBWithTTL* db_ttl_;
  std::map<std::string, ColumnFamilyHandle*> cf_handles_;

  /**
   * true implies that this command can work if the db is opened in read-only
   * mode.
   */
  bool is_read_only_;

  /** If true, the key is input/output as hex in get/put/scan/delete etc. */
  bool is_key_hex_;

  /** If true, the value is input/output as hex in get/put/scan/delete etc. */
  bool is_value_hex_;

  /** If true, the value is treated as timestamp suffixed */
  bool is_db_ttl_;

  // If true, the kvs are output with their insert/modify timestamp in a ttl db
  bool timestamp_;

  // If true, try to construct options from DB's option files.
  bool try_load_options_;

  bool ignore_unknown_options_;

  bool create_if_missing_;

  /**
   * Map of options passed on the command-line.
   */
  const std::map<std::string, std::string> option_map_;

  /**
   * Flags passed on the command-line.
   */
  const std::vector<std::string> flags_;

  /** List of command-line options valid for this command */
  const std::vector<std::string> valid_cmd_line_options_;

  bool ParseKeyValue(const std::string& line, std::string* key,
                     std::string* value, bool is_key_hex, bool is_value_hex);

  LDBCommand(const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags, bool is_read_only,
             const std::vector<std::string>& valid_cmd_line_options);

  void OpenDB();

  void CloseDB();

  ColumnFamilyHandle* GetCfHandle();

  static std::string PrintKeyValue(const std::string& key,
                                   const std::string& value, bool is_key_hex,
                                   bool is_value_hex);

  static std::string PrintKeyValue(const std::string& key,
                                   const std::string& value, bool is_hex);

  /**
   * Return true if the specified flag is present in the specified flags vector
   */
  static bool IsFlagPresent(const std::vector<std::string>& flags,
                            const std::string& flag) {
    return (std::find(flags.begin(), flags.end(), flag) != flags.end());
  }

  static std::string HelpRangeCmdArgs();

  /**
   * A helper function that returns a list of command line options
   * used by this command.  It includes the common options and the ones
   * passed in.
   */
  static std::vector<std::string> BuildCmdLineOptions(
      std::vector<std::string> options);

  bool ParseIntOption(const std::map<std::string, std::string>& options,
                      const std::string& option, int& value,
                      LDBCommandExecuteResult& exec_state);

  bool ParseStringOption(const std::map<std::string, std::string>& options,
                         const std::string& option, std::string* value);

  /**
   * Returns the value of the specified option as a boolean.
   * default_val is used if the option is not found in options.
   * Throws an exception if the value of the option is not
   * "true" or "false" (case insensitive).
   */
  bool ParseBooleanOption(const std::map<std::string, std::string>& options,
                          const std::string& option, bool default_val);

  Options options_;
  std::vector<ColumnFamilyDescriptor> column_families_;
  LDBOptions ldb_options_;

 private:
  /**
   * Interpret command line options and flags to determine if the key
   * should be input/output in hex.
   */
  bool IsKeyHex(const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  /**
   * Interpret command line options and flags to determine if the value
   * should be input/output in hex.
   */
  bool IsValueHex(const std::map<std::string, std::string>& options,
                  const std::vector<std::string>& flags);

  /**
   * Converts val to a boolean.
   * val must be either true or false (case insensitive).
   * Otherwise an exception is thrown.
   */
  bool StringToBool(std::string val);
};

class LDBCommandRunner {
 public:
  static void PrintHelp(const LDBOptions& ldb_options, const char* exec_name);

  static void RunCommand(
      int argc, char** argv, Options options, const LDBOptions& ldb_options,
      const std::vector<ColumnFamilyDescriptor>* column_families);
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
