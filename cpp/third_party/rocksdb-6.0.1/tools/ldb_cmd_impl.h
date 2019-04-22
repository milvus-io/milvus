//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/utilities/ldb_cmd.h"

#include <map>
#include <string>
#include <utility>
#include <vector>

namespace rocksdb {

class CompactorCommand : public LDBCommand {
 public:
  static std::string Name() { return "compact"; }

  CompactorCommand(const std::vector<std::string>& params,
                   const std::map<std::string, std::string>& options,
                   const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  virtual void DoCommand() override;

 private:
  bool null_from_;
  std::string from_;
  bool null_to_;
  std::string to_;
};

class DBFileDumperCommand : public LDBCommand {
 public:
  static std::string Name() { return "dump_live_files"; }

  DBFileDumperCommand(const std::vector<std::string>& params,
                      const std::map<std::string, std::string>& options,
                      const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  virtual void DoCommand() override;
};

class DBDumperCommand : public LDBCommand {
 public:
  static std::string Name() { return "dump"; }

  DBDumperCommand(const std::vector<std::string>& params,
                  const std::map<std::string, std::string>& options,
                  const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  virtual void DoCommand() override;

 private:
  /**
   * Extract file name from the full path. We handle both the forward slash (/)
   * and backslash (\) to make sure that different OS-s are supported.
  */
  static std::string GetFileNameFromPath(const std::string& s) {
    std::size_t n = s.find_last_of("/\\");

    if (std::string::npos == n) {
      return s;
    } else {
      return s.substr(n + 1);
    }
  }

  void DoDumpCommand();

  bool null_from_;
  std::string from_;
  bool null_to_;
  std::string to_;
  int max_keys_;
  std::string delim_;
  bool count_only_;
  bool count_delim_;
  bool print_stats_;
  std::string path_;

  static const std::string ARG_COUNT_ONLY;
  static const std::string ARG_COUNT_DELIM;
  static const std::string ARG_STATS;
  static const std::string ARG_TTL_BUCKET;
};

class InternalDumpCommand : public LDBCommand {
 public:
  static std::string Name() { return "idump"; }

  InternalDumpCommand(const std::vector<std::string>& params,
                      const std::map<std::string, std::string>& options,
                      const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  virtual void DoCommand() override;

 private:
  bool has_from_;
  std::string from_;
  bool has_to_;
  std::string to_;
  int max_keys_;
  std::string delim_;
  bool count_only_;
  bool count_delim_;
  bool print_stats_;
  bool is_input_key_hex_;

  static const std::string ARG_DELIM;
  static const std::string ARG_COUNT_ONLY;
  static const std::string ARG_COUNT_DELIM;
  static const std::string ARG_STATS;
  static const std::string ARG_INPUT_KEY_HEX;
};

class DBLoaderCommand : public LDBCommand {
 public:
  static std::string Name() { return "load"; }

  DBLoaderCommand(std::string& db_name, std::vector<std::string>& args);

  DBLoaderCommand(const std::vector<std::string>& params,
                  const std::map<std::string, std::string>& options,
                  const std::vector<std::string>& flags);

  static void Help(std::string& ret);
  virtual void DoCommand() override;

  virtual Options PrepareOptionsForOpenDB() override;

 private:
  bool disable_wal_;
  bool bulk_load_;
  bool compact_;

  static const std::string ARG_DISABLE_WAL;
  static const std::string ARG_BULK_LOAD;
  static const std::string ARG_COMPACT;
};

class ManifestDumpCommand : public LDBCommand {
 public:
  static std::string Name() { return "manifest_dump"; }

  ManifestDumpCommand(const std::vector<std::string>& params,
                      const std::map<std::string, std::string>& options,
                      const std::vector<std::string>& flags);

  static void Help(std::string& ret);
  virtual void DoCommand() override;

  virtual bool NoDBOpen() override { return true; }

 private:
  bool verbose_;
  bool json_;
  std::string path_;

  static const std::string ARG_VERBOSE;
  static const std::string ARG_JSON;
  static const std::string ARG_PATH;
};

class ListColumnFamiliesCommand : public LDBCommand {
 public:
  static std::string Name() { return "list_column_families"; }

  ListColumnFamiliesCommand(const std::vector<std::string>& params,
                            const std::map<std::string, std::string>& options,
                            const std::vector<std::string>& flags);

  static void Help(std::string& ret);
  virtual void DoCommand() override;

  virtual bool NoDBOpen() override { return true; }

 private:
  std::string dbname_;
};

class CreateColumnFamilyCommand : public LDBCommand {
 public:
  static std::string Name() { return "create_column_family"; }

  CreateColumnFamilyCommand(const std::vector<std::string>& params,
                            const std::map<std::string, std::string>& options,
                            const std::vector<std::string>& flags);

  static void Help(std::string& ret);
  virtual void DoCommand() override;

  virtual bool NoDBOpen() override { return false; }

 private:
  std::string new_cf_name_;
};

class ReduceDBLevelsCommand : public LDBCommand {
 public:
  static std::string Name() { return "reduce_levels"; }

  ReduceDBLevelsCommand(const std::vector<std::string>& params,
                        const std::map<std::string, std::string>& options,
                        const std::vector<std::string>& flags);

  virtual Options PrepareOptionsForOpenDB() override;

  virtual void DoCommand() override;

  virtual bool NoDBOpen() override { return true; }

  static void Help(std::string& msg);

  static std::vector<std::string> PrepareArgs(const std::string& db_path,
                                              int new_levels,
                                              bool print_old_level = false);

 private:
  int old_levels_;
  int new_levels_;
  bool print_old_levels_;

  static const std::string ARG_NEW_LEVELS;
  static const std::string ARG_PRINT_OLD_LEVELS;

  Status GetOldNumOfLevels(Options& opt, int* levels);
};

class ChangeCompactionStyleCommand : public LDBCommand {
 public:
  static std::string Name() { return "change_compaction_style"; }

  ChangeCompactionStyleCommand(
      const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  virtual Options PrepareOptionsForOpenDB() override;

  virtual void DoCommand() override;

  static void Help(std::string& msg);

 private:
  int old_compaction_style_;
  int new_compaction_style_;

  static const std::string ARG_OLD_COMPACTION_STYLE;
  static const std::string ARG_NEW_COMPACTION_STYLE;
};

class WALDumperCommand : public LDBCommand {
 public:
  static std::string Name() { return "dump_wal"; }

  WALDumperCommand(const std::vector<std::string>& params,
                   const std::map<std::string, std::string>& options,
                   const std::vector<std::string>& flags);

  virtual bool NoDBOpen() override { return true; }

  static void Help(std::string& ret);
  virtual void DoCommand() override;

 private:
  bool print_header_;
  std::string wal_file_;
  bool print_values_;
  bool is_write_committed_;  // default will be set to true

  static const std::string ARG_WAL_FILE;
  static const std::string ARG_WRITE_COMMITTED;
  static const std::string ARG_PRINT_HEADER;
  static const std::string ARG_PRINT_VALUE;
};

class GetCommand : public LDBCommand {
 public:
  static std::string Name() { return "get"; }

  GetCommand(const std::vector<std::string>& params,
             const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::string key_;
};

class ApproxSizeCommand : public LDBCommand {
 public:
  static std::string Name() { return "approxsize"; }

  ApproxSizeCommand(const std::vector<std::string>& params,
                    const std::map<std::string, std::string>& options,
                    const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::string start_key_;
  std::string end_key_;
};

class BatchPutCommand : public LDBCommand {
 public:
  static std::string Name() { return "batchput"; }

  BatchPutCommand(const std::vector<std::string>& params,
                  const std::map<std::string, std::string>& options,
                  const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  static void Help(std::string& ret);

  virtual Options PrepareOptionsForOpenDB() override;

 private:
  /**
   * The key-values to be inserted.
   */
  std::vector<std::pair<std::string, std::string>> key_values_;
};

class ScanCommand : public LDBCommand {
 public:
  static std::string Name() { return "scan"; }

  ScanCommand(const std::vector<std::string>& params,
              const std::map<std::string, std::string>& options,
              const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::string start_key_;
  std::string end_key_;
  bool start_key_specified_;
  bool end_key_specified_;
  int max_keys_scanned_;
  bool no_value_;
};

class DeleteCommand : public LDBCommand {
 public:
  static std::string Name() { return "delete"; }

  DeleteCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::string key_;
};

class DeleteRangeCommand : public LDBCommand {
 public:
  static std::string Name() { return "deleterange"; }

  DeleteRangeCommand(const std::vector<std::string>& params,
                     const std::map<std::string, std::string>& options,
                     const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  static void Help(std::string& ret);

 private:
  std::string begin_key_;
  std::string end_key_;
};

class PutCommand : public LDBCommand {
 public:
  static std::string Name() { return "put"; }

  PutCommand(const std::vector<std::string>& params,
             const std::map<std::string, std::string>& options,
             const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  static void Help(std::string& ret);

  virtual Options PrepareOptionsForOpenDB() override;

 private:
  std::string key_;
  std::string value_;
};

/**
 * Command that starts up a REPL shell that allows
 * get/put/delete.
 */
class DBQuerierCommand : public LDBCommand {
 public:
  static std::string Name() { return "query"; }

  DBQuerierCommand(const std::vector<std::string>& params,
                   const std::map<std::string, std::string>& options,
                   const std::vector<std::string>& flags);

  static void Help(std::string& ret);

  virtual void DoCommand() override;

 private:
  static const char* HELP_CMD;
  static const char* GET_CMD;
  static const char* PUT_CMD;
  static const char* DELETE_CMD;
};

class CheckConsistencyCommand : public LDBCommand {
 public:
  static std::string Name() { return "checkconsistency"; }

  CheckConsistencyCommand(const std::vector<std::string>& params,
                          const std::map<std::string, std::string>& options,
                          const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  virtual bool NoDBOpen() override { return true; }

  static void Help(std::string& ret);
};

class CheckPointCommand : public LDBCommand {
 public:
  static std::string Name() { return "checkpoint"; }

  CheckPointCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  static void Help(std::string& ret);

  std::string checkpoint_dir_;
 private:
  static const std::string ARG_CHECKPOINT_DIR;
};

class RepairCommand : public LDBCommand {
 public:
  static std::string Name() { return "repair"; }

  RepairCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  virtual bool NoDBOpen() override { return true; }

  static void Help(std::string& ret);
};

class BackupableCommand : public LDBCommand {
 public:
  BackupableCommand(const std::vector<std::string>& params,
                    const std::map<std::string, std::string>& options,
                    const std::vector<std::string>& flags);

 protected:
  static void Help(const std::string& name, std::string& ret);
  std::string backup_env_uri_;
  std::string backup_dir_;
  int num_threads_;
  std::unique_ptr<Logger> logger_;

 private:
  static const std::string ARG_BACKUP_DIR;
  static const std::string ARG_BACKUP_ENV_URI;
  static const std::string ARG_NUM_THREADS;
  static const std::string ARG_STDERR_LOG_LEVEL;
};

class BackupCommand : public BackupableCommand {
 public:
  static std::string Name() { return "backup"; }
  BackupCommand(const std::vector<std::string>& params,
                const std::map<std::string, std::string>& options,
                const std::vector<std::string>& flags);
  virtual void DoCommand() override;
  static void Help(std::string& ret);
};

class RestoreCommand : public BackupableCommand {
 public:
  static std::string Name() { return "restore"; }
  RestoreCommand(const std::vector<std::string>& params,
                 const std::map<std::string, std::string>& options,
                 const std::vector<std::string>& flags);
  virtual void DoCommand() override;
  virtual bool NoDBOpen() override { return true; }
  static void Help(std::string& ret);
};

class WriteExternalSstFilesCommand : public LDBCommand {
 public:
  static std::string Name() { return "write_extern_sst"; }
  WriteExternalSstFilesCommand(
      const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  virtual bool NoDBOpen() override { return false; }

  virtual Options PrepareOptionsForOpenDB() override;

  static void Help(std::string& ret);

 private:
  std::string output_sst_path_;
};

class IngestExternalSstFilesCommand : public LDBCommand {
 public:
  static std::string Name() { return "ingest_extern_sst"; }
  IngestExternalSstFilesCommand(
      const std::vector<std::string>& params,
      const std::map<std::string, std::string>& options,
      const std::vector<std::string>& flags);

  virtual void DoCommand() override;

  virtual bool NoDBOpen() override { return false; }

  virtual Options PrepareOptionsForOpenDB() override;

  static void Help(std::string& ret);

 private:
  std::string input_sst_path_;
  bool move_files_;
  bool snapshot_consistency_;
  bool allow_global_seqno_;
  bool allow_blocking_flush_;
  bool ingest_behind_;
  bool write_global_seqno_;

  static const std::string ARG_MOVE_FILES;
  static const std::string ARG_SNAPSHOT_CONSISTENCY;
  static const std::string ARG_ALLOW_GLOBAL_SEQNO;
  static const std::string ARG_ALLOW_BLOCKING_FLUSH;
  static const std::string ARG_INGEST_BEHIND;
  static const std::string ARG_WRITE_GLOBAL_SEQNO;
};

}  // namespace rocksdb
