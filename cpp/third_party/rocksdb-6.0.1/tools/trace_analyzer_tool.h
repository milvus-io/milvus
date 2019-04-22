//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include <list>
#include <map>
#include <queue>
#include <set>
#include <utility>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/trace_reader_writer.h"
#include "rocksdb/write_batch.h"
#include "util/trace_replay.h"

namespace rocksdb {

class DBImpl;
class WriteBatch;

enum TraceOperationType : int {
  kGet = 0,
  kPut = 1,
  kDelete = 2,
  kSingleDelete = 3,
  kRangeDelete = 4,
  kMerge = 5,
  kIteratorSeek = 6,
  kIteratorSeekForPrev = 7,
  kTaTypeNum = 8
};

struct TraceUnit {
  uint64_t ts;
  uint32_t type;
  uint32_t cf_id;
  size_t value_size;
  std::string key;
};

struct TypeCorrelation {
  uint64_t count;
  uint64_t total_ts;
};

struct StatsUnit {
  uint64_t key_id;
  uint64_t access_count;
  uint64_t latest_ts;
  uint64_t succ_count;  // current only used to count Get if key found
  uint32_t cf_id;
  size_t value_size;
  std::vector<TypeCorrelation> v_correlation;
};

class AnalyzerOptions {
 public:
  std::vector<std::vector<int>> correlation_map;
  std::vector<std::pair<int, int>> correlation_list;

  AnalyzerOptions();

  ~AnalyzerOptions();

  void SparseCorrelationInput(const std::string& in_str);
};

// Note that, for the variable names  in the trace_analyzer,
// Starting with 'a_' means the variable is used for 'accessed_keys'.
// Starting with 'w_' means it is used for 'the whole key space'.
// Ending with '_f' means a file write or reader pointer.
// For example, 'a_count' means 'accessed_keys_count',
// 'w_key_f' means 'whole_key_space_file'.

struct TraceStats {
  uint32_t cf_id;
  std::string cf_name;
  uint64_t a_count;
  uint64_t a_succ_count;
  uint64_t a_key_id;
  uint64_t a_key_size_sqsum;
  uint64_t a_key_size_sum;
  uint64_t a_key_mid;
  uint64_t a_value_size_sqsum;
  uint64_t a_value_size_sum;
  uint64_t a_value_mid;
  uint32_t a_peak_qps;
  double a_ave_qps;
  std::map<std::string, StatsUnit> a_key_stats;
  std::map<uint64_t, uint64_t> a_count_stats;
  std::map<uint64_t, uint64_t> a_key_size_stats;
  std::map<uint64_t, uint64_t> a_value_size_stats;
  std::map<uint32_t, uint32_t> a_qps_stats;
  std::map<uint32_t, std::map<std::string, uint32_t>> a_qps_prefix_stats;
  std::priority_queue<std::pair<uint64_t, std::string>,
                      std::vector<std::pair<uint64_t, std::string>>,
                      std::greater<std::pair<uint64_t, std::string>>>
      top_k_queue;
  std::priority_queue<std::pair<uint64_t, std::string>,
                      std::vector<std::pair<uint64_t, std::string>>,
                      std::greater<std::pair<uint64_t, std::string>>>
      top_k_prefix_access;
  std::priority_queue<std::pair<double, std::string>,
                      std::vector<std::pair<double, std::string>>,
                      std::greater<std::pair<double, std::string>>>
      top_k_prefix_ave;
  std::priority_queue<std::pair<uint32_t, uint32_t>,
                      std::vector<std::pair<uint32_t, uint32_t>>,
                      std::greater<std::pair<uint32_t, uint32_t>>>
      top_k_qps_sec;
  std::list<TraceUnit> time_series;
  std::vector<std::pair<uint64_t, uint64_t>> correlation_output;
  std::map<uint32_t, uint64_t> uni_key_num;

  std::unique_ptr<rocksdb::WritableFile> time_series_f;
  std::unique_ptr<rocksdb::WritableFile> a_key_f;
  std::unique_ptr<rocksdb::WritableFile> a_count_dist_f;
  std::unique_ptr<rocksdb::WritableFile> a_prefix_cut_f;
  std::unique_ptr<rocksdb::WritableFile> a_value_size_f;
  std::unique_ptr<rocksdb::WritableFile> a_key_size_f;
  std::unique_ptr<rocksdb::WritableFile> a_key_num_f;
  std::unique_ptr<rocksdb::WritableFile> a_qps_f;
  std::unique_ptr<rocksdb::WritableFile> a_top_qps_prefix_f;
  std::unique_ptr<rocksdb::WritableFile> w_key_f;
  std::unique_ptr<rocksdb::WritableFile> w_prefix_cut_f;

  TraceStats();
  ~TraceStats();
  TraceStats(const TraceStats&) = delete;
  TraceStats& operator=(const TraceStats&) = delete;
  TraceStats(TraceStats&&) = default;
  TraceStats& operator=(TraceStats&&) = default;
};

struct TypeUnit {
  std::string type_name;
  bool enabled;
  uint64_t total_keys;
  uint64_t total_access;
  uint64_t total_succ_access;
  uint32_t sample_count;
  std::map<uint32_t, TraceStats> stats;
  TypeUnit() = default;
  ~TypeUnit() = default;
  TypeUnit(const TypeUnit&) = delete;
  TypeUnit& operator=(const TypeUnit&) = delete;
  TypeUnit(TypeUnit&&) = default;
  TypeUnit& operator=(TypeUnit&&) = default;
};

struct CfUnit {
  uint32_t cf_id;
  uint64_t w_count;  // total keys in this cf if we use the whole key space
  uint64_t a_count;  // the total keys in this cf that are accessed
  std::map<uint64_t, uint64_t> w_key_size_stats;  // whole key space key size
                                                  // statistic this cf
  std::map<uint32_t, uint32_t> cf_qps;
};

class TraceAnalyzer {
 public:
  TraceAnalyzer(std::string& trace_path, std::string& output_path,
                AnalyzerOptions _analyzer_opts);
  ~TraceAnalyzer();

  Status PrepareProcessing();

  Status StartProcessing();

  Status MakeStatistics();

  Status ReProcessing();

  Status EndProcessing();

  Status WriteTraceUnit(TraceUnit& unit);

  // The trace  processing functions for different type
  Status HandleGet(uint32_t column_family_id, const std::string& key,
                   const uint64_t& ts, const uint32_t& get_ret);
  Status HandlePut(uint32_t column_family_id, const Slice& key,
                   const Slice& value);
  Status HandleDelete(uint32_t column_family_id, const Slice& key);
  Status HandleSingleDelete(uint32_t column_family_id, const Slice& key);
  Status HandleDeleteRange(uint32_t column_family_id, const Slice& begin_key,
                           const Slice& end_key);
  Status HandleMerge(uint32_t column_family_id, const Slice& key,
                     const Slice& value);
  Status HandleIter(uint32_t column_family_id, const std::string& key,
                    const uint64_t& ts, TraceType& trace_type);
  std::vector<TypeUnit>& GetTaVector() { return ta_; }

 private:
  rocksdb::Env* env_;
  EnvOptions env_options_;
  std::unique_ptr<TraceReader> trace_reader_;
  size_t offset_;
  char buffer_[1024];
  uint64_t c_time_;
  std::string trace_name_;
  std::string output_path_;
  AnalyzerOptions analyzer_opts_;
  uint64_t total_requests_;
  uint64_t total_access_keys_;
  uint64_t total_gets_;
  uint64_t total_writes_;
  uint64_t trace_create_time_;
  uint64_t begin_time_;
  uint64_t end_time_;
  uint64_t time_series_start_;
  uint32_t sample_max_;
  uint32_t cur_time_sec_;
  std::unique_ptr<rocksdb::WritableFile> trace_sequence_f_;  // readable trace
  std::unique_ptr<rocksdb::WritableFile> qps_f_;             // overall qps
  std::unique_ptr<rocksdb::WritableFile> cf_qps_f_;  // The qps of each CF>
  std::unique_ptr<rocksdb::SequentialFile> wkey_input_f_;
  std::vector<TypeUnit> ta_;  // The main statistic collecting data structure
  std::map<uint32_t, CfUnit> cfs_;  // All the cf_id appears in this trace;
  std::vector<uint32_t> qps_peak_;
  std::vector<double> qps_ave_;

  Status ReadTraceHeader(Trace* header);
  Status ReadTraceFooter(Trace* footer);
  Status ReadTraceRecord(Trace* trace);
  Status KeyStatsInsertion(const uint32_t& type, const uint32_t& cf_id,
                           const std::string& key, const size_t value_size,
                           const uint64_t ts);
  Status StatsUnitCorrelationUpdate(StatsUnit& unit, const uint32_t& type,
                                    const uint64_t& ts, const std::string& key);
  Status OpenStatsOutputFiles(const std::string& type, TraceStats& new_stats);
  Status CreateOutputFile(const std::string& type, const std::string& cf_name,
                          const std::string& ending,
                          std::unique_ptr<rocksdb::WritableFile>* f_ptr);
  void CloseOutputFiles();

  void PrintStatistics();
  Status TraceUnitWriter(std::unique_ptr<rocksdb::WritableFile>& f_ptr,
                         TraceUnit& unit);
  Status WriteTraceSequence(const uint32_t& type, const uint32_t& cf_id,
                            const std::string& key, const size_t value_size,
                            const uint64_t ts);
  Status MakeStatisticKeyStatsOrPrefix(TraceStats& stats);
  Status MakeStatisticCorrelation(TraceStats& stats, StatsUnit& unit);
  Status MakeStatisticQPS();
};

// write bach handler to be used for WriteBache iterator
// when processing the write trace
class TraceWriteHandler : public WriteBatch::Handler {
 public:
  TraceWriteHandler() { ta_ptr = nullptr; }
  explicit TraceWriteHandler(TraceAnalyzer* _ta_ptr) { ta_ptr = _ta_ptr; }
  ~TraceWriteHandler() {}

  virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                       const Slice& value) override {
    return ta_ptr->HandlePut(column_family_id, key, value);
  }
  virtual Status DeleteCF(uint32_t column_family_id,
                          const Slice& key) override {
    return ta_ptr->HandleDelete(column_family_id, key);
  }
  virtual Status SingleDeleteCF(uint32_t column_family_id,
                                const Slice& key) override {
    return ta_ptr->HandleSingleDelete(column_family_id, key);
  }
  virtual Status DeleteRangeCF(uint32_t column_family_id,
                               const Slice& begin_key,
                               const Slice& end_key) override {
    return ta_ptr->HandleDeleteRange(column_family_id, begin_key, end_key);
  }
  virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) override {
    return ta_ptr->HandleMerge(column_family_id, key, value);
  }

 private:
  TraceAnalyzer* ta_ptr;
};

int trace_analyzer_tool(int argc, char** argv);

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
