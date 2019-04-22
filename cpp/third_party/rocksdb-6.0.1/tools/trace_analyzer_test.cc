//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef ROCKSDB_LITE
#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run trace_analyzer test\n");
  return 1;
}
#else

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <sstream>
#include <thread>

#include "db/db_test_util.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/trace_reader_writer.h"
#include "tools/trace_analyzer_tool.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "util/trace_replay.h"

namespace rocksdb {

namespace {
static const int kMaxArgCount = 100;
static const size_t kArgBufferSize = 100000;
}  // namespace

// The helper functions for the test
class TraceAnalyzerTest : public testing::Test {
 public:
  TraceAnalyzerTest() : rnd_(0xFB) {
    // test_path_ = test::TmpDir() + "trace_analyzer_test";
    test_path_ = test::PerThreadDBPath("trace_analyzer_test");
    env_ = rocksdb::Env::Default();
    env_->CreateDir(test_path_);
    dbname_ = test_path_ + "/db";
  }

  ~TraceAnalyzerTest() override {}

  void GenerateTrace(std::string trace_path) {
    Options options;
    options.create_if_missing = true;
    options.merge_operator = MergeOperators::CreatePutOperator();
    ReadOptions ro;
    WriteOptions wo;
    TraceOptions trace_opt;
    DB* db_ = nullptr;
    std::string value;
    std::unique_ptr<TraceWriter> trace_writer;
    Iterator* single_iter = nullptr;

    ASSERT_OK(
        NewFileTraceWriter(env_, env_options_, trace_path, &trace_writer));
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    ASSERT_OK(db_->StartTrace(trace_opt, std::move(trace_writer)));

    WriteBatch batch;
    ASSERT_OK(batch.Put("a", "aaaaaaaaa"));
    ASSERT_OK(batch.Merge("b", "aaaaaaaaaaaaaaaaaaaa"));
    ASSERT_OK(batch.Delete("c"));
    ASSERT_OK(batch.SingleDelete("d"));
    ASSERT_OK(batch.DeleteRange("e", "f"));
    ASSERT_OK(db_->Write(wo, &batch));

    ASSERT_OK(db_->Get(ro, "a", &value));
    single_iter = db_->NewIterator(ro);
    single_iter->Seek("a");
    single_iter->SeekForPrev("b");
    delete single_iter;
    std::this_thread::sleep_for (std::chrono::seconds(1));

    db_->Get(ro, "g", &value);

    ASSERT_OK(db_->EndTrace());

    ASSERT_OK(env_->FileExists(trace_path));

    std::unique_ptr<WritableFile> whole_f;
    std::string whole_path = test_path_ + "/0.txt";
    ASSERT_OK(env_->NewWritableFile(whole_path, &whole_f, env_options_));
    std::string whole_str = "0x61\n0x62\n0x63\n0x64\n0x65\n0x66\n";
    ASSERT_OK(whole_f->Append(whole_str));
    delete db_;
    ASSERT_OK(DestroyDB(dbname_, options));
  }

  void RunTraceAnalyzer(const std::vector<std::string>& args) {
    char arg_buffer[kArgBufferSize];
    char* argv[kMaxArgCount];
    int argc = 0;
    int cursor = 0;

    for (const auto& arg : args) {
      ASSERT_LE(cursor + arg.size() + 1, kArgBufferSize);
      ASSERT_LE(argc + 1, kMaxArgCount);
      snprintf(arg_buffer + cursor, arg.size() + 1, "%s", arg.c_str());

      argv[argc++] = arg_buffer + cursor;
      cursor += static_cast<int>(arg.size()) + 1;
    }

    ASSERT_EQ(0, rocksdb::trace_analyzer_tool(argc, argv));
  }

  void CheckFileContent(const std::vector<std::string>& cnt,
                        std::string file_path, bool full_content) {
    ASSERT_OK(env_->FileExists(file_path));
    std::unique_ptr<SequentialFile> f_ptr;
    ASSERT_OK(env_->NewSequentialFile(file_path, &f_ptr, env_options_));

    std::string get_line;
    std::istringstream iss;
    bool has_data = true;
    std::vector<std::string> result;
    uint32_t count;
    Status s;
    for (count = 0; ReadOneLine(&iss, f_ptr.get(), &get_line, &has_data, &s);
         ++count) {
      ASSERT_OK(s);
      result.push_back(get_line);
    }

    ASSERT_EQ(cnt.size(), result.size());
    for (int i = 0; i < static_cast<int>(result.size()); i++) {
      if (full_content) {
        ASSERT_EQ(result[i], cnt[i]);
      } else {
        ASSERT_EQ(result[i][0], cnt[i][0]);
      }
    }

    return;
  }

  void AnalyzeTrace(std::vector<std::string>& paras_diff,
                    std::string output_path, std::string trace_path) {
    std::vector<std::string> paras = {"./trace_analyzer",
                                      "-convert_to_human_readable_trace",
                                      "-output_key_stats",
                                      "-output_access_count_stats",
                                      "-output_prefix=test",
                                      "-output_prefix_cut=1",
                                      "-output_time_series",
                                      "-output_value_distribution",
                                      "-output_qps_stats",
                                      "-no_key",
                                      "-no_print"};
    for (auto& para : paras_diff) {
      paras.push_back(para);
    }
    Status s = env_->FileExists(trace_path);
    if (!s.ok()) {
      GenerateTrace(trace_path);
    }
    env_->CreateDir(output_path);
    RunTraceAnalyzer(paras);
  }

  rocksdb::Env* env_;
  EnvOptions env_options_;
  std::string test_path_;
  std::string dbname_;
  Random rnd_;
};

TEST_F(TraceAnalyzerTest, Get) {
  std::string trace_path = test_path_ + "/trace";
  std::string output_path = test_path_ + "/get";
  std::string file_path;
  std::vector<std::string> paras = {"-analyze_get"};
  paras.push_back("-output_dir=" + output_path);
  paras.push_back("-trace_path=" + trace_path);
  paras.push_back("-key_space_dir=" + test_path_);
  AnalyzeTrace(paras, output_path, trace_path);

  // check the key_stats file
  std::vector<std::string> k_stats = {"0 10 0 1 1.000000", "0 10 1 1 1.000000"};
  file_path = output_path + "/test-get-0-accessed_key_stats.txt";
  CheckFileContent(k_stats, file_path, true);

  // Check the access count distribution
  std::vector<std::string> k_dist = {"access_count: 1 num: 2"};
  file_path = output_path + "/test-get-0-accessed_key_count_distribution.txt";
  CheckFileContent(k_dist, file_path, true);

  // Check the trace sequence
  std::vector<std::string> k_sequence = {"1", "5", "2", "3", "4",
                                         "0", "6", "7", "0"};
  file_path = output_path + "/test-human_readable_trace.txt";
  CheckFileContent(k_sequence, file_path, false);

  // Check the prefix
  std::vector<std::string> k_prefix = {"0 0 0 0.000000 0.000000 0x30",
                                       "1 1 1 1.000000 1.000000 0x61"};
  file_path = output_path + "/test-get-0-accessed_key_prefix_cut.txt";
  CheckFileContent(k_prefix, file_path, true);

  // Check the time series
  std::vector<std::string> k_series = {"0 1533000630 0", "0 1533000630 1"};
  file_path = output_path + "/test-get-0-time_series.txt";
  CheckFileContent(k_series, file_path, false);

  // Check the accessed key in whole key space
  std::vector<std::string> k_whole_access = {"0 1"};
  file_path = output_path + "/test-get-0-whole_key_stats.txt";
  CheckFileContent(k_whole_access, file_path, true);

  // Check the whole key prefix cut
  std::vector<std::string> k_whole_prefix = {"0 0x61", "1 0x62", "2 0x63",
                                             "3 0x64", "4 0x65", "5 0x66"};
  file_path = output_path + "/test-get-0-whole_key_prefix_cut.txt";
  CheckFileContent(k_whole_prefix, file_path, true);

  // Check the overall qps
  std::vector<std::string> all_qps = {"1 0 0 0 0 0 0 0 1"};
  file_path = output_path + "/test-qps_stats.txt";
  CheckFileContent(all_qps, file_path, true);

  // Check the qps of get
  std::vector<std::string> get_qps = {"1"};
  file_path = output_path + "/test-get-0-qps_stats.txt";
  CheckFileContent(get_qps, file_path, true);

  // Check the top k qps prefix cut
  std::vector<std::string> top_qps = {"At time: 0 with QPS: 1",
                                      "The prefix: 0x61 Access count: 1"};
  file_path = output_path + "/test-get-0-accessed_top_k_qps_prefix_cut.txt";
  CheckFileContent(top_qps, file_path, true);
}

// Test analyzing of Put
TEST_F(TraceAnalyzerTest, Put) {
  std::string trace_path = test_path_ + "/trace";
  std::string output_path = test_path_ + "/put";
  std::string file_path;
  std::vector<std::string> paras = {"-analyze_put"};
  paras.push_back("-output_dir=" + output_path);
  paras.push_back("-trace_path=" + trace_path);
  paras.push_back("-key_space_dir=" + test_path_);
  AnalyzeTrace(paras, output_path, trace_path);

  // check the key_stats file
  std::vector<std::string> k_stats = {"0 9 0 1 1.000000"};
  file_path = output_path + "/test-put-0-accessed_key_stats.txt";
  CheckFileContent(k_stats, file_path, true);

  // Check the access count distribution
  std::vector<std::string> k_dist = {"access_count: 1 num: 1"};
  file_path = output_path + "/test-put-0-accessed_key_count_distribution.txt";
  CheckFileContent(k_dist, file_path, true);

  // Check the trace sequence
  std::vector<std::string> k_sequence = {"1", "5", "2", "3", "4",
                                         "0", "6", "7", "0"};
  file_path = output_path + "/test-human_readable_trace.txt";
  CheckFileContent(k_sequence, file_path, false);

  // Check the prefix
  std::vector<std::string> k_prefix = {"0 0 0 0.000000 0.000000 0x30"};
  file_path = output_path + "/test-put-0-accessed_key_prefix_cut.txt";
  CheckFileContent(k_prefix, file_path, true);

  // Check the time series
  std::vector<std::string> k_series = {"1 1533056278 0"};
  file_path = output_path + "/test-put-0-time_series.txt";
  CheckFileContent(k_series, file_path, false);

  // Check the accessed key in whole key space
  std::vector<std::string> k_whole_access = {"0 1"};
  file_path = output_path + "/test-put-0-whole_key_stats.txt";
  CheckFileContent(k_whole_access, file_path, true);

  // Check the whole key prefix cut
  std::vector<std::string> k_whole_prefix = {"0 0x61", "1 0x62", "2 0x63",
                                             "3 0x64", "4 0x65", "5 0x66"};
  file_path = output_path + "/test-put-0-whole_key_prefix_cut.txt";
  CheckFileContent(k_whole_prefix, file_path, true);

  // Check the overall qps
  std::vector<std::string> all_qps = {"1 1 0 0 0 0 0 0 2"};
  file_path = output_path + "/test-qps_stats.txt";
  CheckFileContent(all_qps, file_path, true);

  // Check the qps of Put
  std::vector<std::string> get_qps = {"1"};
  file_path = output_path + "/test-put-0-qps_stats.txt";
  CheckFileContent(get_qps, file_path, true);

  // Check the top k qps prefix cut
  std::vector<std::string> top_qps = {"At time: 0 with QPS: 1",
                                      "The prefix: 0x61 Access count: 1"};
  file_path = output_path + "/test-put-0-accessed_top_k_qps_prefix_cut.txt";
  CheckFileContent(top_qps, file_path, true);

  // Check the value size distribution
  std::vector<std::string> value_dist = {
      "Number_of_value_size_between 0 and 16 is: 1"};
  file_path = output_path + "/test-put-0-accessed_value_size_distribution.txt";
  CheckFileContent(value_dist, file_path, true);
}

// Test analyzing of delete
TEST_F(TraceAnalyzerTest, Delete) {
  std::string trace_path = test_path_ + "/trace";
  std::string output_path = test_path_ + "/delete";
  std::string file_path;
  std::vector<std::string> paras = {"-analyze_delete"};
  paras.push_back("-output_dir=" + output_path);
  paras.push_back("-trace_path=" + trace_path);
  paras.push_back("-key_space_dir=" + test_path_);
  AnalyzeTrace(paras, output_path, trace_path);

  // check the key_stats file
  std::vector<std::string> k_stats = {"0 0 0 1 1.000000"};
  file_path = output_path + "/test-delete-0-accessed_key_stats.txt";
  CheckFileContent(k_stats, file_path, true);

  // Check the access count distribution
  std::vector<std::string> k_dist = {"access_count: 1 num: 1"};
  file_path =
      output_path + "/test-delete-0-accessed_key_count_distribution.txt";
  CheckFileContent(k_dist, file_path, true);

  // Check the trace sequence
  std::vector<std::string> k_sequence = {"1", "5", "2", "3", "4",
                                         "0", "6", "7", "0"};
  file_path = output_path + "/test-human_readable_trace.txt";
  CheckFileContent(k_sequence, file_path, false);

  // Check the prefix
  std::vector<std::string> k_prefix = {"0 0 0 0.000000 0.000000 0x30"};
  file_path = output_path + "/test-delete-0-accessed_key_prefix_cut.txt";
  CheckFileContent(k_prefix, file_path, true);

  // Check the time series
  std::vector<std::string> k_series = {"2 1533000630 0"};
  file_path = output_path + "/test-delete-0-time_series.txt";
  CheckFileContent(k_series, file_path, false);

  // Check the accessed key in whole key space
  std::vector<std::string> k_whole_access = {"2 1"};
  file_path = output_path + "/test-delete-0-whole_key_stats.txt";
  CheckFileContent(k_whole_access, file_path, true);

  // Check the whole key prefix cut
  std::vector<std::string> k_whole_prefix = {"0 0x61", "1 0x62", "2 0x63",
                                             "3 0x64", "4 0x65", "5 0x66"};
  file_path = output_path + "/test-delete-0-whole_key_prefix_cut.txt";
  CheckFileContent(k_whole_prefix, file_path, true);

  // Check the overall qps
  std::vector<std::string> all_qps = {"1 1 1 0 0 0 0 0 3"};
  file_path = output_path + "/test-qps_stats.txt";
  CheckFileContent(all_qps, file_path, true);

  // Check the qps of Delete
  std::vector<std::string> get_qps = {"1"};
  file_path = output_path + "/test-delete-0-qps_stats.txt";
  CheckFileContent(get_qps, file_path, true);

  // Check the top k qps prefix cut
  std::vector<std::string> top_qps = {"At time: 0 with QPS: 1",
                                      "The prefix: 0x63 Access count: 1"};
  file_path = output_path + "/test-delete-0-accessed_top_k_qps_prefix_cut.txt";
  CheckFileContent(top_qps, file_path, true);
}

// Test analyzing of Merge
TEST_F(TraceAnalyzerTest, Merge) {
  std::string trace_path = test_path_ + "/trace";
  std::string output_path = test_path_ + "/merge";
  std::string file_path;
  std::vector<std::string> paras = {"-analyze_merge"};
  paras.push_back("-output_dir=" + output_path);
  paras.push_back("-trace_path=" + trace_path);
  paras.push_back("-key_space_dir=" + test_path_);
  AnalyzeTrace(paras, output_path, trace_path);

  // check the key_stats file
  std::vector<std::string> k_stats = {"0 20 0 1 1.000000"};
  file_path = output_path + "/test-merge-0-accessed_key_stats.txt";
  CheckFileContent(k_stats, file_path, true);

  // Check the access count distribution
  std::vector<std::string> k_dist = {"access_count: 1 num: 1"};
  file_path = output_path + "/test-merge-0-accessed_key_count_distribution.txt";
  CheckFileContent(k_dist, file_path, true);

  // Check the trace sequence
  std::vector<std::string> k_sequence = {"1", "5", "2", "3", "4",
                                         "0", "6", "7", "0"};
  file_path = output_path + "/test-human_readable_trace.txt";
  CheckFileContent(k_sequence, file_path, false);

  // Check the prefix
  std::vector<std::string> k_prefix = {"0 0 0 0.000000 0.000000 0x30"};
  file_path = output_path + "/test-merge-0-accessed_key_prefix_cut.txt";
  CheckFileContent(k_prefix, file_path, true);

  // Check the time series
  std::vector<std::string> k_series = {"5 1533000630 0"};
  file_path = output_path + "/test-merge-0-time_series.txt";
  CheckFileContent(k_series, file_path, false);

  // Check the accessed key in whole key space
  std::vector<std::string> k_whole_access = {"1 1"};
  file_path = output_path + "/test-merge-0-whole_key_stats.txt";
  CheckFileContent(k_whole_access, file_path, true);

  // Check the whole key prefix cut
  std::vector<std::string> k_whole_prefix = {"0 0x61", "1 0x62", "2 0x63",
                                             "3 0x64", "4 0x65", "5 0x66"};
  file_path = output_path + "/test-merge-0-whole_key_prefix_cut.txt";
  CheckFileContent(k_whole_prefix, file_path, true);

  // Check the overall qps
  std::vector<std::string> all_qps = {"1 1 1 0 0 1 0 0 4"};
  file_path = output_path + "/test-qps_stats.txt";
  CheckFileContent(all_qps, file_path, true);

  // Check the qps of Merge
  std::vector<std::string> get_qps = {"1"};
  file_path = output_path + "/test-merge-0-qps_stats.txt";
  CheckFileContent(get_qps, file_path, true);

  // Check the top k qps prefix cut
  std::vector<std::string> top_qps = {"At time: 0 with QPS: 1",
                                      "The prefix: 0x62 Access count: 1"};
  file_path = output_path + "/test-merge-0-accessed_top_k_qps_prefix_cut.txt";
  CheckFileContent(top_qps, file_path, true);

  // Check the value size distribution
  std::vector<std::string> value_dist = {
      "Number_of_value_size_between 0 and 24 is: 1"};
  file_path =
      output_path + "/test-merge-0-accessed_value_size_distribution.txt";
  CheckFileContent(value_dist, file_path, true);
}

// Test analyzing of SingleDelete
TEST_F(TraceAnalyzerTest, SingleDelete) {
  std::string trace_path = test_path_ + "/trace";
  std::string output_path = test_path_ + "/single_delete";
  std::string file_path;
  std::vector<std::string> paras = {"-analyze_single_delete"};
  paras.push_back("-output_dir=" + output_path);
  paras.push_back("-trace_path=" + trace_path);
  paras.push_back("-key_space_dir=" + test_path_);
  AnalyzeTrace(paras, output_path, trace_path);

  // check the key_stats file
  std::vector<std::string> k_stats = {"0 0 0 1 1.000000"};
  file_path = output_path + "/test-single_delete-0-accessed_key_stats.txt";
  CheckFileContent(k_stats, file_path, true);

  // Check the access count distribution
  std::vector<std::string> k_dist = {"access_count: 1 num: 1"};
  file_path =
      output_path + "/test-single_delete-0-accessed_key_count_distribution.txt";
  CheckFileContent(k_dist, file_path, true);

  // Check the trace sequence
  std::vector<std::string> k_sequence = {"1", "5", "2", "3", "4",
                                         "0", "6", "7", "0"};
  file_path = output_path + "/test-human_readable_trace.txt";
  CheckFileContent(k_sequence, file_path, false);

  // Check the prefix
  std::vector<std::string> k_prefix = {"0 0 0 0.000000 0.000000 0x30"};
  file_path = output_path + "/test-single_delete-0-accessed_key_prefix_cut.txt";
  CheckFileContent(k_prefix, file_path, true);

  // Check the time series
  std::vector<std::string> k_series = {"3 1533000630 0"};
  file_path = output_path + "/test-single_delete-0-time_series.txt";
  CheckFileContent(k_series, file_path, false);

  // Check the accessed key in whole key space
  std::vector<std::string> k_whole_access = {"3 1"};
  file_path = output_path + "/test-single_delete-0-whole_key_stats.txt";
  CheckFileContent(k_whole_access, file_path, true);

  // Check the whole key prefix cut
  std::vector<std::string> k_whole_prefix = {"0 0x61", "1 0x62", "2 0x63",
                                             "3 0x64", "4 0x65", "5 0x66"};
  file_path = output_path + "/test-single_delete-0-whole_key_prefix_cut.txt";
  CheckFileContent(k_whole_prefix, file_path, true);

  // Check the overall qps
  std::vector<std::string> all_qps = {"1 1 1 1 0 1 0 0 5"};
  file_path = output_path + "/test-qps_stats.txt";
  CheckFileContent(all_qps, file_path, true);

  // Check the qps of SingleDelete
  std::vector<std::string> get_qps = {"1"};
  file_path = output_path + "/test-single_delete-0-qps_stats.txt";
  CheckFileContent(get_qps, file_path, true);

  // Check the top k qps prefix cut
  std::vector<std::string> top_qps = {"At time: 0 with QPS: 1",
                                      "The prefix: 0x64 Access count: 1"};
  file_path =
      output_path + "/test-single_delete-0-accessed_top_k_qps_prefix_cut.txt";
  CheckFileContent(top_qps, file_path, true);
}

// Test analyzing of delete
TEST_F(TraceAnalyzerTest, DeleteRange) {
  std::string trace_path = test_path_ + "/trace";
  std::string output_path = test_path_ + "/range_delete";
  std::string file_path;
  std::vector<std::string> paras = {"-analyze_range_delete"};
  paras.push_back("-output_dir=" + output_path);
  paras.push_back("-trace_path=" + trace_path);
  paras.push_back("-key_space_dir=" + test_path_);
  AnalyzeTrace(paras, output_path, trace_path);

  // check the key_stats file
  std::vector<std::string> k_stats = {"0 0 0 1 1.000000", "0 0 1 1 1.000000"};
  file_path = output_path + "/test-range_delete-0-accessed_key_stats.txt";
  CheckFileContent(k_stats, file_path, true);

  // Check the access count distribution
  std::vector<std::string> k_dist = {"access_count: 1 num: 2"};
  file_path =
      output_path + "/test-range_delete-0-accessed_key_count_distribution.txt";
  CheckFileContent(k_dist, file_path, true);

  // Check the trace sequence
  std::vector<std::string> k_sequence = {"1", "5", "2", "3", "4",
                                         "0", "6", "7", "0"};
  file_path = output_path + "/test-human_readable_trace.txt";
  CheckFileContent(k_sequence, file_path, false);

  // Check the prefix
  std::vector<std::string> k_prefix = {"0 0 0 0.000000 0.000000 0x30",
                                       "1 1 1 1.000000 1.000000 0x65"};
  file_path = output_path + "/test-range_delete-0-accessed_key_prefix_cut.txt";
  CheckFileContent(k_prefix, file_path, true);

  // Check the time series
  std::vector<std::string> k_series = {"4 1533000630 0", "4 1533060100 1"};
  file_path = output_path + "/test-range_delete-0-time_series.txt";
  CheckFileContent(k_series, file_path, false);

  // Check the accessed key in whole key space
  std::vector<std::string> k_whole_access = {"4 1", "5 1"};
  file_path = output_path + "/test-range_delete-0-whole_key_stats.txt";
  CheckFileContent(k_whole_access, file_path, true);

  // Check the whole key prefix cut
  std::vector<std::string> k_whole_prefix = {"0 0x61", "1 0x62", "2 0x63",
                                             "3 0x64", "4 0x65", "5 0x66"};
  file_path = output_path + "/test-range_delete-0-whole_key_prefix_cut.txt";
  CheckFileContent(k_whole_prefix, file_path, true);

  // Check the overall qps
  std::vector<std::string> all_qps = {"1 1 1 1 2 1 0 0 7"};
  file_path = output_path + "/test-qps_stats.txt";
  CheckFileContent(all_qps, file_path, true);

  // Check the qps of DeleteRange
  std::vector<std::string> get_qps = {"2"};
  file_path = output_path + "/test-range_delete-0-qps_stats.txt";
  CheckFileContent(get_qps, file_path, true);

  // Check the top k qps prefix cut
  std::vector<std::string> top_qps = {"At time: 0 with QPS: 2",
                                      "The prefix: 0x65 Access count: 1",
                                      "The prefix: 0x66 Access count: 1"};
  file_path =
      output_path + "/test-range_delete-0-accessed_top_k_qps_prefix_cut.txt";
  CheckFileContent(top_qps, file_path, true);
}

// Test analyzing of Iterator
TEST_F(TraceAnalyzerTest, Iterator) {
  std::string trace_path = test_path_ + "/trace";
  std::string output_path = test_path_ + "/iterator";
  std::string file_path;
  std::vector<std::string> paras = {"-analyze_iterator"};
  paras.push_back("-output_dir=" + output_path);
  paras.push_back("-trace_path=" + trace_path);
  paras.push_back("-key_space_dir=" + test_path_);
  AnalyzeTrace(paras, output_path, trace_path);

  // Check the output of Seek
  // check the key_stats file
  std::vector<std::string> k_stats = {"0 0 0 1 1.000000"};
  file_path = output_path + "/test-iterator_Seek-0-accessed_key_stats.txt";
  CheckFileContent(k_stats, file_path, true);

  // Check the access count distribution
  std::vector<std::string> k_dist = {"access_count: 1 num: 1"};
  file_path =
      output_path + "/test-iterator_Seek-0-accessed_key_count_distribution.txt";
  CheckFileContent(k_dist, file_path, true);

  // Check the trace sequence
  std::vector<std::string> k_sequence = {"1", "5", "2", "3", "4",
                                         "0", "6", "7", "0"};
  file_path = output_path + "/test-human_readable_trace.txt";
  CheckFileContent(k_sequence, file_path, false);

  // Check the prefix
  std::vector<std::string> k_prefix = {"0 0 0 0.000000 0.000000 0x30"};
  file_path = output_path + "/test-iterator_Seek-0-accessed_key_prefix_cut.txt";
  CheckFileContent(k_prefix, file_path, true);

  // Check the time series
  std::vector<std::string> k_series = {"6 1 0"};
  file_path = output_path + "/test-iterator_Seek-0-time_series.txt";
  CheckFileContent(k_series, file_path, false);

  // Check the accessed key in whole key space
  std::vector<std::string> k_whole_access = {"0 1"};
  file_path = output_path + "/test-iterator_Seek-0-whole_key_stats.txt";
  CheckFileContent(k_whole_access, file_path, true);

  // Check the whole key prefix cut
  std::vector<std::string> k_whole_prefix = {"0 0x61", "1 0x62", "2 0x63",
                                             "3 0x64", "4 0x65", "5 0x66"};
  file_path = output_path + "/test-iterator_Seek-0-whole_key_prefix_cut.txt";
  CheckFileContent(k_whole_prefix, file_path, true);

  // Check the overall qps
  std::vector<std::string> all_qps = {"1 1 1 1 2 1 1 1 9"};
  file_path = output_path + "/test-qps_stats.txt";
  CheckFileContent(all_qps, file_path, true);

  // Check the qps of Iterator_Seek
  std::vector<std::string> get_qps = {"1"};
  file_path = output_path + "/test-iterator_Seek-0-qps_stats.txt";
  CheckFileContent(get_qps, file_path, true);

  // Check the top k qps prefix cut
  std::vector<std::string> top_qps = {"At time: 0 with QPS: 1",
                                      "The prefix: 0x61 Access count: 1"};
  file_path =
      output_path + "/test-iterator_Seek-0-accessed_top_k_qps_prefix_cut.txt";
  CheckFileContent(top_qps, file_path, true);

  // Check the output of SeekForPrev
  // check the key_stats file
  k_stats = {"0 0 0 1 1.000000"};
  file_path =
      output_path + "/test-iterator_SeekForPrev-0-accessed_key_stats.txt";
  CheckFileContent(k_stats, file_path, true);

  // Check the access count distribution
  k_dist = {"access_count: 1 num: 1"};
  file_path =
      output_path +
      "/test-iterator_SeekForPrev-0-accessed_key_count_distribution.txt";
  CheckFileContent(k_dist, file_path, true);

  // Check the prefix
  k_prefix = {"0 0 0 0.000000 0.000000 0x30"};
  file_path =
      output_path + "/test-iterator_SeekForPrev-0-accessed_key_prefix_cut.txt";
  CheckFileContent(k_prefix, file_path, true);

  // Check the time series
  k_series = {"7 0 0"};
  file_path = output_path + "/test-iterator_SeekForPrev-0-time_series.txt";
  CheckFileContent(k_series, file_path, false);

  // Check the accessed key in whole key space
  k_whole_access = {"1 1"};
  file_path = output_path + "/test-iterator_SeekForPrev-0-whole_key_stats.txt";
  CheckFileContent(k_whole_access, file_path, true);

  // Check the whole key prefix cut
  k_whole_prefix = {"0 0x61", "1 0x62", "2 0x63", "3 0x64", "4 0x65", "5 0x66"};
  file_path =
      output_path + "/test-iterator_SeekForPrev-0-whole_key_prefix_cut.txt";
  CheckFileContent(k_whole_prefix, file_path, true);

  // Check the qps of Iterator_SeekForPrev
  get_qps = {"1"};
  file_path = output_path + "/test-iterator_SeekForPrev-0-qps_stats.txt";
  CheckFileContent(get_qps, file_path, true);

  // Check the top k qps prefix cut
  top_qps = {"At time: 0 with QPS: 1", "The prefix: 0x62 Access count: 1"};
  file_path = output_path +
              "/test-iterator_SeekForPrev-0-accessed_top_k_qps_prefix_cut.txt";
  CheckFileContent(top_qps, file_path, true);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#endif  // GFLAG
#else
#include <stdio.h>

int main(int /*argc*/, char** /*argv*/) {
  fprintf(stderr, "Trace_analyzer test is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE  return RUN_ALL_TESTS();
