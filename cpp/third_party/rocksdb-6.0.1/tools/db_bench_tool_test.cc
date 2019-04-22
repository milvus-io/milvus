//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/db_bench_tool.h"
#include "options/options_parser.h"
#include "rocksdb/utilities/options_util.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/testutil.h"

#ifdef GFLAGS
#include "util/gflags_compat.h"

namespace rocksdb {
namespace {
static const int kMaxArgCount = 100;
static const size_t kArgBufferSize = 100000;
}  // namespace

class DBBenchTest : public testing::Test {
 public:
  DBBenchTest() : rnd_(0xFB) {
    test_path_ = test::PerThreadDBPath("db_bench_test");
    Env::Default()->CreateDir(test_path_);
    db_path_ = test_path_ + "/db";
    wal_path_ = test_path_ + "/wal";
  }

  ~DBBenchTest() {
    //  DestroyDB(db_path_, Options());
  }

  void ResetArgs() {
    argc_ = 0;
    cursor_ = 0;
    memset(arg_buffer_, 0, kArgBufferSize);
  }

  void AppendArgs(const std::vector<std::string>& args) {
    for (const auto& arg : args) {
      ASSERT_LE(cursor_ + arg.size() + 1, kArgBufferSize);
      ASSERT_LE(argc_ + 1, kMaxArgCount);
      snprintf(arg_buffer_ + cursor_, arg.size() + 1, "%s", arg.c_str());

      argv_[argc_++] = arg_buffer_ + cursor_;
      cursor_ += arg.size() + 1;
    }
  }

  void RunDbBench(const std::string& options_file_name) {
    AppendArgs({"./db_bench", "--benchmarks=fillseq", "--use_existing_db=0",
                "--num=1000",
                std::string(std::string("--db=") + db_path_).c_str(),
                std::string(std::string("--wal_dir=") + wal_path_).c_str(),
                std::string(std::string("--options_file=") + options_file_name)
                    .c_str()});
    ASSERT_EQ(0, db_bench_tool(argc(), argv()));
  }

  void VerifyOptions(const Options& opt) {
    DBOptions loaded_db_opts;
    std::vector<ColumnFamilyDescriptor> cf_descs;
    ASSERT_OK(LoadLatestOptions(db_path_, Env::Default(), &loaded_db_opts,
                                &cf_descs));

    ASSERT_OK(
        RocksDBOptionsParser::VerifyDBOptions(DBOptions(opt), loaded_db_opts));
    ASSERT_OK(RocksDBOptionsParser::VerifyCFOptions(ColumnFamilyOptions(opt),
                                                    cf_descs[0].options));

    // check with the default rocksdb options and expect failure
    ASSERT_NOK(
        RocksDBOptionsParser::VerifyDBOptions(DBOptions(), loaded_db_opts));
    ASSERT_NOK(RocksDBOptionsParser::VerifyCFOptions(ColumnFamilyOptions(),
                                                     cf_descs[0].options));
  }

  char** argv() { return argv_; }

  int argc() { return argc_; }

  std::string db_path_;
  std::string test_path_;
  std::string wal_path_;

  char arg_buffer_[kArgBufferSize];
  char* argv_[kMaxArgCount];
  int argc_ = 0;
  int cursor_ = 0;
  Random rnd_;
};

namespace {}  // namespace

TEST_F(DBBenchTest, OptionsFile) {
  const std::string kOptionsFileName = test_path_ + "/OPTIONS_test";

  Options opt;
  opt.create_if_missing = true;
  opt.max_open_files = 256;
  opt.max_background_compactions = 10;
  opt.arena_block_size = 8388608;
  ASSERT_OK(PersistRocksDBOptions(DBOptions(opt), {"default"},
                                  {ColumnFamilyOptions(opt)}, kOptionsFileName,
                                  Env::Default()));

  // override the following options as db_bench will not take these
  // options from the options file
  opt.wal_dir = wal_path_;

  RunDbBench(kOptionsFileName);

  VerifyOptions(opt);
}

TEST_F(DBBenchTest, OptionsFileUniversal) {
  const std::string kOptionsFileName = test_path_ + "/OPTIONS_test";

  Options opt;
  opt.compaction_style = kCompactionStyleUniversal;
  opt.num_levels = 1;
  opt.create_if_missing = true;
  opt.max_open_files = 256;
  opt.max_background_compactions = 10;
  opt.arena_block_size = 8388608;
  ASSERT_OK(PersistRocksDBOptions(DBOptions(opt), {"default"},
                                  {ColumnFamilyOptions(opt)}, kOptionsFileName,
                                  Env::Default()));

  // override the following options as db_bench will not take these
  // options from the options file
  opt.wal_dir = wal_path_;

  RunDbBench(kOptionsFileName);

  VerifyOptions(opt);
}

TEST_F(DBBenchTest, OptionsFileMultiLevelUniversal) {
  const std::string kOptionsFileName = test_path_ + "/OPTIONS_test";

  Options opt;
  opt.compaction_style = kCompactionStyleUniversal;
  opt.num_levels = 12;
  opt.create_if_missing = true;
  opt.max_open_files = 256;
  opt.max_background_compactions = 10;
  opt.arena_block_size = 8388608;
  ASSERT_OK(PersistRocksDBOptions(DBOptions(opt), {"default"},
                                  {ColumnFamilyOptions(opt)}, kOptionsFileName,
                                  Env::Default()));

  // override the following options as db_bench will not take these
  // options from the options file
  opt.wal_dir = wal_path_;

  RunDbBench(kOptionsFileName);

  VerifyOptions(opt);
}

const std::string options_file_content = R"OPTIONS_FILE(
[Version]
  rocksdb_version=4.3.1
  options_file_version=1.1

[DBOptions]
  wal_bytes_per_sync=1048576
  delete_obsolete_files_period_micros=0
  WAL_ttl_seconds=0
  WAL_size_limit_MB=0
  db_write_buffer_size=0
  max_subcompactions=1
  table_cache_numshardbits=4
  max_open_files=-1
  max_file_opening_threads=10
  max_background_compactions=5
  use_fsync=false
  use_adaptive_mutex=false
  max_total_wal_size=18446744073709551615
  compaction_readahead_size=0
  new_table_reader_for_compaction_inputs=false
  keep_log_file_num=10
  skip_stats_update_on_db_open=false
  max_manifest_file_size=18446744073709551615
  db_log_dir=
  skip_log_error_on_recovery=false
  writable_file_max_buffer_size=1048576
  paranoid_checks=true
  is_fd_close_on_exec=true
  bytes_per_sync=1048576
  enable_thread_tracking=true
  recycle_log_file_num=0
  create_missing_column_families=false
  log_file_time_to_roll=0
  max_background_flushes=1
  create_if_missing=true
  error_if_exists=false
  delayed_write_rate=1048576
  manifest_preallocation_size=4194304
  allow_mmap_reads=false
  allow_mmap_writes=false
  use_direct_reads=false
  use_direct_io_for_flush_and_compaction=false
  stats_dump_period_sec=600
  allow_fallocate=true
  max_log_file_size=83886080
  random_access_max_buffer_size=1048576
  advise_random_on_open=true


[CFOptions "default"]
  compaction_filter_factory=nullptr
  table_factory=BlockBasedTable
  prefix_extractor=nullptr
  comparator=leveldb.BytewiseComparator
  compression_per_level=
  max_bytes_for_level_base=104857600
  bloom_locality=0
  target_file_size_base=10485760
  memtable_huge_page_size=0
  max_successive_merges=1000
  max_sequential_skip_in_iterations=8
  arena_block_size=52428800
  target_file_size_multiplier=1
  source_compaction_factor=1
  min_write_buffer_number_to_merge=1
  max_write_buffer_number=2
  write_buffer_size=419430400
  max_grandparent_overlap_factor=10
  max_bytes_for_level_multiplier=10
  memtable_factory=SkipListFactory
  compression=kSnappyCompression
  min_partial_merge_operands=2
  level0_stop_writes_trigger=100
  num_levels=1
  level0_slowdown_writes_trigger=50
  level0_file_num_compaction_trigger=10
  expanded_compaction_factor=25
  soft_rate_limit=0.000000
  max_write_buffer_number_to_maintain=0
  verify_checksums_in_compaction=true
  merge_operator=nullptr
  memtable_prefix_bloom_bits=0
  memtable_whole_key_filtering=true
  paranoid_file_checks=false
  inplace_update_num_locks=10000
  optimize_filters_for_hits=false
  level_compaction_dynamic_level_bytes=false
  inplace_update_support=false
  compaction_style=kCompactionStyleUniversal
  memtable_prefix_bloom_probes=6
  purge_redundant_kvs_while_flush=true
  filter_deletes=false
  hard_pending_compaction_bytes_limit=0
  disable_auto_compactions=false
  compaction_measure_io_stats=false

[TableOptions/BlockBasedTable "default"]
  format_version=0
  skip_table_builder_flush=false
  cache_index_and_filter_blocks=false
  flush_block_policy_factory=FlushBlockBySizePolicyFactory
  hash_index_allow_collision=true
  index_type=kBinarySearch
  whole_key_filtering=true
  checksum=kCRC32c
  no_block_cache=false
  block_size=32768
  block_size_deviation=10
  block_restart_interval=16
  filter_policy=rocksdb.BuiltinBloomFilter
)OPTIONS_FILE";

TEST_F(DBBenchTest, OptionsFileFromFile) {
  const std::string kOptionsFileName = test_path_ + "/OPTIONS_flash";
  std::unique_ptr<WritableFile> writable;
  ASSERT_OK(Env::Default()->NewWritableFile(kOptionsFileName, &writable,
                                            EnvOptions()));
  ASSERT_OK(writable->Append(options_file_content));
  ASSERT_OK(writable->Close());

  DBOptions db_opt;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  ASSERT_OK(LoadOptionsFromFile(kOptionsFileName, Env::Default(), &db_opt,
                                &cf_descs));
  Options opt(db_opt, cf_descs[0].options);

  opt.create_if_missing = true;

  // override the following options as db_bench will not take these
  // options from the options file
  opt.wal_dir = wal_path_;

  RunDbBench(kOptionsFileName);

  VerifyOptions(opt);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}

#else

int main(int argc, char** argv) {
  printf("Skip db_bench_tool_test as the required library GFLAG is missing.");
}
#endif  // #ifdef GFLAGS
