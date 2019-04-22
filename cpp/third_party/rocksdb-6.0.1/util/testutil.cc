//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/testutil.h"

#include <cctype>
#include <sstream>

#include "db/memtable_list.h"
#include "port/port.h"
#include "util/file_reader_writer.h"

namespace rocksdb {
namespace test {

const uint32_t kDefaultFormatVersion = BlockBasedTableOptions().format_version;
const uint32_t kLatestFormatVersion = 4u;

Slice RandomString(Random* rnd, int len, std::string* dst) {
  dst->resize(len);
  for (int i = 0; i < len; i++) {
    (*dst)[i] = static_cast<char>(' ' + rnd->Uniform(95));  // ' ' .. '~'
  }
  return Slice(*dst);
}

extern std::string RandomHumanReadableString(Random* rnd, int len) {
  std::string ret;
  ret.resize(len);
  for (int i = 0; i < len; ++i) {
    ret[i] = static_cast<char>('a' + rnd->Uniform(26));
  }
  return ret;
}

std::string RandomKey(Random* rnd, int len, RandomKeyType type) {
  // Make sure to generate a wide variety of characters so we
  // test the boundary conditions for short-key optimizations.
  static const char kTestChars[] = {'\0', '\1', 'a',    'b',    'c',
                                    'd',  'e',  '\xfd', '\xfe', '\xff'};
  std::string result;
  for (int i = 0; i < len; i++) {
    std::size_t indx = 0;
    switch (type) {
      case RandomKeyType::RANDOM:
        indx = rnd->Uniform(sizeof(kTestChars));
        break;
      case RandomKeyType::LARGEST:
        indx = sizeof(kTestChars) - 1;
        break;
      case RandomKeyType::MIDDLE:
        indx = sizeof(kTestChars) / 2;
        break;
      case RandomKeyType::SMALLEST:
        indx = 0;
        break;
    }
    result += kTestChars[indx];
  }
  return result;
}

extern Slice CompressibleString(Random* rnd, double compressed_fraction,
                                int len, std::string* dst) {
  int raw = static_cast<int>(len * compressed_fraction);
  if (raw < 1) raw = 1;
  std::string raw_data;
  RandomString(rnd, raw, &raw_data);

  // Duplicate the random data until we have filled "len" bytes
  dst->clear();
  while (dst->size() < (unsigned int)len) {
    dst->append(raw_data);
  }
  dst->resize(len);
  return Slice(*dst);
}

namespace {
class Uint64ComparatorImpl : public Comparator {
 public:
  Uint64ComparatorImpl() {}

  const char* Name() const override { return "rocksdb.Uint64Comparator"; }

  int Compare(const Slice& a, const Slice& b) const override {
    assert(a.size() == sizeof(uint64_t) && b.size() == sizeof(uint64_t));
    const uint64_t* left = reinterpret_cast<const uint64_t*>(a.data());
    const uint64_t* right = reinterpret_cast<const uint64_t*>(b.data());
    uint64_t leftValue;
    uint64_t rightValue;
    GetUnaligned(left, &leftValue);
    GetUnaligned(right, &rightValue);
    if (leftValue == rightValue) {
      return 0;
    } else if (leftValue < rightValue) {
      return -1;
    } else {
      return 1;
    }
  }

  void FindShortestSeparator(std::string* /*start*/,
                             const Slice& /*limit*/) const override {
    return;
  }

  void FindShortSuccessor(std::string* /*key*/) const override { return; }
};
}  // namespace

const Comparator* Uint64Comparator() {
  static Uint64ComparatorImpl uint64comp;
  return &uint64comp;
}

WritableFileWriter* GetWritableFileWriter(WritableFile* wf,
                                          const std::string& fname) {
  std::unique_ptr<WritableFile> file(wf);
  return new WritableFileWriter(std::move(file), fname, EnvOptions());
}

RandomAccessFileReader* GetRandomAccessFileReader(RandomAccessFile* raf) {
  std::unique_ptr<RandomAccessFile> file(raf);
  return new RandomAccessFileReader(std::move(file),
                                    "[test RandomAccessFileReader]");
}

SequentialFileReader* GetSequentialFileReader(SequentialFile* se,
                                              const std::string& fname) {
  std::unique_ptr<SequentialFile> file(se);
  return new SequentialFileReader(std::move(file), fname);
}

void CorruptKeyType(InternalKey* ikey) {
  std::string keystr = ikey->Encode().ToString();
  keystr[keystr.size() - 8] = kTypeLogData;
  ikey->DecodeFrom(Slice(keystr.data(), keystr.size()));
}

std::string KeyStr(const std::string& user_key, const SequenceNumber& seq,
                   const ValueType& t, bool corrupt) {
  InternalKey k(user_key, seq, t);
  if (corrupt) {
    CorruptKeyType(&k);
  }
  return k.Encode().ToString();
}

std::string RandomName(Random* rnd, const size_t len) {
  std::stringstream ss;
  for (size_t i = 0; i < len; ++i) {
    ss << static_cast<char>(rnd->Uniform(26) + 'a');
  }
  return ss.str();
}

CompressionType RandomCompressionType(Random* rnd) {
  return static_cast<CompressionType>(rnd->Uniform(6));
}

void RandomCompressionTypeVector(const size_t count,
                                 std::vector<CompressionType>* types,
                                 Random* rnd) {
  types->clear();
  for (size_t i = 0; i < count; ++i) {
    types->emplace_back(RandomCompressionType(rnd));
  }
}

const SliceTransform* RandomSliceTransform(Random* rnd, int pre_defined) {
  int random_num = pre_defined >= 0 ? pre_defined : rnd->Uniform(4);
  switch (random_num) {
    case 0:
      return NewFixedPrefixTransform(rnd->Uniform(20) + 1);
    case 1:
      return NewCappedPrefixTransform(rnd->Uniform(20) + 1);
    case 2:
      return NewNoopTransform();
    default:
      return nullptr;
  }
}

BlockBasedTableOptions RandomBlockBasedTableOptions(Random* rnd) {
  BlockBasedTableOptions opt;
  opt.cache_index_and_filter_blocks = rnd->Uniform(2);
  opt.pin_l0_filter_and_index_blocks_in_cache = rnd->Uniform(2);
  opt.pin_top_level_index_and_filter = rnd->Uniform(2);
  opt.index_type = rnd->Uniform(2) ? BlockBasedTableOptions::kBinarySearch
                                   : BlockBasedTableOptions::kHashSearch;
  opt.hash_index_allow_collision = rnd->Uniform(2);
  opt.checksum = static_cast<ChecksumType>(rnd->Uniform(3));
  opt.block_size = rnd->Uniform(10000000);
  opt.block_size_deviation = rnd->Uniform(100);
  opt.block_restart_interval = rnd->Uniform(100);
  opt.index_block_restart_interval = rnd->Uniform(100);
  opt.whole_key_filtering = rnd->Uniform(2);

  return opt;
}

TableFactory* RandomTableFactory(Random* rnd, int pre_defined) {
#ifndef ROCKSDB_LITE
  int random_num = pre_defined >= 0 ? pre_defined : rnd->Uniform(4);
  switch (random_num) {
    case 0:
      return NewPlainTableFactory();
    case 1:
      return NewCuckooTableFactory();
    default:
      return NewBlockBasedTableFactory();
  }
#else
  (void)rnd;
  (void)pre_defined;
  return NewBlockBasedTableFactory();
#endif  // !ROCKSDB_LITE
}

MergeOperator* RandomMergeOperator(Random* rnd) {
  return new ChanglingMergeOperator(RandomName(rnd, 10));
}

CompactionFilter* RandomCompactionFilter(Random* rnd) {
  return new ChanglingCompactionFilter(RandomName(rnd, 10));
}

CompactionFilterFactory* RandomCompactionFilterFactory(Random* rnd) {
  return new ChanglingCompactionFilterFactory(RandomName(rnd, 10));
}

void RandomInitDBOptions(DBOptions* db_opt, Random* rnd) {
  // boolean options
  db_opt->advise_random_on_open = rnd->Uniform(2);
  db_opt->allow_mmap_reads = rnd->Uniform(2);
  db_opt->allow_mmap_writes = rnd->Uniform(2);
  db_opt->use_direct_reads = rnd->Uniform(2);
  db_opt->use_direct_io_for_flush_and_compaction = rnd->Uniform(2);
  db_opt->create_if_missing = rnd->Uniform(2);
  db_opt->create_missing_column_families = rnd->Uniform(2);
  db_opt->enable_thread_tracking = rnd->Uniform(2);
  db_opt->error_if_exists = rnd->Uniform(2);
  db_opt->is_fd_close_on_exec = rnd->Uniform(2);
  db_opt->paranoid_checks = rnd->Uniform(2);
  db_opt->skip_log_error_on_recovery = rnd->Uniform(2);
  db_opt->skip_stats_update_on_db_open = rnd->Uniform(2);
  db_opt->use_adaptive_mutex = rnd->Uniform(2);
  db_opt->use_fsync = rnd->Uniform(2);
  db_opt->recycle_log_file_num = rnd->Uniform(2);
  db_opt->avoid_flush_during_recovery = rnd->Uniform(2);
  db_opt->avoid_flush_during_shutdown = rnd->Uniform(2);

  // int options
  db_opt->max_background_compactions = rnd->Uniform(100);
  db_opt->max_background_flushes = rnd->Uniform(100);
  db_opt->max_file_opening_threads = rnd->Uniform(100);
  db_opt->max_open_files = rnd->Uniform(100);
  db_opt->table_cache_numshardbits = rnd->Uniform(100);

  // size_t options
  db_opt->db_write_buffer_size = rnd->Uniform(10000);
  db_opt->keep_log_file_num = rnd->Uniform(10000);
  db_opt->log_file_time_to_roll = rnd->Uniform(10000);
  db_opt->manifest_preallocation_size = rnd->Uniform(10000);
  db_opt->max_log_file_size = rnd->Uniform(10000);

  // std::string options
  db_opt->db_log_dir = "path/to/db_log_dir";
  db_opt->wal_dir = "path/to/wal_dir";

  // uint32_t options
  db_opt->max_subcompactions = rnd->Uniform(100000);

  // uint64_t options
  static const uint64_t uint_max = static_cast<uint64_t>(UINT_MAX);
  db_opt->WAL_size_limit_MB = uint_max + rnd->Uniform(100000);
  db_opt->WAL_ttl_seconds = uint_max + rnd->Uniform(100000);
  db_opt->bytes_per_sync = uint_max + rnd->Uniform(100000);
  db_opt->delayed_write_rate = uint_max + rnd->Uniform(100000);
  db_opt->delete_obsolete_files_period_micros = uint_max + rnd->Uniform(100000);
  db_opt->max_manifest_file_size = uint_max + rnd->Uniform(100000);
  db_opt->max_total_wal_size = uint_max + rnd->Uniform(100000);
  db_opt->wal_bytes_per_sync = uint_max + rnd->Uniform(100000);

  // unsigned int options
  db_opt->stats_dump_period_sec = rnd->Uniform(100000);
}

void RandomInitCFOptions(ColumnFamilyOptions* cf_opt, Random* rnd) {
  cf_opt->compaction_style = (CompactionStyle)(rnd->Uniform(4));

  // boolean options
  cf_opt->report_bg_io_stats = rnd->Uniform(2);
  cf_opt->disable_auto_compactions = rnd->Uniform(2);
  cf_opt->inplace_update_support = rnd->Uniform(2);
  cf_opt->level_compaction_dynamic_level_bytes = rnd->Uniform(2);
  cf_opt->optimize_filters_for_hits = rnd->Uniform(2);
  cf_opt->paranoid_file_checks = rnd->Uniform(2);
  cf_opt->purge_redundant_kvs_while_flush = rnd->Uniform(2);
  cf_opt->force_consistency_checks = rnd->Uniform(2);
  cf_opt->compaction_options_fifo.allow_compaction = rnd->Uniform(2);
  cf_opt->memtable_whole_key_filtering = rnd->Uniform(2);

  // double options
  cf_opt->hard_rate_limit = static_cast<double>(rnd->Uniform(10000)) / 13;
  cf_opt->soft_rate_limit = static_cast<double>(rnd->Uniform(10000)) / 13;
  cf_opt->memtable_prefix_bloom_size_ratio =
      static_cast<double>(rnd->Uniform(10000)) / 20000.0;

  // int options
  cf_opt->level0_file_num_compaction_trigger = rnd->Uniform(100);
  cf_opt->level0_slowdown_writes_trigger = rnd->Uniform(100);
  cf_opt->level0_stop_writes_trigger = rnd->Uniform(100);
  cf_opt->max_bytes_for_level_multiplier = rnd->Uniform(100);
  cf_opt->max_mem_compaction_level = rnd->Uniform(100);
  cf_opt->max_write_buffer_number = rnd->Uniform(100);
  cf_opt->max_write_buffer_number_to_maintain = rnd->Uniform(100);
  cf_opt->min_write_buffer_number_to_merge = rnd->Uniform(100);
  cf_opt->num_levels = rnd->Uniform(100);
  cf_opt->target_file_size_multiplier = rnd->Uniform(100);

  // vector int options
  cf_opt->max_bytes_for_level_multiplier_additional.resize(cf_opt->num_levels);
  for (int i = 0; i < cf_opt->num_levels; i++) {
    cf_opt->max_bytes_for_level_multiplier_additional[i] = rnd->Uniform(100);
  }

  // size_t options
  cf_opt->arena_block_size = rnd->Uniform(10000);
  cf_opt->inplace_update_num_locks = rnd->Uniform(10000);
  cf_opt->max_successive_merges = rnd->Uniform(10000);
  cf_opt->memtable_huge_page_size = rnd->Uniform(10000);
  cf_opt->write_buffer_size = rnd->Uniform(10000);

  // uint32_t options
  cf_opt->bloom_locality = rnd->Uniform(10000);
  cf_opt->max_bytes_for_level_base = rnd->Uniform(10000);

  // uint64_t options
  static const uint64_t uint_max = static_cast<uint64_t>(UINT_MAX);
  cf_opt->ttl = uint_max + rnd->Uniform(10000);
  cf_opt->max_sequential_skip_in_iterations = uint_max + rnd->Uniform(10000);
  cf_opt->target_file_size_base = uint_max + rnd->Uniform(10000);
  cf_opt->max_compaction_bytes =
      cf_opt->target_file_size_base * rnd->Uniform(100);
  cf_opt->compaction_options_fifo.max_table_files_size =
      uint_max + rnd->Uniform(10000);

  // unsigned int options
  cf_opt->rate_limit_delay_max_milliseconds = rnd->Uniform(10000);

  // pointer typed options
  cf_opt->prefix_extractor.reset(RandomSliceTransform(rnd));
  cf_opt->table_factory.reset(RandomTableFactory(rnd));
  cf_opt->merge_operator.reset(RandomMergeOperator(rnd));
  if (cf_opt->compaction_filter) {
    delete cf_opt->compaction_filter;
  }
  cf_opt->compaction_filter = RandomCompactionFilter(rnd);
  cf_opt->compaction_filter_factory.reset(RandomCompactionFilterFactory(rnd));

  // custom typed options
  cf_opt->compression = RandomCompressionType(rnd);
  RandomCompressionTypeVector(cf_opt->num_levels,
                              &cf_opt->compression_per_level, rnd);
}

Status DestroyDir(Env* env, const std::string& dir) {
  Status s;
  if (env->FileExists(dir).IsNotFound()) {
    return s;
  }
  std::vector<std::string> files_in_dir;
  s = env->GetChildren(dir, &files_in_dir);
  if (s.ok()) {
    for (auto& file_in_dir : files_in_dir) {
      if (file_in_dir == "." || file_in_dir == "..") {
        continue;
      }
      s = env->DeleteFile(dir + "/" + file_in_dir);
      if (!s.ok()) {
        break;
      }
    }
  }

  if (s.ok()) {
    s = env->DeleteDir(dir);
  }
  return s;
}

bool IsDirectIOSupported(Env* env, const std::string& dir) {
  EnvOptions env_options;
  env_options.use_mmap_writes = false;
  env_options.use_direct_writes = true;
  std::string tmp = TempFileName(dir, 999);
  Status s;
  {
    std::unique_ptr<WritableFile> file;
    s = env->NewWritableFile(tmp, &file, env_options);
  }
  if (s.ok()) {
    s = env->DeleteFile(tmp);
  }
  return s.ok();
}

}  // namespace test
}  // namespace rocksdb
