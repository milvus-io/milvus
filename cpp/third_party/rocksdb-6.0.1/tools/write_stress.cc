// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//
// The goal of this tool is to be a simple stress test with focus on catching:
// * bugs in compaction/flush processes, especially the ones that cause
// assertion errors
// * bugs in the code that deletes obsolete files
//
// There are two parts of the test:
// * write_stress, a binary that writes to the database
// * write_stress_runner.py, a script that invokes and kills write_stress
//
// Here are some interesting parts of write_stress:
// * Runs with very high concurrency of compactions and flushes (32 threads
// total) and tries to create a huge amount of small files
// * The keys written to the database are not uniformly distributed -- there is
// a 3-character prefix that mutates occasionally (in prefix mutator thread), in
// such a way that the first character mutates slower than second, which mutates
// slower than third character. That way, the compaction stress tests some
// interesting compaction features like trivial moves and bottommost level
// calculation
// * There is a thread that creates an iterator, holds it for couple of seconds
// and then iterates over all keys. This is supposed to test RocksDB's abilities
// to keep the files alive when there are references to them.
// * Some writes trigger WAL sync. This is stress testing our WAL sync code.
// * At the end of the run, we make sure that we didn't leak any of the sst
// files
//
// write_stress_runner.py changes the mode in which we run write_stress and also
// kills and restarts it. There are some interesting characteristics:
// * At the beginning we divide the full test runtime into smaller parts --
// shorter runtimes (couple of seconds) and longer runtimes (100, 1000) seconds
// * The first time we run write_stress, we destroy the old DB. Every next time
// during the test, we use the same DB.
// * We can run in kill mode or clean-restart mode. Kill mode kills the
// write_stress violently.
// * We can run in mode where delete_obsolete_files_with_fullscan is true or
// false
// * We can run with low_open_files mode turned on or off. When it's turned on,
// we configure table cache to only hold a couple of files -- that way we need
// to reopen files every time we access them.
//
// Another goal was to create a stress test without a lot of parameters. So
// tools/write_stress_runner.py should only take one parameter -- runtime_sec
// and it should figure out everything else on its own.

#include <cstdio>

#ifndef GFLAGS
int main() {
  fprintf(stderr, "Please install gflags to run rocksdb tools\n");
  return 1;
}
#else

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif // __STDC_FORMAT_MACROS

#include <inttypes.h>
#include <atomic>
#include <random>
#include <set>
#include <string>
#include <thread>

#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "util/filename.h"
#include "util/gflags_compat.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::RegisterFlagValidator;
using GFLAGS_NAMESPACE::SetUsageMessage;

DEFINE_int32(key_size, 10, "Key size");
DEFINE_int32(value_size, 100, "Value size");
DEFINE_string(db, "", "Use the db with the following name.");
DEFINE_bool(destroy_db, true,
            "Destroy the existing DB before running the test");

DEFINE_int32(runtime_sec, 10 * 60, "How long are we running for, in seconds");
DEFINE_int32(seed, 139, "Random seed");

DEFINE_double(prefix_mutate_period_sec, 1.0,
              "How often are we going to mutate the prefix");
DEFINE_double(first_char_mutate_probability, 0.1,
              "How likely are we to mutate the first char every period");
DEFINE_double(second_char_mutate_probability, 0.2,
              "How likely are we to mutate the second char every period");
DEFINE_double(third_char_mutate_probability, 0.5,
              "How likely are we to mutate the third char every period");

DEFINE_int32(iterator_hold_sec, 5,
             "How long will the iterator hold files before it gets destroyed");

DEFINE_double(sync_probability, 0.01, "How often are we syncing writes");
DEFINE_bool(delete_obsolete_files_with_fullscan, false,
            "If true, we delete obsolete files after each compaction/flush "
            "using GetChildren() API");
DEFINE_bool(low_open_files_mode, false,
            "If true, we set max_open_files to 20, so that every file access "
            "needs to reopen it");

namespace rocksdb {

static const int kPrefixSize = 3;

class WriteStress {
 public:
  WriteStress() : stop_(false) {
    // initialize key_prefix
    for (int i = 0; i < kPrefixSize; ++i) {
      key_prefix_[i].store('a');
    }

    // Choose a location for the test database if none given with --db=<path>
    if (FLAGS_db.empty()) {
      std::string default_db_path;
      Env::Default()->GetTestDirectory(&default_db_path);
      default_db_path += "/write_stress";
      FLAGS_db = default_db_path;
    }

    Options options;
    if (FLAGS_destroy_db) {
      DestroyDB(FLAGS_db, options);  // ignore
    }

    // make the LSM tree deep, so that we have many concurrent flushes and
    // compactions
    options.create_if_missing = true;
    options.write_buffer_size = 256 * 1024;              // 256k
    options.max_bytes_for_level_base = 1 * 1024 * 1024;  // 1MB
    options.target_file_size_base = 100 * 1024;          // 100k
    options.max_write_buffer_number = 16;
    options.max_background_compactions = 16;
    options.max_background_flushes = 16;
    options.max_open_files = FLAGS_low_open_files_mode ? 20 : -1;
    if (FLAGS_delete_obsolete_files_with_fullscan) {
      options.delete_obsolete_files_period_micros = 0;
    }

    // open DB
    DB* db;
    Status s = DB::Open(options, FLAGS_db, &db);
    if (!s.ok()) {
      fprintf(stderr, "Can't open database: %s\n", s.ToString().c_str());
      std::abort();
    }
    db_.reset(db);
  }

  void WriteThread() {
    std::mt19937 rng(static_cast<unsigned int>(FLAGS_seed));
    std::uniform_real_distribution<double> dist(0, 1);

    auto random_string = [](std::mt19937& r, int len) {
      std::uniform_int_distribution<int> char_dist('a', 'z');
      std::string ret;
      for (int i = 0; i < len; ++i) {
        ret += static_cast<char>(char_dist(r));
      }
      return ret;
    };

    while (!stop_.load(std::memory_order_relaxed)) {
      std::string prefix;
      prefix.resize(kPrefixSize);
      for (int i = 0; i < kPrefixSize; ++i) {
        prefix[i] = key_prefix_[i].load(std::memory_order_relaxed);
      }
      auto key = prefix + random_string(rng, FLAGS_key_size - kPrefixSize);
      auto value = random_string(rng, FLAGS_value_size);
      WriteOptions woptions;
      woptions.sync = dist(rng) < FLAGS_sync_probability;
      auto s = db_->Put(woptions, key, value);
      if (!s.ok()) {
        fprintf(stderr, "Write to DB failed: %s\n", s.ToString().c_str());
        std::abort();
      }
    }
  }

  void IteratorHoldThread() {
    while (!stop_.load(std::memory_order_relaxed)) {
      std::unique_ptr<Iterator> iterator(db_->NewIterator(ReadOptions()));
      Env::Default()->SleepForMicroseconds(FLAGS_iterator_hold_sec * 1000 *
                                           1000LL);
      for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
      }
      if (!iterator->status().ok()) {
        fprintf(stderr, "Iterator statuts not OK: %s\n",
                iterator->status().ToString().c_str());
        std::abort();
      }
    }
  }

  void PrefixMutatorThread() {
    std::mt19937 rng(static_cast<unsigned int>(FLAGS_seed));
    std::uniform_real_distribution<double> dist(0, 1);
    std::uniform_int_distribution<int> char_dist('a', 'z');
    while (!stop_.load(std::memory_order_relaxed)) {
      Env::Default()->SleepForMicroseconds(static_cast<int>(
                                           FLAGS_prefix_mutate_period_sec *
                                           1000 * 1000LL));
      if (dist(rng) < FLAGS_first_char_mutate_probability) {
        key_prefix_[0].store(static_cast<char>(char_dist(rng)), std::memory_order_relaxed);
      }
      if (dist(rng) < FLAGS_second_char_mutate_probability) {
        key_prefix_[1].store(static_cast<char>(char_dist(rng)), std::memory_order_relaxed);
      }
      if (dist(rng) < FLAGS_third_char_mutate_probability) {
        key_prefix_[2].store(static_cast<char>(char_dist(rng)), std::memory_order_relaxed);
      }
    }
  }

  int Run() {
    threads_.emplace_back([&]() { WriteThread(); });
    threads_.emplace_back([&]() { PrefixMutatorThread(); });
    threads_.emplace_back([&]() { IteratorHoldThread(); });

    if (FLAGS_runtime_sec == -1) {
      // infinite runtime, until we get killed
      while (true) {
        Env::Default()->SleepForMicroseconds(1000 * 1000);
      }
    }

    Env::Default()->SleepForMicroseconds(FLAGS_runtime_sec * 1000 * 1000);

    stop_.store(true, std::memory_order_relaxed);
    for (auto& t : threads_) {
      t.join();
    }
    threads_.clear();

// Skip checking for leaked files in ROCKSDB_LITE since we don't have access to
// function GetLiveFilesMetaData
#ifndef ROCKSDB_LITE
    // let's see if we leaked some files
    db_->PauseBackgroundWork();
    std::vector<LiveFileMetaData> metadata;
    db_->GetLiveFilesMetaData(&metadata);
    std::set<uint64_t> sst_file_numbers;
    for (const auto& file : metadata) {
      uint64_t number;
      FileType type;
      if (!ParseFileName(file.name, &number, "LOG", &type)) {
        continue;
      }
      if (type == kTableFile) {
        sst_file_numbers.insert(number);
      }
    }

    std::vector<std::string> children;
    Env::Default()->GetChildren(FLAGS_db, &children);
    for (const auto& child : children) {
      uint64_t number;
      FileType type;
      if (!ParseFileName(child, &number, "LOG", &type)) {
        continue;
      }
      if (type == kTableFile) {
        if (sst_file_numbers.find(number) == sst_file_numbers.end()) {
          fprintf(stderr,
                  "Found a table file in DB path that should have been "
                  "deleted: %s\n",
                  child.c_str());
          std::abort();
        }
      }
    }
    db_->ContinueBackgroundWork();
#endif  // !ROCKSDB_LITE

    return 0;
  }

 private:
  // each key is prepended with this prefix. we occasionally change it. third
  // letter is changed more frequently than second, which is changed more
  // frequently than the first one.
  std::atomic<char> key_prefix_[kPrefixSize];
  std::atomic<bool> stop_;
  std::vector<port::Thread> threads_;
  std::unique_ptr<DB> db_;
};

}  // namespace rocksdb

int main(int argc, char** argv) {
  SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                  " [OPTIONS]...");
  ParseCommandLineFlags(&argc, &argv, true);
  rocksdb::WriteStress write_stress;
  return write_stress.Run();
}

#endif  // GFLAGS
