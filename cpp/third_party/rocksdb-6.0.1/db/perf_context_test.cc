//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include <algorithm>
#include <iostream>
#include <thread>
#include <vector>

#include "monitoring/histogram.h"
#include "monitoring/instrumented_mutex.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/slice_transform.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "utilities/merge_operators.h"

bool FLAGS_random_key = false;
bool FLAGS_use_set_based_memetable = false;
int FLAGS_total_keys = 100;
int FLAGS_write_buffer_size = 1000000000;
int FLAGS_max_write_buffer_number = 8;
int FLAGS_min_write_buffer_number_to_merge = 7;
bool FLAGS_verbose = false;

// Path to the database on file system
const std::string kDbName = rocksdb::test::PerThreadDBPath("perf_context_test");

namespace rocksdb {

std::shared_ptr<DB> OpenDb(bool read_only = false) {
    DB* db;
    Options options;
    options.create_if_missing = true;
    options.max_open_files = -1;
    options.write_buffer_size = FLAGS_write_buffer_size;
    options.max_write_buffer_number = FLAGS_max_write_buffer_number;
    options.min_write_buffer_number_to_merge =
      FLAGS_min_write_buffer_number_to_merge;

    if (FLAGS_use_set_based_memetable) {
#ifndef ROCKSDB_LITE
      options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(0));
      options.memtable_factory.reset(NewHashSkipListRepFactory());
#endif  // ROCKSDB_LITE
    }

    Status s;
    if (!read_only) {
      s = DB::Open(options, kDbName, &db);
    } else {
      s = DB::OpenForReadOnly(options, kDbName, &db);
    }
    EXPECT_OK(s);
    return std::shared_ptr<DB>(db);
}

class PerfContextTest : public testing::Test {};

TEST_F(PerfContextTest, SeekIntoDeletion) {
  DestroyDB(kDbName, Options());
  auto db = OpenDb();
  WriteOptions write_options;
  ReadOptions read_options;

  for (int i = 0; i < FLAGS_total_keys; ++i) {
    std::string key = "k" + ToString(i);
    std::string value = "v" + ToString(i);

    db->Put(write_options, key, value);
  }

  for (int i = 0; i < FLAGS_total_keys -1 ; ++i) {
    std::string key = "k" + ToString(i);
    db->Delete(write_options, key);
  }

  HistogramImpl hist_get;
  HistogramImpl hist_get_time;
  for (int i = 0; i < FLAGS_total_keys - 1; ++i) {
    std::string key = "k" + ToString(i);
    std::string value;

    get_perf_context()->Reset();
    StopWatchNano timer(Env::Default());
    timer.Start();
    auto status = db->Get(read_options, key, &value);
    auto elapsed_nanos = timer.ElapsedNanos();
    ASSERT_TRUE(status.IsNotFound());
    hist_get.Add(get_perf_context()->user_key_comparison_count);
    hist_get_time.Add(elapsed_nanos);
  }

  if (FLAGS_verbose) {
    std::cout << "Get user key comparison: \n" << hist_get.ToString()
              << "Get time: \n" << hist_get_time.ToString();
  }

  {
    HistogramImpl hist_seek_to_first;
    std::unique_ptr<Iterator> iter(db->NewIterator(read_options));

    get_perf_context()->Reset();
    StopWatchNano timer(Env::Default(), true);
    iter->SeekToFirst();
    hist_seek_to_first.Add(get_perf_context()->user_key_comparison_count);
    auto elapsed_nanos = timer.ElapsedNanos();

    if (FLAGS_verbose) {
      std::cout << "SeekToFirst uesr key comparison: \n"
                << hist_seek_to_first.ToString()
                << "ikey skipped: " << get_perf_context()->internal_key_skipped_count
                << "\n"
                << "idelete skipped: "
                << get_perf_context()->internal_delete_skipped_count << "\n"
                << "elapsed: " << elapsed_nanos << "\n";
    }
  }

  HistogramImpl hist_seek;
  for (int i = 0; i < FLAGS_total_keys; ++i) {
    std::unique_ptr<Iterator> iter(db->NewIterator(read_options));
    std::string key = "k" + ToString(i);

    get_perf_context()->Reset();
    StopWatchNano timer(Env::Default(), true);
    iter->Seek(key);
    auto elapsed_nanos = timer.ElapsedNanos();
    hist_seek.Add(get_perf_context()->user_key_comparison_count);
    if (FLAGS_verbose) {
      std::cout << "seek cmp: " << get_perf_context()->user_key_comparison_count
                << " ikey skipped " << get_perf_context()->internal_key_skipped_count
                << " idelete skipped "
                << get_perf_context()->internal_delete_skipped_count
                << " elapsed: " << elapsed_nanos << "ns\n";
    }

    get_perf_context()->Reset();
    ASSERT_TRUE(iter->Valid());
    StopWatchNano timer2(Env::Default(), true);
    iter->Next();
    auto elapsed_nanos2 = timer2.ElapsedNanos();
    if (FLAGS_verbose) {
      std::cout << "next cmp: " << get_perf_context()->user_key_comparison_count
                << "elapsed: " << elapsed_nanos2 << "ns\n";
    }
  }

  if (FLAGS_verbose) {
    std::cout << "Seek uesr key comparison: \n" << hist_seek.ToString();
  }
}

TEST_F(PerfContextTest, StopWatchNanoOverhead) {
  // profile the timer cost by itself!
  const int kTotalIterations = 1000000;
  std::vector<uint64_t> timings(kTotalIterations);

  StopWatchNano timer(Env::Default(), true);
  for (auto& timing : timings) {
    timing = timer.ElapsedNanos(true /* reset */);
  }

  HistogramImpl histogram;
  for (const auto timing : timings) {
    histogram.Add(timing);
  }

  if (FLAGS_verbose) {
    std::cout << histogram.ToString();
  }
}

TEST_F(PerfContextTest, StopWatchOverhead) {
  // profile the timer cost by itself!
  const int kTotalIterations = 1000000;
  uint64_t elapsed = 0;
  std::vector<uint64_t> timings(kTotalIterations);

  StopWatch timer(Env::Default(), nullptr, 0, &elapsed);
  for (auto& timing : timings) {
    timing = elapsed;
  }

  HistogramImpl histogram;
  uint64_t prev_timing = 0;
  for (const auto timing : timings) {
    histogram.Add(timing - prev_timing);
    prev_timing = timing;
  }

  if (FLAGS_verbose) {
    std::cout << histogram.ToString();
  }
}

void ProfileQueries(bool enabled_time = false) {
  DestroyDB(kDbName, Options());    // Start this test with a fresh DB

  auto db = OpenDb();

  WriteOptions write_options;
  ReadOptions read_options;

  HistogramImpl hist_put;

  HistogramImpl hist_get;
  HistogramImpl hist_get_snapshot;
  HistogramImpl hist_get_memtable;
  HistogramImpl hist_get_files;
  HistogramImpl hist_get_post_process;
  HistogramImpl hist_num_memtable_checked;

  HistogramImpl hist_mget;
  HistogramImpl hist_mget_snapshot;
  HistogramImpl hist_mget_memtable;
  HistogramImpl hist_mget_files;
  HistogramImpl hist_mget_post_process;
  HistogramImpl hist_mget_num_memtable_checked;

  HistogramImpl hist_write_pre_post;
  HistogramImpl hist_write_wal_time;
  HistogramImpl hist_write_memtable_time;
  HistogramImpl hist_write_delay_time;
  HistogramImpl hist_write_thread_wait_nanos;
  HistogramImpl hist_write_scheduling_time;

  uint64_t total_db_mutex_nanos = 0;

  if (FLAGS_verbose) {
    std::cout << "Inserting " << FLAGS_total_keys << " key/value pairs\n...\n";
  }

  std::vector<int> keys;
  const int kFlushFlag = -1;
  for (int i = 0; i < FLAGS_total_keys; ++i) {
    keys.push_back(i);
    if (i == FLAGS_total_keys / 2) {
      // Issuing a flush in the middle.
      keys.push_back(kFlushFlag);
    }
  }

  if (FLAGS_random_key) {
    std::random_shuffle(keys.begin(), keys.end());
  }
#ifndef NDEBUG
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT, 1U);
#endif
  int num_mutex_waited = 0;
  for (const int i : keys) {
    if (i == kFlushFlag) {
      FlushOptions fo;
      db->Flush(fo);
      continue;
    }

    std::string key = "k" + ToString(i);
    std::string value = "v" + ToString(i);

    std::vector<std::string> values;

    get_perf_context()->Reset();
    db->Put(write_options, key, value);
    if (++num_mutex_waited > 3) {
#ifndef NDEBUG
      ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT, 0U);
#endif
    }
    hist_write_pre_post.Add(
        get_perf_context()->write_pre_and_post_process_time);
    hist_write_wal_time.Add(get_perf_context()->write_wal_time);
    hist_write_memtable_time.Add(get_perf_context()->write_memtable_time);
    hist_write_delay_time.Add(get_perf_context()->write_delay_time);
    hist_write_thread_wait_nanos.Add(
        get_perf_context()->write_thread_wait_nanos);
    hist_write_scheduling_time.Add(
        get_perf_context()->write_scheduling_flushes_compactions_time);
    hist_put.Add(get_perf_context()->user_key_comparison_count);
    total_db_mutex_nanos += get_perf_context()->db_mutex_lock_nanos;
  }
#ifndef NDEBUG
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT, 0U);
#endif

  for (const int i : keys) {
    if (i == kFlushFlag) {
      continue;
    }
    std::string key = "k" + ToString(i);
    std::string expected_value = "v" + ToString(i);
    std::string value;

    std::vector<Slice> multiget_keys = {Slice(key)};
    std::vector<std::string> values;

    get_perf_context()->Reset();
    ASSERT_OK(db->Get(read_options, key, &value));
    ASSERT_EQ(expected_value, value);
    hist_get_snapshot.Add(get_perf_context()->get_snapshot_time);
    hist_get_memtable.Add(get_perf_context()->get_from_memtable_time);
    hist_get_files.Add(get_perf_context()->get_from_output_files_time);
    hist_num_memtable_checked.Add(get_perf_context()->get_from_memtable_count);
    hist_get_post_process.Add(get_perf_context()->get_post_process_time);
    hist_get.Add(get_perf_context()->user_key_comparison_count);

    get_perf_context()->Reset();
    db->MultiGet(read_options, multiget_keys, &values);
    hist_mget_snapshot.Add(get_perf_context()->get_snapshot_time);
    hist_mget_memtable.Add(get_perf_context()->get_from_memtable_time);
    hist_mget_files.Add(get_perf_context()->get_from_output_files_time);
    hist_mget_num_memtable_checked.Add(get_perf_context()->get_from_memtable_count);
    hist_mget_post_process.Add(get_perf_context()->get_post_process_time);
    hist_mget.Add(get_perf_context()->user_key_comparison_count);
  }

  if (FLAGS_verbose) {
    std::cout << "Put uesr key comparison: \n" << hist_put.ToString()
              << "Get uesr key comparison: \n" << hist_get.ToString()
              << "MultiGet uesr key comparison: \n" << hist_get.ToString();
    std::cout << "Put(): Pre and Post Process Time: \n"
              << hist_write_pre_post.ToString() << " Writing WAL time: \n"
              << hist_write_wal_time.ToString() << "\n"
              << " Writing Mem Table time: \n"
              << hist_write_memtable_time.ToString() << "\n"
              << " Write Delay: \n" << hist_write_delay_time.ToString() << "\n"
              << " Waiting for Batch time: \n"
              << hist_write_thread_wait_nanos.ToString() << "\n"
              << " Scheduling Flushes and Compactions Time: \n"
              << hist_write_scheduling_time.ToString() << "\n"
              << " Total DB mutex nanos: \n" << total_db_mutex_nanos << "\n";

    std::cout << "Get(): Time to get snapshot: \n"
              << hist_get_snapshot.ToString()
              << " Time to get value from memtables: \n"
              << hist_get_memtable.ToString() << "\n"
              << " Time to get value from output files: \n"
              << hist_get_files.ToString() << "\n"
              << " Number of memtables checked: \n"
              << hist_num_memtable_checked.ToString() << "\n"
              << " Time to post process: \n" << hist_get_post_process.ToString()
              << "\n";

    std::cout << "MultiGet(): Time to get snapshot: \n"
              << hist_mget_snapshot.ToString()
              << " Time to get value from memtables: \n"
              << hist_mget_memtable.ToString() << "\n"
              << " Time to get value from output files: \n"
              << hist_mget_files.ToString() << "\n"
              << " Number of memtables checked: \n"
              << hist_mget_num_memtable_checked.ToString() << "\n"
              << " Time to post process: \n"
              << hist_mget_post_process.ToString() << "\n";
  }

  if (enabled_time) {
    ASSERT_GT(hist_get.Average(), 0);
    ASSERT_GT(hist_get_snapshot.Average(), 0);
    ASSERT_GT(hist_get_memtable.Average(), 0);
    ASSERT_GT(hist_get_files.Average(), 0);
    ASSERT_GT(hist_get_post_process.Average(), 0);
    ASSERT_GT(hist_num_memtable_checked.Average(), 0);

    ASSERT_GT(hist_mget.Average(), 0);
    ASSERT_GT(hist_mget_snapshot.Average(), 0);
    ASSERT_GT(hist_mget_memtable.Average(), 0);
    ASSERT_GT(hist_mget_files.Average(), 0);
    ASSERT_GT(hist_mget_post_process.Average(), 0);
    ASSERT_GT(hist_mget_num_memtable_checked.Average(), 0);

    EXPECT_GT(hist_write_pre_post.Average(), 0);
    EXPECT_GT(hist_write_wal_time.Average(), 0);
    EXPECT_GT(hist_write_memtable_time.Average(), 0);
    EXPECT_EQ(hist_write_delay_time.Average(), 0);
    EXPECT_EQ(hist_write_thread_wait_nanos.Average(), 0);
    EXPECT_GT(hist_write_scheduling_time.Average(), 0);

#ifndef NDEBUG
    ASSERT_GT(total_db_mutex_nanos, 2000U);
#endif
  }

  db.reset();
  db = OpenDb(true);

  hist_get.Clear();
  hist_get_snapshot.Clear();
  hist_get_memtable.Clear();
  hist_get_files.Clear();
  hist_get_post_process.Clear();
  hist_num_memtable_checked.Clear();

  hist_mget.Clear();
  hist_mget_snapshot.Clear();
  hist_mget_memtable.Clear();
  hist_mget_files.Clear();
  hist_mget_post_process.Clear();
  hist_mget_num_memtable_checked.Clear();

  for (const int i : keys) {
    if (i == kFlushFlag) {
      continue;
    }
    std::string key = "k" + ToString(i);
    std::string expected_value = "v" + ToString(i);
    std::string value;

    std::vector<Slice> multiget_keys = {Slice(key)};
    std::vector<std::string> values;

    get_perf_context()->Reset();
    ASSERT_OK(db->Get(read_options, key, &value));
    ASSERT_EQ(expected_value, value);
    hist_get_snapshot.Add(get_perf_context()->get_snapshot_time);
    hist_get_memtable.Add(get_perf_context()->get_from_memtable_time);
    hist_get_files.Add(get_perf_context()->get_from_output_files_time);
    hist_num_memtable_checked.Add(get_perf_context()->get_from_memtable_count);
    hist_get_post_process.Add(get_perf_context()->get_post_process_time);
    hist_get.Add(get_perf_context()->user_key_comparison_count);

    get_perf_context()->Reset();
    db->MultiGet(read_options, multiget_keys, &values);
    hist_mget_snapshot.Add(get_perf_context()->get_snapshot_time);
    hist_mget_memtable.Add(get_perf_context()->get_from_memtable_time);
    hist_mget_files.Add(get_perf_context()->get_from_output_files_time);
    hist_mget_num_memtable_checked.Add(get_perf_context()->get_from_memtable_count);
    hist_mget_post_process.Add(get_perf_context()->get_post_process_time);
    hist_mget.Add(get_perf_context()->user_key_comparison_count);
  }

  if (FLAGS_verbose) {
    std::cout << "ReadOnly Get uesr key comparison: \n" << hist_get.ToString()
              << "ReadOnly MultiGet uesr key comparison: \n"
              << hist_mget.ToString();

    std::cout << "ReadOnly Get(): Time to get snapshot: \n"
              << hist_get_snapshot.ToString()
              << " Time to get value from memtables: \n"
              << hist_get_memtable.ToString() << "\n"
              << " Time to get value from output files: \n"
              << hist_get_files.ToString() << "\n"
              << " Number of memtables checked: \n"
              << hist_num_memtable_checked.ToString() << "\n"
              << " Time to post process: \n" << hist_get_post_process.ToString()
              << "\n";

    std::cout << "ReadOnly MultiGet(): Time to get snapshot: \n"
              << hist_mget_snapshot.ToString()
              << " Time to get value from memtables: \n"
              << hist_mget_memtable.ToString() << "\n"
              << " Time to get value from output files: \n"
              << hist_mget_files.ToString() << "\n"
              << " Number of memtables checked: \n"
              << hist_mget_num_memtable_checked.ToString() << "\n"
              << " Time to post process: \n"
              << hist_mget_post_process.ToString() << "\n";
  }

  if (enabled_time) {
    ASSERT_GT(hist_get.Average(), 0);
    ASSERT_GT(hist_get_memtable.Average(), 0);
    ASSERT_GT(hist_get_files.Average(), 0);
    ASSERT_GT(hist_num_memtable_checked.Average(), 0);
    // In read-only mode Get(), no super version operation is needed
    ASSERT_EQ(hist_get_post_process.Average(), 0);
    ASSERT_GT(hist_get_snapshot.Average(), 0);

    ASSERT_GT(hist_mget.Average(), 0);
    ASSERT_GT(hist_mget_snapshot.Average(), 0);
    ASSERT_GT(hist_mget_memtable.Average(), 0);
    ASSERT_GT(hist_mget_files.Average(), 0);
    ASSERT_GT(hist_mget_post_process.Average(), 0);
    ASSERT_GT(hist_mget_num_memtable_checked.Average(), 0);
  }
}

#ifndef ROCKSDB_LITE
TEST_F(PerfContextTest, KeyComparisonCount) {
  SetPerfLevel(kEnableCount);
  ProfileQueries();

  SetPerfLevel(kDisable);
  ProfileQueries();

  SetPerfLevel(kEnableTime);
  ProfileQueries(true);
}
#endif  // ROCKSDB_LITE

// make perf_context_test
// export ROCKSDB_TESTS=PerfContextTest.SeekKeyComparison
// For one memtable:
// ./perf_context_test --write_buffer_size=500000 --total_keys=10000
// For two memtables:
// ./perf_context_test --write_buffer_size=250000 --total_keys=10000
// Specify --random_key=1 to shuffle the key before insertion
// Results show that, for sequential insertion, worst-case Seek Key comparison
// is close to the total number of keys (linear), when there is only one
// memtable. When there are two memtables, even the avg Seek Key comparison
// starts to become linear to the input size.

TEST_F(PerfContextTest, SeekKeyComparison) {
  DestroyDB(kDbName, Options());
  auto db = OpenDb();
  WriteOptions write_options;
  ReadOptions read_options;

  if (FLAGS_verbose) {
    std::cout << "Inserting " << FLAGS_total_keys << " key/value pairs\n...\n";
  }

  std::vector<int> keys;
  for (int i = 0; i < FLAGS_total_keys; ++i) {
    keys.push_back(i);
  }

  if (FLAGS_random_key) {
    std::random_shuffle(keys.begin(), keys.end());
  }

  HistogramImpl hist_put_time;
  HistogramImpl hist_wal_time;
  HistogramImpl hist_time_diff;

  SetPerfLevel(kEnableTime);
  StopWatchNano timer(Env::Default());
  for (const int i : keys) {
    std::string key = "k" + ToString(i);
    std::string value = "v" + ToString(i);

    get_perf_context()->Reset();
    timer.Start();
    db->Put(write_options, key, value);
    auto put_time = timer.ElapsedNanos();
    hist_put_time.Add(put_time);
    hist_wal_time.Add(get_perf_context()->write_wal_time);
    hist_time_diff.Add(put_time - get_perf_context()->write_wal_time);
  }

  if (FLAGS_verbose) {
    std::cout << "Put time:\n" << hist_put_time.ToString() << "WAL time:\n"
              << hist_wal_time.ToString() << "time diff:\n"
              << hist_time_diff.ToString();
  }

  HistogramImpl hist_seek;
  HistogramImpl hist_next;

  for (int i = 0; i < FLAGS_total_keys; ++i) {
    std::string key = "k" + ToString(i);
    std::string value = "v" + ToString(i);

    std::unique_ptr<Iterator> iter(db->NewIterator(read_options));
    get_perf_context()->Reset();
    iter->Seek(key);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->value().ToString(), value);
    hist_seek.Add(get_perf_context()->user_key_comparison_count);
  }

  std::unique_ptr<Iterator> iter(db->NewIterator(read_options));
  for (iter->SeekToFirst(); iter->Valid();) {
    get_perf_context()->Reset();
    iter->Next();
    hist_next.Add(get_perf_context()->user_key_comparison_count);
  }

  if (FLAGS_verbose) {
    std::cout << "Seek:\n" << hist_seek.ToString() << "Next:\n"
              << hist_next.ToString();
  }
}

TEST_F(PerfContextTest, DBMutexLockCounter) {
  int stats_code[] = {0, static_cast<int>(DB_MUTEX_WAIT_MICROS)};
  for (PerfLevel perf_level_test :
       {PerfLevel::kEnableTimeExceptForMutex, PerfLevel::kEnableTime}) {
    for (int c = 0; c < 2; ++c) {
    InstrumentedMutex mutex(nullptr, Env::Default(), stats_code[c]);
    mutex.Lock();
    rocksdb::port::Thread child_thread([&] {
      SetPerfLevel(perf_level_test);
      get_perf_context()->Reset();
      ASSERT_EQ(get_perf_context()->db_mutex_lock_nanos, 0);
      mutex.Lock();
      mutex.Unlock();
      if (perf_level_test == PerfLevel::kEnableTimeExceptForMutex ||
          stats_code[c] != DB_MUTEX_WAIT_MICROS) {
        ASSERT_EQ(get_perf_context()->db_mutex_lock_nanos, 0);
      } else {
        // increment the counter only when it's a DB Mutex
        ASSERT_GT(get_perf_context()->db_mutex_lock_nanos, 0);
      }
    });
    Env::Default()->SleepForMicroseconds(100);
    mutex.Unlock();
    child_thread.join();
  }
  }
}

TEST_F(PerfContextTest, FalseDBMutexWait) {
  SetPerfLevel(kEnableTime);
  int stats_code[] = {0, static_cast<int>(DB_MUTEX_WAIT_MICROS)};
  for (int c = 0; c < 2; ++c) {
    InstrumentedMutex mutex(nullptr, Env::Default(), stats_code[c]);
    InstrumentedCondVar lock(&mutex);
    get_perf_context()->Reset();
    mutex.Lock();
    lock.TimedWait(100);
    mutex.Unlock();
    if (stats_code[c] == static_cast<int>(DB_MUTEX_WAIT_MICROS)) {
      // increment the counter only when it's a DB Mutex
      ASSERT_GT(get_perf_context()->db_condition_wait_nanos, 0);
    } else {
      ASSERT_EQ(get_perf_context()->db_condition_wait_nanos, 0);
    }
  }
}

TEST_F(PerfContextTest, ToString) {
  get_perf_context()->Reset();
  get_perf_context()->block_read_count = 12345;

  std::string zero_included = get_perf_context()->ToString();
  ASSERT_NE(std::string::npos, zero_included.find("= 0"));
  ASSERT_NE(std::string::npos, zero_included.find("= 12345"));

  std::string zero_excluded = get_perf_context()->ToString(true);
  ASSERT_EQ(std::string::npos, zero_excluded.find("= 0"));
  ASSERT_NE(std::string::npos, zero_excluded.find("= 12345"));
}

TEST_F(PerfContextTest, MergeOperatorTime) {
  DestroyDB(kDbName, Options());
  DB* db;
  Options options;
  options.create_if_missing = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  Status s = DB::Open(options, kDbName, &db);
  EXPECT_OK(s);

  std::string val;
  ASSERT_OK(db->Merge(WriteOptions(), "k1", "val1"));
  ASSERT_OK(db->Merge(WriteOptions(), "k1", "val2"));
  ASSERT_OK(db->Merge(WriteOptions(), "k1", "val3"));
  ASSERT_OK(db->Merge(WriteOptions(), "k1", "val4"));

  SetPerfLevel(kEnableTime);
  get_perf_context()->Reset();
  ASSERT_OK(db->Get(ReadOptions(), "k1", &val));
#ifdef OS_SOLARIS
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(db->Get(ReadOptions(), "k1", &val));
  }
#endif
  EXPECT_GT(get_perf_context()->merge_operator_time_nanos, 0);

  ASSERT_OK(db->Flush(FlushOptions()));

  get_perf_context()->Reset();
  ASSERT_OK(db->Get(ReadOptions(), "k1", &val));
#ifdef OS_SOLARIS
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(db->Get(ReadOptions(), "k1", &val));
  }
#endif
  EXPECT_GT(get_perf_context()->merge_operator_time_nanos, 0);

  ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  get_perf_context()->Reset();
  ASSERT_OK(db->Get(ReadOptions(), "k1", &val));
#ifdef OS_SOLARIS
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(db->Get(ReadOptions(), "k1", &val));
  }
#endif
  EXPECT_GT(get_perf_context()->merge_operator_time_nanos, 0);

  delete db;
}

TEST_F(PerfContextTest, CopyAndMove) {
  // Assignment operator
  {
    get_perf_context()->Reset();
    get_perf_context()->EnablePerLevelPerfContext();
    PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_useful, 1, 5);
    ASSERT_EQ(
        1,
        (*(get_perf_context()->level_to_perf_context))[5].bloom_filter_useful);
    PerfContext perf_context_assign;
    perf_context_assign = *get_perf_context();
    ASSERT_EQ(
        1,
        (*(perf_context_assign.level_to_perf_context))[5].bloom_filter_useful);
    get_perf_context()->ClearPerLevelPerfContext();
    get_perf_context()->Reset();
    ASSERT_EQ(
        1,
        (*(perf_context_assign.level_to_perf_context))[5].bloom_filter_useful);
    perf_context_assign.ClearPerLevelPerfContext();
    perf_context_assign.Reset();
  }
  // Copy constructor
  {
    get_perf_context()->Reset();
    get_perf_context()->EnablePerLevelPerfContext();
    PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_useful, 1, 5);
    ASSERT_EQ(
        1,
        (*(get_perf_context()->level_to_perf_context))[5].bloom_filter_useful);
    PerfContext perf_context_copy(*get_perf_context());
    ASSERT_EQ(
        1, (*(perf_context_copy.level_to_perf_context))[5].bloom_filter_useful);
    get_perf_context()->ClearPerLevelPerfContext();
    get_perf_context()->Reset();
    ASSERT_EQ(
        1, (*(perf_context_copy.level_to_perf_context))[5].bloom_filter_useful);
    perf_context_copy.ClearPerLevelPerfContext();
    perf_context_copy.Reset();
  }
  // Move constructor
  {
    get_perf_context()->Reset();
    get_perf_context()->EnablePerLevelPerfContext();
    PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_useful, 1, 5);
    ASSERT_EQ(
        1,
        (*(get_perf_context()->level_to_perf_context))[5].bloom_filter_useful);
    PerfContext perf_context_move = std::move(*get_perf_context());
    ASSERT_EQ(
        1, (*(perf_context_move.level_to_perf_context))[5].bloom_filter_useful);
    get_perf_context()->ClearPerLevelPerfContext();
    get_perf_context()->Reset();
    ASSERT_EQ(
        1, (*(perf_context_move.level_to_perf_context))[5].bloom_filter_useful);
    perf_context_move.ClearPerLevelPerfContext();
    perf_context_move.Reset();
  }
}

TEST_F(PerfContextTest, PerfContextDisableEnable) {
  get_perf_context()->Reset();
  get_perf_context()->EnablePerLevelPerfContext();
  PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_full_positive, 1, 0);
  get_perf_context()->DisablePerLevelPerfContext();
  PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_useful, 1, 5);
  get_perf_context()->EnablePerLevelPerfContext();
  PERF_COUNTER_BY_LEVEL_ADD(block_cache_hit_count, 1, 0);
  get_perf_context()->DisablePerLevelPerfContext();
  PerfContext perf_context_copy(*get_perf_context());
  ASSERT_EQ(1, (*(perf_context_copy.level_to_perf_context))[0]
                   .bloom_filter_full_positive);
  // this was set when per level perf context is disabled, should not be copied
  ASSERT_NE(
      1, (*(perf_context_copy.level_to_perf_context))[5].bloom_filter_useful);
  ASSERT_EQ(
      1, (*(perf_context_copy.level_to_perf_context))[0].block_cache_hit_count);
  perf_context_copy.ClearPerLevelPerfContext();
  perf_context_copy.Reset();
  get_perf_context()->ClearPerLevelPerfContext();
  get_perf_context()->Reset();
}

TEST_F(PerfContextTest, PerfContextByLevelGetSet) {
  get_perf_context()->Reset();
  get_perf_context()->EnablePerLevelPerfContext();
  PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_full_positive, 1, 0);
  PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_useful, 1, 5);
  PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_useful, 1, 7);
  PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_useful, 1, 7);
  PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_full_true_positive, 1, 2);
  PERF_COUNTER_BY_LEVEL_ADD(block_cache_hit_count, 1, 0);
  PERF_COUNTER_BY_LEVEL_ADD(block_cache_hit_count, 5, 2);
  PERF_COUNTER_BY_LEVEL_ADD(block_cache_miss_count, 2, 3);
  PERF_COUNTER_BY_LEVEL_ADD(block_cache_miss_count, 4, 1);
  ASSERT_EQ(
      0, (*(get_perf_context()->level_to_perf_context))[0].bloom_filter_useful);
  ASSERT_EQ(
      1, (*(get_perf_context()->level_to_perf_context))[5].bloom_filter_useful);
  ASSERT_EQ(
      2, (*(get_perf_context()->level_to_perf_context))[7].bloom_filter_useful);
  ASSERT_EQ(1, (*(get_perf_context()->level_to_perf_context))[0]
                   .bloom_filter_full_positive);
  ASSERT_EQ(1, (*(get_perf_context()->level_to_perf_context))[2]
                   .bloom_filter_full_true_positive);
  ASSERT_EQ(1, (*(get_perf_context()->level_to_perf_context))[0]
                  .block_cache_hit_count);
  ASSERT_EQ(5, (*(get_perf_context()->level_to_perf_context))[2]
                  .block_cache_hit_count);
  ASSERT_EQ(2, (*(get_perf_context()->level_to_perf_context))[3]
                  .block_cache_miss_count);
  ASSERT_EQ(4, (*(get_perf_context()->level_to_perf_context))[1]
                  .block_cache_miss_count);
  std::string zero_excluded = get_perf_context()->ToString(true);
  ASSERT_NE(std::string::npos,
            zero_excluded.find("bloom_filter_useful = 1@level5, 2@level7"));
  ASSERT_NE(std::string::npos,
            zero_excluded.find("bloom_filter_full_positive = 1@level0"));
  ASSERT_NE(std::string::npos,
            zero_excluded.find("bloom_filter_full_true_positive = 1@level2"));
  ASSERT_NE(std::string::npos,
            zero_excluded.find("block_cache_hit_count = 1@level0, 5@level2"));
  ASSERT_NE(std::string::npos,
            zero_excluded.find("block_cache_miss_count = 4@level1, 2@level3"));
}
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  for (int i = 1; i < argc; i++) {
    int n;
    char junk;

    if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1) {
      FLAGS_write_buffer_size = n;
    }

    if (sscanf(argv[i], "--total_keys=%d%c", &n, &junk) == 1) {
      FLAGS_total_keys = n;
    }

    if (sscanf(argv[i], "--random_key=%d%c", &n, &junk) == 1 &&
        (n == 0 || n == 1)) {
      FLAGS_random_key = n;
    }

    if (sscanf(argv[i], "--use_set_based_memetable=%d%c", &n, &junk) == 1 &&
        (n == 0 || n == 1)) {
      FLAGS_use_set_based_memetable = n;
    }

    if (sscanf(argv[i], "--verbose=%d%c", &n, &junk) == 1 &&
        (n == 0 || n == 1)) {
      FLAGS_verbose = n;
    }
  }

  if (FLAGS_verbose) {
    std::cout << kDbName << "\n";
  }

  return RUN_ALL_TESTS();
}
