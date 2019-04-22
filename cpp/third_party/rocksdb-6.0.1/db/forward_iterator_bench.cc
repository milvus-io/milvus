//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#if !defined(GFLAGS) || defined(ROCKSDB_LITE)
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run rocksdb tools\n");
  return 1;
}
#elif defined(OS_MACOSX) || defined(OS_WIN)
// Block forward_iterator_bench under MAC and Windows
int main() { return 0; }
#else
#include <semaphore.h>
#include <atomic>
#include <bitset>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <limits>
#include <mutex>
#include <queue>
#include <random>
#include <thread>

#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "util/gflags_compat.h"
#include "util/testharness.h"

const int MAX_SHARDS = 100000;

DEFINE_int32(writers, 8, "");
DEFINE_int32(readers, 8, "");
DEFINE_int64(rate, 100000, "");
DEFINE_int64(value_size, 300, "");
DEFINE_int64(shards, 1000, "");
DEFINE_int64(memtable_size, 500000000, "");
DEFINE_int64(block_cache_size, 300000000, "");
DEFINE_int64(block_size, 65536, "");
DEFINE_double(runtime, 300.0, "");
DEFINE_bool(cache_only_first, true, "");
DEFINE_bool(iterate_upper_bound, true, "");

struct Stats {
  char pad1[128] __attribute__((__unused__));
  std::atomic<uint64_t> written{0};
  char pad2[128] __attribute__((__unused__));
  std::atomic<uint64_t> read{0};
  std::atomic<uint64_t> cache_misses{0};
  char pad3[128] __attribute__((__unused__));
} stats;

struct Key {
  Key() {}
  Key(uint64_t shard_in, uint64_t seqno_in)
      : shard_be(htobe64(shard_in)), seqno_be(htobe64(seqno_in)) {}

  uint64_t shard() const { return be64toh(shard_be); }
  uint64_t seqno() const { return be64toh(seqno_be); }

 private:
  uint64_t shard_be;
  uint64_t seqno_be;
} __attribute__((__packed__));

struct Reader;
struct Writer;

struct ShardState {
  char pad1[128] __attribute__((__unused__));
  std::atomic<uint64_t> last_written{0};
  Writer* writer;
  Reader* reader;
  char pad2[128] __attribute__((__unused__));
  std::atomic<uint64_t> last_read{0};
  std::unique_ptr<rocksdb::Iterator> it;
  std::unique_ptr<rocksdb::Iterator> it_cacheonly;
  Key upper_bound;
  rocksdb::Slice upper_bound_slice;
  char pad3[128] __attribute__((__unused__));
};

struct Reader {
 public:
  explicit Reader(std::vector<ShardState>* shard_states, rocksdb::DB* db)
      : shard_states_(shard_states), db_(db) {
    sem_init(&sem_, 0, 0);
    thread_ = port::Thread(&Reader::run, this);
  }

  void run() {
    while (1) {
      sem_wait(&sem_);
      if (done_.load()) {
        break;
      }

      uint64_t shard;
      {
        std::lock_guard<std::mutex> guard(queue_mutex_);
        assert(!shards_pending_queue_.empty());
        shard = shards_pending_queue_.front();
        shards_pending_queue_.pop();
        shards_pending_set_.reset(shard);
      }
      readOnceFromShard(shard);
    }
  }

  void readOnceFromShard(uint64_t shard) {
    ShardState& state = (*shard_states_)[shard];
    if (!state.it) {
      // Initialize iterators
      rocksdb::ReadOptions options;
      options.tailing = true;
      if (FLAGS_iterate_upper_bound) {
        state.upper_bound = Key(shard, std::numeric_limits<uint64_t>::max());
        state.upper_bound_slice = rocksdb::Slice(
            (const char*)&state.upper_bound, sizeof(state.upper_bound));
        options.iterate_upper_bound = &state.upper_bound_slice;
      }

      state.it.reset(db_->NewIterator(options));

      if (FLAGS_cache_only_first) {
        options.read_tier = rocksdb::ReadTier::kBlockCacheTier;
        state.it_cacheonly.reset(db_->NewIterator(options));
      }
    }

    const uint64_t upto = state.last_written.load();
    for (rocksdb::Iterator* it : {state.it_cacheonly.get(), state.it.get()}) {
      if (it == nullptr) {
        continue;
      }
      if (state.last_read.load() >= upto) {
        break;
      }
      bool need_seek = true;
      for (uint64_t seq = state.last_read.load() + 1; seq <= upto; ++seq) {
        if (need_seek) {
          Key from(shard, state.last_read.load() + 1);
          it->Seek(rocksdb::Slice((const char*)&from, sizeof(from)));
          need_seek = false;
        } else {
          it->Next();
        }
        if (it->status().IsIncomplete()) {
          ++::stats.cache_misses;
          break;
        }
        assert(it->Valid());
        assert(it->key().size() == sizeof(Key));
        Key key;
        memcpy(&key, it->key().data(), it->key().size());
        // fprintf(stderr, "Expecting (%ld, %ld) read (%ld, %ld)\n",
        //         shard, seq, key.shard(), key.seqno());
        assert(key.shard() == shard);
        assert(key.seqno() == seq);
        state.last_read.store(seq);
        ++::stats.read;
      }
    }
  }

  void onWrite(uint64_t shard) {
    {
      std::lock_guard<std::mutex> guard(queue_mutex_);
      if (!shards_pending_set_.test(shard)) {
        shards_pending_queue_.push(shard);
        shards_pending_set_.set(shard);
        sem_post(&sem_);
      }
    }
  }

  ~Reader() {
    done_.store(true);
    sem_post(&sem_);
    thread_.join();
  }

 private:
  char pad1[128] __attribute__((__unused__));
  std::vector<ShardState>* shard_states_;
  rocksdb::DB* db_;
  rocksdb::port::Thread thread_;
  sem_t sem_;
  std::mutex queue_mutex_;
  std::bitset<MAX_SHARDS + 1> shards_pending_set_;
  std::queue<uint64_t> shards_pending_queue_;
  std::atomic<bool> done_{false};
  char pad2[128] __attribute__((__unused__));
};

struct Writer {
  explicit Writer(std::vector<ShardState>* shard_states, rocksdb::DB* db)
      : shard_states_(shard_states), db_(db) {}

  void start() { thread_ = port::Thread(&Writer::run, this); }

  void run() {
    std::queue<std::chrono::steady_clock::time_point> workq;
    std::chrono::steady_clock::time_point deadline(
        std::chrono::steady_clock::now() +
        std::chrono::nanoseconds((uint64_t)(1000000000 * FLAGS_runtime)));
    std::vector<uint64_t> my_shards;
    for (int i = 1; i <= FLAGS_shards; ++i) {
      if ((*shard_states_)[i].writer == this) {
        my_shards.push_back(i);
      }
    }

    std::mt19937 rng{std::random_device()()};
    std::uniform_int_distribution<int> shard_dist(
        0, static_cast<int>(my_shards.size()) - 1);
    std::string value(FLAGS_value_size, '*');

    while (1) {
      auto now = std::chrono::steady_clock::now();
      if (FLAGS_runtime >= 0 && now >= deadline) {
        break;
      }
      if (workq.empty()) {
        for (int i = 0; i < FLAGS_rate; i += FLAGS_writers) {
          std::chrono::nanoseconds offset(1000000000LL * i / FLAGS_rate);
          workq.push(now + offset);
        }
      }
      while (!workq.empty() && workq.front() < now) {
        workq.pop();
        uint64_t shard = my_shards[shard_dist(rng)];
        ShardState& state = (*shard_states_)[shard];
        uint64_t seqno = state.last_written.load() + 1;
        Key key(shard, seqno);
        // fprintf(stderr, "Writing (%ld, %ld)\n", shard, seqno);
        rocksdb::Status status =
            db_->Put(rocksdb::WriteOptions(),
                     rocksdb::Slice((const char*)&key, sizeof(key)),
                     rocksdb::Slice(value));
        assert(status.ok());
        state.last_written.store(seqno);
        state.reader->onWrite(shard);
        ++::stats.written;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    // fprintf(stderr, "Writer done\n");
  }

  ~Writer() { thread_.join(); }

 private:
  char pad1[128] __attribute__((__unused__));
  std::vector<ShardState>* shard_states_;
  rocksdb::DB* db_;
  rocksdb::port::Thread thread_;
  char pad2[128] __attribute__((__unused__));
};

struct StatsThread {
  explicit StatsThread(rocksdb::DB* db)
      : db_(db), thread_(&StatsThread::run, this) {}

  void run() {
    //    using namespace std::chrono;
    auto tstart = std::chrono::steady_clock::now(), tlast = tstart;
    uint64_t wlast = 0, rlast = 0;
    while (!done_.load()) {
      {
        std::unique_lock<std::mutex> lock(cvm_);
        cv_.wait_for(lock, std::chrono::seconds(1));
      }
      auto now = std::chrono::steady_clock::now();
      double elapsed =
          std::chrono::duration_cast<std::chrono::duration<double> >(
              now - tlast).count();
      uint64_t w = ::stats.written.load();
      uint64_t r = ::stats.read.load();
      fprintf(stderr,
              "%s elapsed %4lds | written %10ld | w/s %10.0f | read %10ld | "
              "r/s %10.0f | cache misses %10ld\n",
              db_->GetEnv()->TimeToString(time(nullptr)).c_str(),
              std::chrono::duration_cast<std::chrono::seconds>(now - tstart)
                  .count(),
              w, (w - wlast) / elapsed, r, (r - rlast) / elapsed,
              ::stats.cache_misses.load());
      wlast = w;
      rlast = r;
      tlast = now;
    }
  }

  ~StatsThread() {
    {
      std::lock_guard<std::mutex> guard(cvm_);
      done_.store(true);
    }
    cv_.notify_all();
    thread_.join();
  }

 private:
  rocksdb::DB* db_;
  std::mutex cvm_;
  std::condition_variable cv_;
  rocksdb::port::Thread thread_;
  std::atomic<bool> done_{false};
};

int main(int argc, char** argv) {
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);

  std::mt19937 rng{std::random_device()()};
  rocksdb::Status status;
  std::string path = rocksdb::test::PerThreadDBPath("forward_iterator_test");
  fprintf(stderr, "db path is %s\n", path.c_str());
  rocksdb::Options options;
  options.create_if_missing = true;
  options.compression = rocksdb::CompressionType::kNoCompression;
  options.compaction_style = rocksdb::CompactionStyle::kCompactionStyleNone;
  options.level0_slowdown_writes_trigger = 99999;
  options.level0_stop_writes_trigger = 99999;
  options.use_direct_io_for_flush_and_compaction = true;
  options.write_buffer_size = FLAGS_memtable_size;
  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_cache = rocksdb::NewLRUCache(FLAGS_block_cache_size);
  table_options.block_size = FLAGS_block_size;
  options.table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(table_options));

  status = rocksdb::DestroyDB(path, options);
  assert(status.ok());
  rocksdb::DB* db_raw;
  status = rocksdb::DB::Open(options, path, &db_raw);
  assert(status.ok());
  std::unique_ptr<rocksdb::DB> db(db_raw);

  std::vector<ShardState> shard_states(FLAGS_shards + 1);
  std::deque<Reader> readers;
  while (static_cast<int>(readers.size()) < FLAGS_readers) {
    readers.emplace_back(&shard_states, db_raw);
  }
  std::deque<Writer> writers;
  while (static_cast<int>(writers.size()) < FLAGS_writers) {
    writers.emplace_back(&shard_states, db_raw);
  }

  // Each shard gets a random reader and random writer assigned to it
  for (int i = 1; i <= FLAGS_shards; ++i) {
    std::uniform_int_distribution<int> reader_dist(0, FLAGS_readers - 1);
    std::uniform_int_distribution<int> writer_dist(0, FLAGS_writers - 1);
    shard_states[i].reader = &readers[reader_dist(rng)];
    shard_states[i].writer = &writers[writer_dist(rng)];
  }

  StatsThread stats_thread(db_raw);
  for (Writer& w : writers) {
    w.start();
  }

  writers.clear();
  readers.clear();
}
#endif  // !defined(GFLAGS) || defined(ROCKSDB_LITE)
