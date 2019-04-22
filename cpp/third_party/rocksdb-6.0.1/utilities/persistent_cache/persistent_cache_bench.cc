//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#ifndef GFLAGS
#include <cstdio>
int main() { fprintf(stderr, "Please install gflags to run tools\n"); }
#else
#include <atomic>
#include <functional>
#include <memory>
#include <sstream>
#include <unordered_map>

#include "rocksdb/env.h"

#include "utilities/persistent_cache/block_cache_tier.h"
#include "utilities/persistent_cache/persistent_cache_tier.h"
#include "utilities/persistent_cache/volatile_tier_impl.h"

#include "monitoring/histogram.h"
#include "port/port.h"
#include "table/block_builder.h"
#include "util/gflags_compat.h"
#include "util/mutexlock.h"
#include "util/stop_watch.h"

DEFINE_int32(nsec, 10, "nsec");
DEFINE_int32(nthread_write, 1, "Insert threads");
DEFINE_int32(nthread_read, 1, "Lookup threads");
DEFINE_string(path, "/tmp/microbench/blkcache", "Path for cachefile");
DEFINE_string(log_path, "/tmp/log", "Path for the log file");
DEFINE_uint64(cache_size, std::numeric_limits<uint64_t>::max(), "Cache size");
DEFINE_int32(iosize, 4 * 1024, "Read IO size");
DEFINE_int32(writer_iosize, 4 * 1024, "File writer IO size");
DEFINE_int32(writer_qdepth, 1, "File writer qdepth");
DEFINE_bool(enable_pipelined_writes, false, "Enable async writes");
DEFINE_string(cache_type, "block_cache",
              "Cache type. (block_cache, volatile, tiered)");
DEFINE_bool(benchmark, false, "Benchmark mode");
DEFINE_int32(volatile_cache_pct, 10, "Percentage of cache in memory tier.");

namespace rocksdb {

std::unique_ptr<PersistentCacheTier> NewVolatileCache() {
  assert(FLAGS_cache_size != std::numeric_limits<uint64_t>::max());
  std::unique_ptr<PersistentCacheTier> pcache(
      new VolatileCacheTier(FLAGS_cache_size));
  return pcache;
}

std::unique_ptr<PersistentCacheTier> NewBlockCache() {
  std::shared_ptr<Logger> log;
  if (!Env::Default()->NewLogger(FLAGS_log_path, &log).ok()) {
    fprintf(stderr, "Error creating log %s \n", FLAGS_log_path.c_str());
    return nullptr;
  }

  PersistentCacheConfig opt(Env::Default(), FLAGS_path, FLAGS_cache_size, log);
  opt.writer_dispatch_size = FLAGS_writer_iosize;
  opt.writer_qdepth = FLAGS_writer_qdepth;
  opt.pipeline_writes = FLAGS_enable_pipelined_writes;
  opt.max_write_pipeline_backlog_size = std::numeric_limits<uint64_t>::max();
  std::unique_ptr<PersistentCacheTier> cache(new BlockCacheTier(opt));
  Status status = cache->Open();
  return cache;
}

// create a new cache tier
// construct a tiered RAM+Block cache
std::unique_ptr<PersistentTieredCache> NewTieredCache(
    const size_t mem_size, const PersistentCacheConfig& opt) {
  std::unique_ptr<PersistentTieredCache> tcache(new PersistentTieredCache());
  // create primary tier
  assert(mem_size);
  auto pcache =
      std::shared_ptr<PersistentCacheTier>(new VolatileCacheTier(mem_size));
  tcache->AddTier(pcache);
  // create secondary tier
  auto scache = std::shared_ptr<PersistentCacheTier>(new BlockCacheTier(opt));
  tcache->AddTier(scache);

  Status s = tcache->Open();
  assert(s.ok());
  return tcache;
}

std::unique_ptr<PersistentTieredCache> NewTieredCache() {
  std::shared_ptr<Logger> log;
  if (!Env::Default()->NewLogger(FLAGS_log_path, &log).ok()) {
    fprintf(stderr, "Error creating log %s \n", FLAGS_log_path.c_str());
    abort();
  }

  auto pct = FLAGS_volatile_cache_pct / static_cast<double>(100);
  PersistentCacheConfig opt(Env::Default(), FLAGS_path,
                            (1 - pct) * FLAGS_cache_size, log);
  opt.writer_dispatch_size = FLAGS_writer_iosize;
  opt.writer_qdepth = FLAGS_writer_qdepth;
  opt.pipeline_writes = FLAGS_enable_pipelined_writes;
  opt.max_write_pipeline_backlog_size = std::numeric_limits<uint64_t>::max();
  return NewTieredCache(FLAGS_cache_size * pct, opt);
}

//
// Benchmark driver
//
class CacheTierBenchmark {
 public:
  explicit CacheTierBenchmark(std::shared_ptr<PersistentCacheTier>&& cache)
      : cache_(cache) {
    if (FLAGS_nthread_read) {
      fprintf(stdout, "Pre-populating\n");
      Prepop();
      fprintf(stdout, "Pre-population completed\n");
    }

    stats_.Clear();

    // Start IO threads
    std::list<port::Thread> threads;
    Spawn(FLAGS_nthread_write, &threads,
          std::bind(&CacheTierBenchmark::Write, this));
    Spawn(FLAGS_nthread_read, &threads,
          std::bind(&CacheTierBenchmark::Read, this));

    // Wait till FLAGS_nsec and then signal to quit
    StopWatchNano t(Env::Default(), /*auto_start=*/true);
    size_t sec = t.ElapsedNanos() / 1000000000ULL;
    while (!quit_) {
      sec = t.ElapsedNanos() / 1000000000ULL;
      quit_ = sec > size_t(FLAGS_nsec);
      /* sleep override */ sleep(1);
    }

    // Wait for threads to exit
    Join(&threads);
    // Print stats
    PrintStats(sec);
    // Close the cache
    cache_->TEST_Flush();
    cache_->Close();
  }

 private:
  void PrintStats(const size_t sec) {
    std::ostringstream msg;
    msg << "Test stats" << std::endl
        << "* Elapsed: " << sec << " s" << std::endl
        << "* Write Latency:" << std::endl
        << stats_.write_latency_.ToString() << std::endl
        << "* Read Latency:" << std::endl
        << stats_.read_latency_.ToString() << std::endl
        << "* Bytes written:" << std::endl
        << stats_.bytes_written_.ToString() << std::endl
        << "* Bytes read:" << std::endl
        << stats_.bytes_read_.ToString() << std::endl
        << "Cache stats:" << std::endl
        << cache_->PrintStats() << std::endl;
    fprintf(stderr, "%s\n", msg.str().c_str());
  }

  //
  // Insert implementation and corresponding helper functions
  //
  void Prepop() {
    for (uint64_t i = 0; i < 1024 * 1024; ++i) {
      InsertKey(i);
      insert_key_limit_++;
      read_key_limit_++;
    }

    // Wait until data is flushed
    cache_->TEST_Flush();
    // warmup the cache
    for (uint64_t i = 0; i < 1024 * 1024; ReadKey(i++)) {
    }
  }

  void Write() {
    while (!quit_) {
      InsertKey(insert_key_limit_++);
    }
  }

  void InsertKey(const uint64_t key) {
    // construct key
    uint64_t k[3];
    Slice block_key = FillKey(k, key);

    // construct value
    auto block = NewBlock(key);

    // insert
    StopWatchNano timer(Env::Default(), /*auto_start=*/true);
    while (true) {
      Status status = cache_->Insert(block_key, block.get(), FLAGS_iosize);
      if (status.ok()) {
        break;
      }

      // transient error is possible if we run without pipelining
      assert(!FLAGS_enable_pipelined_writes);
    }

    // adjust stats
    const size_t elapsed_micro = timer.ElapsedNanos() / 1000;
    stats_.write_latency_.Add(elapsed_micro);
    stats_.bytes_written_.Add(FLAGS_iosize);
  }

  //
  // Read implementation
  //
  void Read() {
    while (!quit_) {
      ReadKey(random() % read_key_limit_);
    }
  }

  void ReadKey(const uint64_t val) {
    // construct key
    uint64_t k[3];
    Slice key = FillKey(k, val);

    // Lookup in cache
    StopWatchNano timer(Env::Default(), /*auto_start=*/true);
    std::unique_ptr<char[]> block;
    size_t size;
    Status status = cache_->Lookup(key, &block, &size);
    if (!status.ok()) {
      fprintf(stderr, "%s\n", status.ToString().c_str());
    }
    assert(status.ok());
    assert(size == (size_t) FLAGS_iosize);

    // adjust stats
    const size_t elapsed_micro = timer.ElapsedNanos() / 1000;
    stats_.read_latency_.Add(elapsed_micro);
    stats_.bytes_read_.Add(FLAGS_iosize);

    // verify content
    if (!FLAGS_benchmark) {
      auto expected_block = NewBlock(val);
      assert(memcmp(block.get(), expected_block.get(), FLAGS_iosize) == 0);
    }
  }

  // create data for a key by filling with a certain pattern
  std::unique_ptr<char[]> NewBlock(const uint64_t val) {
    std::unique_ptr<char[]> data(new char[FLAGS_iosize]);
    memset(data.get(), val % 255, FLAGS_iosize);
    return data;
  }

  // spawn threads
  void Spawn(const size_t n, std::list<port::Thread>* threads,
             const std::function<void()>& fn) {
    for (size_t i = 0; i < n; ++i) {
      threads->emplace_back(fn);
    }
  }

  // join threads
  void Join(std::list<port::Thread>* threads) {
    for (auto& th : *threads) {
      th.join();
    }
  }

  // construct key
  Slice FillKey(uint64_t (&k)[3], const uint64_t val) {
    k[0] = k[1] = 0;
    k[2] = val;
    void* p = static_cast<void*>(&k);
    return Slice(static_cast<char*>(p), sizeof(k));
  }

  // benchmark stats
  struct Stats {
    void Clear() {
      bytes_written_.Clear();
      bytes_read_.Clear();
      read_latency_.Clear();
      write_latency_.Clear();
    }

    HistogramImpl bytes_written_;
    HistogramImpl bytes_read_;
    HistogramImpl read_latency_;
    HistogramImpl write_latency_;
  };

  std::shared_ptr<PersistentCacheTier> cache_;  // cache implementation
  std::atomic<uint64_t> insert_key_limit_{0};   // data inserted upto
  std::atomic<uint64_t> read_key_limit_{0};     // data can be read safely upto
  bool quit_ = false;                           // Quit thread ?
  mutable Stats stats_;                         // Stats
};

}  // namespace rocksdb

//
// main
//
int main(int argc, char** argv) {
  GFLAGS_NAMESPACE::SetUsageMessage(std::string("\nUSAGE:\n") +
                                    std::string(argv[0]) + " [OPTIONS]...");
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, false);

  std::ostringstream msg;
  msg << "Config" << std::endl
      << "======" << std::endl
      << "* nsec=" << FLAGS_nsec << std::endl
      << "* nthread_write=" << FLAGS_nthread_write << std::endl
      << "* path=" << FLAGS_path << std::endl
      << "* cache_size=" << FLAGS_cache_size << std::endl
      << "* iosize=" << FLAGS_iosize << std::endl
      << "* writer_iosize=" << FLAGS_writer_iosize << std::endl
      << "* writer_qdepth=" << FLAGS_writer_qdepth << std::endl
      << "* enable_pipelined_writes=" << FLAGS_enable_pipelined_writes
      << std::endl
      << "* cache_type=" << FLAGS_cache_type << std::endl
      << "* benchmark=" << FLAGS_benchmark << std::endl
      << "* volatile_cache_pct=" << FLAGS_volatile_cache_pct << std::endl;

  fprintf(stderr, "%s\n", msg.str().c_str());

  std::shared_ptr<rocksdb::PersistentCacheTier> cache;
  if (FLAGS_cache_type == "block_cache") {
    fprintf(stderr, "Using block cache implementation\n");
    cache = rocksdb::NewBlockCache();
  } else if (FLAGS_cache_type == "volatile") {
    fprintf(stderr, "Using volatile cache implementation\n");
    cache = rocksdb::NewVolatileCache();
  } else if (FLAGS_cache_type == "tiered") {
    fprintf(stderr, "Using tiered cache implementation\n");
    cache = rocksdb::NewTieredCache();
  } else {
    fprintf(stderr, "Unknown option for cache\n");
  }

  assert(cache);
  if (!cache) {
    fprintf(stderr, "Error creating cache\n");
    abort();
  }

  std::unique_ptr<rocksdb::CacheTierBenchmark> benchmark(
      new rocksdb::CacheTierBenchmark(std::move(cache)));

  return 0;
}
#endif  // #ifndef GFLAGS
#else
int main(int, char**) { return 0; }
#endif
