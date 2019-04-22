//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/sim_cache.h"
#include <atomic>
#include "monitoring/statistics.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "util/file_reader_writer.h"
#include "util/mutexlock.h"
#include "util/string_util.h"

namespace rocksdb {

namespace {

class CacheActivityLogger {
 public:
  CacheActivityLogger()
      : activity_logging_enabled_(false), max_logging_size_(0) {}

  ~CacheActivityLogger() {
    MutexLock l(&mutex_);

    StopLoggingInternal();
  }

  Status StartLogging(const std::string& activity_log_file, Env* env,
                      uint64_t max_logging_size = 0) {
    assert(activity_log_file != "");
    assert(env != nullptr);

    Status status;
    EnvOptions env_opts;
    std::unique_ptr<WritableFile> log_file;

    MutexLock l(&mutex_);

    // Stop existing logging if any
    StopLoggingInternal();

    // Open log file
    status = env->NewWritableFile(activity_log_file, &log_file, env_opts);
    if (!status.ok()) {
      return status;
    }
    file_writer_.reset(new WritableFileWriter(std::move(log_file),
                                              activity_log_file, env_opts));

    max_logging_size_ = max_logging_size;
    activity_logging_enabled_.store(true);

    return status;
  }

  void StopLogging() {
    MutexLock l(&mutex_);

    StopLoggingInternal();
  }

  void ReportLookup(const Slice& key) {
    if (activity_logging_enabled_.load() == false) {
      return;
    }

    std::string log_line = "LOOKUP - " + key.ToString(true) + "\n";

    // line format: "LOOKUP - <KEY>"
    MutexLock l(&mutex_);
    Status s = file_writer_->Append(log_line);
    if (!s.ok() && bg_status_.ok()) {
      bg_status_ = s;
    }
    if (MaxLoggingSizeReached() || !bg_status_.ok()) {
      // Stop logging if we have reached the max file size or
      // encountered an error
      StopLoggingInternal();
    }
  }

  void ReportAdd(const Slice& key, size_t size) {
    if (activity_logging_enabled_.load() == false) {
      return;
    }

    std::string log_line = "ADD - ";
    log_line += key.ToString(true);
    log_line += " - ";
    AppendNumberTo(&log_line, size);
  // @lint-ignore TXT2 T25377293 Grandfathered in
		log_line += "\n";

    // line format: "ADD - <KEY> - <KEY-SIZE>"
    MutexLock l(&mutex_);
    Status s = file_writer_->Append(log_line);
    if (!s.ok() && bg_status_.ok()) {
      bg_status_ = s;
    }

    if (MaxLoggingSizeReached() || !bg_status_.ok()) {
      // Stop logging if we have reached the max file size or
      // encountered an error
      StopLoggingInternal();
    }
  }

  Status& bg_status() {
    MutexLock l(&mutex_);
    return bg_status_;
  }

 private:
  bool MaxLoggingSizeReached() {
    mutex_.AssertHeld();

    return (max_logging_size_ > 0 &&
            file_writer_->GetFileSize() >= max_logging_size_);
  }

  void StopLoggingInternal() {
    mutex_.AssertHeld();

    if (!activity_logging_enabled_) {
      return;
    }

    activity_logging_enabled_.store(false);
    Status s = file_writer_->Close();
    if (!s.ok() && bg_status_.ok()) {
      bg_status_ = s;
    }
  }

  // Mutex to sync writes to file_writer, and all following
  // class data members
  port::Mutex mutex_;
  // Indicates if logging is currently enabled
  // atomic to allow reads without mutex
  std::atomic<bool> activity_logging_enabled_;
  // When reached, we will stop logging and close the file
  // Value of 0 means unlimited
  uint64_t max_logging_size_;
  std::unique_ptr<WritableFileWriter> file_writer_;
  Status bg_status_;
};

// SimCacheImpl definition
class SimCacheImpl : public SimCache {
 public:
  // capacity for real cache (ShardedLRUCache)
  // test_capacity for key only cache
  SimCacheImpl(std::shared_ptr<Cache> cache, size_t sim_capacity,
               int num_shard_bits)
      : cache_(cache),
        key_only_cache_(NewLRUCache(sim_capacity, num_shard_bits)),
        miss_times_(0),
        hit_times_(0),
        stats_(nullptr) {}

  ~SimCacheImpl() override {}
  void SetCapacity(size_t capacity) override { cache_->SetCapacity(capacity); }

  void SetStrictCapacityLimit(bool strict_capacity_limit) override {
    cache_->SetStrictCapacityLimit(strict_capacity_limit);
  }

  Status Insert(const Slice& key, void* value, size_t charge,
                void (*deleter)(const Slice& key, void* value), Handle** handle,
                Priority priority) override {
    // The handle and value passed in are for real cache, so we pass nullptr
    // to key_only_cache_ for both instead. Also, the deleter function pointer
    // will be called by user to perform some external operation which should
    // be applied only once. Thus key_only_cache accepts an empty function.
    // *Lambda function without capture can be assgined to a function pointer
    Handle* h = key_only_cache_->Lookup(key);
    if (h == nullptr) {
      key_only_cache_->Insert(key, nullptr, charge,
                              [](const Slice& /*k*/, void* /*v*/) {}, nullptr,
                              priority);
    } else {
      key_only_cache_->Release(h);
    }

    cache_activity_logger_.ReportAdd(key, charge);

    return cache_->Insert(key, value, charge, deleter, handle, priority);
  }

  Handle* Lookup(const Slice& key, Statistics* stats) override {
    Handle* h = key_only_cache_->Lookup(key);
    if (h != nullptr) {
      key_only_cache_->Release(h);
      inc_hit_counter();
      RecordTick(stats, SIM_BLOCK_CACHE_HIT);
    } else {
      inc_miss_counter();
      RecordTick(stats, SIM_BLOCK_CACHE_MISS);
    }

    cache_activity_logger_.ReportLookup(key);

    return cache_->Lookup(key, stats);
  }

  bool Ref(Handle* handle) override { return cache_->Ref(handle); }

  bool Release(Handle* handle, bool force_erase = false) override {
    return cache_->Release(handle, force_erase);
  }

  void Erase(const Slice& key) override {
    cache_->Erase(key);
    key_only_cache_->Erase(key);
  }

  void* Value(Handle* handle) override { return cache_->Value(handle); }

  uint64_t NewId() override { return cache_->NewId(); }

  size_t GetCapacity() const override { return cache_->GetCapacity(); }

  bool HasStrictCapacityLimit() const override {
    return cache_->HasStrictCapacityLimit();
  }

  size_t GetUsage() const override { return cache_->GetUsage(); }

  size_t GetUsage(Handle* handle) const override {
    return cache_->GetUsage(handle);
  }

  size_t GetPinnedUsage() const override { return cache_->GetPinnedUsage(); }

  void DisownData() override {
    cache_->DisownData();
    key_only_cache_->DisownData();
  }

  void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                              bool thread_safe) override {
    // only apply to _cache since key_only_cache doesn't hold value
    cache_->ApplyToAllCacheEntries(callback, thread_safe);
  }

  void EraseUnRefEntries() override {
    cache_->EraseUnRefEntries();
    key_only_cache_->EraseUnRefEntries();
  }

  size_t GetSimCapacity() const override {
    return key_only_cache_->GetCapacity();
  }
  size_t GetSimUsage() const override { return key_only_cache_->GetUsage(); }
  void SetSimCapacity(size_t capacity) override {
    key_only_cache_->SetCapacity(capacity);
  }

  uint64_t get_miss_counter() const override {
    return miss_times_.load(std::memory_order_relaxed);
  }

  uint64_t get_hit_counter() const override {
    return hit_times_.load(std::memory_order_relaxed);
  }

  void reset_counter() override {
    miss_times_.store(0, std::memory_order_relaxed);
    hit_times_.store(0, std::memory_order_relaxed);
    SetTickerCount(stats_, SIM_BLOCK_CACHE_HIT, 0);
    SetTickerCount(stats_, SIM_BLOCK_CACHE_MISS, 0);
  }

  std::string ToString() const override {
    std::string res;
    res.append("SimCache MISSes: " + std::to_string(get_miss_counter()) + "\n");
    res.append("SimCache HITs:    " + std::to_string(get_hit_counter()) + "\n");
    char buff[350];
    auto lookups = get_miss_counter() + get_hit_counter();
    snprintf(buff, sizeof(buff), "SimCache HITRATE: %.2f%%\n",
             (lookups == 0 ? 0 : get_hit_counter() * 100.0f / lookups));
    res.append(buff);
    return res;
  }

  std::string GetPrintableOptions() const override {
    std::string ret;
    ret.reserve(20000);
    ret.append("    cache_options:\n");
    ret.append(cache_->GetPrintableOptions());
    ret.append("    sim_cache_options:\n");
    ret.append(key_only_cache_->GetPrintableOptions());
    return ret;
  }

  Status StartActivityLogging(const std::string& activity_log_file, Env* env,
                              uint64_t max_logging_size = 0) override {
    return cache_activity_logger_.StartLogging(activity_log_file, env,
                                               max_logging_size);
  }

  void StopActivityLogging() override { cache_activity_logger_.StopLogging(); }

  Status GetActivityLoggingStatus() override {
    return cache_activity_logger_.bg_status();
  }

 private:
  std::shared_ptr<Cache> cache_;
  std::shared_ptr<Cache> key_only_cache_;
  std::atomic<uint64_t> miss_times_;
  std::atomic<uint64_t> hit_times_;
  Statistics* stats_;
  CacheActivityLogger cache_activity_logger_;

  void inc_miss_counter() {
    miss_times_.fetch_add(1, std::memory_order_relaxed);
  }
  void inc_hit_counter() { hit_times_.fetch_add(1, std::memory_order_relaxed); }
};

}  // end anonymous namespace

// For instrumentation purpose, use NewSimCache instead
std::shared_ptr<SimCache> NewSimCache(std::shared_ptr<Cache> cache,
                                      size_t sim_capacity, int num_shard_bits) {
  if (num_shard_bits >= 20) {
    return nullptr;  // the cache cannot be sharded into too many fine pieces
  }
  return std::make_shared<SimCacheImpl>(cache, sim_capacity, num_shard_bits);
}

}  // end namespace rocksdb
