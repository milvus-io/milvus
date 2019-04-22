//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#ifndef ROCKSDB_LITE

#include "utilities/persistent_cache/block_cache_tier.h"

#include <regex>
#include <utility>
#include <vector>

#include "port/port.h"
#include "util/logging.h"
#include "util/stop_watch.h"
#include "util/sync_point.h"
#include "utilities/persistent_cache/block_cache_tier_file.h"

namespace rocksdb {

//
// BlockCacheImpl
//
Status BlockCacheTier::Open() {
  Status status;

  WriteLock _(&lock_);

  assert(!size_);

  // Check the validity of the options
  status = opt_.ValidateSettings();
  assert(status.ok());
  if (!status.ok()) {
    Error(opt_.log, "Invalid block cache options");
    return status;
  }

  // Create base directory or cleanup existing directory
  status = opt_.env->CreateDirIfMissing(opt_.path);
  if (!status.ok()) {
    Error(opt_.log, "Error creating directory %s. %s", opt_.path.c_str(),
          status.ToString().c_str());
    return status;
  }

  // Create base/<cache dir> directory
  status = opt_.env->CreateDir(GetCachePath());
  if (!status.ok()) {
    // directory already exists, clean it up
    status = CleanupCacheFolder(GetCachePath());
    assert(status.ok());
    if (!status.ok()) {
      Error(opt_.log, "Error creating directory %s. %s", opt_.path.c_str(),
            status.ToString().c_str());
      return status;
    }
  }

  // create a new file
  assert(!cache_file_);
  status = NewCacheFile();
  if (!status.ok()) {
    Error(opt_.log, "Error creating new file %s. %s", opt_.path.c_str(),
          status.ToString().c_str());
    return status;
  }

  assert(cache_file_);

  if (opt_.pipeline_writes) {
    assert(!insert_th_.joinable());
    insert_th_ = port::Thread(&BlockCacheTier::InsertMain, this);
  }

  return Status::OK();
}

bool IsCacheFile(const std::string& file) {
  // check if the file has .rc suffix
  // Unfortunately regex support across compilers is not even, so we use simple
  // string parsing
  size_t pos = file.find(".");
  if (pos == std::string::npos) {
    return false;
  }

  std::string suffix = file.substr(pos);
  return suffix == ".rc";
}

Status BlockCacheTier::CleanupCacheFolder(const std::string& folder) {
  std::vector<std::string> files;
  Status status = opt_.env->GetChildren(folder, &files);
  if (!status.ok()) {
    Error(opt_.log, "Error getting files for %s. %s", folder.c_str(),
          status.ToString().c_str());
    return status;
  }

  // cleanup files with the patter :digi:.rc
  for (auto file : files) {
    if (IsCacheFile(file)) {
      // cache file
      Info(opt_.log, "Removing file %s.", file.c_str());
      status = opt_.env->DeleteFile(folder + "/" + file);
      if (!status.ok()) {
        Error(opt_.log, "Error deleting file %s. %s", file.c_str(),
              status.ToString().c_str());
        return status;
      }
    } else {
      ROCKS_LOG_DEBUG(opt_.log, "Skipping file %s", file.c_str());
    }
  }
  return Status::OK();
}

Status BlockCacheTier::Close() {
  // stop the insert thread
  if (opt_.pipeline_writes && insert_th_.joinable()) {
    InsertOp op(/*quit=*/true);
    insert_ops_.Push(std::move(op));
    insert_th_.join();
  }

  // stop the writer before
  writer_.Stop();

  // clear all metadata
  WriteLock _(&lock_);
  metadata_.Clear();
  return Status::OK();
}

template<class T>
void Add(std::map<std::string, double>* stats, const std::string& key,
         const T& t) {
  stats->insert({key, static_cast<double>(t)});
}

PersistentCache::StatsType BlockCacheTier::Stats() {
  std::map<std::string, double> stats;
  Add(&stats, "persistentcache.blockcachetier.bytes_piplined",
      stats_.bytes_pipelined_.Average());
  Add(&stats, "persistentcache.blockcachetier.bytes_written",
      stats_.bytes_written_.Average());
  Add(&stats, "persistentcache.blockcachetier.bytes_read",
      stats_.bytes_read_.Average());
  Add(&stats, "persistentcache.blockcachetier.insert_dropped",
      stats_.insert_dropped_);
  Add(&stats, "persistentcache.blockcachetier.cache_hits",
      stats_.cache_hits_);
  Add(&stats, "persistentcache.blockcachetier.cache_misses",
      stats_.cache_misses_);
  Add(&stats, "persistentcache.blockcachetier.cache_errors",
      stats_.cache_errors_);
  Add(&stats, "persistentcache.blockcachetier.cache_hits_pct",
      stats_.CacheHitPct());
  Add(&stats, "persistentcache.blockcachetier.cache_misses_pct",
      stats_.CacheMissPct());
  Add(&stats, "persistentcache.blockcachetier.read_hit_latency",
      stats_.read_hit_latency_.Average());
  Add(&stats, "persistentcache.blockcachetier.read_miss_latency",
      stats_.read_miss_latency_.Average());
  Add(&stats, "persistentcache.blockcachetier.write_latency",
      stats_.write_latency_.Average());

  auto out = PersistentCacheTier::Stats();
  out.push_back(stats);
  return out;
}

Status BlockCacheTier::Insert(const Slice& key, const char* data,
                              const size_t size) {
  // update stats
  stats_.bytes_pipelined_.Add(size);

  if (opt_.pipeline_writes) {
    // off load the write to the write thread
    insert_ops_.Push(
        InsertOp(key.ToString(), std::move(std::string(data, size))));
    return Status::OK();
  }

  assert(!opt_.pipeline_writes);
  return InsertImpl(key, Slice(data, size));
}

void BlockCacheTier::InsertMain() {
  while (true) {
    InsertOp op(insert_ops_.Pop());

    if (op.signal_) {
      // that is a secret signal to exit
      break;
    }

    size_t retry = 0;
    Status s;
    while ((s = InsertImpl(Slice(op.key_), Slice(op.data_))).IsTryAgain()) {
      if (retry > kMaxRetry) {
        break;
      }

      // this can happen when the buffers are full, we wait till some buffers
      // are free. Why don't we wait inside the code. This is because we want
      // to support both pipelined and non-pipelined mode
      buffer_allocator_.WaitUntilUsable();
      retry++;
    }

    if (!s.ok()) {
      stats_.insert_dropped_++;
    }
  }
}

Status BlockCacheTier::InsertImpl(const Slice& key, const Slice& data) {
  // pre-condition
  assert(key.size());
  assert(data.size());
  assert(cache_file_);

  StopWatchNano timer(opt_.env, /*auto_start=*/ true);

  WriteLock _(&lock_);

  LBA lba;
  if (metadata_.Lookup(key, &lba)) {
    // the key already exists, this is duplicate insert
    return Status::OK();
  }

  while (!cache_file_->Append(key, data, &lba)) {
    if (!cache_file_->Eof()) {
      ROCKS_LOG_DEBUG(opt_.log, "Error inserting to cache file %d",
                      cache_file_->cacheid());
      stats_.write_latency_.Add(timer.ElapsedNanos() / 1000);
      return Status::TryAgain();
    }

    assert(cache_file_->Eof());
    Status status = NewCacheFile();
    if (!status.ok()) {
      return status;
    }
  }

  // Insert into lookup index
  BlockInfo* info = metadata_.Insert(key, lba);
  assert(info);
  if (!info) {
    return Status::IOError("Unexpected error inserting to index");
  }

  // insert to cache file reverse mapping
  cache_file_->Add(info);

  // update stats
  stats_.bytes_written_.Add(data.size());
  stats_.write_latency_.Add(timer.ElapsedNanos() / 1000);
  return Status::OK();
}

Status BlockCacheTier::Lookup(const Slice& key, std::unique_ptr<char[]>* val,
                              size_t* size) {
  StopWatchNano timer(opt_.env, /*auto_start=*/ true);

  LBA lba;
  bool status;
  status = metadata_.Lookup(key, &lba);
  if (!status) {
    stats_.cache_misses_++;
    stats_.read_miss_latency_.Add(timer.ElapsedNanos() / 1000);
    return Status::NotFound("blockcache: key not found");
  }

  BlockCacheFile* const file = metadata_.Lookup(lba.cache_id_);
  if (!file) {
    // this can happen because the block index and cache file index are
    // different, and the cache file might be removed between the two lookups
    stats_.cache_misses_++;
    stats_.read_miss_latency_.Add(timer.ElapsedNanos() / 1000);
    return Status::NotFound("blockcache: cache file not found");
  }

  assert(file->refs_);

  std::unique_ptr<char[]> scratch(new char[lba.size_]);
  Slice blk_key;
  Slice blk_val;

  status = file->Read(lba, &blk_key, &blk_val, scratch.get());
  --file->refs_;
  if (!status) {
    stats_.cache_misses_++;
    stats_.cache_errors_++;
    stats_.read_miss_latency_.Add(timer.ElapsedNanos() / 1000);
    return Status::NotFound("blockcache: error reading data");
  }

  assert(blk_key == key);

  val->reset(new char[blk_val.size()]);
  memcpy(val->get(), blk_val.data(), blk_val.size());
  *size = blk_val.size();

  stats_.bytes_read_.Add(*size);
  stats_.cache_hits_++;
  stats_.read_hit_latency_.Add(timer.ElapsedNanos() / 1000);

  return Status::OK();
}

bool BlockCacheTier::Erase(const Slice& key) {
  WriteLock _(&lock_);
  BlockInfo* info = metadata_.Remove(key);
  assert(info);
  delete info;
  return true;
}

Status BlockCacheTier::NewCacheFile() {
  lock_.AssertHeld();

  TEST_SYNC_POINT_CALLBACK("BlockCacheTier::NewCacheFile:DeleteDir",
                           (void*)(GetCachePath().c_str()));

  std::unique_ptr<WriteableCacheFile> f(
    new WriteableCacheFile(opt_.env, &buffer_allocator_, &writer_,
                           GetCachePath(), writer_cache_id_,
                           opt_.cache_file_size, opt_.log));

  bool status = f->Create(opt_.enable_direct_writes, opt_.enable_direct_reads);
  if (!status) {
    return Status::IOError("Error creating file");
  }

  Info(opt_.log, "Created cache file %d", writer_cache_id_);

  writer_cache_id_++;
  cache_file_ = f.release();

  // insert to cache files tree
  status = metadata_.Insert(cache_file_);
  assert(status);
  if (!status) {
    Error(opt_.log, "Error inserting to metadata");
    return Status::IOError("Error inserting to metadata");
  }

  return Status::OK();
}

bool BlockCacheTier::Reserve(const size_t size) {
  WriteLock _(&lock_);
  assert(size_ <= opt_.cache_size);

  if (size + size_ <= opt_.cache_size) {
    // there is enough space to write
    size_ += size;
    return true;
  }

  assert(size + size_ >= opt_.cache_size);
  // there is not enough space to fit the requested data
  // we can clear some space by evicting cold data

  const double retain_fac = (100 - kEvictPct) / static_cast<double>(100);
  while (size + size_ > opt_.cache_size * retain_fac) {
    std::unique_ptr<BlockCacheFile> f(metadata_.Evict());
    if (!f) {
      // nothing is evictable
      return false;
    }
    assert(!f->refs_);
    uint64_t file_size;
    if (!f->Delete(&file_size).ok()) {
      // unable to delete file
      return false;
    }

    assert(file_size <= size_);
    size_ -= file_size;
  }

  size_ += size;
  assert(size_ <= opt_.cache_size * 0.9);
  return true;
}

Status NewPersistentCache(Env* const env, const std::string& path,
                          const uint64_t size,
                          const std::shared_ptr<Logger>& log,
                          const bool optimized_for_nvm,
                          std::shared_ptr<PersistentCache>* cache) {
  if (!cache) {
    return Status::IOError("invalid argument cache");
  }

  auto opt = PersistentCacheConfig(env, path, size, log);
  if (optimized_for_nvm) {
    // the default settings are optimized for SSD
    // NVM devices are better accessed with 4K direct IO and written with
    // parallelism
    opt.enable_direct_writes = true;
    opt.writer_qdepth = 4;
    opt.writer_dispatch_size = 4 * 1024;
  }

  auto pcache = std::make_shared<BlockCacheTier>(opt);
  Status s = pcache->Open();

  if (!s.ok()) {
    return s;
  }

  *cache = pcache;
  return s;
}

}  // namespace rocksdb

#endif  // ifndef ROCKSDB_LITE
