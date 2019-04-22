//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "utilities/persistent_cache/persistent_cache_tier.h"

#include "inttypes.h"

#include <string>
#include <sstream>

namespace rocksdb {

std::string PersistentCacheConfig::ToString() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];

  snprintf(buffer, kBufferSize, "    path: %s\n", path.c_str());
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "    enable_direct_reads: %d\n",
           enable_direct_reads);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "    enable_direct_writes: %d\n",
           enable_direct_writes);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "    cache_size: %" PRIu64 "\n", cache_size);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "    cache_file_size: %" PRIu32 "\n",
           cache_file_size);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "    writer_qdepth: %" PRIu32 "\n",
           writer_qdepth);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "    pipeline_writes: %d\n", pipeline_writes);
  ret.append(buffer);
  snprintf(buffer, kBufferSize,
           "    max_write_pipeline_backlog_size: %" PRIu64 "\n",
           max_write_pipeline_backlog_size);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "    write_buffer_size: %" PRIu32 "\n",
           write_buffer_size);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "    writer_dispatch_size: %" PRIu64 "\n",
           writer_dispatch_size);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "    is_compressed: %d\n", is_compressed);
  ret.append(buffer);

  return ret;
}

//
// PersistentCacheTier implementation
//
Status PersistentCacheTier::Open() {
  if (next_tier_) {
    return next_tier_->Open();
  }
  return Status::OK();
}

Status PersistentCacheTier::Close() {
  if (next_tier_) {
    return next_tier_->Close();
  }
  return Status::OK();
}

bool PersistentCacheTier::Reserve(const size_t /*size*/) {
  // default implementation is a pass through
  return true;
}

bool PersistentCacheTier::Erase(const Slice& /*key*/) {
  // default implementation is a pass through since not all cache tiers might
  // support erase
  return true;
}

std::string PersistentCacheTier::PrintStats() {
  std::ostringstream os;
  for (auto tier_stats : Stats()) {
    os << "---- next tier -----" << std::endl;
    for (auto stat : tier_stats) {
      os << stat.first << ": " << stat.second << std::endl;
    }
  }
  return os.str();
}

PersistentCache::StatsType PersistentCacheTier::Stats() {
  if (next_tier_) {
    return next_tier_->Stats();
  }
  return PersistentCache::StatsType{};
}

//
// PersistentTieredCache implementation
//
PersistentTieredCache::~PersistentTieredCache() { assert(tiers_.empty()); }

Status PersistentTieredCache::Open() {
  assert(!tiers_.empty());
  return tiers_.front()->Open();
}

Status PersistentTieredCache::Close() {
  assert(!tiers_.empty());
  Status status = tiers_.front()->Close();
  if (status.ok()) {
    tiers_.clear();
  }
  return status;
}

bool PersistentTieredCache::Erase(const Slice& key) {
  assert(!tiers_.empty());
  return tiers_.front()->Erase(key);
}

PersistentCache::StatsType PersistentTieredCache::Stats() {
  assert(!tiers_.empty());
  return tiers_.front()->Stats();
}

std::string PersistentTieredCache::PrintStats() {
  assert(!tiers_.empty());
  return tiers_.front()->PrintStats();
}

Status PersistentTieredCache::Insert(const Slice& page_key, const char* data,
                                     const size_t size) {
  assert(!tiers_.empty());
  return tiers_.front()->Insert(page_key, data, size);
}

Status PersistentTieredCache::Lookup(const Slice& page_key,
                                     std::unique_ptr<char[]>* data,
                                     size_t* size) {
  assert(!tiers_.empty());
  return tiers_.front()->Lookup(page_key, data, size);
}

void PersistentTieredCache::AddTier(const Tier& tier) {
  if (!tiers_.empty()) {
    tiers_.back()->set_next_tier(tier);
  }
  tiers_.push_back(tier);
}

bool PersistentTieredCache::IsCompressed() {
  assert(tiers_.size());
  return tiers_.front()->IsCompressed();
}

}  // namespace rocksdb

#endif
