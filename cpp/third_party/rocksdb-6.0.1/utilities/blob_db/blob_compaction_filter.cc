//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE

#include "utilities/blob_db/blob_compaction_filter.h"
#include "db/dbformat.h"

namespace rocksdb {
namespace blob_db {

namespace {

// CompactionFilter to delete expired blob index from base DB.
class BlobIndexCompactionFilter : public CompactionFilter {
 public:
  BlobIndexCompactionFilter(BlobCompactionContext context,
                            uint64_t current_time, Statistics* statistics)
      : context_(context),
        current_time_(current_time),
        statistics_(statistics) {}

  ~BlobIndexCompactionFilter() override {
    RecordTick(statistics_, BLOB_DB_BLOB_INDEX_EXPIRED_COUNT, expired_count_);
    RecordTick(statistics_, BLOB_DB_BLOB_INDEX_EXPIRED_SIZE, expired_size_);
    RecordTick(statistics_, BLOB_DB_BLOB_INDEX_EVICTED_COUNT, evicted_count_);
    RecordTick(statistics_, BLOB_DB_BLOB_INDEX_EVICTED_SIZE, evicted_size_);
  }

  const char* Name() const override { return "BlobIndexCompactionFilter"; }

  // Filter expired blob indexes regardless of snapshots.
  bool IgnoreSnapshots() const override { return true; }

  Decision FilterV2(int /*level*/, const Slice& key, ValueType value_type,
                    const Slice& value, std::string* /*new_value*/,
                    std::string* /*skip_until*/) const override {
    if (value_type != kBlobIndex) {
      return Decision::kKeep;
    }
    BlobIndex blob_index;
    Status s = blob_index.DecodeFrom(value);
    if (!s.ok()) {
      // Unable to decode blob index. Keeping the value.
      return Decision::kKeep;
    }
    if (blob_index.HasTTL() && blob_index.expiration() <= current_time_) {
      // Expired
      expired_count_++;
      expired_size_ += key.size() + value.size();
      return Decision::kRemove;
    }
    if (!blob_index.IsInlined() &&
        blob_index.file_number() < context_.next_file_number &&
        context_.current_blob_files.count(blob_index.file_number()) == 0) {
      // Corresponding blob file gone. Could have been garbage collected or
      // evicted by FIFO eviction.
      evicted_count_++;
      evicted_size_ += key.size() + value.size();
      return Decision::kRemove;
    }
    if (context_.fifo_eviction_seq > 0 && blob_index.HasTTL() &&
        blob_index.expiration() < context_.evict_expiration_up_to) {
      // Hack: Internal key is passed to BlobIndexCompactionFilter for it to
      // get sequence number.
      ParsedInternalKey ikey;
      bool ok = ParseInternalKey(key, &ikey);
      // Remove keys that could have been remove by last FIFO eviction.
      // If get error while parsing key, ignore and continue.
      if (ok && ikey.sequence < context_.fifo_eviction_seq) {
        evicted_count_++;
        evicted_size_ += key.size() + value.size();
        return Decision::kRemove;
      }
    }
    return Decision::kKeep;
  }

 private:
  BlobCompactionContext context_;
  const uint64_t current_time_;
  Statistics* statistics_;
  // It is safe to not using std::atomic since the compaction filter, created
  // from a compaction filter factroy, will not be called from multiple threads.
  mutable uint64_t expired_count_ = 0;
  mutable uint64_t expired_size_ = 0;
  mutable uint64_t evicted_count_ = 0;
  mutable uint64_t evicted_size_ = 0;
};

}  // anonymous namespace

std::unique_ptr<CompactionFilter>
BlobIndexCompactionFilterFactory::CreateCompactionFilter(
    const CompactionFilter::Context& /*context*/) {
  int64_t current_time = 0;
  Status s = env_->GetCurrentTime(&current_time);
  if (!s.ok()) {
    return nullptr;
  }
  assert(current_time >= 0);

  BlobCompactionContext context;
  blob_db_impl_->GetCompactionContext(&context);

  return std::unique_ptr<CompactionFilter>(new BlobIndexCompactionFilter(
      context, static_cast<uint64_t>(current_time), statistics_));
}

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
