//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#include "utilities/table_properties_collectors/compact_on_deletion_collector.h"

#include <memory>
#include "rocksdb/utilities/table_properties_collectors.h"

namespace rocksdb {

CompactOnDeletionCollector::CompactOnDeletionCollector(
    size_t sliding_window_size, size_t deletion_trigger)
    : bucket_size_((sliding_window_size + kNumBuckets - 1) / kNumBuckets),
      current_bucket_(0),
      num_keys_in_current_bucket_(0),
      num_deletions_in_observation_window_(0),
      deletion_trigger_(deletion_trigger),
      need_compaction_(false),
      finished_(false) {
  memset(num_deletions_in_buckets_, 0, sizeof(size_t) * kNumBuckets);
}

// AddUserKey() will be called when a new key/value pair is inserted into the
// table.
// @params key    the user key that is inserted into the table.
// @params value  the value that is inserted into the table.
// @params file_size  file size up to now
Status CompactOnDeletionCollector::AddUserKey(const Slice& /*key*/,
                                              const Slice& /*value*/,
                                              EntryType type,
                                              SequenceNumber /*seq*/,
                                              uint64_t /*file_size*/) {
  assert(!finished_);
  if (bucket_size_ == 0) {
    // This collector is effectively disabled
    return Status::OK();
  }

  if (need_compaction_) {
    // If the output file already needs to be compacted, skip the check.
    return Status::OK();
  }

  if (num_keys_in_current_bucket_ == bucket_size_) {
    // When the current bucket is full, advance the cursor of the
    // ring buffer to the next bucket.
    current_bucket_ = (current_bucket_ + 1) % kNumBuckets;

    // Update the current count of observed deletion keys by excluding
    // the number of deletion keys in the oldest bucket in the
    // observation window.
    assert(num_deletions_in_observation_window_ >=
        num_deletions_in_buckets_[current_bucket_]);
    num_deletions_in_observation_window_ -=
        num_deletions_in_buckets_[current_bucket_];
    num_deletions_in_buckets_[current_bucket_] = 0;
    num_keys_in_current_bucket_ = 0;
  }

  num_keys_in_current_bucket_++;
  if (type == kEntryDelete) {
    num_deletions_in_observation_window_++;
    num_deletions_in_buckets_[current_bucket_]++;
    if (num_deletions_in_observation_window_ >= deletion_trigger_) {
      need_compaction_ = true;
    }
  }
  return Status::OK();
}

TablePropertiesCollector*
CompactOnDeletionCollectorFactory::CreateTablePropertiesCollector(
    TablePropertiesCollectorFactory::Context /*context*/) {
  return new CompactOnDeletionCollector(
      sliding_window_size_.load(), deletion_trigger_.load());
}

std::shared_ptr<CompactOnDeletionCollectorFactory>
    NewCompactOnDeletionCollectorFactory(
        size_t sliding_window_size,
        size_t deletion_trigger) {
  return std::shared_ptr<CompactOnDeletionCollectorFactory>(
      new CompactOnDeletionCollectorFactory(
          sliding_window_size, deletion_trigger));
}
}  // namespace rocksdb
#endif  // !ROCKSDB_LITE
