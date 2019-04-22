//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once
#ifndef ROCKSDB_LITE

#include <unordered_set>

#include "monitoring/statistics.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/env.h"
#include "utilities/blob_db/blob_db_impl.h"
#include "utilities/blob_db/blob_index.h"

namespace rocksdb {
namespace blob_db {

struct BlobCompactionContext {
  uint64_t next_file_number;
  std::unordered_set<uint64_t> current_blob_files;
  SequenceNumber fifo_eviction_seq;
  uint64_t evict_expiration_up_to;
};

class BlobIndexCompactionFilterFactory : public CompactionFilterFactory {
 public:
  BlobIndexCompactionFilterFactory(BlobDBImpl* blob_db_impl, Env* env,
                                   Statistics* statistics)
      : blob_db_impl_(blob_db_impl), env_(env), statistics_(statistics) {}

  virtual const char* Name() const override {
    return "BlobIndexCompactionFilterFactory";
  }

  virtual std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override;

 private:
  BlobDBImpl* blob_db_impl_;
  Env* env_;
  Statistics* statistics_;
};

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
