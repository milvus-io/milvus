//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include <atomic>

#include "rocksdb/listener.h"
#include "util/mutexlock.h"
#include "utilities/blob_db/blob_db_impl.h"

namespace rocksdb {
namespace blob_db {

class BlobDBListener : public EventListener {
 public:
  explicit BlobDBListener(BlobDBImpl* blob_db_impl)
      : blob_db_impl_(blob_db_impl) {}

  void OnFlushBegin(DB* /*db*/, const FlushJobInfo& /*info*/) override {
    assert(blob_db_impl_ != nullptr);
    blob_db_impl_->SyncBlobFiles();
  }

  void OnFlushCompleted(DB* /*db*/, const FlushJobInfo& /*info*/) override {
    assert(blob_db_impl_ != nullptr);
    blob_db_impl_->UpdateLiveSSTSize();
  }

  void OnCompactionCompleted(DB* /*db*/,
                             const CompactionJobInfo& /*info*/) override {
    assert(blob_db_impl_ != nullptr);
    blob_db_impl_->UpdateLiveSSTSize();
  }

 private:
  BlobDBImpl* blob_db_impl_;
};

}  // namespace blob_db
}  // namespace rocksdb
#endif  // !ROCKSDB_LITE
