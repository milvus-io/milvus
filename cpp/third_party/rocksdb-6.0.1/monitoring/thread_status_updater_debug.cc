// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <mutex>

#include "db/column_family.h"
#include "monitoring/thread_status_updater.h"

namespace rocksdb {

#ifndef NDEBUG
#ifdef ROCKSDB_USING_THREAD_STATUS
void ThreadStatusUpdater::TEST_VerifyColumnFamilyInfoMap(
    const std::vector<ColumnFamilyHandle*>& handles, bool check_exist) {
  std::unique_lock<std::mutex> lock(thread_list_mutex_);
  if (check_exist) {
    assert(cf_info_map_.size() == handles.size());
  }
  for (auto* handle : handles) {
    auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(handle)->cfd();
    auto iter __attribute__((__unused__)) = cf_info_map_.find(cfd);
    if (check_exist) {
      assert(iter != cf_info_map_.end());
      assert(iter->second.cf_name == cfd->GetName());
    } else {
      assert(iter == cf_info_map_.end());
    }
  }
}

#else

void ThreadStatusUpdater::TEST_VerifyColumnFamilyInfoMap(
    const std::vector<ColumnFamilyHandle*>& /*handles*/, bool /*check_exist*/) {
}

#endif  // ROCKSDB_USING_THREAD_STATUS
#endif  // !NDEBUG

}  // namespace rocksdb
