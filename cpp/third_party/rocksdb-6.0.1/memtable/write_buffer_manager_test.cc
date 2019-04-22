//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/write_buffer_manager.h"
#include "util/testharness.h"

namespace rocksdb {

class WriteBufferManagerTest : public testing::Test {};

#ifndef ROCKSDB_LITE
TEST_F(WriteBufferManagerTest, ShouldFlush) {
  // A write buffer manager of size 10MB
  std::unique_ptr<WriteBufferManager> wbf(
      new WriteBufferManager(10 * 1024 * 1024));

  wbf->ReserveMem(8 * 1024 * 1024);
  ASSERT_FALSE(wbf->ShouldFlush());
  // 90% of the hard limit will hit the condition
  wbf->ReserveMem(1 * 1024 * 1024);
  ASSERT_TRUE(wbf->ShouldFlush());
  // Scheduling for freeing will release the condition
  wbf->ScheduleFreeMem(1 * 1024 * 1024);
  ASSERT_FALSE(wbf->ShouldFlush());

  wbf->ReserveMem(2 * 1024 * 1024);
  ASSERT_TRUE(wbf->ShouldFlush());

  wbf->ScheduleFreeMem(4 * 1024 * 1024);
  // 11MB total, 6MB mutable. hard limit still hit
  ASSERT_TRUE(wbf->ShouldFlush());

  wbf->ScheduleFreeMem(2 * 1024 * 1024);
  // 11MB total, 4MB mutable. hard limit stills but won't flush because more
  // than half data is already being flushed.
  ASSERT_FALSE(wbf->ShouldFlush());

  wbf->ReserveMem(4 * 1024 * 1024);
  // 15 MB total, 8MB mutable.
  ASSERT_TRUE(wbf->ShouldFlush());

  wbf->FreeMem(7 * 1024 * 1024);
  // 9MB total, 8MB mutable.
  ASSERT_FALSE(wbf->ShouldFlush());
}

TEST_F(WriteBufferManagerTest, CacheCost) {
  // 1GB cache
  std::shared_ptr<Cache> cache = NewLRUCache(1024 * 1024 * 1024, 4);
  // A write buffer manager of size 50MB
  std::unique_ptr<WriteBufferManager> wbf(
      new WriteBufferManager(50 * 1024 * 1024, cache));

  // Allocate 1.5MB will allocate 2MB
  wbf->ReserveMem(1536 * 1024);
  ASSERT_GE(cache->GetPinnedUsage(), 2 * 1024 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 2 * 1024 * 1024 + 10000);

  // Allocate another 2MB
  wbf->ReserveMem(2 * 1024 * 1024);
  ASSERT_GE(cache->GetPinnedUsage(), 4 * 1024 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 4 * 1024 * 1024 + 10000);

  // Allocate another 20MB
  wbf->ReserveMem(20 * 1024 * 1024);
  ASSERT_GE(cache->GetPinnedUsage(), 24 * 1024 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 24 * 1024 * 1024 + 10000);

  // Free 2MB will not cause any change in cache cost
  wbf->FreeMem(2 * 1024 * 1024);
  ASSERT_GE(cache->GetPinnedUsage(), 24 * 1024 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 24 * 1024 * 1024 + 10000);

  ASSERT_FALSE(wbf->ShouldFlush());

  // Allocate another 30MB
  wbf->ReserveMem(30 * 1024 * 1024);
  ASSERT_GE(cache->GetPinnedUsage(), 52 * 1024 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 52 * 1024 * 1024 + 10000);
  ASSERT_TRUE(wbf->ShouldFlush());

  ASSERT_TRUE(wbf->ShouldFlush());

  wbf->ScheduleFreeMem(20 * 1024 * 1024);
  ASSERT_GE(cache->GetPinnedUsage(), 52 * 1024 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 52 * 1024 * 1024 + 10000);

  // Still need flush as the hard limit hits
  ASSERT_TRUE(wbf->ShouldFlush());

  // Free 20MB will releae 1MB from cache
  wbf->FreeMem(20 * 1024 * 1024);
  ASSERT_GE(cache->GetPinnedUsage(), 51 * 1024 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 51 * 1024 * 1024 + 10000);

  ASSERT_FALSE(wbf->ShouldFlush());

  // Every free will release 1MB if still not hit 3/4
  wbf->FreeMem(16 * 1024);
  ASSERT_GE(cache->GetPinnedUsage(), 50 * 1024 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 50 * 1024 * 1024 + 10000);

  wbf->FreeMem(16 * 1024);
  ASSERT_GE(cache->GetPinnedUsage(), 49 * 1024 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 49 * 1024 * 1024 + 10000);

  // Free 2MB will not cause any change in cache cost
  wbf->ReserveMem(2 * 1024 * 1024);
  ASSERT_GE(cache->GetPinnedUsage(), 49 * 1024 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 49 * 1024 * 1024 + 10000);

  wbf->FreeMem(16 * 1024);
  ASSERT_GE(cache->GetPinnedUsage(), 48 * 1024 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 48 * 1024 * 1024 + 10000);

  // Destory write buffer manger should free everything
  wbf.reset();
  ASSERT_LT(cache->GetPinnedUsage(), 1024 * 1024);
}

TEST_F(WriteBufferManagerTest, NoCapCacheCost) {
  // 1GB cache
  std::shared_ptr<Cache> cache = NewLRUCache(1024 * 1024 * 1024, 4);
  // A write buffer manager of size 256MB
  std::unique_ptr<WriteBufferManager> wbf(new WriteBufferManager(0, cache));
  // Allocate 1.5MB will allocate 2MB
  wbf->ReserveMem(10 * 1024 * 1024);
  ASSERT_GE(cache->GetPinnedUsage(), 10 * 1024 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 10 * 1024 * 1024 + 10000);
  ASSERT_FALSE(wbf->ShouldFlush());

  wbf->FreeMem(9 * 1024 * 1024);
  for (int i = 0; i < 10; i++) {
    wbf->FreeMem(16 * 1024);
  }
  ASSERT_GE(cache->GetPinnedUsage(), 1024 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 1024 * 1024 + 10000);
}
#endif  // ROCKSDB_LITE
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
