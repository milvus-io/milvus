// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <string>
#include <vector>

#include "exec/expression/ExprCache.h"
#include "common/Types.h"

using milvus::exec::ExprResCacheManager;

namespace {

milvus::TargetBitmap
MakeBits(size_t n, bool v = true) {
    milvus::TargetBitmap b(n);
    if (v)
        b.set();
    else
        b.reset();
    return b;
}

}  // namespace

TEST(ExprResCacheManagerTest, PutGetBasic) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();
    mgr.SetCapacityBytes(1ULL << 20);  // 1MB

    ExprResCacheManager::Key k{123, "expr:A"};
    ExprResCacheManager::Value v;
    v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    v.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(128));
    v.active_count = 128;

    mgr.Put(k, v);

    ExprResCacheManager::Value got;
    ASSERT_TRUE(mgr.Get(k, got));
    ASSERT_TRUE(got.result);
    ASSERT_EQ(got.result->size(), 128);
    ASSERT_TRUE(got.valid_result);
    ASSERT_EQ(got.valid_result->size(), 128);

    // restore global state
    mgr.Clear();
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerTest, LruEvictionByCapacity) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();
    // roughly allow 2 entries
    // (8192 / 8 * 2 + 32) * 2 = 4160
    mgr.SetCapacityBytes(4300);

    const size_t N = 8192;  // bits
    for (int i = 0; i < 3; ++i) {
        ExprResCacheManager::Key k{1, "expr:" + std::to_string(i)};
        ExprResCacheManager::Value v;
        v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(N));
        v.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(N));
        mgr.Put(k, v);
    }

    // The first one should be evicted by LRU
    ExprResCacheManager::Value out;
    ASSERT_FALSE(mgr.Get({1, "expr:0"}, out));
    ASSERT_TRUE(mgr.Get({1, "expr:1"}, out));
    ASSERT_TRUE(mgr.Get({1, "expr:2"}, out));

    // restore global state
    mgr.Clear();
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerTest, EraseSegment) {
    auto& mgr = ExprResCacheManager::Instance();
    ExprResCacheManager::SetEnabled(true);
    mgr.Clear();
    mgr.SetCapacityBytes(1ULL << 20);

    ExprResCacheManager::Key k1{10, "sig1"};
    ExprResCacheManager::Key k2{10, "sig2"};
    ExprResCacheManager::Key k3{11, "sig3"};
    ExprResCacheManager::Value v;
    v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(64));
    v.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(64));
    mgr.Put(k1, v);
    mgr.Put(k2, v);
    mgr.Put(k3, v);

    size_t erased = mgr.EraseSegment(10);
    ASSERT_EQ(erased, 2);

    ExprResCacheManager::Value out;
    ASSERT_FALSE(mgr.Get(k1, out));
    ASSERT_FALSE(mgr.Get(k2, out));
    ASSERT_TRUE(mgr.Get(k3, out));

    // restore global state
    mgr.Clear();
    ExprResCacheManager::SetEnabled(false);
}

TEST(ExprResCacheManagerTest, EnableDisable) {
    auto& mgr = ExprResCacheManager::Instance();
    mgr.Clear();
    mgr.SetCapacityBytes(1ULL << 20);

    ExprResCacheManager::SetEnabled(false);
    ExprResCacheManager::Key k{7, "x"};
    ExprResCacheManager::Value v;
    v.result = std::make_shared<milvus::TargetBitmap>(MakeBits(32));
    v.valid_result = std::make_shared<milvus::TargetBitmap>(MakeBits(32));
    mgr.Put(k, v);

    ExprResCacheManager::Value out;
    // When disabled, Get should not hit
    ASSERT_FALSE(mgr.Get(k, out));

    ExprResCacheManager::SetEnabled(true);
    mgr.Put(k, v);
    ASSERT_TRUE(mgr.Get(k, out));

    // restore global state
    mgr.Clear();
    ExprResCacheManager::SetEnabled(false);
}
