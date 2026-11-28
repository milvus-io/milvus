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

#include "exec/expression/EntryPool.h"

#include <atomic>
#include <thread>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"

namespace milvus::exec {

class EntryPoolTestPeer {
 public:
    static void
    AssertValid(const EntryPool& pool) {
        std::shared_lock lock(pool.mutex_);
        if (pool.entries_.empty()) {
            ASSERT_EQ(pool.clock_hand_, nullptr);
            return;
        }
        ASSERT_NE(pool.clock_hand_, nullptr);
        std::unordered_set<const EntryPool::Entry*> members;
        for (const auto& [key, entry] : pool.entries_) {
            ASSERT_NE(entry, nullptr);
            ASSERT_NE(entry->payload, nullptr);
            EXPECT_EQ(entry->key, &key);
            members.insert(entry.get());
        }
        const auto* entry = pool.clock_hand_;
        for (size_t i = 0; i < pool.entries_.size(); ++i) {
            // Check membership before dereferencing potentially stale links.
            ASSERT_EQ(members.erase(entry), 1u);
            ASSERT_NE(entry->next, nullptr);
            ASSERT_NE(entry->prev, nullptr);
            EXPECT_EQ(entry->next->prev, entry);
            EXPECT_EQ(entry->prev->next, entry);
            entry = entry->next;
        }
        EXPECT_EQ(entry, pool.clock_hand_);
        EXPECT_TRUE(members.empty());
    }

    static const EntryPool::Entry*
    Hand(const EntryPool& pool) {
        std::shared_lock lock(pool.mutex_);
        return pool.clock_hand_;
    }

    static void
    Rehash(EntryPool& pool) {
        std::unique_lock lock(pool.mutex_);
        pool.entries_.rehash(pool.entries_.bucket_count() * 2);
    }
};

namespace {
constexpr size_t kRows = 1024;
constexpr size_t kCapacity = 16U << 20;

class EntryPoolClockTest : public ::testing::Test {
 protected:
    EntryPoolClockTest()
        : pool_(kCapacity), result_(kRows, false), valid_(kRows, true) {
        pool_.Configure(kCapacity, false, 0);
    }

    void
    Put(int64_t segment, const std::string& signature = "expr") {
        pool_.Put(segment, signature, kRows, result_, valid_);
    }

    void
    ExpectValue(int64_t segment, const std::string& signature = "expr") {
        auto payload = pool_.Lookup(segment, signature, kRows);
        ASSERT_NE(payload, nullptr);
        TargetBitmap decoded, valid;
        ASSERT_TRUE(payload->Decode(decoded, valid));
        EXPECT_EQ(decoded.size(), kRows);
        EXPECT_TRUE(decoded.none());
        EXPECT_TRUE(valid.all());
    }

    EntryPool pool_;
    TargetBitmap result_;
    TargetBitmap valid_;
};

TEST_F(EntryPoolClockTest, SustainedWritesPreserveHandAndReplaceInPlace) {
    Put(1);
    const auto charge = pool_.GetCurrentBytes();
    pool_.Configure(charge * 3, false, 0);
    Put(2);
    Put(3);
    const auto* first = EntryPoolTestPeer::Hand(pool_);
    Put(1);
    EXPECT_EQ(EntryPoolTestPeer::Hand(pool_), first);

    for (int64_t segment = 4; segment <= 200; ++segment) {
        SCOPED_TRACE(segment);
        // Replacement of the next victim must not reset the hand or append
        // a duplicate member. Every resident receives the same usage count.
        const auto* before = EntryPoolTestPeer::Hand(pool_);
        Put(segment - 3);
        EXPECT_EQ(EntryPoolTestPeer::Hand(pool_), before);
        Put(segment);
        EXPECT_EQ(pool_.Lookup(segment - 3, "expr", kRows), nullptr);
        for (int64_t resident = segment - 2; resident <= segment; ++resident) {
            ExpectValue(resident);
        }
        EXPECT_EQ(pool_.GetEntryCount(), 3u);
        EXPECT_EQ(pool_.GetCurrentBytes(), charge * 3);
        EntryPoolTestPeer::AssertValid(pool_);
    }
}

TEST_F(EntryPoolClockTest, LongKeysSurviveRehashAndEviction) {
    const std::string signature(8192, 'x');
    constexpr int64_t count = 256;
    for (int64_t segment = 0; segment < count; ++segment) {
        Put(segment, signature);
    }
    ASSERT_EQ(pool_.GetEntryCount(), count);
    const auto capacity = pool_.GetCurrentBytes();
    const auto* hand = EntryPoolTestPeer::Hand(pool_);
    const auto* key = hand->key;
    const auto* signature_data = key->signature.data();
    EntryPoolTestPeer::Rehash(pool_);
    EXPECT_EQ(EntryPoolTestPeer::Hand(pool_), hand);
    EXPECT_EQ(hand->key, key);
    EXPECT_EQ(hand->key->signature.data(), signature_data);
    EntryPoolTestPeer::AssertValid(pool_);

    pool_.Configure(capacity, false, 0);
    for (int64_t segment = count; segment < count * 2; ++segment) {
        Put(segment, signature);
        EXPECT_EQ(pool_.Lookup(segment - count, signature, kRows), nullptr);
        ExpectValue(segment, signature);
        EXPECT_EQ(pool_.GetEntryCount(), count);
        EXPECT_EQ(pool_.GetCurrentBytes(), capacity);
    }
    EntryPoolTestPeer::AssertValid(pool_);
}

TEST_F(EntryPoolClockTest, HeldReplacementKeepsNodeAndBothPayloadCharges) {
    Put(1);
    const auto charge = pool_.GetCurrentBytes();
    Put(2);
    pool_.Configure(charge * 3, false, 0);
    const auto* hand = EntryPoolTestPeer::Hand(pool_);
    const auto* key = hand->key;
    auto old = pool_.Lookup(1, "expr", kRows);
    ASSERT_NE(old, nullptr);

    pool_.Put(1, "expr", kRows, valid_, valid_);
    EXPECT_EQ(pool_.GetEntryCount(), 2u);
    EXPECT_EQ(pool_.GetCurrentBytes(), charge * 3);
    EXPECT_EQ(EntryPoolTestPeer::Hand(pool_), hand);
    EXPECT_EQ(hand->key, key);
    EntryPoolTestPeer::AssertValid(pool_);
    TargetBitmap result, valid;
    ASSERT_TRUE(old->Decode(result, valid));
    EXPECT_TRUE(result.none());
    auto replacement = pool_.Lookup(1, "expr", kRows);
    ASSERT_NE(replacement, nullptr);
    ASSERT_TRUE(replacement->Decode(result, valid));
    EXPECT_TRUE(result.all());
    old.reset();
    EXPECT_EQ(pool_.GetCurrentBytes(), charge * 2);
    pool_.Clear();
    EntryPoolTestPeer::AssertValid(pool_);
    EXPECT_EQ(pool_.GetCurrentBytes(), charge);
    replacement.reset();
    EXPECT_EQ(pool_.GetCurrentBytes(), 0u);
}

TEST_F(EntryPoolClockTest, EraseHeadMiddleTailAndLastThenReuse) {
    for (int64_t segment = 1; segment <= 5; ++segment) {
        Put(segment);
    }
    const auto charge = pool_.GetCurrentBytes() / 5;
    auto held = pool_.Lookup(3, "expr", kRows);
    for (const int64_t segment : {1, 3, 5, 2, 4}) {
        SCOPED_TRACE(segment);
        EXPECT_EQ(pool_.EraseSegment(segment), 1u);
        EXPECT_EQ(pool_.EraseSegment(segment), 0u);
        EXPECT_EQ(pool_.Lookup(segment, "expr", kRows), nullptr);
        EntryPoolTestPeer::AssertValid(pool_);
    }
    EXPECT_EQ(pool_.GetCurrentBytes(), charge);
    TargetBitmap result, valid;
    ASSERT_TRUE(held->Decode(result, valid));
    EXPECT_TRUE(result.none());
    held.reset();
    EXPECT_EQ(pool_.GetCurrentBytes(), 0u);

    pool_.Configure(charge, false, 0);
    Put(6);
    Put(7);  // Evict the only member and recreate the ring.
    EXPECT_EQ(pool_.Lookup(6, "expr", kRows), nullptr);
    ExpectValue(7);
    EntryPoolTestPeer::AssertValid(pool_);
    pool_.Clear();
    pool_.Clear();
    EntryPoolTestPeer::AssertValid(pool_);
    Put(8);
    ExpectValue(8);
    EntryPoolTestPeer::AssertValid(pool_);
}

TEST_F(EntryPoolClockTest, RejectedMultiVictimPutRestoresRing) {
    for (int64_t segment = 0; segment < 8; ++segment) {
        Put(segment);
    }
    const auto capacity = pool_.GetCurrentBytes();
    pool_.Configure(capacity, false, 0);
    std::vector<EntryPool::Handle> held;
    for (int64_t segment = 0; segment < 8; segment += 2) {
        held.push_back(pool_.Lookup(segment, "expr", kRows));
    }
    TargetBitmap large(kRows * 12, true);
    EntryPool probe(kCapacity);
    probe.Configure(kCapacity, false, 0);
    probe.Put(8, "expr", large.size(), large, large);
    ASSERT_GT(probe.GetCurrentBytes(), capacity / 2);
    ASSERT_LE(probe.GetCurrentBytes(), capacity);

    for (int attempt = 0; attempt < 3; ++attempt) {
        pool_.Put(8, "expr", large.size(), large, large);
        EXPECT_EQ(pool_.GetEntryCount(), 8u);
        EXPECT_EQ(pool_.GetCurrentBytes(), capacity);
        EntryPoolTestPeer::AssertValid(pool_);
        for (int64_t segment = 0; segment < 8; ++segment) {
            ExpectValue(segment);
        }
    }
    held.clear();
    pool_.Put(8, "expr", large.size(), large, large);
    ASSERT_NE(pool_.Lookup(8, "expr", large.size()), nullptr);
    EXPECT_LE(pool_.GetCurrentBytes(), capacity);
    EntryPoolTestPeer::AssertValid(pool_);
    pool_.Clear();
    EXPECT_EQ(pool_.GetCurrentBytes(), 0u);
}

TEST_F(EntryPoolClockTest, RetiredBudgetRestoresAnEntireCandidateRing) {
    auto budget = std::make_shared<ExprCacheMemoryBudget>(kCapacity);
    EntryPool pool(kCapacity, budget);
    pool.Configure(kCapacity, false, 0);
    TargetBitmap large(kRows * 16, true);
    pool.Put(0, "expr", large.size(), large, large);
    const auto retired_bytes = pool.GetCurrentBytes();
    auto retired = pool.Lookup(0, "expr", large.size());
    pool.Clear();
    for (int64_t segment = 1; segment <= 3; ++segment) {
        pool.Put(segment, "expr", kRows, result_, valid_);
    }
    const auto capacity = pool.GetCurrentBytes();
    pool.Configure(capacity, false, 0);
    TargetBitmap incoming(kRows * 8, false);
    EntryPool probe(kCapacity);
    probe.Configure(kCapacity, false, 0);
    probe.Put(4, "expr", incoming.size(), incoming, incoming);
    ASSERT_GT(probe.GetCurrentBytes(), capacity - retired_bytes);
    ASSERT_LE(probe.GetCurrentBytes(), capacity);

    // All current members can be staged, but the retired payload still
    // prevents admission. Restoring must handle an empty remaining ring.
    pool.Put(4, "expr", incoming.size(), incoming, incoming);
    EXPECT_EQ(pool.GetEntryCount(), 3u);
    EXPECT_EQ(pool.GetCurrentBytes(), capacity);
    EntryPoolTestPeer::AssertValid(pool);
    retired.reset();
    pool.Put(4, "expr", incoming.size(), incoming, incoming);
    ASSERT_NE(pool.Lookup(4, "expr", incoming.size()), nullptr);
    EntryPoolTestPeer::AssertValid(pool);
}

TEST_F(EntryPoolClockTest, ConcurrentReadersWithWritesEraseAndClear) {
    Put(0);
    pool_.Configure(pool_.GetCurrentBytes() * 16, false, 0);
    std::atomic<bool> start{false};
    std::atomic<int> ready{0};
    std::atomic<int> bad_reads{0};
    std::vector<std::thread> readers;
    for (int reader = 0; reader < 3; ++reader) {
        readers.emplace_back([&, reader]() {
            auto held = pool_.Lookup(0, "expr", kRows);
            EXPECT_NE(held, nullptr);
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int i = 0; i < 2000; ++i) {
                auto payload = pool_.Lookup((i + reader) % 32, "expr", kRows);
                if (payload) {
                    TargetBitmap result, valid;
                    if (!payload->Decode(result, valid) ||
                        result.size() != kRows || !result.none() ||
                        !valid.all()) {
                        bad_reads.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        });
    }
    while (ready.load(std::memory_order_acquire) != 3) {
        std::this_thread::yield();
    }
    auto write = [&](int offset) {
        for (int i = 0; i < 1000; ++i) {
            Put((i + offset) % 32);
            if (i % 7 == 0) {
                pool_.EraseSegment((i + offset + 3) % 32);
            }
            if (i % 53 == 0) {
                pool_.Clear();
            }
        }
    };
    start.store(true, std::memory_order_release);
    std::thread writer([&]() { write(17); });
    write(0);
    writer.join();
    for (auto& reader : readers) {
        reader.join();
    }
    EXPECT_EQ(bad_reads.load(), 0);
    EntryPoolTestPeer::AssertValid(pool_);
    pool_.Clear();
    EXPECT_EQ(pool_.GetCurrentBytes(), 0u);
}

}  // namespace
}  // namespace milvus::exec
