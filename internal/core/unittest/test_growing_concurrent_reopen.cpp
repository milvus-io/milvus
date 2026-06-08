// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "common/OpContext.h"
#include "common/Schema.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"

using namespace milvus;
using namespace milvus::segcore;

/**
 * Regression test for Issue #50377: Standalone SIGSEGV in query
 * (bulk_subscript) on a nullable field of a growing segment, concurrent with
 * schema evolution.
 *
 * Root Cause:
 * - InsertRecordGrowing holds data_ / valid_data_ as plain std::unordered_map.
 * - The query path (SegmentGrowingImpl::bulk_subscript -> get_data_base /
 *   get_valid_data) read these maps WITHOUT any lock.
 * - Reopen() (schema evolution adding a field) calls append_field_meta ->
 *   append_data / append_valid_data, which emplace into the maps and can
 *   trigger a rehash.
 * - A rehash concurrent with a lock-free find()/at() on a reader thread walks
 *   freed bucket memory -> SIGSEGV (exit 139), exactly matching the production
 *   crash: GISFunctionFilterExpr -> bulk_subscript -> get_valid_data ->
 *   std::unordered_map.
 *
 * Fix:
 * - Guard the structure of data_ / valid_data_ with a dedicated shared_mutex
 *   (field_map_mutex_): structural writes take it unique, lookups take it
 *   shared.
 *
 * Before fix: this test reliably crashes (SIGSEGV / ASan heap-use-after-free)
 * under the concurrent rehash. After fix: queries either succeed or throw
 * cleanly; no crash.
 *
 * Reliability notes: a rehash only corrupts a concurrent reader during the
 * brief window the bucket array is reallocated, so the reproduction maximizes
 * overlap by (1) running many fresh-segment rounds, (2) issuing single-offset
 * bulk_subscript so readers spend almost all their time inside the map
 * find()/at() rather than copying payload, (3) using many reader threads, and
 * (4) yielding in the writer between field additions to widen the window.
 */
TEST(GrowingConcurrentReopenTest,
     ConcurrentBulkSubscriptAndReopenShouldNotCrash) {
    // The race fires within the first round without the fix (manifests as a
    // SIGSEGV in a Release build, or a failed map lookup under debug/ASan), so
    // a modest round count keeps the test fast while remaining reliable.
    const int kRounds = 30;
    const int kExtraFields = 48;
    const unsigned kReaders = std::max(4u, std::thread::hardware_concurrency());

    // Pre-build the schema chain once; each schema adds one more nullable field
    // so Reopening through them forces repeated emplace() into data_ /
    // valid_data_ (the structural mutation that races with readers).
    auto make_schema = [](int extra) {
        auto sch = std::make_shared<Schema>();
        sch->AddDebugField(
            "vec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
        auto pk = sch->AddDebugField("pk", DataType::INT64);
        sch->AddDebugField("nullable_i64", DataType::INT64, /*nullable=*/true);
        for (int j = 1; j <= extra; ++j) {
            sch->AddDebugField("evo_field_" + std::to_string(j),
                               DataType::INT64,
                               /*nullable=*/true);
        }
        sch->set_primary_field_id(pk);
        sch->set_schema_version(1 + extra);
        return sch;
    };

    auto base = make_schema(0);
    auto nullable_fid = base->get_field_id(FieldName("nullable_i64"));
    std::vector<SchemaPtr> evolutions;
    for (int k = 1; k <= kExtraFields; ++k) {
        evolutions.push_back(make_schema(k));
    }

    std::vector<std::exception_ptr> exceptions;
    std::mutex exceptions_mutex;
    std::atomic<int64_t> query_count{0};

    for (int round = 0; round < kRounds; ++round) {
        auto segment = CreateGrowingSegment(base, nullptr);
        auto seg_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
        ASSERT_NE(seg_impl, nullptr);

        // Seed a few rows so bulk_subscript has data to read.
        const int N = 16;
        auto data = DataGen(base, N, /*seed=*/round + 1);
        segment->PreInsert(N);
        segment->Insert(
            0, N, data.row_ids_.data(), data.timestamps_.data(), data.raw_);

        std::atomic<bool> stop_flag{false};

        // Reader threads: single-offset bulk_subscript in a tight loop. With
        // count == 1 a reader is almost always inside get_data_base /
        // get_valid_data (the unordered_map lookups), maximizing overlap with a
        // concurrent rehash.
        auto reader = [&](int tid) {
            milvus::OpContext op_ctx;
            int64_t offset = tid % N;
            while (!stop_flag.load(std::memory_order_relaxed)) {
                try {
                    auto res = seg_impl->bulk_subscript(
                        &op_ctx, nullable_fid, &offset, 1);
                    (void)res;
                    query_count.fetch_add(1, std::memory_order_relaxed);
                } catch (...) {
                    std::lock_guard<std::mutex> lock(exceptions_mutex);
                    exceptions.push_back(std::current_exception());
                    break;
                }
            }
        };

        std::vector<std::thread> readers;
        for (unsigned i = 0; i < kReaders; ++i) {
            readers.emplace_back(reader, static_cast<int>(i));
        }

        // Writer: drive schema evolution, adding one field at a time so the
        // map repeatedly rehashes underneath the readers.
        for (auto& sch : evolutions) {
            try {
                seg_impl->Reopen(sch);
            } catch (...) {
                std::lock_guard<std::mutex> lock(exceptions_mutex);
                exceptions.push_back(std::current_exception());
                break;
            }
            std::this_thread::yield();
        }

        stop_flag.store(true);
        for (auto& t : readers) {
            t.join();
        }

        if (!exceptions.empty()) {
            break;
        }
    }

    EXPECT_TRUE(exceptions.empty())
        << "Concurrent bulk_subscript / Reopen raised exceptions";
    EXPECT_GT(query_count.load(), 0) << "Expected some queries to complete";
}
