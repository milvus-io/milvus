// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

/**
 * Test for race condition between Reopen() and Search() in SegmentGrowingImpl.
 *
 * The bug: sch_mutex_ is only held during Insert and Reopen, but NOT during
 * Search. This means:
 * - Thread A: Search() reads from insert_record_ (no lock)
 * - Thread B: Reopen() -> fill_empty_field() writes to insert_record_
 *   (holds sch_mutex_)
 *
 * Since Search doesn't acquire sch_mutex_, there's a data race.
 *
 * To trigger this race:
 * 1. Create a growing segment with initial schema (version 1)
 * 2. Insert data
 * 3. Start concurrent Search threads (use schema version 1)
 * 4. Call Reopen with new schema (version 2) from another thread
 * 5. Reopen's fill_empty_field writes to insert_record_
 * 6. Search threads read from insert_record_ concurrently
 *
 * Expected: With ThreadSanitizer, this should report a data race.
 * Without TSan, we might see crashes or corrupted results intermittently.
 */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "common/Schema.h"
#include "pb/schema.pb.h"
#include "query/Plan.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

class ReopenRaceTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        // Create initial schema (version 1)
        schema_v1_ = std::make_shared<Schema>();
        auto vec_fid = schema_v1_->AddDebugField(
            "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
        auto age_fid = schema_v1_->AddDebugField("age", DataType::INT64);
        schema_v1_->set_primary_field_id(age_fid);
        schema_v1_->set_schema_version(1);

        // Create schema v2 with additional field (version 2)
        schema_v2_ = std::make_shared<Schema>();
        schema_v2_->AddDebugField(
            "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
        auto age_fid_v2 = schema_v2_->AddDebugField("age", DataType::INT64);
        schema_v2_->set_primary_field_id(age_fid_v2);
        // Add new field with default value
        proto::schema::ValueField default_value;
        default_value.set_long_data(42);
        schema_v2_->AddDebugFieldWithDefaultValue(
            "new_field", DataType::INT64, default_value, true);
        schema_v2_->set_schema_version(2);
    }

    SchemaPtr schema_v1_;
    SchemaPtr schema_v2_;
};

TEST_F(ReopenRaceTest, ConcurrentReopenAndSearch) {
    // Create segment with schema v1
    auto segment = CreateGrowingSegment(schema_v1_, empty_index_meta);

    // Insert data
    const int64_t N = 10000;
    auto dataset = DataGen(schema_v1_, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // Prepare search plan with schema v1
    const char* raw_plan = R"(vector_anns: <
        field_id: 100
        query_info: <
            topk: 10
            round_decimal: 3
            metric_type: "L2"
            search_params: "{\"nprobe\": 10}"
        >
        placeholder_tag: "$0"
    >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
    auto plan =
        CreateSearchPlanByExpr(schema_v1_, plan_str.data(), plan_str.size());

    auto ph_group_raw = CreatePlaceholderGroup(1, 16, 1024);
    auto ph_group =
        ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

    // Synchronization primitives
    std::atomic<bool> start_flag{false};
    std::atomic<bool> stop_flag{false};
    std::atomic<int> search_count{0};
    std::atomic<int> error_count{0};

    // Start search threads
    const int num_search_threads = 4;
    std::vector<std::thread> search_threads;

    for (int i = 0; i < num_search_threads; i++) {
        search_threads.emplace_back([&, i]() {
            // Wait for start signal
            while (!start_flag.load()) {
                std::this_thread::yield();
            }

            // Continuously search until stop signal
            while (!stop_flag.load()) {
                try {
                    // This Search call reads from insert_record_ WITHOUT
                    // holding sch_mutex_
                    auto sr =
                        segment->Search(plan.get(), ph_group.get(), 1000000);
                    search_count.fetch_add(1);

                    // Verify we got results
                    if (sr->total_nq_ != 1) {
                        error_count.fetch_add(1);
                    }
                } catch (const std::exception& e) {
                    error_count.fetch_add(1);
                }
            }
        });
    }

    // Start reopen thread
    std::thread reopen_thread([&]() {
        // Wait for start signal
        while (!start_flag.load()) {
            std::this_thread::yield();
        }

        // Small delay to let searches get going
        std::this_thread::sleep_for(std::chrono::milliseconds(1));

        // Call Reopen with new schema - this triggers fill_empty_field
        // which writes to insert_record_ while holding sch_mutex_
        // But Search threads are reading from insert_record_ without any lock!
        segment->Reopen(schema_v2_);
    });

    // Start all threads
    start_flag.store(true);

    // Let threads run
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Stop search threads
    stop_flag.store(true);

    // Wait for all threads
    for (auto& t : search_threads) {
        t.join();
    }
    reopen_thread.join();

    std::cout << "Search operations: " << search_count.load() << std::endl;
    std::cout << "Errors: " << error_count.load() << std::endl;

    // Verify final state
    EXPECT_GT(search_count.load(), 0);

    // With TSan enabled, this test should report a data race between:
    // - fill_empty_field writing to insert_record_
    // - Search reading from insert_record_
}

TEST_F(ReopenRaceTest, RepeatedConcurrentReopenAndSearch) {
    // Run multiple iterations to increase chance of hitting the race
    const int num_iterations = 10;

    for (int iter = 0; iter < num_iterations; iter++) {
        // Create fresh segment for each iteration
        auto segment = CreateGrowingSegment(schema_v1_, empty_index_meta);

        // Insert data
        const int64_t N = 5000;
        auto dataset = DataGen(schema_v1_, N);
        segment->PreInsert(N);
        segment->Insert(0,
                        N,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);

        // Prepare search
        const char* raw_plan = R"(vector_anns: <
            field_id: 100
            query_info: <
                topk: 5
                metric_type: "L2"
                search_params: "{\"nprobe\": 10}"
            >
            placeholder_tag: "$0"
        >)";
        auto plan_str = translate_text_plan_to_binary_plan(raw_plan);
        auto plan = CreateSearchPlanByExpr(
            schema_v1_, plan_str.data(), plan_str.size());
        auto ph_group_raw = CreatePlaceholderGroup(1, 16, 1024);
        auto ph_group = ParsePlaceholderGroup(
            plan.get(), ph_group_raw.SerializeAsString());

        std::atomic<bool> start{false};
        std::atomic<bool> done{false};
        std::atomic<int> searches{0};

        // Search threads
        std::vector<std::thread> threads;
        for (int t = 0; t < 4; t++) {
            threads.emplace_back([&]() {
                while (!start.load())
                    std::this_thread::yield();
                while (!done.load()) {
                    try {
                        segment->Search(plan.get(), ph_group.get(), 1000000);
                        searches.fetch_add(1);
                    } catch (...) {
                    }
                }
            });
        }

        // Reopen thread
        threads.emplace_back([&]() {
            while (!start.load())
                std::this_thread::yield();
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            segment->Reopen(schema_v2_);
        });

        start.store(true);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        done.store(true);

        for (auto& t : threads) {
            t.join();
        }

        std::cout << "Iteration " << iter << ": " << searches.load()
                  << " searches" << std::endl;
    }
}

// Test to verify the race with LazyCheckSchema (the real-world scenario)
TEST_F(ReopenRaceTest, ConcurrentLazyCheckSchemaAndSearch) {
    auto segment = CreateGrowingSegment(schema_v1_, empty_index_meta);

    // Insert data
    const int64_t N = 10000;
    auto dataset = DataGen(schema_v1_, N);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    // Prepare search plans - one with old schema, one with new
    const char* raw_plan = R"(vector_anns: <
        field_id: 100
        query_info: <
            topk: 10
            metric_type: "L2"
            search_params: "{\"nprobe\": 10}"
        >
        placeholder_tag: "$0"
    >)";
    auto plan_str = translate_text_plan_to_binary_plan(raw_plan);

    // Plan with old schema (won't trigger Reopen)
    auto plan_v1 =
        CreateSearchPlanByExpr(schema_v1_, plan_str.data(), plan_str.size());
    auto ph_group_raw = CreatePlaceholderGroup(1, 16, 1024);
    auto ph_group_v1 =
        ParsePlaceholderGroup(plan_v1.get(), ph_group_raw.SerializeAsString());

    // Plan with new schema (will trigger Reopen via LazyCheckSchema)
    auto plan_v2 =
        CreateSearchPlanByExpr(schema_v2_, plan_str.data(), plan_str.size());
    auto ph_group_v2 =
        ParsePlaceholderGroup(plan_v2.get(), ph_group_raw.SerializeAsString());

    std::atomic<bool> start{false};
    std::atomic<bool> stop{false};
    std::atomic<int> v1_searches{0};
    std::atomic<int> v2_searches{0};

    // Thread using old schema (no Reopen triggered)
    std::thread old_schema_thread([&]() {
        while (!start.load())
            std::this_thread::yield();
        while (!stop.load()) {
            // This simulates: LazyCheckSchema(v1) -> no-op, then Search
            segment->LazyCheckSchema(schema_v1_);
            segment->Search(plan_v1.get(), ph_group_v1.get(), 1000000);
            v1_searches.fetch_add(1);
        }
    });

    // Thread using new schema (triggers Reopen)
    std::thread new_schema_thread([&]() {
        while (!start.load())
            std::this_thread::yield();
        // Small delay to let old schema thread start searching
        std::this_thread::sleep_for(std::chrono::milliseconds(1));

        // This triggers: LazyCheckSchema(v2) -> Reopen -> fill_empty_field
        // While old_schema_thread is in Search reading from insert_record_
        segment->LazyCheckSchema(schema_v2_);

        // Then continue searching
        while (!stop.load()) {
            segment->Search(plan_v2.get(), ph_group_v2.get(), 1000000);
            v2_searches.fetch_add(1);
        }
    });

    start.store(true);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    stop.store(true);

    old_schema_thread.join();
    new_schema_thread.join();

    std::cout << "V1 schema searches: " << v1_searches.load() << std::endl;
    std::cout << "V2 schema searches: " << v2_searches.load() << std::endl;

    EXPECT_GT(v1_searches.load(), 0);
    EXPECT_GT(v2_searches.load(), 0);
}
