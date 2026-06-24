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
#include <memory>
#include <numeric>
#include <thread>
#include <vector>

#include "common/Schema.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;

/**
 * Test for Issue #46656: QueryNode panic with "unordered_map::at" during ProcessInsert
 *
 * Root Cause:
 * - IndexingRecord holds a const reference to Schema (schema_)
 * - When SegmentGrowingImpl::Reopen() updates schema_ to a new Schema object,
 *   the old Schema object may be destroyed
 * - IndexingRecord::schema_ becomes a dangling reference
 * - Accessing schema_.get_fields().at(fieldId) causes undefined behavior:
 *   - std::out_of_range if the hash table is partially corrupted
 *   - SIGFPE if the hash table's bucket_count becomes 0
 *
 * Fix:
 * - AppendingIndex no longer accesses internal schema_ reference
 * - Caller passes in valid field_meta from current schema
 */
class SchemaReopenTest : public testing::Test {
 protected:
    void
    SetUp() override {
        // Create initial schema V1 with vector field
        schema_v1_ = std::make_shared<Schema>();
        auto vec_fid = schema_v1_->AddDebugField(
            "vec", DataType::VECTOR_FLOAT, 128, knowhere::metric::L2);
        auto pk_fid = schema_v1_->AddDebugField("pk", DataType::INT64);
        schema_v1_->set_primary_field_id(pk_fid);
        schema_v1_->set_schema_version(1);

        // Create schema V2 with additional field
        schema_v2_ = std::make_shared<Schema>();
        schema_v2_->AddDebugField(
            "vec", DataType::VECTOR_FLOAT, 128, knowhere::metric::L2);
        auto pk_fid_v2 = schema_v2_->AddDebugField("pk", DataType::INT64);
        // Added fields must be nullable (enforced by the proxy on
        // AddCollectionField); an absent non-nullable scalar without a
        // default value is rejected by bulk_subscript_not_exist_field.
        schema_v2_->AddDebugField("new_field",
                                  DataType::VARCHAR,
                                  /*nullable=*/true);  // New field in V2
        schema_v2_->set_primary_field_id(pk_fid_v2);
        schema_v2_->set_schema_version(2);
    }

    SchemaPtr schema_v1_;
    SchemaPtr schema_v2_;
};

/**
 * Test: Insert after Reopen should not crash
 *
 * Before fix: This test would crash with std::out_of_range or SIGFPE
 * because IndexingRecord::AppendingIndex() accessed the dangling schema_ reference.
 *
 * After fix: Insert uses caller-provided field_meta, avoiding dangling reference.
 */
TEST_F(SchemaReopenTest, InsertAfterReopenShouldNotCrash) {
    // Step 1: Create segment with schema V1
    auto segment = CreateGrowingSegment(schema_v1_, nullptr);
    auto seg_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(seg_impl, nullptr);

    // Step 2: Insert some data with schema V1
    int N = 100;
    auto data_v1 = DataGen(schema_v1_, N, /*seed=*/42);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    data_v1.row_ids_.data(),
                    data_v1.timestamps_.data(),
                    data_v1.raw_);

    // Step 3: Reopen with schema V2 (this may destroy old Schema object)
    seg_impl->Reopen(schema_v2_);

    // Step 4: Insert more data - this should NOT crash
    // Before fix: This would crash because IndexingRecord::schema_ is dangling
    // After fix: This works because AppendingIndex uses caller-provided field_meta
    auto data_v2 = DataGen(schema_v2_, N, /*seed=*/100);
    segment->PreInsert(N);

    // This insert should complete without crashing
    EXPECT_NO_THROW({
        segment->Insert(N,
                        N,
                        data_v2.row_ids_.data(),
                        data_v2.timestamps_.data(),
                        data_v2.raw_);
    });

    // Verify segment state is valid
    EXPECT_EQ(segment->get_row_count(), 2 * N);
}

/**
 * Test: Concurrent Insert and Reopen should not crash
 *
 * This simulates the chaos testing scenario where Insert and schema updates
 * happen concurrently after pod recovery.
 */
TEST_F(SchemaReopenTest, ConcurrentInsertAndReopenShouldNotCrash) {
    auto segment = CreateGrowingSegment(schema_v1_, nullptr);
    auto seg_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(seg_impl, nullptr);

    std::atomic<bool> stop_flag{false};
    std::atomic<int> insert_count{0};
    std::vector<std::exception_ptr> exceptions;
    std::mutex exceptions_mutex;

    // Thread 1: Continuous inserts
    auto insert_thread = std::thread([&]() {
        int offset = 0;
        while (!stop_flag.load()) {
            try {
                int N = 10;
                auto data = DataGen(schema_v1_, N, offset);
                segment->PreInsert(N);
                segment->Insert(offset,
                                N,
                                data.row_ids_.data(),
                                data.timestamps_.data(),
                                data.raw_);
                offset += N;
                insert_count.fetch_add(1);
            } catch (...) {
                std::lock_guard<std::mutex> lock(exceptions_mutex);
                exceptions.push_back(std::current_exception());
                break;
            }
        }
    });

    // Thread 2: Periodic Reopen
    auto reopen_thread = std::thread([&]() {
        for (int i = 0; i < 5 && !stop_flag.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            try {
                // Alternate between V1 and V2
                if (i % 2 == 0) {
                    seg_impl->Reopen(schema_v2_);
                } else {
                    seg_impl->Reopen(schema_v1_);
                }
            } catch (...) {
                std::lock_guard<std::mutex> lock(exceptions_mutex);
                exceptions.push_back(std::current_exception());
                break;
            }
        }
        stop_flag.store(true);
    });

    insert_thread.join();
    reopen_thread.join();

    // Verify no exceptions were thrown
    EXPECT_TRUE(exceptions.empty())
        << "Concurrent Insert/Reopen caused exceptions";

    // Verify some inserts completed
    EXPECT_GT(insert_count.load(), 0) << "Expected some inserts to complete";
}

/**
 * Test: Field not in IndexingRecord should be handled gracefully
 *
 * When a new field is added via Reopen, it won't exist in IndexingRecord's
 * field_indexings_ map. The code should handle this gracefully.
 */
TEST_F(SchemaReopenTest, NewFieldAfterReopenShouldBeSkipped) {
    auto segment = CreateGrowingSegment(schema_v1_, nullptr);
    auto seg_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(seg_impl, nullptr);

    // Reopen with V2 which has a new field
    seg_impl->Reopen(schema_v2_);

    // Insert data that includes the new field
    int N = 100;
    auto data = DataGen(schema_v2_, N, 42);
    segment->PreInsert(N);

    // This should not crash - new field should be skipped in AppendingIndex
    // because it doesn't exist in field_indexings_
    EXPECT_NO_THROW({
        segment->Insert(
            0, N, data.row_ids_.data(), data.timestamps_.data(), data.raw_);
    });
}

/**
 * Test for Issue #50366: StreamingNode crashes in segcore retrieve when a
 * growing segment is loaded from binlogs that predate an AddField of a
 * nullable vector field.
 *
 * Scenario:
 * - A growing segment is recovered via LoadGrowing after a node restart.
 * - Its binlogs were written before `AddField(new_vec)` so they carry no data
 *   for the new column, while the segment is constructed with the new schema.
 * - Before the fix, the post-load backfill skipped all vector fields, so the
 *   column's validity bitmap stayed empty; FilterVectorValidOffsets then
 *   returned valid_count == count with an EMPTY valid_offsets vector and
 *   bulk_subscript dereferenced the empty vector's data() (nullptr) as the
 *   offsets array -> SIGSEGV.
 *
 * After the fix, FillAbsentFields (called from Load) backfills the validity
 * bitmap of absent nullable vector fields, so the column reads as all-null.
 */
TEST_F(SchemaReopenTest, LoadWithAbsentNullableVectorFieldShouldReadAllNull) {
    // Schema V2 shares the first two fields with V1 (same field ids) and has
    // an extra nullable FLOAT_VECTOR field added by AddField. Added fields must
    // be nullable (enforced by the proxy on AddCollectionField).
    auto schema_v2 = std::make_shared<Schema>();
    schema_v2->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 128, knowhere::metric::L2);
    auto pk_fid = schema_v2->AddDebugField("pk", DataType::INT64);
    auto added_fid = schema_v2->AddDebugField("new_vec",
                                              DataType::VECTOR_FLOAT,
                                              128,
                                              knowhere::metric::L2,
                                              /*nullable=*/true);
    schema_v2->set_primary_field_id(pk_fid);
    schema_v2->set_schema_version(2);

    auto segment = CreateGrowingSegment(schema_v2, milvus::empty_index_meta);
    auto seg_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(seg_impl, nullptr);

    // The binlogs only contain the V1 columns: no data for new_vec.
    int N = 100;
    auto dataset = DataGen(schema_v1_, N, /*seed=*/42);
    LoadGeneratedDataIntoSegment(dataset, seg_impl);
    ASSERT_EQ(segment->get_row_count(), N);

    std::vector<int64_t> offsets(N);
    std::iota(offsets.begin(), offsets.end(), 0);
    milvus::OpContext op_ctx;

    // Load() runs FillAbsentFields to backfill the validity bitmap of the
    // absent nullable vector column; reading it must not crash and every
    // row must read as null.
    seg_impl->FillAbsentFields();
    auto col = seg_impl->bulk_subscript(&op_ctx, added_fid, offsets.data(), N);
    ASSERT_EQ(col->valid_data_size(), N);
    for (int i = 0; i < N; ++i) {
        ASSERT_FALSE(col->valid_data(i)) << "row " << i << " should be null";
    }
    ASSERT_EQ(col->vectors().float_vector().data_size(), 0);

    // WAL replay after recovery delivers inserts written with the old
    // schema; Insert patches the missing column with nulls. The bitmap must
    // stay aligned across the loaded prefix and the replayed tail.
    auto data_v1 = DataGen(schema_v1_, N, /*seed=*/100);
    segment->PreInsert(N);
    segment->Insert(N,
                    N,
                    data_v1.row_ids_.data(),
                    data_v1.timestamps_.data(),
                    data_v1.raw_);
    ASSERT_EQ(segment->get_row_count(), 2 * N);

    std::vector<int64_t> all_offsets(2 * N);
    std::iota(all_offsets.begin(), all_offsets.end(), 0);
    col =
        seg_impl->bulk_subscript(&op_ctx, added_fid, all_offsets.data(), 2 * N);
    ASSERT_EQ(col->valid_data_size(), 2 * N);
    for (int i = 0; i < 2 * N; ++i) {
        ASSERT_FALSE(col->valid_data(i)) << "row " << i << " should be null";
    }
}
