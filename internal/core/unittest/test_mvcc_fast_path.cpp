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
#include <cstdint>
#include <memory>
#include <vector>

#include "common/Schema.h"
#include "exec/QueryContext.h"
#include "exec/Task.h"
#include "plan/PlanNode.h"
#include "segcore/SegmentSealed.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/Types.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::exec;
using namespace milvus::segcore;

class MvccFastPathTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        schema_ = std::make_shared<Schema>();
        vec_fid_ = schema_->AddDebugField(
            "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
        int64_fid_ = schema_->AddDebugField("counter", DataType::INT64);
        schema_->set_primary_field_id(int64_fid_);
    }

    // Helper: create sealed segment with no deletes
    SegmentSealedSPtr
    CreateSealedSegment() {
        auto raw_data = DataGen(schema_, N_);
        auto segment = CreateSealedWithFieldDataLoaded(schema_, raw_data);
        return SegmentSealedSPtr(segment.release());
    }

    // Helper: create sealed segment with some rows deleted
    SegmentSealedSPtr
    CreateSealedSegmentWithDeletes(int64_t num_deletes) {
        auto raw_data = DataGen(schema_, N_);
        auto segment = CreateSealedWithFieldDataLoaded(schema_, raw_data);

        std::vector<idx_t> pks;
        for (int64_t i = 0; i < num_deletes; i++) {
            pks.push_back(i);
        }
        auto ids = std::make_unique<IdArray>();
        ids->mutable_int_id()->mutable_data()->Add(pks.begin(), pks.end());
        std::vector<Timestamp> timestamps(num_deletes, 10);

        LoadDeletedRecordInfo info = {
            timestamps.data(), ids.get(), num_deletes};
        segment->LoadDeletedRecord(info);

        return SegmentSealedSPtr(segment.release());
    }

    // Helper: execute MvccNode-only plan and return results
    // MvccNode as source (no upstream FilterBitsNode) = no scalar filter
    struct MvccResult {
        int64_t num_rows;
        bool all_rows_visible;
        RowVectorPtr output;
    };

    MvccResult
    RunMvccPlan(const SegmentInternalInterface* segment,
                Timestamp collection_ttl = 0,
                Timestamp query_timestamp = MAX_TIMESTAMP) {
        // Build plan: just MvccNode as source (empty sources)
        auto mvcc_node = std::make_shared<plan::MvccNode>("mvcc_1");
        auto plan = plan::PlanFragment(mvcc_node);

        auto query_context = std::make_shared<QueryContext>(
            "test_mvcc",
            segment,
            N_,
            query_timestamp,
            collection_ttl,
            0,
            query::PlanOptions{false},
            std::make_shared<QueryConfig>(
                std::unordered_map<std::string, std::string>{}));

        auto task = Task::Create("task_mvcc", plan, 0, query_context);
        MvccResult result{0, false, nullptr};
        for (;;) {
            auto output = task->Next();
            if (!output) {
                break;
            }
            result.num_rows += output->size();
            result.output = output;
        }
        result.all_rows_visible = query_context->get_all_rows_visible();
        return result;
    }

    SchemaPtr schema_;
    FieldId vec_fid_;
    FieldId int64_fid_;
    int64_t N_ = 1000;
};

// ---------------------------------------------------------------------------
// Level 1: Sealed + no deletes + no TTL + source node
// Expected: all_rows_visible = true, output is all-zero bitmap
// ---------------------------------------------------------------------------
TEST_F(MvccFastPathTest, Level1_SealedNoDeletes_SkipFilter) {
    auto segment = CreateSealedSegment();
    auto result = RunMvccPlan(segment.get());

    EXPECT_EQ(result.num_rows, N_);
    EXPECT_TRUE(result.all_rows_visible)
        << "Level 1: sealed + no deletes should set all_rows_visible=true";

    // Verify output bitmap is all zeros (no rows filtered out)
    ASSERT_NE(result.output, nullptr);
    auto col = std::dynamic_pointer_cast<ColumnVector>(result.output->child(0));
    ASSERT_NE(col, nullptr);
    TargetBitmapView view(col->GetRawData(), col->size());
    EXPECT_EQ(view.count(), 0)
        << "Level 1: bitmap should be all zeros (no filtering)";
}

// ---------------------------------------------------------------------------
// Level 2: Sealed + has deletes + no TTL + source node
// Expected: all_rows_visible = false, output has delete mask applied
// ---------------------------------------------------------------------------
TEST_F(MvccFastPathTest, Level2_SealedWithDeletes_DeleteMaskOnly) {
    int64_t num_deletes = 5;
    auto segment = CreateSealedSegmentWithDeletes(num_deletes);
    auto result = RunMvccPlan(segment.get());

    EXPECT_EQ(result.num_rows, N_);
    EXPECT_FALSE(result.all_rows_visible)
        << "Level 2: sealed + has deletes should NOT set all_rows_visible";

    // Verify output bitmap has some bits set (deleted rows marked)
    ASSERT_NE(result.output, nullptr);
    auto col = std::dynamic_pointer_cast<ColumnVector>(result.output->child(0));
    ASSERT_NE(col, nullptr);
    TargetBitmapView view(col->GetRawData(), col->size());
    EXPECT_GT(view.count(), 0)
        << "Level 2: bitmap should have deleted rows marked";
}

// ---------------------------------------------------------------------------
// Level 3: Sealed + TTL set -> falls through to default path
// Expected: all_rows_visible = false
// ---------------------------------------------------------------------------
TEST_F(MvccFastPathTest, Level3_SealedWithTTL_DefaultPath) {
    auto segment = CreateSealedSegment();
    // Pass non-zero collection_ttl to force Level 3
    auto result = RunMvccPlan(segment.get(), /*collection_ttl=*/100);

    EXPECT_EQ(result.num_rows, N_);
    EXPECT_FALSE(result.all_rows_visible)
        << "Level 3: TTL set should NOT trigger fast path";
}

// ---------------------------------------------------------------------------
// Level 3: Growing segment -> falls through to default path
// Expected: all_rows_visible = false
// ---------------------------------------------------------------------------
TEST_F(MvccFastPathTest, Level3_GrowingSegment_DefaultPath) {
    auto raw_data = DataGen(schema_, N_);
    auto segment = CreateGrowingSegment(schema_, empty_index_meta);
    segment->PreInsert(N_);
    segment->Insert(0,
                    N_,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    raw_data.raw_);

    // Build plan manually for growing segment
    auto mvcc_node = std::make_shared<plan::MvccNode>("mvcc_1");
    auto plan = plan::PlanFragment(mvcc_node);

    auto query_context = std::make_shared<QueryContext>(
        "test_mvcc_growing",
        segment.get(),
        N_,
        MAX_TIMESTAMP,
        0,
        0,
        query::PlanOptions{false},
        std::make_shared<QueryConfig>(
            std::unordered_map<std::string, std::string>{}));

    auto task = Task::Create("task_mvcc_growing", plan, 0, query_context);
    int64_t num_rows = 0;
    for (;;) {
        auto output = task->Next();
        if (!output) {
            break;
        }
        num_rows += output->size();
    }
    EXPECT_EQ(num_rows, N_);
    EXPECT_FALSE(query_context->get_all_rows_visible())
        << "Growing segment should NOT trigger fast path";
}

// ---------------------------------------------------------------------------
// QueryContext all_rows_visible flag: default is false
// ---------------------------------------------------------------------------
TEST_F(MvccFastPathTest, QueryContext_SkipFilter_DefaultFalse) {
    auto segment = CreateSealedSegment();
    auto query_context = std::make_shared<QueryContext>(
        "test_default", segment.get(), N_, MAX_TIMESTAMP);
    EXPECT_FALSE(query_context->get_all_rows_visible())
        << "all_rows_visible should default to false";
}

// ---------------------------------------------------------------------------
// QueryContext all_rows_visible flag: set/get round-trip
// ---------------------------------------------------------------------------
TEST_F(MvccFastPathTest, QueryContext_SkipFilter_SetGet) {
    auto segment = CreateSealedSegment();
    auto query_context = std::make_shared<QueryContext>(
        "test_setget", segment.get(), N_, MAX_TIMESTAMP);
    query_context->set_all_rows_visible(true);
    EXPECT_TRUE(query_context->get_all_rows_visible());
    query_context->set_all_rows_visible(false);
    EXPECT_FALSE(query_context->get_all_rows_visible());
}

// ---------------------------------------------------------------------------
// Timestamp guard: query_timestamp < max_insert_timestamp -> Level 3 fallback
// Expected: all_rows_visible = false (fast path NOT taken)
// ---------------------------------------------------------------------------
TEST_F(MvccFastPathTest, TimestampGuard_OldQueryTs_FallsBackToLevel3) {
    auto segment = CreateSealedSegment();

    // Use query_timestamp = 1, which is less than any insert timestamp
    // generated by DataGen (timestamps start from 0 and go up to N_-1)
    // With the guard, this should fall through to Level 3 (default path)
    auto result = RunMvccPlan(segment.get(),
                              /*collection_ttl=*/0,
                              /*query_timestamp=*/1);

    EXPECT_EQ(result.num_rows, N_);
    EXPECT_FALSE(result.all_rows_visible)
        << "query_timestamp < max_insert_timestamp should NOT trigger fast "
           "path";
}

// ---------------------------------------------------------------------------
// Timestamp guard: query_timestamp >= max_insert_timestamp -> Level 1
// Expected: all_rows_visible = true (fast path taken)
// ---------------------------------------------------------------------------
TEST_F(MvccFastPathTest, TimestampGuard_CurrentQueryTs_TakesFastPath) {
    auto segment = CreateSealedSegment();

    // Use MAX_TIMESTAMP, which is always >= max_insert_timestamp
    auto result = RunMvccPlan(segment.get(),
                              /*collection_ttl=*/0,
                              /*query_timestamp=*/MAX_TIMESTAMP);

    EXPECT_EQ(result.num_rows, N_);
    EXPECT_TRUE(result.all_rows_visible)
        << "query_timestamp >= max_insert_timestamp should trigger fast path";
}

// ---------------------------------------------------------------------------
// Regression: sequential Level 1 queries must not share mutable bitmap state.
// A downstream operator (e.g. ElementFilterBitsNode) may flip() the bitmap
// in-place.  The second query must still see a clean all-zero bitmap.
// ---------------------------------------------------------------------------
TEST_F(MvccFastPathTest, Level1_NoCachePollution_SequentialQueries) {
    auto segment = CreateSealedSegment();

    // First query – Level 1
    auto result1 = RunMvccPlan(segment.get());
    ASSERT_TRUE(result1.all_rows_visible);
    auto col1 =
        std::dynamic_pointer_cast<ColumnVector>(result1.output->child(0));
    ASSERT_NE(col1, nullptr);
    TargetBitmapView view1(col1->GetRawData(), col1->size());
    EXPECT_EQ(view1.count(), 0);

    // Simulate downstream mutation (ElementFilterBitsNode does doc_bitset.flip)
    view1.flip();
    EXPECT_EQ(view1.count(), N_);

    // Second query on the same thread – must NOT see the flipped bits
    auto result2 = RunMvccPlan(segment.get());
    ASSERT_TRUE(result2.all_rows_visible);
    auto col2 =
        std::dynamic_pointer_cast<ColumnVector>(result2.output->child(0));
    ASSERT_NE(col2, nullptr);
    TargetBitmapView view2(col2->GetRawData(), col2->size());
    EXPECT_EQ(view2.count(), 0)
        << "Second query must return clean bitmap, not polluted cache";
}
