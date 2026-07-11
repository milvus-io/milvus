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
#include <vector>

#include "common/Schema.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;

class SchemaReopenTest : public testing::Test {
 protected:
    void
    SetUp() override {
        // Schema V1: the pre-AddField schema that the loaded binlogs were
        // written with (vector + primary key).
        schema_v1_ = std::make_shared<Schema>();
        schema_v1_->AddDebugField(
            "vec", DataType::VECTOR_FLOAT, 128, knowhere::metric::L2);
        auto pk_fid = schema_v1_->AddDebugField("pk", DataType::INT64);
        schema_v1_->set_primary_field_id(pk_fid);
        schema_v1_->set_schema_version(1);
    }

    SchemaPtr schema_v1_;
};

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

// #50484: Reopen must build the text index for an enable_match field added
// by schema evolution and index the pre-existing rows (nulls here);
// otherwise text_match throws TextIndexNotFound.
TEST_F(SchemaReopenTest, ReopenBuildsTextIndexForNewEnableMatchField) {
    // V1 has no text field, so the constructor builds no text index.
    auto segment = CreateGrowingSegment(schema_v1_, milvus::empty_index_meta);
    auto* seg_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(seg_impl, nullptr);

    int N = 20;
    auto dataset = DataGen(schema_v1_, N, /*seed=*/7);
    auto reserved = segment->PreInsert(N);
    segment->Insert(reserved,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);
    ASSERT_EQ(segment->get_row_count(), N);

    // V2 shares V1's field ids and adds a nullable enable_match VARCHAR.
    auto schema_v2 = std::make_shared<Schema>();
    schema_v2->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 128, knowhere::metric::L2);
    auto pk_fid = schema_v2->AddDebugField("pk", DataType::INT64);
    std::map<std::string, std::string> analyzer_params;
    auto text_fid = schema_v2->AddDebugVarcharField(FieldName("text_content"),
                                                    DataType::VARCHAR,
                                                    /*max_length=*/65535,
                                                    /*nullable=*/true,
                                                    /*enable_match=*/true,
                                                    /*enable_analyzer=*/true,
                                                    analyzer_params,
                                                    std::nullopt);
    schema_v2->set_primary_field_id(pk_fid);
    schema_v2->set_schema_version(2);

    milvus::OpContext op_ctx;
    EXPECT_ANY_THROW(seg_impl->GetTextIndex(&op_ctx, text_fid));

    seg_impl->Reopen(schema_v2);

    ASSERT_NO_THROW(seg_impl->GetTextIndex(&op_ctx, text_fid));
    auto pw = seg_impl->GetTextIndex(&op_ctx, text_fid);
    auto* index = pw.get();
    ASSERT_NE(index, nullptr);

    // No explicit Commit/Reload: Reopen already made the backfill visible.
    // No default value -> all rows null, nothing matches.
    EXPECT_EQ(index->MatchQuery("anything", 1).count(), 0);
    auto not_null = index->IsNotNull();
    ASSERT_EQ(not_null.size(), static_cast<size_t>(N));
    EXPECT_EQ(not_null.count(), 0);
}

// #50484: when the added enable_match field has a default value, Reopen's
// backfill must index the default text for pre-existing rows, matching
// sealed's create-from-raw results.
TEST_F(SchemaReopenTest, ReopenTextIndexIndexesDefaultValueForOldRows) {
    auto segment = CreateGrowingSegment(schema_v1_, milvus::empty_index_meta);
    auto* seg_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(seg_impl, nullptr);

    int N = 20;
    auto dataset = DataGen(schema_v1_, N, /*seed=*/11);
    auto reserved = segment->PreInsert(N);
    segment->Insert(reserved,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);
    ASSERT_EQ(segment->get_row_count(), N);

    auto schema_v2 = std::make_shared<Schema>();
    schema_v2->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 128, knowhere::metric::L2);
    auto pk_fid = schema_v2->AddDebugField("pk", DataType::INT64);
    std::map<std::string, std::string> analyzer_params;
    DefaultValueType default_value;
    default_value.set_string_data("sample default text");
    auto text_fid =
        schema_v2->AddDebugVarcharField(FieldName("text_content"),
                                        DataType::VARCHAR,
                                        /*max_length=*/65535,
                                        /*nullable=*/true,
                                        /*enable_match=*/true,
                                        /*enable_analyzer=*/true,
                                        analyzer_params,
                                        std::make_optional(default_value));
    schema_v2->set_primary_field_id(pk_fid);
    schema_v2->set_schema_version(2);

    seg_impl->Reopen(schema_v2);

    milvus::OpContext op_ctx;
    auto pw = seg_impl->GetTextIndex(&op_ctx, text_fid);
    auto* index = pw.get();
    ASSERT_NE(index, nullptr);

    // No explicit Commit/Reload: every old row carries the default text.
    EXPECT_EQ(index->MatchQuery("default", 1).count(), static_cast<size_t>(N));
    EXPECT_EQ(index->MatchQuery("absent-token", 1).count(), 0);
    auto not_null = index->IsNotNull();
    ASSERT_EQ(not_null.size(), static_cast<size_t>(N));
    EXPECT_EQ(not_null.count(), static_cast<size_t>(N));
}

// #50484: one Reopen may add several enable_match fields; the staged indexes
// are published together, so every field must come out complete.
TEST_F(SchemaReopenTest, ReopenBuildsTextIndexesForMultipleNewFields) {
    auto segment = CreateGrowingSegment(schema_v1_, milvus::empty_index_meta);
    auto* seg_impl = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(seg_impl, nullptr);

    int N = 20;
    auto dataset = DataGen(schema_v1_, N, /*seed=*/13);
    auto reserved = segment->PreInsert(N);
    segment->Insert(reserved,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);
    ASSERT_EQ(segment->get_row_count(), N);

    auto schema_v2 = std::make_shared<Schema>();
    schema_v2->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 128, knowhere::metric::L2);
    auto pk_fid = schema_v2->AddDebugField("pk", DataType::INT64);
    std::map<std::string, std::string> analyzer_params;
    auto null_fid = schema_v2->AddDebugVarcharField(FieldName("text_null"),
                                                    DataType::VARCHAR,
                                                    /*max_length=*/65535,
                                                    /*nullable=*/true,
                                                    /*enable_match=*/true,
                                                    /*enable_analyzer=*/true,
                                                    analyzer_params,
                                                    std::nullopt);
    DefaultValueType default_value;
    default_value.set_string_data("sample default text");
    auto default_fid =
        schema_v2->AddDebugVarcharField(FieldName("text_default"),
                                        DataType::VARCHAR,
                                        /*max_length=*/65535,
                                        /*nullable=*/true,
                                        /*enable_match=*/true,
                                        /*enable_analyzer=*/true,
                                        analyzer_params,
                                        std::make_optional(default_value));
    schema_v2->set_primary_field_id(pk_fid);
    schema_v2->set_schema_version(2);

    seg_impl->Reopen(schema_v2);

    milvus::OpContext op_ctx;
    auto null_pw = seg_impl->GetTextIndex(&op_ctx, null_fid);
    ASSERT_NE(null_pw.get(), nullptr);
    EXPECT_EQ(null_pw.get()->MatchQuery("anything", 1).count(), 0);
    EXPECT_EQ(null_pw.get()->IsNotNull().count(), 0);

    auto default_pw = seg_impl->GetTextIndex(&op_ctx, default_fid);
    ASSERT_NE(default_pw.get(), nullptr);
    EXPECT_EQ(default_pw.get()->MatchQuery("default", 1).count(),
              static_cast<size_t>(N));
    EXPECT_EQ(default_pw.get()->IsNotNull().count(), static_cast<size_t>(N));
}
