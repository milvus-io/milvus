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

#include <cstdint>
#include <memory>
#include <numeric>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/Types.h"
#include "exec/expression/ConjunctExpr.h"
#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"
#include "index/ScalarIndexSort.h"
#include "knowhere/comp/index_param.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegcoreConfig.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::exec;
using namespace milvus::segcore;

namespace {

std::unique_ptr<SegmentSealed>
CreateTwoChunkSealed(const SchemaPtr& schema,
                     const GeneratedData& first,
                     const GeneratedData& second) {
    std::unordered_map<int64_t, std::vector<FieldDataPtr>> field_chunks;

    auto append_dataset = [&](const GeneratedData& dataset) {
        const auto row_count = dataset.row_ids_.size();

        auto row_ids =
            std::make_shared<FieldData<int64_t>>(DataType::INT64, false);
        row_ids->FillFieldData(dataset.row_ids_.data(), row_count);
        field_chunks[RowFieldID.get()].push_back(std::move(row_ids));

        auto timestamps =
            std::make_shared<FieldData<int64_t>>(DataType::INT64, false);
        timestamps->FillFieldData(dataset.timestamps_.data(), row_count);
        field_chunks[TimestampFieldID.get()].push_back(std::move(timestamps));

        const auto fields = schema->get_fields();
        for (const auto& data : dataset.raw_->fields_data()) {
            const auto field_id = data.field_id();
            field_chunks[field_id].push_back(CreateFieldDataFromDataArray(
                row_count, &data, fields.at(FieldId(field_id))));
        }
    };

    append_dataset(first);
    append_dataset(second);

    LoadFieldDataInfo combined_load_info;
    auto cm = storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    for (auto& [field_id, chunks] : field_chunks) {
        auto field_load_info = PrepareSingleFieldInsertBinlog(kCollectionID,
                                                              kPartitionID,
                                                              kSegmentID,
                                                              field_id,
                                                              std::move(chunks),
                                                              cm);
        combined_load_info.field_infos.merge(field_load_info.field_infos);
    }

    auto segment = CreateSealedSegment(schema, empty_index_meta);
    const auto status = LoadFieldData(segment.get(), &combined_load_info);
    AssertInfo(status.error_code == Success,
               "Failed to load two-chunk sealed data: {}",
               status.error_msg);
    return segment;
}

void
SetInt64FieldData(GeneratedData& dataset,
                  FieldId field_id,
                  const std::vector<int64_t>& values) {
    AssertInfo(dataset.raw_->num_rows() == values.size(),
               "values size must match row count");
    for (int i = 0; i < dataset.raw_->fields_data_size(); ++i) {
        auto* field_data = dataset.raw_->mutable_fields_data(i);
        if (field_data->field_id() != field_id.get()) {
            continue;
        }
        auto* data =
            field_data->mutable_scalars()->mutable_long_data()->mutable_data();
        data->Clear();
        data->Add(values.data(), values.data() + values.size());
        return;
    }
    ThrowInfo(FieldIDInvalid, "field id {} not found", field_id.get());
}

void
SetFloatFieldData(GeneratedData& dataset,
                  FieldId field_id,
                  const std::vector<float>& values) {
    AssertInfo(dataset.raw_->num_rows() == values.size(),
               "values size must match row count");
    for (int i = 0; i < dataset.raw_->fields_data_size(); ++i) {
        auto* field_data = dataset.raw_->mutable_fields_data(i);
        if (field_data->field_id() != field_id.get()) {
            continue;
        }
        auto* data =
            field_data->mutable_scalars()->mutable_float_data()->mutable_data();
        data->Clear();
        data->Add(values.data(), values.data() + values.size());
        return;
    }
    ThrowInfo(FieldIDInvalid, "field id {} not found", field_id.get());
}

template <typename T>
void
VerifySkipCursorContract(SegmentExpr& segment_expr,
                         OffsetVector* input,
                         int skipped_chunk_id) {
    TargetBitmap bitmap_input(input->size(), false);
    for (size_t i = input->size() / 2; i < input->size(); ++i) {
        bitmap_input[i] = true;
    }

    auto skip_chunk = [skipped_chunk_id](
                          const SkipIndex&, FieldId, int chunk_id) {
        return chunk_id == skipped_chunk_id;
    };

    int64_t processed_cursor = 0;
    int64_t null_rows = 0;
    auto evaluate_batch = [&]<FilterType filter_type = FilterType::sequential>(
        const T* data,
        const bool* valid_data,
        const int32_t* offsets,
        const int size,
        TargetBitmapView res,
        TargetBitmapView valid_res) {
        if (data == nullptr) {
            null_rows += size;
            processed_cursor += size;
            return;
        }
        for (int i = 0; i < size; ++i) {
            if (bitmap_input[processed_cursor + i]) {
                res[i] = true;
            }
        }
        processed_cursor += size;
    };

    TargetBitmap res(input->size(), false);
    TargetBitmap valid(input->size(), true);
    const auto processed =
        segment_expr.ProcessDataByOffsets<T>(evaluate_batch,
                                             skip_chunk,
                                             input,
                                             TargetBitmapView(res),
                                             TargetBitmapView(valid));

    EXPECT_EQ(processed, int64_t(input->size()));
    EXPECT_EQ(processed_cursor, int64_t(input->size()));
    EXPECT_EQ(null_rows, int64_t(input->size() / 2));
    for (size_t i = 0; i < input->size(); ++i) {
        EXPECT_EQ(bool(res[i]), i >= input->size() / 2)
            << "candidate " << i
            << ": cursor desync shifted the bitmap_input read";
    }
}

void
VerifyElementFullScanSkipCursor(SegmentExpr& segment_expr,
                                int64_t element_count,
                                int skipped_chunk_id,
                                int64_t expected_skipped_elements) {
    TargetBitmap bitmap_input(element_count, false);
    for (int64_t i = expected_skipped_elements; i < element_count; ++i) {
        bitmap_input[i] = true;
    }

    auto skip_chunk = [skipped_chunk_id](
                          const SkipIndex&, FieldId, int chunk_id) {
        return chunk_id == skipped_chunk_id;
    };

    int64_t processed_cursor = 0;
    int64_t null_elements = 0;
    auto evaluate_batch = [&]<FilterType filter_type = FilterType::sequential>(
        const int64_t* data,
        const bool* valid_data,
        const int32_t* offsets,
        const int size,
        TargetBitmapView res,
        TargetBitmapView valid_res) {
        if (data == nullptr) {
            null_elements += size;
            processed_cursor += size;
            return;
        }
        for (int i = 0; i < size; ++i) {
            if (bitmap_input[processed_cursor + i]) {
                res[i] = true;
            }
        }
        processed_cursor += size;
    };

    TargetBitmap res(element_count, false);
    TargetBitmap valid(element_count, true);
    const auto processed =
        segment_expr.ProcessDataChunksForElementLevel<int64_t>(
            evaluate_batch,
            skip_chunk,
            TargetBitmapView(res),
            TargetBitmapView(valid));

    EXPECT_EQ(processed, element_count);
    EXPECT_EQ(processed_cursor, element_count);
    EXPECT_EQ(null_elements, expected_skipped_elements);
    for (int64_t i = 0; i < element_count; ++i) {
        EXPECT_EQ(bool(res[i]), i >= expected_skipped_elements)
            << "element " << i
            << ": skipped chunk desynced the full-scan bitmap cursor";
        EXPECT_TRUE(valid[i]);
    }
}

void
VerifyNullableElementFullScanLogicalCount(
    const SegmentInternalInterface* segment,
    FieldId array_fid,
    int64_t row_count,
    int skipped_chunk_id,
    int64_t expected_skipped_elements,
    int64_t expected_live_elements) {
    auto query_context = std::make_shared<QueryContext>(
        DEAFULT_QUERY_ID, segment, row_count, MAX_TIMESTAMP);
    auto array_offsets = segment->GetArrayOffsets(array_fid);
    ASSERT_NE(array_offsets, nullptr);
    ASSERT_EQ(array_offsets->GetTotalElementCount(),
              expected_skipped_elements + expected_live_elements);

    // DataGen keeps a physical payload for nullable ARRAY rows in growing
    // storage even when the row validity bit is false. Sealed binlog
    // serialization may discard that payload, but ArrayOffsets maps the NULL
    // row to zero logical elements in either layout.
    if (segment->type() == SegmentType::Sealed) {
        auto pw = segment->get_batch_views<ArrayView>(
            query_context->get_op_context(), array_fid, 0, 0, 2);
        const auto& [data, valid] = pw.get();
        ASSERT_EQ(data.size(), 2);
        ASSERT_EQ(valid.size(), 2);
        EXPECT_FALSE(valid[1]);
    } else {
        auto pw = segment->chunk_data<Array>(
            query_context->get_op_context(), array_fid, 0);
        auto chunk = pw.get();
        ASSERT_EQ(chunk.row_count(), 2);
        ASSERT_NE(chunk.valid_data(), nullptr);
        EXPECT_FALSE(chunk.valid_data()[1]);
        EXPECT_EQ(chunk.data()[1].length(), 2);
    }
    const auto null_row_range = array_offsets->ElementIDRangeOfRow(1);
    EXPECT_EQ(null_row_range.first, null_row_range.second);

    SegmentExpr segment_expr(std::vector<ExprPtr>{},
                             "nullable element full-scan probe",
                             query_context->get_op_context(),
                             segment,
                             array_fid,
                             std::vector<std::string>{},
                             DataType::INT64,
                             row_count,
                             row_count,
                             query_context->get_consistency_level());

    auto skip_chunk = [skipped_chunk_id](
                          const SkipIndex&, FieldId, int chunk_id) {
        return chunk_id == skipped_chunk_id;
    };
    int64_t processed_cursor = 0;
    int64_t null_elements = 0;
    int64_t live_elements = 0;
    auto evaluate_batch = [&]<FilterType filter_type = FilterType::sequential>(
        const int64_t* data,
        const bool* valid_data,
        const int32_t* offsets,
        const int size,
        TargetBitmapView res,
        TargetBitmapView valid_res) {
        processed_cursor += size;
        if (data == nullptr) {
            null_elements += size;
            return;
        }
        live_elements += size;
        res.set(0, size, true);
    };

    const auto logical_elements = array_offsets->GetTotalElementCount();
    TargetBitmap res(logical_elements, false);
    TargetBitmap valid(logical_elements, true);
    const auto processed =
        segment_expr.ProcessDataChunksForElementLevel<int64_t>(
            evaluate_batch,
            skip_chunk,
            TargetBitmapView(res),
            TargetBitmapView(valid));

    EXPECT_EQ(processed, logical_elements);
    EXPECT_EQ(processed_cursor, logical_elements);
    EXPECT_EQ(null_elements, expected_skipped_elements);
    EXPECT_EQ(live_elements, expected_live_elements);
    for (int64_t i = 0; i < logical_elements; ++i) {
        EXPECT_EQ(bool(res[i]), i >= expected_skipped_elements);
        EXPECT_TRUE(valid[i]);
    }
}

}  // namespace

// ---------------------------------------------------------------------------
// Every logical row or element must advance the evaluator callback cursor.
// SkipIndex-pruned candidates and nullable scalar-index reverse lookups arrive
// as null batches (data == nullptr). The tests cover growing and sealed raw
// offsets, scalar-index-only offsets, element offsets, and element full scans.
// ---------------------------------------------------------------------------

class OffsetsEvalCorrectnessTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        schema_ = std::make_shared<Schema>();
        schema_->AddDebugField(
            "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
        auto pk_fid = schema_->AddDebugField("pk", DataType::INT64);
        i64_fid_ = schema_->AddDebugField("value", DataType::INT64);
        varchar_fid_ = schema_->AddDebugField("text", DataType::VARCHAR);
        array_fid_ = schema_->AddDebugArrayField(
            "structA[element_value]", DataType::INT64, false);
        vector_array_fid_ =
            schema_->AddDebugVectorArrayField("structA[vector_array]",
                                              DataType::VECTOR_FLOAT,
                                              4,
                                              knowhere::metric::L2);
        schema_->set_primary_field_id(pk_fid);

        // Keep one element per array row so element IDs equal row offsets in
        // the direct ProcessElementLevelByOffsets regression below.
        auto dataset = DataGen(schema_, N, 42, 0, 1, 1);
        // Growing segment with small chunks so one offset batch spans
        // multiple chunks (required to interleave skipped and probed rows).
        SegcoreConfig config = SegcoreConfig::default_config();
        config.set_chunk_rows(kChunkRows);
        growing_ = CreateGrowingSegment(schema_, empty_index_meta, 1, config);
        growing_->PreInsert(N);
        growing_->Insert(0,
                         N,
                         dataset.row_ids_.data(),
                         dataset.timestamps_.data(),
                         dataset.raw_);
        ASSERT_NE(growing_->GetArrayOffsets(array_fid_), nullptr);

        auto sealed_first = DataGen(schema_, N / 2, 43, 0, 1, 1);
        auto sealed_second = DataGen(schema_, N / 2, 44, 0, 1, 1);
        sealed_ = CreateTwoChunkSealed(schema_, sealed_first, sealed_second);
        ASSERT_NE(sealed_->GetArrayOffsets(array_fid_), nullptr);
    }

    std::shared_ptr<SegmentExpr>
    MakeDirectSegmentExpr(const SegmentInternalInterface* segment,
                          FieldId field_id,
                          DataType value_type,
                          QueryContext& query_context) {
        return std::make_shared<SegmentExpr>(
            std::vector<ExprPtr>{},
            "offset contract probe",
            query_context.get_op_context(),
            segment,
            field_id,
            std::vector<std::string>{},
            value_type,
            N,
            N,
            query_context.get_consistency_level());
    }

    static constexpr int64_t kChunkRows = 8;
    static constexpr size_t N = 32;  // 4 growing chunks

    SchemaPtr schema_;
    FieldId i64_fid_, varchar_fid_, array_fid_, vector_array_fid_;
    SegmentGrowingPtr growing_;
    std::unique_ptr<SegmentSealed> sealed_;
};

// Every candidate must reach the callback: skipped rows as null batches.
TEST_F(OffsetsEvalCorrectnessTest, SkipBranchDrivesCallbackPerCandidate) {
    auto query_context = std::make_shared<QueryContext>(
        DEAFULT_QUERY_ID, growing_.get(), N, MAX_TIMESTAMP);
    auto seg_expr = MakeDirectSegmentExpr(
        growing_.get(), i64_fid_, DataType::INT64, *query_context);

    // Interleave chunk-0 rows (skipped below) with rows of other chunks.
    OffsetVector input;
    for (auto o : std::vector<int32_t>{8, 0, 9, 1, 16, 2, 24, 3}) {
        input.emplace_back(o);
    }
    const int expected_skipped = 4;  // offsets 0,1,2,3 live in chunk 0

    auto skip_chunk0 = [](const milvus::SkipIndex&,
                          FieldId,
                          int chunk_id) -> bool { return chunk_id == 0; };

    int64_t rows_seen = 0;
    int64_t null_rows = 0;
    int64_t data_rows = 0;
    auto probe = [&]<FilterType filter_type = FilterType::sequential>(
        const int64_t* data,
        const bool* valid_data,
        const int32_t* offsets,
        const int size,
        TargetBitmapView res,
        TargetBitmapView valid_res) {
        rows_seen += size;
        if (data == nullptr) {
            null_rows += size;
            return;
        }
        data_rows += size;
    };

    TargetBitmap res(input.size(), false);
    TargetBitmap valid(input.size(), true);
    auto processed =
        seg_expr->ProcessDataByOffsets<int64_t>(probe,
                                                skip_chunk0,
                                                &input,
                                                TargetBitmapView(res),
                                                TargetBitmapView(valid));

    EXPECT_EQ(processed, int64_t(input.size()));
    // The contract this fix pins: no candidate row may bypass the callback.
    EXPECT_EQ(rows_seen, int64_t(input.size()))
        << "skipped rows must still reach the callback as null batches";
    EXPECT_EQ(null_rows, expected_skipped);
    EXPECT_EQ(data_rows, int64_t(input.size()) - expected_skipped);
}

// The user-visible consequence: emulate the exact cursor-tracking pattern the
// production callbacks use (processed_cursor indexes bitmap_input by batch
// position) and prove a skipped span does not shift later reads.
TEST_F(OffsetsEvalCorrectnessTest, SkipBranchKeepsBitmapCursorAligned) {
    auto query_context = std::make_shared<QueryContext>(
        DEAFULT_QUERY_ID, growing_.get(), N, MAX_TIMESTAMP);
    auto seg_expr = MakeDirectSegmentExpr(
        growing_.get(), i64_fid_, DataType::INT64, *query_context);

    // First 4 candidates in chunk 0 (skipped), next 4 in chunk 1 (probed).
    OffsetVector input;
    for (auto o : std::vector<int32_t>{0, 1, 2, 3, 8, 9, 10, 11}) {
        input.emplace_back(o);
    }
    // bitmap_input by BATCH POSITION: the AND conjunct passed bit=1 exactly
    // for the probed candidates.
    TargetBitmap bitmap_input(input.size(), false);
    for (size_t i = 4; i < 8; ++i) {
        bitmap_input[i] = true;
    }

    auto skip_chunk0 = [](const milvus::SkipIndex&,
                          FieldId,
                          int chunk_id) -> bool { return chunk_id == 0; };

    // Mirror of the production pattern (e.g. UnaryExpr): a closure-local
    // processed_cursor advanced by every invocation, including null batches.
    int64_t processed_cursor = 0;
    auto probe = [&]<FilterType filter_type = FilterType::sequential>(
        const int64_t* data,
        const bool* valid_data,
        const int32_t* offsets,
        const int size,
        TargetBitmapView res,
        TargetBitmapView valid_res) {
        if (data == nullptr) {
            processed_cursor += size;
            return;
        }
        for (int i = 0; i < size; ++i) {
            if (bitmap_input[processed_cursor + i]) {
                res[i] = true;  // probe passes for every real value here
            }
        }
        processed_cursor += size;
    };

    TargetBitmap res(input.size(), false);
    TargetBitmap valid(input.size(), true);
    seg_expr->ProcessDataByOffsets<int64_t>(probe,
                                            skip_chunk0,
                                            &input,
                                            TargetBitmapView(res),
                                            TargetBitmapView(valid));

    for (size_t k = 0; k < input.size(); ++k) {
        const bool expected = k >= 4;  // skipped rows false, probed rows true
        EXPECT_EQ(bool(res[k]), expected)
            << "candidate " << k
            << ": cursor desync shifted the bitmap_input read";
    }
}

// Element-level filters use a separate offsets helper. It must invoke the
// evaluator for skipped candidates too, or its bitmap cursor shifts exactly as
// it did in the scalar offset path.
TEST_F(OffsetsEvalCorrectnessTest,
       ElementLevelSkipBranchKeepsBitmapCursorAligned) {
    auto query_context = std::make_shared<QueryContext>(
        DEAFULT_QUERY_ID, growing_.get(), N, MAX_TIMESTAMP);
    auto seg_expr = MakeDirectSegmentExpr(
        growing_.get(), array_fid_, DataType::INT64, *query_context);

    // One element per row: element IDs 0..3 are in skipped chunk 0; 8..11
    // are in chunk 1 and must retain their batch-position bitmap bits.
    OffsetVector element_ids;
    for (auto id : std::vector<int32_t>{0, 1, 2, 3, 8, 9, 10, 11}) {
        element_ids.emplace_back(id);
    }
    TargetBitmap bitmap_input(element_ids.size(), false);
    for (size_t i = 4; i < element_ids.size(); ++i) {
        bitmap_input[i] = true;
    }

    auto skip_chunk0 = [](const milvus::SkipIndex&,
                          FieldId,
                          int chunk_id) -> bool { return chunk_id == 0; };

    int64_t processed_cursor = 0;
    int64_t null_batches = 0;
    auto evaluate_batch = [&]<FilterType filter_type = FilterType::sequential>(
        const int64_t* data,
        const bool* valid_data,
        const int32_t* offsets,
        const int size,
        TargetBitmapView res,
        TargetBitmapView valid_res) {
        if (data == nullptr) {
            null_batches += size;
            processed_cursor += size;
            return;
        }
        for (int i = 0; i < size; ++i) {
            if (bitmap_input[processed_cursor + i]) {
                res[i] = true;
            }
        }
        processed_cursor += size;
    };

    TargetBitmap res(element_ids.size(), false);
    TargetBitmap valid(element_ids.size(), true);
    const auto processed = seg_expr->ProcessElementLevelByOffsets<int64_t>(
        evaluate_batch,
        skip_chunk0,
        &element_ids,
        TargetBitmapView(res),
        TargetBitmapView(valid));

    EXPECT_EQ(processed, int64_t(element_ids.size()));
    EXPECT_EQ(null_batches, 4);
    for (size_t i = 0; i < element_ids.size(); ++i) {
        EXPECT_EQ(bool(res[i]), i >= 4)
            << "element candidate " << i
            << ": cursor desync shifted the bitmap_input read";
    }
}

// Sealed raw data is chunked by binlog. Cover the fixed-width branch with
// candidates from a skipped first binlog followed by a probed second binlog.
TEST_F(OffsetsEvalCorrectnessTest,
       SealedScalarSkipBranchKeepsBitmapCursorAligned) {
    auto query_context = std::make_shared<QueryContext>(
        DEAFULT_QUERY_ID, sealed_.get(), N, MAX_TIMESTAMP);
    auto seg_expr = MakeDirectSegmentExpr(
        sealed_.get(), i64_fid_, DataType::INT64, *query_context);
    OffsetVector offsets;
    for (auto offset : std::vector<int32_t>{0, 1, 2, 3, 16, 17, 18, 19}) {
        offsets.emplace_back(offset);
    }

    VerifySkipCursorContract<int64_t>(*seg_expr, &offsets, 0);
}

// Exercise the production expression stack used by iterative filtering:
// logical AND -> PhyConjunctFilterExpr -> offset-input Unary evaluators.  The
// first predicate creates bitmap_input=[0,0,1,0].  The second predicate is
// pruned by real SkipIndex statistics for chunk 0; it must still advance past
// the first two candidates before reading bitmap_input for chunk 1.
TEST(OffsetsEvalProductionRegressionTest,
     ConjunctOffsetInputKeepsMatchAfterRealSkipIndexPruning) {
    constexpr int64_t kChunkRows = 4;
    constexpr int64_t kRowCount = kChunkRows * 2;

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto gate_fid = schema->AddDebugField("gate", DataType::INT64);
    auto prune_fid = schema->AddDebugField("prune", DataType::FLOAT);
    schema->set_primary_field_id(pk_fid);

    auto first = DataGen(schema, kChunkRows, 500, 0, 1, 1);
    auto second = DataGen(schema, kChunkRows, 501, 0, 1, 1);

    // gate==999 matches an unselected row in chunk 0, so its SkipIndex cannot
    // prune that chunk.  Among the selected candidates, only global offset 4
    // (the first row of chunk 1) passes the gate.
    SetInt64FieldData(first, gate_fid, {1, 2, 999, 4});
    SetInt64FieldData(second, gate_fid, {999, 6, 7, 8});

    // prune>60 rejects all of chunk 0 by statistics and accepts chunk 1.
    SetFloatFieldData(first, prune_fid, {10.0F, 20.0F, 30.0F, 40.0F});
    SetFloatFieldData(second, prune_fid, {70.0F, 80.0F, 90.0F, 100.0F});

    auto segment = CreateTwoChunkSealed(schema, first, second);
    auto skip_index = segment->GetSkipIndex();
    ASSERT_FALSE(skip_index->CanSkipUnaryRange<int64_t>(
        gate_fid, 0, proto::plan::OpType::Equal, 999));
    ASSERT_FALSE(skip_index->CanSkipUnaryRange<int64_t>(
        gate_fid, 1, proto::plan::OpType::Equal, 999));
    ASSERT_TRUE(skip_index->CanSkipUnaryRange<float>(
        prune_fid, 0, proto::plan::OpType::GreaterThan, 60.0F));
    ASSERT_FALSE(skip_index->CanSkipUnaryRange<float>(
        prune_fid, 1, proto::plan::OpType::GreaterThan, 60.0F));

    proto::plan::GenericValue gate_value;
    gate_value.set_int64_val(999);
    auto gate_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(gate_fid, DataType::INT64),
        proto::plan::OpType::Equal,
        gate_value,
        std::vector<proto::plan::GenericValue>{});

    proto::plan::GenericValue prune_value;
    prune_value.set_float_val(60.0F);
    auto prune_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(prune_fid, DataType::FLOAT),
        proto::plan::OpType::GreaterThan,
        prune_value,
        std::vector<proto::plan::GenericValue>{});
    auto and_expr = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::And, gate_expr, prune_expr);

    // The normal full scan is the ground truth for the candidate-only run.
    auto filter_node =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, and_expr);
    auto full_output = milvus::test::gen_filter_res(
        filter_node.get(), segment.get(), kRowCount, MAX_TIMESTAMP);
    ASSERT_EQ(full_output->size(), kRowCount);
    TargetBitmapView full_result(full_output->GetRawData(),
                                 full_output->size());
    ASSERT_EQ(full_result.count(), 1);
    ASSERT_TRUE(full_result[4]);

    auto query_context = std::make_shared<QueryContext>(
        DEAFULT_QUERY_ID, segment.get(), kRowCount, MAX_TIMESTAMP);
    ExecContext exec_context(query_context.get());
    ExprSet expr_set({and_expr}, &exec_context);
    ASSERT_EQ(expr_set.size(), 1);
    auto conjunct =
        std::dynamic_pointer_cast<PhyConjunctFilterExpr>(expr_set.expr(0));
    ASSERT_NE(conjunct, nullptr);
    ASSERT_EQ(conjunct->GetReorder(), (std::vector<size_t>{0, 1}));

    // The candidate order is exactly what iterative filtering supplies: two
    // offsets from the pruned chunk followed by two from the live chunk.
    OffsetVector offsets{0, 1, 4, 5};
    EvalCtx eval_context(&exec_context, &offsets);
    std::vector<VectorPtr> results;
    expr_set.Eval(eval_context, results);

    ASSERT_EQ(results.size(), 1);
    auto output = milvus::test::GetColumnVectorForTest(results[0]);
    ASSERT_NE(output, nullptr);
    ASSERT_EQ(output->size(), offsets.size());
    TargetBitmapView result(output->GetRawData(), output->size());
    TargetBitmapView valid(output->GetValidRawData(), output->size());

    EXPECT_FALSE(result[0]);
    EXPECT_FALSE(result[1]);
    EXPECT_TRUE(result[2])
        << "global offset 4 was lost after chunk-0 SkipIndex pruning";
    EXPECT_FALSE(result[3]);
    EXPECT_TRUE(valid.all());
    for (size_t i = 0; i < offsets.size(); ++i) {
        EXPECT_EQ(result[i], full_result[offsets[i]])
            << "candidate " << i << " (global offset " << offsets[i]
            << ") differs from full-scan ground truth";
    }
}

// VARCHAR/JSON/ARRAY use get_views_by_offsets rather than chunk_data. Pin the
// view branch separately because its validity storage and ownership differ.
TEST_F(OffsetsEvalCorrectnessTest,
       SealedViewSkipBranchKeepsBitmapCursorAligned) {
    auto query_context = std::make_shared<QueryContext>(
        DEAFULT_QUERY_ID, sealed_.get(), N, MAX_TIMESTAMP);
    auto seg_expr = MakeDirectSegmentExpr(
        sealed_.get(), varchar_fid_, DataType::VARCHAR, *query_context);
    OffsetVector offsets;
    for (auto offset : std::vector<int32_t>{0, 1, 2, 3, 16, 17, 18, 19}) {
        offsets.emplace_back(offset);
    }

    VerifySkipCursorContract<std::string_view>(*seg_expr, &offsets, 0);
}

// VECTOR_ARRAY has its own chunk_view branch. There is no production
// SkipIndex predicate for vector-array length today, but the generic helper
// accepts one; keep its null-batch contract covered so future callers cannot
// silently reintroduce cursor drift.
TEST_F(OffsetsEvalCorrectnessTest,
       VectorArrayViewSkipBranchKeepsBitmapCursorAligned) {
    auto query_context = std::make_shared<QueryContext>(
        DEAFULT_QUERY_ID, sealed_.get(), N, MAX_TIMESTAMP);
    auto seg_expr = MakeDirectSegmentExpr(sealed_.get(),
                                          vector_array_fid_,
                                          DataType::VECTOR_ARRAY,
                                          *query_context);
    OffsetVector offsets;
    for (auto offset : std::vector<int32_t>{0, 1, 2, 3, 16, 17, 18, 19}) {
        offsets.emplace_back(offset);
    }

    VerifySkipCursorContract<VectorArrayView>(*seg_expr, &offsets, 0);
}

TEST_F(OffsetsEvalCorrectnessTest,
       SealedElementLevelSkipBranchKeepsBitmapCursorAligned) {
    auto query_context = std::make_shared<QueryContext>(
        DEAFULT_QUERY_ID, sealed_.get(), N, MAX_TIMESTAMP);
    auto seg_expr = MakeDirectSegmentExpr(
        sealed_.get(), array_fid_, DataType::INT64, *query_context);

    // Each array row contains one element, so element IDs map directly to
    // row offsets. The two groups therefore live in sealed chunks 0 and 1.
    OffsetVector element_ids;
    for (auto id : std::vector<int32_t>{0, 1, 2, 3, 16, 17, 18, 19}) {
        element_ids.emplace_back(id);
    }
    TargetBitmap bitmap_input(element_ids.size(), false);
    for (size_t i = element_ids.size() / 2; i < element_ids.size(); ++i) {
        bitmap_input[i] = true;
    }

    auto skip_chunk0 = [](const SkipIndex&, FieldId, int chunk_id) {
        return chunk_id == 0;
    };
    int64_t processed_cursor = 0;
    int64_t null_rows = 0;
    auto evaluate_batch = [&]<FilterType filter_type = FilterType::sequential>(
        const int64_t* data,
        const bool* valid_data,
        const int32_t* offsets,
        const int size,
        TargetBitmapView res,
        TargetBitmapView valid_res) {
        if (data == nullptr) {
            null_rows += size;
            processed_cursor += size;
            return;
        }
        for (int i = 0; i < size; ++i) {
            if (bitmap_input[processed_cursor + i]) {
                res[i] = true;
            }
        }
        processed_cursor += size;
    };

    TargetBitmap res(element_ids.size(), false);
    TargetBitmap valid(element_ids.size(), true);
    const auto processed = seg_expr->ProcessElementLevelByOffsets<int64_t>(
        evaluate_batch,
        skip_chunk0,
        &element_ids,
        TargetBitmapView(res),
        TargetBitmapView(valid));

    EXPECT_EQ(processed, int64_t(element_ids.size()));
    EXPECT_EQ(processed_cursor, int64_t(element_ids.size()));
    EXPECT_EQ(null_rows, int64_t(element_ids.size() / 2));
    for (size_t i = 0; i < element_ids.size(); ++i) {
        EXPECT_EQ(bool(res[i]), i >= element_ids.size() / 2)
            << "element candidate " << i
            << ": cursor desync shifted the bitmap_input read";
    }
}

TEST_F(OffsetsEvalCorrectnessTest,
       GrowingElementFullScanSkipKeepsBitmapCursorAligned) {
    auto query_context = std::make_shared<QueryContext>(
        DEAFULT_QUERY_ID, growing_.get(), N, MAX_TIMESTAMP);
    auto seg_expr = MakeDirectSegmentExpr(
        growing_.get(), array_fid_, DataType::INT64, *query_context);

    VerifyElementFullScanSkipCursor(
        *seg_expr, N, 0, OffsetsEvalCorrectnessTest::kChunkRows);
}

TEST_F(OffsetsEvalCorrectnessTest,
       SealedElementFullScanSkipKeepsBitmapCursorAligned) {
    auto query_context = std::make_shared<QueryContext>(
        DEAFULT_QUERY_ID, sealed_.get(), N, MAX_TIMESTAMP);
    auto seg_expr = MakeDirectSegmentExpr(
        sealed_.get(), array_fid_, DataType::INT64, *query_context);

    VerifyElementFullScanSkipCursor(*seg_expr, N, 0, N / 2);
}

TEST(OffsetsEvalNullableElementTest,
     GrowingFullScanUsesLogicalCountForNullPayloadRows) {
    constexpr int64_t kRowCount = 4;
    constexpr int64_t kChunkRows = 2;
    constexpr int64_t kArrayLength = 2;

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto array_fid = schema->AddDebugArrayField(
        "structA[nullable_values]", DataType::INT64, true);
    schema->set_primary_field_id(pk_fid);

    auto dataset = DataGen(schema, kRowCount, 600, 0, 1, kArrayLength);
    SegcoreConfig config = SegcoreConfig::default_config();
    config.set_chunk_rows(kChunkRows);
    auto segment = CreateGrowingSegment(schema, empty_index_meta, 1, config);
    segment->PreInsert(kRowCount);
    segment->Insert(0,
                    kRowCount,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);

    VerifyNullableElementFullScanLogicalCount(
        segment.get(), array_fid, kRowCount, 0, 2, 2);
}

TEST(OffsetsEvalNullableElementTest,
     GrowingLoadFieldDataUsesLogicalCountForNullPayloadRows) {
    constexpr int64_t kRowCount = 4;
    constexpr int64_t kChunkRows = 2;
    constexpr int64_t kArrayLength = 2;

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto array_fid = schema->AddDebugArrayField(
        "structA[nullable_values]", DataType::INT64, true);
    schema->set_primary_field_id(pk_fid);

    auto dataset = DataGen(schema, kRowCount, 650, 0, 1, kArrayLength);
    SegcoreConfig config = SegcoreConfig::default_config();
    config.set_chunk_rows(kChunkRows);
    auto segment = CreateGrowingSegment(schema, empty_index_meta, 1, config);
    ASSERT_EQ(segment->PreInsert(kRowCount), 0);

    // Mimic the growing recovery path after storage decoding.  FieldData<Array>
    // keeps dense physical payloads for invalid rows, so the offsets producer
    // must consult validity rather than count those payloads as elements.
    auto* growing = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(growing, nullptr);
    const auto& field_meta = schema->operator[](array_fid);
    bool loaded = false;
    for (const auto& data : dataset.raw_->fields_data()) {
        if (data.field_id() != array_fid.get()) {
            continue;
        }
        auto field_data =
            CreateFieldDataFromDataArray(kRowCount, &data, field_meta);
        growing->load_field_data_common(
            array_fid, 0, {field_data}, pk_fid, kRowCount);
        loaded = true;
        break;
    }
    ASSERT_TRUE(loaded);

    VerifyNullableElementFullScanLogicalCount(
        segment.get(), array_fid, kRowCount, 0, 2, 2);
}

TEST(OffsetsEvalNullableElementTest,
     SealedFullScanUsesLogicalCountForNullPayloadRows) {
    constexpr int64_t kRowsPerChunk = 2;
    constexpr int64_t kRowCount = kRowsPerChunk * 2;
    constexpr int64_t kArrayLength = 2;

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto array_fid = schema->AddDebugArrayField(
        "structA[nullable_values]", DataType::INT64, true);
    schema->set_primary_field_id(pk_fid);

    auto first = DataGen(schema, kRowsPerChunk, 700, 0, 1, kArrayLength);
    auto second = DataGen(schema, kRowsPerChunk, 701, 0, 1, kArrayLength);
    auto segment = CreateTwoChunkSealed(schema, first, second);

    VerifyNullableElementFullScanLogicalCount(
        segment.get(), array_fid, kRowCount, 0, 2, 2);
}

TEST(OffsetsEvalIndexOnlyCorrectnessTest,
     NullableReverseLookupAdvancesCursorAndIgnoresChunkSkip) {
    constexpr int64_t kRowCount = 8;
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto value_fid = schema->AddDebugField("value", DataType::INT64, true);
    schema->set_primary_field_id(pk_fid);

    auto first = DataGen(schema, kRowCount / 2, 100, 0, 1, 1);
    auto second = DataGen(schema, kRowCount / 2, 101, 0, 1, 1);
    auto segment = CreateTwoChunkSealed(schema, first, second);

    std::vector<int64_t> index_values(kRowCount);
    std::iota(index_values.begin(), index_values.end(), int64_t{0});
    auto index_valid = std::make_unique<bool[]>(kRowCount);
    for (int64_t i = 0; i < kRowCount; ++i) {
        index_valid[i] = true;
    }
    index_valid[1] = false;

    auto scalar_index = index::CreateScalarIndexSort<int64_t>();
    scalar_index->Build(kRowCount, index_values.data(), index_valid.get());
    LoadIndexInfo load_index_info;
    load_index_info.field_id = value_fid.get();
    load_index_info.field_type = DataType::INT64;
    load_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    load_index_info.index_params = GenIndexParams(scalar_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("offset-index-only", std::move(scalar_index));
    segment->LoadIndex(load_index_info);
    segment->DropFieldData(value_fid);
    ASSERT_FALSE(segment->HasFieldData(value_fid));

    auto query_context = std::make_shared<QueryContext>(
        DEAFULT_QUERY_ID, segment.get(), kRowCount, MAX_TIMESTAMP);
    auto seg_expr =
        std::make_shared<SegmentExpr>(std::vector<ExprPtr>{},
                                      "index-only offset contract probe",
                                      query_context->get_op_context(),
                                      segment.get(),
                                      value_fid,
                                      std::vector<std::string>{},
                                      DataType::INT64,
                                      kRowCount,
                                      kRowCount,
                                      query_context->get_consistency_level());

    OffsetVector input{0, 1, 2, 5};
    TargetBitmap bitmap_input(input.size(), true);
    int64_t processed_cursor = 0;
    int64_t null_rows = 0;
    auto evaluate_batch = [&]<FilterType filter_type = FilterType::sequential>(
        const int64_t* data,
        const bool* valid_data,
        const int32_t* offsets,
        const int size,
        TargetBitmapView res,
        TargetBitmapView valid_res) {
        if (data == nullptr) {
            null_rows += size;
            processed_cursor += size;
            return;
        }
        for (int i = 0; i < size; ++i) {
            if (bitmap_input[processed_cursor + i]) {
                res[i] = true;
            }
        }
        processed_cursor += size;
    };
    auto skip_everything = [](const SkipIndex&, FieldId, int) { return true; };

    TargetBitmap res(input.size(), false);
    TargetBitmap valid(input.size(), true);
    const auto processed =
        seg_expr->ProcessDataByOffsets<int64_t>(evaluate_batch,
                                                skip_everything,
                                                &input,
                                                TargetBitmapView(res),
                                                TargetBitmapView(valid));

    EXPECT_EQ(processed, int64_t(input.size()));
    EXPECT_EQ(processed_cursor, int64_t(input.size()));
    EXPECT_EQ(null_rows, 1);
    EXPECT_TRUE(res[0]);
    EXPECT_FALSE(res[1]);
    EXPECT_TRUE(res[2]);
    EXPECT_TRUE(res[3]);
    EXPECT_TRUE(valid[0]);
    EXPECT_FALSE(valid[1]);
    EXPECT_TRUE(valid[2]);
    EXPECT_TRUE(valid[3]);
}

TEST(OffsetsEvalIndexOnlyCorrectnessTest,
     NullableTimestamptzCallbackHandlesNullData) {
    constexpr int64_t kRowCount = 8;
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto timestamp_fid =
        schema->AddDebugField("ts", DataType::TIMESTAMPTZ, true);
    schema->set_primary_field_id(pk_fid);

    auto first = DataGen(schema, kRowCount / 2, 200, 0, 1, 1);
    auto second = DataGen(schema, kRowCount / 2, 201, 0, 1, 1);
    auto segment = CreateTwoChunkSealed(schema, first, second);

    std::vector<int64_t> index_values(kRowCount);
    std::iota(index_values.begin(), index_values.end(), int64_t{0});
    auto index_valid = std::make_unique<bool[]>(kRowCount);
    for (int64_t i = 0; i < kRowCount; ++i) {
        index_valid[i] = true;
    }
    index_valid[1] = false;

    auto scalar_index = index::CreateScalarIndexSort<int64_t>();
    scalar_index->Build(kRowCount, index_values.data(), index_valid.get());
    LoadIndexInfo load_index_info;
    load_index_info.field_id = timestamp_fid.get();
    load_index_info.field_type = DataType::TIMESTAMPTZ;
    load_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    load_index_info.index_params = GenIndexParams(scalar_index.get());
    load_index_info.cache_index = CreateTestCacheIndex(
        "offset-timestamptz-index-only", std::move(scalar_index));
    segment->LoadIndex(load_index_info);
    segment->DropFieldData(timestamp_fid);
    ASSERT_FALSE(segment->HasFieldData(timestamp_fid));

    proto::plan::Interval interval;
    interval.set_months(1);
    proto::plan::GenericValue compare_value;
    compare_value.set_int64_val(4102444800LL * 1000000);  // 2100-01-01
    auto typed_expr =
        std::make_shared<milvus::expr::TimestamptzArithCompareExpr>(
            expr::ColumnInfo(timestamp_fid, DataType::TIMESTAMPTZ),
            proto::plan::ArithOpType::Add,
            interval,
            proto::plan::OpType::LessThan,
            compare_value);
    auto filter_node = std::make_shared<milvus::plan::FilterBitsNode>(
        DEFAULT_PLANNODE_ID, typed_expr);

    OffsetVector input{0, 1, 2, 5};
    auto col_vec = milvus::test::gen_filter_res(
        filter_node.get(), segment.get(), kRowCount, MAX_TIMESTAMP, &input);
    ASSERT_EQ(col_vec->size(), input.size());

    TargetBitmapView result(col_vec->GetRawData(), col_vec->size());
    TargetBitmapView valid(col_vec->GetValidRawData(), col_vec->size());
    EXPECT_TRUE(result[0]);
    EXPECT_FALSE(result[1]);
    EXPECT_TRUE(result[2]);
    EXPECT_TRUE(result[3]);
    EXPECT_TRUE(valid[0]);
    EXPECT_FALSE(valid[1]);
    EXPECT_TRUE(valid[2]);
    EXPECT_TRUE(valid[3]);
}
