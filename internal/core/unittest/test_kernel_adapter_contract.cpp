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

// Contract tests for KernelAdapter / SegmentExpr::EvalKernel over the real
// readers: position derivation, candidate slicing, SkipIndex, NULL folding,
// constant kernels, pruned-row reset and index-only reverse lookup.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <numeric>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/Types.h"
#include "common/Utils.h"
#include "exec/expression/Expr.h"
#include "exec/expression/ScanKernel.h"
#include "index/ScalarIndexSort.h"
#include "knowhere/comp/index_param.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegcoreConfig.h"
#include "test_utils/DataGen.h"
#include "test_utils/SegcoreConfigUtils.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::exec;
using namespace milvus::segcore;

namespace {

// Same construction as test_offsets_eval_correctness.cpp: one binlog per
// dataset, so a sealed field has one data chunk per dataset.
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

// ------------------------------------------------------------------ kernels

// Marks every candidate row TRUE and counts rows that reached Eval.
template <typename T>
struct ProbeKernel {
    int64_t* rows_seen;

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<T>& batch, TriStateOut out) const {
        *rows_seen += static_cast<int64_t>(batch.size);
        EXPECT_NE(batch.data, nullptr);
        for (size_t i = 0; i < batch.size; ++i) {
            if (batch.IsCandidate(i)) {
                out.SetTrue(i);
            }
        }
    }
};

template <typename T>
struct SkipChunkKernel : ProbeKernel<T> {
    int64_t skipped_chunk;

    bool
    CanSkip(const SkipIndex&, FieldId, int64_t chunk_id) const {
        return chunk_id == skipped_chunk;
    }
};

template <typename T>
struct SkipAllKernel : ProbeKernel<T> {
    bool
    CanSkip(const SkipIndex&, FieldId, int64_t) const {
        return true;
    }
};

struct DivisibleValueKernel {
    int64_t divisor;

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<int64_t>& batch, TriStateOut out) const {
        for (size_t i = 0; i < batch.size; ++i) {
            if (batch.IsCandidate(i) && batch.data[i] % divisor == 0) {
                out.SetTrue(i);
            }
        }
    }
};

// Writes TRUE into every row of match wholesale, like a SIMD kernel.
template <typename T>
struct WholesaleTrueKernel {
    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<T>&, TriStateOut out) const {
        out.match.set();
    }
};

template <typename T>
struct NullKnownFalseTrueKernel : WholesaleTrueKernel<T> {
    static constexpr bool kNullRowsKnownFalse = true;
};

// Marks every row UNKNOWN, ignoring candidates.
template <typename T>
struct AllUnknownKernel {
    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<T>& batch, TriStateOut out) const {
        for (size_t i = 0; i < batch.size; ++i) {
            out.SetUnknown(i);
        }
    }
};

template <typename T>
struct ConstantKernel {
    bool always_false;
    bool always_true;

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<T>&, TriStateOut) const {
        ADD_FAILURE() << "constant kernels must not be evaluated";
    }

    bool
    AlwaysFalse() const {
        return always_false;
    }

    bool
    AlwaysTrue() const {
        return always_true;
    }
};

template <typename T>
struct SegmentOffsetRecorder {
    static constexpr bool kNeedsSegmentOffsets = true;
    std::vector<int32_t>* seen;

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<T>& batch, TriStateOut) const {
        ASSERT_NE(batch.segment_offsets, nullptr);
        seen->insert(seen->end(),
                     batch.segment_offsets,
                     batch.segment_offsets + batch.size);
    }
};

template <typename T>
struct SegmentOffsetForbidden {
    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<T>& batch, TriStateOut) const {
        EXPECT_EQ(batch.segment_offsets, nullptr);
    }
};

// Sets match on candidate rows but leaves them unknown.
template <typename T>
struct MatchWithoutKnownKernel {
    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<T>& batch, TriStateOut out) const {
        for (size_t i = 0; i < batch.size; ++i) {
            if (batch.IsCandidate(i)) {
                out.match[i] = true;
                out.known[i] = false;
            }
        }
    }
};

template <typename T>
struct NullKnownFalseConstantKernel : ConstantKernel<T> {
    static constexpr bool kNullRowsKnownFalse = true;
};

// ------------------------------------------------------------------ helpers

std::string
TriStates(const VectorPtr& vec) {
    auto col = std::dynamic_pointer_cast<ColumnVector>(vec);
    EXPECT_NE(col, nullptr);
    if (col == nullptr) {
        return "<null>";
    }
    TargetBitmapView match(col->GetRawData(), col->size());
    TargetBitmapView known(col->GetValidRawData(), col->size());
    std::string out;
    for (size_t i = 0; i < col->size(); ++i) {
        out.push_back(!known[i] ? 'U' : (match[i] ? 'T' : 'F'));
    }
    return out;
}

TargetBitmap
BitmapFrom(const std::string& bits) {
    TargetBitmap bitmap(bits.size(), false);
    for (size_t i = 0; i < bits.size(); ++i) {
        bitmap[i] = bits[i] == '1';
    }
    return bitmap;
}

std::shared_ptr<SegmentExpr>
MakeExpr(const SegmentInternalInterface* segment,
         FieldId field_id,
         DataType value_type,
         QueryContext& query_context,
         int64_t active_count,
         int64_t batch_size) {
    return std::make_shared<SegmentExpr>(std::vector<ExprPtr>{},
                                         "kernel adapter contract probe",
                                         query_context.get_op_context(),
                                         segment,
                                         field_id,
                                         std::vector<std::string>{},
                                         value_type,
                                         active_count,
                                         batch_size,
                                         query_context.get_consistency_level());
}

const proto::schema::FieldData&
FieldOf(const GeneratedData& dataset, FieldId field_id) {
    const auto& fields = dataset.raw_->fields_data();
    auto it = std::find_if(fields.begin(), fields.end(), [&](const auto& f) {
        return f.field_id() == field_id.get();
    });
    AssertInfo(it != fields.end(), "field id {} not found", field_id.get());
    return *it;
}

std::vector<int64_t>
Int64Values(const std::vector<const GeneratedData*>& datasets,
            FieldId field_id) {
    std::vector<int64_t> values;
    for (const auto* dataset : datasets) {
        const auto& data = FieldOf(*dataset, field_id).scalars().long_data();
        values.insert(values.end(), data.data().begin(), data.data().end());
    }
    return values;
}

std::vector<bool>
Validity(const std::vector<const GeneratedData*>& datasets, FieldId field_id) {
    std::vector<bool> valid;
    for (const auto* dataset : datasets) {
        const auto& field_valid =
            GetFieldDataRowValidData(FieldOf(*dataset, field_id));
        const auto rows = dataset->raw_->num_rows();
        for (int64_t i = 0; i < rows; ++i) {
            valid.push_back(field_valid.empty() || field_valid.Get(i));
        }
    }
    return valid;
}

// Evaluates batches until the expression is exhausted and concatenates them.
template <typename T, typename Kernel, typename BitmapFor>
std::string
EvalAllBatches(SegmentExpr& expr,
               ExecContext& exec_context,
               Kernel kernel,
               int64_t batch_size,
               int64_t total_rows,
               BitmapFor bitmap_for) {
    std::string out;
    int64_t position = 0;
    while (true) {
        EvalCtx ctx(&exec_context);
        const int64_t expected = std::min(batch_size, total_rows - position);
        if (expected > 0) {
            ctx.set_bitmap_input(bitmap_for(position, expected));
        }
        auto res = expr.EvalKernel<T>(ctx, kernel, false);
        if (res == nullptr) {
            break;
        }
        out += TriStates(res);
        position += static_cast<int64_t>(res->size());
    }
    return out;
}

size_t
MatchCount(const VectorPtr& vec) {
    auto col = std::dynamic_pointer_cast<ColumnVector>(vec);
    EXPECT_NE(col, nullptr);
    if (col == nullptr) {
        return 0;
    }
    return TargetBitmapView(col->GetRawData(), col->size()).count();
}

// Irregular candidate pattern, so a sub-batch evaluated at the wrong position
// changes the result.
bool
IrregularCandidate(int64_t position) {
    return (position * 7 + 3) % 11 < 7;
}

TargetBitmap
IrregularBitmap(int64_t start, int64_t size) {
    TargetBitmap bitmap(size, false);
    for (int64_t i = 0; i < size; ++i) {
        bitmap[i] = IrregularCandidate(start + i);
    }
    return bitmap;
}

// Instantiates EvalKernel for the value types leaf expressions use; never
// called.
[[maybe_unused]] void
InstantiateEvalKernelValueTypes(SegmentExpr& expr, EvalCtx& ctx) {
    int64_t rows_seen = 0;
    expr.EvalKernel<bool>(ctx, ProbeKernel<bool>{&rows_seen}, false);
    expr.EvalKernel<double>(ctx, SkipAllKernel<double>{{&rows_seen}}, true);
    expr.EvalKernel<std::string>(
        ctx, ProbeKernel<std::string>{&rows_seen}, false);
    expr.EvalKernel<std::string>(ctx, ConstantKernel<std::string>{}, false);
    expr.EvalKernel<std::string_view>(
        ctx, SkipAllKernel<std::string_view>{{&rows_seen}}, false);
    expr.EvalKernel<milvus::Json>(
        ctx, ProbeKernel<milvus::Json>{&rows_seen}, false);
    expr.EvalKernel<milvus::Json>(
        ctx, NullKnownFalseConstantKernel<milvus::Json>{}, false);
    expr.EvalKernel<ArrayView>(ctx, ProbeKernel<ArrayView>{&rows_seen}, false);
    expr.EvalKernel<ArrayView>(ctx, ConstantKernel<ArrayView>{}, true);
}

// Nullable INT64 "value" field. DataGen makes every odd row of a dataset
// NULL; the sealed segment has two chunks of rows / 2.
struct NullableSegments {
    SchemaPtr schema;
    FieldId value_fid;
    GeneratedData growing_data;
    GeneratedData sealed_first;
    GeneratedData sealed_second;
    SegmentGrowingPtr growing;
    std::unique_ptr<SegmentSealed> sealed;

    const SegmentInternalInterface*
    Segment(bool is_growing) const {
        return is_growing
                   ? static_cast<const SegmentInternalInterface*>(growing.get())
                   : sealed.get();
    }

    std::vector<bool>
    Valid(bool is_growing) const {
        return is_growing ? Validity({&growing_data}, value_fid)
                          : Validity({&sealed_first, &sealed_second}, value_fid);
    }

    std::vector<int64_t>
    Values(bool is_growing) const {
        return is_growing
                   ? Int64Values({&growing_data}, value_fid)
                   : Int64Values({&sealed_first, &sealed_second}, value_fid);
    }
};

NullableSegments
MakeNullableSegments(int64_t rows) {
    NullableSegments out;
    out.schema = std::make_shared<Schema>();
    out.schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
    auto pk_fid = out.schema->AddDebugField("pk", DataType::INT64);
    out.value_fid = out.schema->AddDebugField("value", DataType::INT64, true);
    out.schema->set_primary_field_id(pk_fid);

    out.growing_data = DataGen(out.schema, rows, 900, 0, 1, 1);
    out.growing = CreateGrowingSegment(out.schema, empty_index_meta);
    out.growing->PreInsert(rows);
    out.growing->Insert(0,
                        rows,
                        out.growing_data.row_ids_.data(),
                        out.growing_data.timestamps_.data(),
                        out.growing_data.raw_);
    out.sealed_first = DataGen(out.schema, rows / 2, 901, 0, 1, 1);
    out.sealed_second = DataGen(out.schema, rows / 2, 902, 0, 1, 1);
    out.sealed =
        CreateTwoChunkSealed(out.schema, out.sealed_first, out.sealed_second);
    return out;
}

}  // namespace

// -------------------------------------------------------------- unit tests

TEST(KernelAdapterContractUnitTest, DerivesPositionFromResultView) {
    TargetBitmap match(8, false);
    TargetBitmap known(8, true);
    TargetBitmapView res(match);
    TargetBitmapView valid(known);
    const TargetBitmap bitmap = BitmapFrom("11110000");
    const std::vector<int64_t> values = {10, 11, 12, 13, 14, 15, 16, 17};
    const bool validity[8] = {true, true, false, true, true, true, true, false};

    int64_t rows_seen = 0;
    ProbeKernel<int64_t> kernel{&rows_seen};
    KernelAdapter<int64_t, ProbeKernel<int64_t>> adapter(
        &kernel, &bitmap, res, valid);

    // A reader visiting the second half first, then one random row, then a
    // skipped row.
    adapter(values.data() + 4,
            ValidityView::FromExpanded(validity + 4),
            nullptr,
            int64_t{4},
            res + 4,
            valid + 4);
    adapter.template operator()<FilterType::random>(
        values.data() + 1,
        ValidityView::FromExpanded(validity + 1),
        nullptr,
        int64_t{1},
        res + 1,
        valid + 1);
    adapter(nullptr, ValidityView{}, nullptr, int64_t{1}, res + 2, valid + 2);

    EXPECT_EQ(rows_seen, 5);
    std::string states;
    for (size_t i = 0; i < 8; ++i) {
        states.push_back(!known[i] ? 'U' : (match[i] ? 'T' : 'F'));
    }
    // 1: candidate TRUE. 2: skipped by the reader. 4-7: non-candidates.
    // 7: NULL folded to UNKNOWN before the batch-level reset.
    EXPECT_EQ(states, "FTFFFFFU");

    TargetBitmap other(8, false);
    TargetBitmap other_valid(8, true);
    EXPECT_ANY_THROW(adapter(values.data(),
                             ValidityView{},
                             nullptr,
                             int64_t{1},
                             TargetBitmapView(other),
                             TargetBitmapView(other_valid)));
    // Skipped rows are checked too.
    EXPECT_ANY_THROW(adapter(nullptr,
                             ValidityView{},
                             nullptr,
                             int64_t{1},
                             TargetBitmapView(other),
                             TargetBitmapView(other_valid)));
    EXPECT_ANY_THROW(
        adapter(nullptr, ValidityView{}, nullptr, int64_t{2}, res + 7, valid + 7));
}

TEST(KernelAdapterContractUnitTest, RandomBatchSlicesCandidatesAndPackedValidity) {
    // Validity of rows 5-10 is 1, 0, 1, 1, 0, 1, stored as packed bits 3-8.
    const uint8_t packed[2] = {0b01101000, 0b00000001};
    const auto validity = ValidityView::FromPacked(packed).Subview(3);
    const TargetBitmap bitmap = BitmapFrom("0000011010110000");
    const std::vector<int64_t> values = {20, 21, 22, 23, 24, 25};

    {
        TargetBitmap match(16, false);
        TargetBitmap known(16, true);
        TargetBitmapView res(match);
        TargetBitmapView valid(known);
        int64_t rows_seen = 0;
        ProbeKernel<int64_t> kernel{&rows_seen};
        KernelAdapter<int64_t, ProbeKernel<int64_t>> adapter(
            &kernel, &bitmap, res, valid);
        adapter.template operator()<FilterType::random>(
            values.data(), validity, nullptr, int64_t{6}, res + 5, valid + 5);

        EXPECT_EQ(rows_seen, 6);
        std::string states;
        for (size_t i = 0; i < 16; ++i) {
            states.push_back(!known[i] ? 'U' : (match[i] ? 'T' : 'F'));
        }
        EXPECT_EQ(states, "FFFFFTUFTUTFFFFF");
    }
    {
        TargetBitmap match(16, false);
        TargetBitmap known(16, true);
        TargetBitmapView res(match);
        TargetBitmapView valid(known);
        NullKnownFalseTrueKernel<int64_t> kernel;
        KernelAdapter<int64_t, NullKnownFalseTrueKernel<int64_t>> adapter(
            &kernel, &bitmap, res, valid);
        adapter.template operator()<FilterType::random>(
            values.data(), validity, nullptr, int64_t{6}, res + 5, valid + 5);

        std::string states;
        for (size_t i = 0; i < 16; ++i) {
            states.push_back(!known[i] ? 'U' : (match[i] ? 'T' : 'F'));
        }
        // Non-candidate rows keep TRUE here; EvalKernel resets them later.
        EXPECT_EQ(states, "FFFFFTFTTFTFFFFF");
    }
}

TEST(KernelAdapterContractUnitTest, NullRowsKnownFalseClearsOnlyMatch) {
    TargetBitmap match(4, false);
    TargetBitmap known(4, true);
    TargetBitmapView res(match);
    TargetBitmapView valid(known);
    const std::vector<int64_t> values = {1, 2, 3, 4};
    const bool validity[4] = {true, false, true, false};

    NullKnownFalseTrueKernel<int64_t> kernel;
    KernelAdapter<int64_t, NullKnownFalseTrueKernel<int64_t>> adapter(
        &kernel, nullptr, res, valid);
    adapter(values.data(),
            ValidityView::FromExpanded(validity),
            nullptr,
            int64_t{4},
            res,
            valid);

    EXPECT_TRUE(match[0]);
    EXPECT_FALSE(match[1]);
    EXPECT_TRUE(match[2]);
    EXPECT_FALSE(match[3]);
    EXPECT_TRUE(known.all());
}

// ------------------------------------------------------------ segment tests

class KernelAdapterContractTest : public ::testing::Test {
 protected:
    ScopedSegcoreConfigRestore config_restore_;

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

        // One element per array row, so element ids equal row offsets.
        growing_data_ = DataGen(schema_, N, 42, 0, 1, 1);
        SegcoreConfig config = SegcoreConfig::default_config();
        config.set_chunk_rows(kChunkRows);
        growing_ = CreateGrowingSegment(schema_, empty_index_meta, 1, config);
        growing_->PreInsert(N);
        growing_->Insert(0,
                         N,
                         growing_data_.row_ids_.data(),
                         growing_data_.timestamps_.data(),
                         growing_data_.raw_);

        sealed_first_ = DataGen(schema_, N / 2, 43, 0, 1, 1);
        sealed_second_ = DataGen(schema_, N / 2, 44, 0, 1, 1);
        sealed_ = CreateTwoChunkSealed(schema_, sealed_first_, sealed_second_);
    }

    template <typename T>
    void
    VerifyOffsetsSkip(const SegmentInternalInterface* segment,
                      FieldId field_id,
                      DataType value_type,
                      std::vector<int32_t> offsets) {
        auto query_context = std::make_shared<QueryContext>(
            DEAFULT_QUERY_ID, segment, N, MAX_TIMESTAMP);
        ExecContext exec_context(query_context.get());
        auto expr =
            MakeExpr(segment, field_id, value_type, *query_context, N, N);
        expr->SetHasOffsetInput(true);

        OffsetVector input(offsets.begin(), offsets.end());
        EvalCtx ctx(&exec_context, &input);
        ctx.set_bitmap_input(BitmapFrom("00001111"));

        int64_t rows_seen = 0;
        auto res = expr->EvalKernel<T>(
            ctx, SkipChunkKernel<T>{{&rows_seen}, 0}, false);

        EXPECT_EQ(TriStates(res), "FFFFTTTT");
        EXPECT_EQ(rows_seen, 4)
            << "rows of a skipped chunk or non-candidates reached Eval";
    }

    const SegmentInternalInterface*
    SegmentFor(bool is_growing) const {
        return is_growing
                   ? static_cast<const SegmentInternalInterface*>(growing_.get())
                   : sealed_.get();
    }

    static constexpr int64_t kChunkRows = 8;
    static constexpr int64_t N = 32;  // 4 growing chunks, 2 sealed chunks

    SchemaPtr schema_;
    FieldId i64_fid_, varchar_fid_, array_fid_, vector_array_fid_;
    GeneratedData growing_data_;
    GeneratedData sealed_first_;
    GeneratedData sealed_second_;
    SegmentGrowingPtr growing_;
    std::unique_ptr<SegmentSealed> sealed_;
};

TEST_F(KernelAdapterContractTest, OffsetsSkipKeepsCandidatesAligned) {
    VerifyOffsetsSkip<int64_t>(
        growing_.get(), i64_fid_, DataType::INT64, {0, 1, 2, 3, 8, 9, 10, 11});
    VerifyOffsetsSkip<int64_t>(sealed_.get(),
                               i64_fid_,
                               DataType::INT64,
                               {0, 1, 2, 3, 16, 17, 18, 19});
    VerifyOffsetsSkip<std::string_view>(sealed_.get(),
                                        varchar_fid_,
                                        DataType::VARCHAR,
                                        {0, 1, 2, 3, 16, 17, 18, 19});
    VerifyOffsetsSkip<VectorArrayView>(sealed_.get(),
                                       vector_array_fid_,
                                       DataType::VECTOR_ARRAY,
                                       {0, 1, 2, 3, 16, 17, 18, 19});
}

TEST_F(KernelAdapterContractTest, ElementOffsetsSkipKeepsCandidatesAligned) {
    for (const bool is_growing : {true, false}) {
        const SegmentInternalInterface* segment =
            is_growing ? static_cast<const SegmentInternalInterface*>(
                             growing_.get())
                       : sealed_.get();
        auto query_context = std::make_shared<QueryContext>(
            DEAFULT_QUERY_ID, segment, N, MAX_TIMESTAMP);
        ExecContext exec_context(query_context.get());
        auto expr =
            MakeExpr(segment, array_fid_, DataType::INT64, *query_context, N, N);
        expr->SetHasOffsetInput(true);

        OffsetVector element_ids =
            is_growing ? OffsetVector{0, 1, 2, 3, 8, 9, 10, 11}
                       : OffsetVector{0, 1, 2, 3, 16, 17, 18, 19};
        EvalCtx ctx(&exec_context, &element_ids);
        ctx.set_bitmap_input(BitmapFrom("00001111"));

        int64_t rows_seen = 0;
        auto res = expr->EvalKernel<int64_t>(
            ctx, SkipChunkKernel<int64_t>{{&rows_seen}, 0}, true);

        EXPECT_EQ(TriStates(res), "FFFFTTTT")
            << (is_growing ? "growing" : "sealed");
        EXPECT_EQ(rows_seen, 4);
    }
}

TEST_F(KernelAdapterContractTest, ElementFullScanSkipsWholeChunk) {
    for (const bool is_growing : {true, false}) {
        const SegmentInternalInterface* segment =
            is_growing ? static_cast<const SegmentInternalInterface*>(
                             growing_.get())
                       : sealed_.get();
        const int64_t skipped = is_growing ? kChunkRows : N / 2;
        auto query_context = std::make_shared<QueryContext>(
            DEAFULT_QUERY_ID, segment, N, MAX_TIMESTAMP);
        ExecContext exec_context(query_context.get());
        auto expr =
            MakeExpr(segment, array_fid_, DataType::INT64, *query_context, N, N);

        EvalCtx ctx(&exec_context);
        int64_t rows_seen = 0;
        auto res = expr->EvalKernel<int64_t>(
            ctx, SkipChunkKernel<int64_t>{{&rows_seen}, 0}, true);

        EXPECT_EQ(TriStates(res),
                  std::string(skipped, 'F') + std::string(N - skipped, 'T'))
            << (is_growing ? "growing" : "sealed");
        EXPECT_EQ(rows_seen, N - skipped);
    }
}

TEST_F(KernelAdapterContractTest, FullScanMultiBatchMatchesValues) {
    constexpr int64_t kBatch = 5;

    for (const bool is_growing : {true, false}) {
        const SegmentInternalInterface* segment =
            is_growing ? static_cast<const SegmentInternalInterface*>(
                             growing_.get())
                       : sealed_.get();
        const auto values =
            is_growing ? Int64Values({&growing_data_}, i64_fid_)
                       : Int64Values({&sealed_first_, &sealed_second_},
                                     i64_fid_);
        std::string expected;
        for (int64_t i = 0; i < N; ++i) {
            expected.push_back(
                IrregularCandidate(i) && values[i] % 2 == 0 ? 'T' : 'F');
        }

        auto query_context = std::make_shared<QueryContext>(
            DEAFULT_QUERY_ID, segment, N, MAX_TIMESTAMP);
        ExecContext exec_context(query_context.get());
        auto expr = MakeExpr(
            segment, i64_fid_, DataType::INT64, *query_context, N, kBatch);
        EXPECT_EQ(EvalAllBatches<int64_t>(*expr,
                                          exec_context,
                                          DivisibleValueKernel{2},
                                          kBatch,
                                          N,
                                          IrregularBitmap),
                  expected)
            << (is_growing ? "growing" : "sealed");
    }
}

TEST_F(KernelAdapterContractTest, PrunedRowsResetToFalseKnown) {
    auto query_context = std::make_shared<QueryContext>(
        DEAFULT_QUERY_ID, growing_.get(), N, MAX_TIMESTAMP);
    ExecContext exec_context(query_context.get());
    auto expr = MakeExpr(
        growing_.get(), i64_fid_, DataType::INT64, *query_context, N, N);

    std::string bits;
    std::string expected;
    for (int64_t i = 0; i < N; ++i) {
        bits.push_back(i % 2 == 0 ? '1' : '0');
        expected.push_back(i % 2 == 0 ? 'U' : 'F');
    }
    EvalCtx ctx(&exec_context);
    ctx.set_bitmap_input(BitmapFrom(bits));

    EXPECT_EQ(TriStates(expr->EvalKernel<int64_t>(
                  ctx, AllUnknownKernel<int64_t>{}, false)),
              expected);
}

TEST_F(KernelAdapterContractTest, SegmentOffsetsOnlyForKernelsThatAskForThem) {
    for (const bool is_growing : {true, false}) {
        const SegmentInternalInterface* segment =
            is_growing ? static_cast<const SegmentInternalInterface*>(
                             growing_.get())
                       : sealed_.get();
        auto query_context = std::make_shared<QueryContext>(
            DEAFULT_QUERY_ID, segment, N, MAX_TIMESTAMP);
        ExecContext exec_context(query_context.get());
        {
            auto expr = MakeExpr(
                segment, i64_fid_, DataType::INT64, *query_context, N, N);
            EvalCtx ctx(&exec_context);
            std::vector<int32_t> seen;
            expr->EvalKernel<int64_t>(
                ctx, SegmentOffsetRecorder<int64_t>{&seen}, false);
            std::vector<int32_t> expected(N);
            std::iota(expected.begin(), expected.end(), 0);
            EXPECT_EQ(seen, expected) << (is_growing ? "growing" : "sealed");
        }
        {
            auto expr = MakeExpr(
                segment, i64_fid_, DataType::INT64, *query_context, N, N);
            EvalCtx ctx(&exec_context);
            expr->EvalKernel<int64_t>(
                ctx, SegmentOffsetForbidden<int64_t>{}, false);
        }
    }
}

TEST_F(KernelAdapterContractTest, BitmapSizeMismatchThrows) {
    auto query_context = std::make_shared<QueryContext>(
        DEAFULT_QUERY_ID, growing_.get(), N, MAX_TIMESTAMP);
    ExecContext exec_context(query_context.get());
    auto expr = MakeExpr(
        growing_.get(), i64_fid_, DataType::INT64, *query_context, N, N);
    EvalCtx ctx(&exec_context);
    ctx.set_bitmap_input(TargetBitmap(N - 1, true));
    int64_t rows_seen = 0;
    EXPECT_ANY_THROW(expr->EvalKernel<int64_t>(
        ctx, ProbeKernel<int64_t>{&rows_seen}, false));
}

TEST_F(KernelAdapterContractTest, UnknownRowsNeverMatch) {
    for (const bool is_growing : {true, false}) {
        const auto* segment = SegmentFor(is_growing);
        auto query_context = std::make_shared<QueryContext>(
            DEAFULT_QUERY_ID, segment, N, MAX_TIMESTAMP);
        ExecContext exec_context(query_context.get());
        auto expr =
            MakeExpr(segment, i64_fid_, DataType::INT64, *query_context, N, N);
        EvalCtx ctx(&exec_context);
        ctx.set_bitmap_input(IrregularBitmap(0, N));

        std::string expected;
        for (int64_t i = 0; i < N; ++i) {
            expected.push_back(IrregularCandidate(i) ? 'U' : 'F');
        }
        auto res = expr->EvalKernel<int64_t>(
            ctx, MatchWithoutKnownKernel<int64_t>{}, false);
        EXPECT_EQ(TriStates(res), expected)
            << (is_growing ? "growing" : "sealed");
        EXPECT_EQ(MatchCount(res), 0u) << (is_growing ? "growing" : "sealed");
    }
}

TEST_F(KernelAdapterContractTest, SequentialMaskSkipsChunkAndPassesOffsets) {
    for (const bool is_growing : {true, false}) {
        const auto* segment = SegmentFor(is_growing);
        const int64_t skipped = is_growing ? kChunkRows : N / 2;
        const std::string where = is_growing ? "growing" : "sealed";
        auto query_context = std::make_shared<QueryContext>(
            DEAFULT_QUERY_ID, segment, N, MAX_TIMESTAMP);
        ExecContext exec_context(query_context.get());
        {
            auto expr = MakeExpr(
                segment, i64_fid_, DataType::INT64, *query_context, N, N);
            EvalCtx ctx(&exec_context);
            ctx.set_bitmap_input(IrregularBitmap(0, N));
            std::string expected;
            for (int64_t i = 0; i < N; ++i) {
                expected.push_back(
                    i >= skipped && IrregularCandidate(i) ? 'T' : 'F');
            }
            int64_t rows_seen = 0;
            EXPECT_EQ(TriStates(expr->EvalKernel<int64_t>(
                          ctx, SkipChunkKernel<int64_t>{{&rows_seen}, 0}, false)),
                      expected)
                << where;
            EXPECT_LE(rows_seen, N - skipped) << where;
        }
        {
            auto expr = MakeExpr(
                segment, i64_fid_, DataType::INT64, *query_context, N, N);
            EvalCtx ctx(&exec_context);
            ctx.set_bitmap_input(IrregularBitmap(0, N));
            std::vector<int32_t> seen;
            expr->EvalKernel<int64_t>(
                ctx, SegmentOffsetRecorder<int64_t>{&seen}, false);
            for (int32_t i = 0; i < N; ++i) {
                if (IrregularCandidate(i)) {
                    EXPECT_NE(std::find(seen.begin(), seen.end(), i),
                              seen.end())
                        << where << ": no segment offset for candidate " << i;
                }
            }
        }
    }
}

TEST_F(KernelAdapterContractTest, ElementLevelConstantBatches) {
    constexpr int64_t kBatch = 5;
    for (const bool is_growing : {true, false}) {
        const auto* segment = SegmentFor(is_growing);
        const std::string where = is_growing ? "growing" : "sealed";
        auto query_context = std::make_shared<QueryContext>(
            DEAFULT_QUERY_ID, segment, N, MAX_TIMESTAMP);
        ExecContext exec_context(query_context.get());

        for (const bool always_true : {false, true}) {
            const ConstantKernel<int64_t> kernel{!always_true, always_true};
            auto expr = MakeExpr(
                segment, array_fid_, DataType::INT64, *query_context, N, kBatch);
            std::string out;
            while (true) {
                EvalCtx ctx(&exec_context);
                auto res = expr->EvalKernel<int64_t>(ctx, kernel, true);
                if (res == nullptr) {
                    break;
                }
                out += TriStates(res);
                ASSERT_LE(static_cast<int64_t>(out.size()), N)
                    << where << ": constant batches must advance the cursor";
            }
            EXPECT_EQ(out, std::string(N, always_true ? 'T' : 'F')) << where;
        }

        auto expr =
            MakeExpr(segment, array_fid_, DataType::INT64, *query_context, N, N);
        expr->SetHasOffsetInput(true);
        OffsetVector element_ids{0, 5, 17, 30};
        EvalCtx ctx(&exec_context, &element_ids);
        ctx.set_bitmap_input(BitmapFrom("1011"));
        EXPECT_EQ(TriStates(expr->EvalKernel<int64_t>(
                      ctx, ConstantKernel<int64_t>{false, true}, true)),
                  "TFTT")
            << where;
    }
}

TEST(KernelAdapterContractNullableTest, NullRowsFoldAndConstantsAdvance) {
    constexpr int64_t kRows = 16;
    auto fixture = MakeNullableSegments(kRows);

    for (const bool is_growing : {true, false}) {
        const auto* segment = fixture.Segment(is_growing);
        const auto valid = fixture.Valid(is_growing);
        std::string expect_true, expect_false, expect_known_false;
        for (int64_t i = 0; i < kRows; ++i) {
            expect_true.push_back(valid[i] ? 'T' : 'U');
            expect_false.push_back(valid[i] ? 'F' : 'U');
            expect_known_false.push_back(valid[i] ? 'T' : 'F');
        }
        ASSERT_NE(expect_true.find('U'), std::string::npos)
            << "choose DataGen seeds that produce at least one NULL row";

        auto query_context = std::make_shared<QueryContext>(
            DEAFULT_QUERY_ID, segment, kRows, MAX_TIMESTAMP);
        ExecContext exec_context(query_context.get());
        const std::string where = is_growing ? "growing" : "sealed";
        auto make_expr = [&](int64_t batch_size) {
            return MakeExpr(segment,
                            fixture.value_fid,
                            DataType::INT64,
                            *query_context,
                            kRows,
                            batch_size);
        };

        {
            auto expr = make_expr(kRows);
            EvalCtx ctx(&exec_context);
            EXPECT_EQ(TriStates(expr->EvalKernel<int64_t>(
                          ctx, WholesaleTrueKernel<int64_t>{}, false)),
                      expect_true)
                << where;
        }
        {
            auto expr = make_expr(kRows);
            EvalCtx ctx(&exec_context);
            EXPECT_EQ(TriStates(expr->EvalKernel<int64_t>(
                          ctx, NullKnownFalseTrueKernel<int64_t>{}, false)),
                      expect_known_false)
                << where;
        }
        for (const bool always_true : {false, true}) {
            const int64_t half = kRows / 2;
            auto expr = make_expr(half);
            EvalCtx ctx(&exec_context);
            const ConstantKernel<int64_t> kernel{!always_true, always_true};
            const auto& expected = always_true ? expect_true : expect_false;
            EXPECT_EQ(TriStates(expr->EvalKernel<int64_t>(ctx, kernel, false)),
                      expected.substr(0, half))
                << where;
            EXPECT_EQ(TriStates(expr->EvalKernel<int64_t>(ctx, kernel, false)),
                      expected.substr(half))
                << where << ": constant batches must advance the cursor";
            EXPECT_EQ(expr->EvalKernel<int64_t>(ctx, kernel, false), nullptr)
                << where;
        }
    }
}

TEST(KernelAdapterContractNullableTest, OffsetsFoldNullsWithMask) {
    constexpr int64_t kRows = 16;
    auto fixture = MakeNullableSegments(kRows);
    const OffsetVector offsets{0, 1, 2, 3, 9, 10, 13};
    const std::string bits = "1011011";

    for (const bool is_growing : {true, false}) {
        const auto* segment = fixture.Segment(is_growing);
        const auto valid = fixture.Valid(is_growing);
        const std::string where = is_growing ? "growing" : "sealed";
        auto expected = [&](char on_valid, char on_null) {
            std::string out;
            for (size_t k = 0; k < offsets.size(); ++k) {
                out.push_back(bits[k] == '0'        ? 'F'
                              : valid[offsets[k]] ? on_valid
                                                  : on_null);
            }
            return out;
        };
        ASSERT_NE(expected('T', 'U').find('U'), std::string::npos) << where;

        auto query_context = std::make_shared<QueryContext>(
            DEAFULT_QUERY_ID, segment, kRows, MAX_TIMESTAMP);
        ExecContext exec_context(query_context.get());
        auto eval = [&](const auto& kernel) {
            auto expr = MakeExpr(segment,
                                 fixture.value_fid,
                                 DataType::INT64,
                                 *query_context,
                                 kRows,
                                 kRows);
            expr->SetHasOffsetInput(true);
            OffsetVector input = offsets;
            EvalCtx ctx(&exec_context, &input);
            ctx.set_bitmap_input(BitmapFrom(bits));
            return TriStates(expr->EvalKernel<int64_t>(ctx, kernel, false));
        };

        int64_t rows_seen = 0;
        EXPECT_EQ(eval(ProbeKernel<int64_t>{&rows_seen}), expected('T', 'U'))
            << where;
        EXPECT_EQ(eval(NullKnownFalseTrueKernel<int64_t>{}), expected('T', 'F'))
            << where;
        EXPECT_EQ(eval(ConstantKernel<int64_t>{false, true}), expected('T', 'U'))
            << where;
        EXPECT_EQ(eval(ConstantKernel<int64_t>{true, false}), expected('F', 'U'))
            << where;
        EXPECT_EQ(eval(NullKnownFalseConstantKernel<int64_t>{{false, true}}),
                  expected('T', 'F'))
            << where;
    }
}

TEST(KernelAdapterContractNullableTest, BatchesCrossBitmapWords) {
    constexpr int64_t kRows = 130;
    constexpr int64_t kBatch = 100;
    auto fixture = MakeNullableSegments(kRows);

    for (const bool is_growing : {true, false}) {
        const auto* segment = fixture.Segment(is_growing);
        const auto valid = fixture.Valid(is_growing);
        const auto values = fixture.Values(is_growing);
        const std::string where = is_growing ? "growing" : "sealed";
        auto expected = [&](auto on_valid, char on_null) {
            std::string out;
            for (int64_t i = 0; i < kRows; ++i) {
                out.push_back(!IrregularCandidate(i) ? 'F'
                              : valid[i]             ? on_valid(i)
                                                     : on_null);
            }
            return out;
        };
        auto always_t = [](int64_t) { return 'T'; };

        auto query_context = std::make_shared<QueryContext>(
            DEAFULT_QUERY_ID, segment, kRows, MAX_TIMESTAMP);
        ExecContext exec_context(query_context.get());
        auto eval = [&](const auto& kernel) {
            auto expr = MakeExpr(segment,
                                 fixture.value_fid,
                                 DataType::INT64,
                                 *query_context,
                                 kRows,
                                 kBatch);
            return EvalAllBatches<int64_t>(
                *expr, exec_context, kernel, kBatch, kRows, IrregularBitmap);
        };

        EXPECT_EQ(eval(DivisibleValueKernel{4}),
                  expected(
                      [&](int64_t i) { return values[i] % 4 == 0 ? 'T' : 'F'; },
                      'U'))
            << where;
        EXPECT_EQ(eval(NullKnownFalseTrueKernel<int64_t>{}),
                  expected(always_t, 'F'))
            << where;
        EXPECT_EQ(eval(ConstantKernel<int64_t>{false, true}),
                  expected(always_t, 'U'))
            << where;
    }
}

namespace {

struct IndexOnlySegment {
    SchemaPtr schema;
    FieldId value_fid;
    std::unique_ptr<SegmentSealed> segment;
};

// Eight rows in two raw chunks; the value field is then served only by a
// sort index whose row 1 is NULL.
IndexOnlySegment
MakeIndexOnlySealed() {
    constexpr int64_t kRows = 8;
    IndexOnlySegment out;
    out.schema = std::make_shared<Schema>();
    out.schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
    auto pk_fid = out.schema->AddDebugField("pk", DataType::INT64);
    out.value_fid = out.schema->AddDebugField("value", DataType::INT64, true);
    out.schema->set_primary_field_id(pk_fid);

    auto first = DataGen(out.schema, kRows / 2, 100, 0, 1, 1);
    auto second = DataGen(out.schema, kRows / 2, 101, 0, 1, 1);
    out.segment = CreateTwoChunkSealed(out.schema, first, second);

    std::vector<int64_t> index_values(kRows);
    std::iota(index_values.begin(), index_values.end(), int64_t{0});
    auto index_valid = std::make_unique<bool[]>(kRows);
    for (int64_t i = 0; i < kRows; ++i) {
        index_valid[i] = true;
    }
    index_valid[1] = false;

    auto scalar_index = index::CreateScalarIndexSort<int64_t>();
    scalar_index->Build(kRows, index_values.data(), index_valid.get());
    LoadIndexInfo load_index_info;
    load_index_info.field_id = out.value_fid.get();
    load_index_info.field_type = DataType::INT64;
    load_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    load_index_info.index_params = GenIndexParams(scalar_index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("kernel-adapter-index-only", std::move(scalar_index));
    out.segment->LoadIndex(load_index_info);
    out.segment->DropFieldData(out.value_fid);
    AssertInfo(!out.segment->HasFieldData(out.value_fid),
               "index-only segment still has raw data");
    return out;
}

}  // namespace

TEST(KernelAdapterContractIndexOnlyTest, OffsetsReverseLookupFoldsNullAndPrunes) {
    constexpr int64_t kRows = 8;
    auto fixture = MakeIndexOnlySealed();
    auto query_context = std::make_shared<QueryContext>(
        DEAFULT_QUERY_ID, fixture.segment.get(), kRows, MAX_TIMESTAMP);
    ExecContext exec_context(query_context.get());
    auto expr = MakeExpr(fixture.segment.get(),
                         fixture.value_fid,
                         DataType::INT64,
                         *query_context,
                         kRows,
                         kRows);
    expr->SetHasOffsetInput(true);

    OffsetVector input{0, 1, 2, 5};
    EvalCtx ctx(&exec_context, &input);
    ctx.set_bitmap_input(BitmapFrom("1110"));

    int64_t rows_seen = 0;
    auto res = expr->EvalKernel<int64_t>(
        ctx, SkipAllKernel<int64_t>{{&rows_seen}}, false);

    // Row 1 is NULL in the index; row 3 is pruned; chunk SkipIndex does not
    // apply to reverse lookup.
    EXPECT_EQ(TriStates(res), "TUTF");
    EXPECT_EQ(rows_seen, 2);
}

TEST(KernelAdapterContractIndexOnlyTest, SequentialReverseLookupAdvancesCursor) {
    constexpr int64_t kRows = 8;
    auto fixture = MakeIndexOnlySealed();
    auto query_context = std::make_shared<QueryContext>(
        DEAFULT_QUERY_ID, fixture.segment.get(), kRows, MAX_TIMESTAMP);
    ExecContext exec_context(query_context.get());
    auto expr = MakeExpr(fixture.segment.get(),
                         fixture.value_fid,
                         DataType::INT64,
                         *query_context,
                         kRows,
                         kRows);

    EvalCtx ctx(&exec_context);
    ctx.set_bitmap_input(BitmapFrom("11101111"));
    int64_t rows_seen = 0;
    auto res = expr->EvalKernel<int64_t>(
        ctx, ProbeKernel<int64_t>{&rows_seen}, false);
    EXPECT_EQ(TriStates(res), "TUTFTTTT");
    EXPECT_EQ(rows_seen, 6);

    EvalCtx next(&exec_context);
    EXPECT_EQ(expr->EvalKernel<int64_t>(
                  next, ProbeKernel<int64_t>{&rows_seen}, false),
              nullptr);
}

TEST(KernelAdapterContractIndexOnlyTest, ConstantBatchesReadIndexValidity) {
    constexpr int64_t kRows = 8;
    auto fixture = MakeIndexOnlySealed();
    auto query_context = std::make_shared<QueryContext>(
        DEAFULT_QUERY_ID, fixture.segment.get(), kRows, MAX_TIMESTAMP);
    ExecContext exec_context(query_context.get());
    auto make_expr = [&] {
        return MakeExpr(fixture.segment.get(),
                        fixture.value_fid,
                        DataType::INT64,
                        *query_context,
                        kRows,
                        kRows);
    };

    {
        auto expr = make_expr();
        const ConstantKernel<int64_t> kernel{false, true};
        EvalCtx ctx(&exec_context);
        EXPECT_EQ(TriStates(expr->EvalKernel<int64_t>(ctx, kernel, false)),
                  "TUTTTTTT");
        EvalCtx next(&exec_context);
        EXPECT_EQ(expr->EvalKernel<int64_t>(next, kernel, false), nullptr);
    }
    {
        auto expr = make_expr();
        EvalCtx ctx(&exec_context);
        ctx.set_bitmap_input(BitmapFrom("11101111"));
        EXPECT_EQ(TriStates(expr->EvalKernel<int64_t>(
                      ctx,
                      NullKnownFalseConstantKernel<int64_t>{{false, true}},
                      false)),
                  "TFTFTTTT");
    }
    {
        auto expr = make_expr();
        expr->SetHasOffsetInput(true);
        OffsetVector input{0, 1, 2, 5};
        EvalCtx ctx(&exec_context, &input);
        ctx.set_bitmap_input(BitmapFrom("1110"));
        EXPECT_EQ(TriStates(expr->EvalKernel<int64_t>(
                      ctx, ConstantKernel<int64_t>{true, false}, false)),
                  "FUFF");
    }
    {
        auto expr = make_expr();
        EvalCtx ctx(&exec_context);
        EXPECT_ANY_THROW(expr->EvalKernel<int64_t>(
            ctx, NullKnownFalseTrueKernel<int64_t>{}, false));
    }
}
