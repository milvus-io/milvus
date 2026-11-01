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

#include <boost/container/vector.hpp>
#include <folly/FBVector.h>
#include <folly/ScopeGuard.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <numeric>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "bitset/bitset.h"
#include "common/Array.h"
#include "common/Consts.h"
#include "common/FieldData.h"
#include "common/IndexMeta.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "common/Vector.h"
#include "common/protobuf_utils.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/ExprBatchTestUtils.h"
#include "exec/expression/ExprIndexIntegrationTestUtils.h"
#include "exec/expression/UnaryExpr.h"
#include "expr/ITypeExpr.h"
#include "filemanager/InputStream.h"
#include "google/protobuf/util/message_differencer.h"
#include "gtest/gtest.h"
#include "index/Meta.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "exec/expression/ExprBatchTestUtils.h"
#include "segcore/test_utils/ConsumerIndexTestUtils.h"
#include "index/contracts/query/IVectorReader.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IScalarValueReader.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/version.h"
#include "pb/plan.pb.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/Plan.h"
#include "query/PlanImpl.h"
#include "query/PlanNode.h"
#include "query/Utils.h"
#include "storage/Util.h"
#include "storage/ChunkManager.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Utils.h"
#include "segcore/arrow_field_utils.h"
#include "segcore/storagev2translator/GroupCTMeta.h"
#include "storage/MmapManager.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/ManifestTestUtil.h"
#include "test_utils/SegcoreConfigUtils.h"
#include "test_utils/storage_test_utils.h"
#include "test_utils/cachinglayer_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

namespace {

proto::plan::GenericValue
Int64Value(int64_t value) {
    proto::plan::GenericValue val;
    val.set_int64_val(value);
    return val;
}

void
SetInt64ArrayFieldData(GeneratedData& raw_data,
                       FieldId field_id,
                       const std::vector<std::vector<int64_t>>& rows) {
    ASSERT_EQ(raw_data.raw_->num_rows(), static_cast<int64_t>(rows.size()));
    for (int i = 0; i < raw_data.raw_->fields_data_size(); i++) {
        auto* field_data = raw_data.raw_->mutable_fields_data(i);
        if (field_data->field_id() != field_id.get()) {
            continue;
        }

        auto* arrays =
            field_data->mutable_scalars()->mutable_array_data()->mutable_data();
        arrays->Clear();
        for (const auto& row : rows) {
            auto* array_data = arrays->Add();
            array_data->mutable_long_data()->mutable_data()->Add(
                row.data(), row.data() + row.size());
        }
        return;
    }
    FAIL() << "field id not found: " << field_id.get();
}

// Like SetInt64ArrayFieldData, but for a nullable field: std::nullopt marks a
// NULL row (dense placeholder entry + valid_data bit cleared).
void
SetNullableInt64ArrayFieldData(
    GeneratedData& raw_data,
    FieldId field_id,
    const std::vector<std::optional<std::vector<int64_t>>>& rows) {
    ASSERT_EQ(raw_data.raw_->num_rows(), static_cast<int64_t>(rows.size()));
    for (int i = 0; i < raw_data.raw_->fields_data_size(); i++) {
        auto* field_data = raw_data.raw_->mutable_fields_data(i);
        if (field_data->field_id() != field_id.get()) {
            continue;
        }

        auto* arrays =
            field_data->mutable_scalars()->mutable_array_data()->mutable_data();
        arrays->Clear();
        // Row validity lives in ScalarField.valid_data for new payloads
        // (see GetFieldDataRowValidData); replace what DataGen filled in.
        auto* valid_data = MutableFieldDataRowValidData(field_data);
        valid_data->Clear();
        for (const auto& row : rows) {
            auto* array_data = arrays->Add();
            auto* long_data = array_data->mutable_long_data()->mutable_data();
            if (row.has_value()) {
                long_data->Add(row->data(), row->data() + row->size());
            }
            valid_data->Add(row.has_value());
        }
        return;
    }
    FAIL() << "field id not found: " << field_id.get();
}

void
AssertColumnVector(const ColumnVectorPtr& vec,
                   const std::vector<bool>& expected_bits,
                   const std::vector<bool>& expected_valid,
                   const std::string& label) {
    ASSERT_EQ(vec->size(), expected_bits.size()) << label;
    ASSERT_EQ(expected_bits.size(), expected_valid.size()) << label;
    BitsetTypeView bits(vec->GetRawData(), vec->size());
    BitsetTypeView valid(vec->GetValidRawData(), vec->size());
    for (size_t i = 0; i < expected_bits.size(); ++i) {
        EXPECT_EQ(bits[i], expected_bits[i]) << label << ", row " << i;
        EXPECT_EQ(valid[i], expected_valid[i]) << label << ", row " << i;
    }
}

std::vector<uint8_t>
BuildValidBitmap(const FixedVector<bool>& valid_data) {
    std::vector<uint8_t> valid_bitmap((valid_data.size() + 7) / 8, 0);
    for (size_t i = 0; i < valid_data.size(); ++i) {
        if (valid_data[i]) {
            valid_bitmap[i >> 3] |= 1 << (i & 0x07);
        }
    }
    return valid_bitmap;
}

int64_t
CountValidRows(const FixedVector<bool>& valid_data) {
    return std::count(valid_data.begin(), valid_data.end(), true);
}

std::vector<float>
MakeCompactFloatVectors(int64_t valid_count, int64_t dim) {
    std::vector<float> vectors(valid_count * dim);
    for (int64_t i = 0; i < valid_count; ++i) {
        for (int64_t d = 0; d < dim; ++d) {
            vectors[i * dim + d] = static_cast<float>(i * dim + d);
        }
    }
    return vectors;
}

std::shared_ptr<expr::NullExpr>
MakeVectorNullExpr(FieldId field_id,
                   bool nullable,
                   proto::plan::NullExpr_NullOp op,
                   DataType data_type = DataType::VECTOR_FLOAT) {
    return std::make_shared<expr::NullExpr>(
        expr::ColumnInfo(field_id, data_type, {}, nullable), op);
}

std::shared_ptr<plan::FilterBitsNode>
MakeVectorNullPlan(FieldId field_id,
                   bool nullable,
                   proto::plan::NullExpr_NullOp op,
                   DataType data_type = DataType::VECTOR_FLOAT) {
    return std::make_shared<plan::FilterBitsNode>(
        DEFAULT_PLANNODE_ID,
        MakeVectorNullExpr(field_id, nullable, op, data_type));
}

IndexMetaPtr
MakeFloatVectorIndexMeta(FieldId field_id, int64_t dim, int64_t max_rows) {
    std::map<std::string, std::string> index_params = {
        {"index_type", knowhere::IndexEnum::INDEX_FAISS_IVFFLAT},
        {"metric_type", knowhere::metric::L2},
        {"nlist", "1"}};
    std::map<std::string, std::string> type_params = {
        {"dim", std::to_string(dim)}};
    FieldIndexMeta field_index_meta(
        field_id, std::move(index_params), std::move(type_params));
    std::map<FieldId, FieldIndexMeta> field_map = {
        {field_id, field_index_meta}};
    return std::make_shared<CollectionIndexMeta>(max_rows,
                                                 std::move(field_map));
}

SegmentSealedUPtr
MakeSealedVectorIndexOnlySegment(const SchemaPtr& schema,
                                 FieldId vector_fid,
                                 const FixedVector<bool>& valid_data,
                                 int64_t dim,
                                 const std::string& cache_key) {
    const auto row_count = static_cast<int64_t>(valid_data.size());
    const auto valid_count = CountValidRows(valid_data);
    auto vectors =
        MakeCompactFloatVectors(std::max<int64_t>(valid_count, 1), dim);
    std::unique_ptr<bool[]> valid_data_bool(new bool[row_count]);
    for (int64_t i = 0; i < row_count; ++i) {
        valid_data_bool[i] = valid_data[i];
    }
    auto opened = test::consumer::BuildVectorReader<float>(
        DataType::VECTOR_FLOAT,
        knowhere::IndexEnum::INDEX_FAISS_IDMAP,
        knowhere::metric::L2,
        dim,
        row_count,
        vectors.data(),
        Config::object(),
        true,
        ValidityView::FromExpanded(valid_data_bool.get()),
        valid_count);
    const auto* vec_indexing =
        dynamic_cast<const index::IVectorReader*>(opened.reader.get());
    AssertInfo(vec_indexing != nullptr, "invalid generated vector reader");

    TargetBitmap range_storage(row_count + 6, false);
    auto range_result = range_storage.view(3, row_count);
    for (int64_t i = 0; i < row_count; ++i) {
        range_result[i] = true;
    }
    vec_indexing->ApplyValidDataByRange(0, row_count, range_result);
    for (int64_t i = 0; i < row_count; ++i) {
        EXPECT_EQ(range_result[i], valid_data[i]) << "range row " << i;
    }
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_FALSE(range_storage[i]);
        EXPECT_FALSE(range_storage[row_count + 3 + i]);
    }

    const std::vector<int64_t> offsets = {-1, 0, 1, row_count - 1, row_count};
    TargetBitmap offset_result(offsets.size(), true);
    vec_indexing->ApplyValidDataByOffsets(
        offsets.data(), offsets.size(), TargetBitmapView(offset_result));
    EXPECT_FALSE(offset_result[0]);
    EXPECT_EQ(offset_result[1], valid_data[0]);
    EXPECT_EQ(offset_result[2], valid_data[1]);
    EXPECT_EQ(offset_result[3], valid_data[row_count - 1]);
    EXPECT_FALSE(offset_result[4]);

    auto sealed_segment = CreateSealedSegment(schema);
    LoadIndexInfo load_index_info;
    load_index_info.collection_id = kCollectionID;
    load_index_info.partition_id = kPartitionID;
    load_index_info.segment_id = kSegmentID;
    load_index_info.field_id = vector_fid.get();
    load_index_info.field_type = DataType::VECTOR_FLOAT;
    load_index_info.element_type = DataType::NONE;
    load_index_info.enable_mmap = false;
    load_index_info.index_size = 0;
    load_index_info.dim = dim;
    load_index_info.num_rows = row_count;
    load_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    load_index_info.index_params = GenIndexParams(opened.reader.get());
    load_index_info.index_family = opened.family;
    load_index_info.index_value_type = opened.reader->ValueType();
    load_index_info.index_caps = opened.caps;
    load_index_info.index_params["metric_type"] = knowhere::metric::L2;
    load_index_info.cache_index =
        CreateTestCacheIndex(cache_key, std::move(opened.reader));
    sealed_segment->LoadIndex(load_index_info);
    return sealed_segment;
}

BitsetType
ExecuteQueryExprWithBatchSize(std::shared_ptr<plan::FilterBitsNode> filter_plan,
                              const SegmentInternalInterface* segment,
                              int64_t active_count,
                              Timestamp timestamp,
                              int64_t batch_size) {
    auto plan_fragment = plan::PlanFragment(filter_plan);
    auto query_config = std::make_shared<milvus::exec::QueryConfig>(
        std::unordered_map<std::string, std::string>{
            {milvus::exec::QueryConfig::kExprEvalBatchSize,
             std::to_string(batch_size)}});
    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        DEAFULT_QUERY_ID,
        segment,
        active_count,
        timestamp,
        0,
        0,
        milvus::query::PlanOptions(),
        query_config);
    auto row = ExecPlanNodeVisitor::ExecuteTask(plan_fragment, query_context);
    AssertInfo(row != nullptr,
               "ExecuteTask returned null row vector for query expression");
    AssertInfo(
        row->childrens().size() == 1,
        "query expr operator's result vector's children size not equal one");
    auto col_vec = milvus::query::GetColumnVectorForTest(row->childrens()[0]);
    AssertInfo(col_vec != nullptr, "failed to cast to ColumnVector");
    BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
    BitsetType query_view(view);
    query_view.flip();
    return query_view;
}

void
AssertVectorNullExprMatches(const SegmentInternalInterface* segment,
                            FieldId field_id,
                            bool nullable,
                            const FixedVector<bool>& valid_data,
                            int64_t row_count,
                            DataType data_type = DataType::VECTOR_FLOAT) {
    std::vector<
        std::pair<proto::plan::NullExpr_NullOp, std::function<bool(bool)>>>
        testcases = {
            {proto::plan::NullExpr_NullOp_IsNull,
             [](bool valid) { return !valid; }},
            {proto::plan::NullExpr_NullOp_IsNotNull,
             [](bool valid) { return valid; }},
        };

    for (auto [op, ref_func] : testcases) {
        auto plan = MakeVectorNullPlan(field_id, nullable, op, data_type);
        auto final = ExecuteQueryExpr(plan, segment, row_count, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), row_count);
        for (int64_t i = 0; i < row_count; ++i) {
            ASSERT_EQ(final[i], ref_func(valid_data[i]))
                << "segment type " << segment->type() << ", row " << i;
        }

        auto batched_final = ExecuteQueryExprWithBatchSize(
            plan, segment, row_count, MAX_TIMESTAMP, 17);
        ASSERT_EQ(batched_final.size(), row_count);
        for (int64_t i = 0; i < row_count; ++i) {
            ASSERT_EQ(batched_final[i], ref_func(valid_data[i]))
                << "segment type " << segment->type() << ", batch row " << i;
        }

        milvus::exec::OffsetVector offsets;
        offsets.reserve(row_count / 2);
        for (int64_t i = 0; i < row_count; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), segment, row_count, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        ASSERT_EQ(view.size(), offsets.size());
        for (int64_t i = 0; i < static_cast<int64_t>(offsets.size()); ++i) {
            ASSERT_EQ(view[i], ref_func(valid_data[offsets[i]]))
                << "segment type " << segment->type() << ", offset row "
                << offsets[i];
        }
    }
}

void
AssertSegmentFieldValidDataByOffsets(
    const SegmentInternalInterface* segment,
    FieldId field_id,
    const std::vector<int64_t>& offsets,
    const std::vector<bool>& expected_valid_data) {
    ASSERT_EQ(offsets.size(), expected_valid_data.size());

    TargetBitmap valid_res(offsets.size(), true);
    segment->ApplyFieldValidDataByOffsets(nullptr,
                                          field_id,
                                          offsets.data(),
                                          offsets.size(),
                                          TargetBitmapView(valid_res));
    for (size_t i = 0; i < expected_valid_data.size(); ++i) {
        EXPECT_EQ(valid_res[i], expected_valid_data[i])
            << "segment type " << segment->type() << ", offset index " << i
            << ", offset " << offsets[i];
    }
}

// Keep the real sealed loading and expression paths, but count the legacy
// materialization that used to create a complete ArrayView vector per hit.
class ArrayEqualityCountingSegment : public ChunkedSegmentSealedImpl {
 public:
    using ChunkedSegmentSealedImpl::ChunkedSegmentSealedImpl;
    mutable int64_t materialized_array_rows = 0;
    mutable int64_t data_resource_reads = 0;

    std::pair<std::shared_ptr<ChunkedColumnInterface>, FieldSkipMetricsView>
    GetDataScanResources(FieldId field_id) const override {
        ++data_resource_reads;
        return ChunkedSegmentSealedImpl::GetDataScanResources(field_id);
    }

 protected:
    PinWrapper<std::pair<std::vector<ArrayView>, ValidityView>>
    chunk_array_view_impl(
        milvus::OpContext* op_ctx,
        FieldId field_id,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> offset_len) const override {
        auto views = ChunkedSegmentSealedImpl::chunk_array_view_impl(
            op_ctx, field_id, chunk_id, offset_len);
        materialized_array_rows += views.get().first.size();
        return views;
    }
};

class ArrayEqualityCountingIndex final
    : public index::IIndexReaderBase,
      public index::IScalarPredicateReader<int64_t>,
      public index::IScalarValueReader<int64_t>,
      public index::INullReader {
 public:
    explicit ArrayEqualityCountingIndex(index::IIndexReaderBasePtr reader)
        : reader_(std::move(reader)),
          predicates_(
              dynamic_cast<const index::IScalarPredicateReader<int64_t>*>(
                  reader_.get())),
          nulls_(dynamic_cast<const index::INullReader*>(reader_.get())),
          values_(dynamic_cast<const index::IScalarValueReader<int64_t>*>(
              reader_.get())) {
        AssertInfo(predicates_ != nullptr && nulls_ != nullptr,
                   "ARRAY equality fixture needs predicate and null readers");
        AssertInfo(!reader_->Caps().value_lookup || values_ != nullptr,
                   "ARRAY equality fixture needs the advertised value reader");
    }

    mutable size_t callback_calls = 0;

    index::ReaderCaps
    Caps() const override {
        return reader_->Caps();
    }
    index::Domain
    CoordDomain() const override {
        return reader_->CoordDomain();
    }
    int64_t
    Count() const override {
        return reader_->Count();
    }
    DataType
    ValueType() const override {
        return reader_->ValueType();
    }
    int64_t
    MemoryUsage() const override {
        return reader_->MemoryUsage();
    }
    cachinglayer::ResourceUsage
    CellByteSize() const override {
        return reader_->CellByteSize();
    }

    TargetBitmap
    In(size_t n, const int64_t* values) const override {
        ++callback_calls;
        return predicates_->In(n, values);
    }

    TargetBitmap
    NotIn(size_t n, const int64_t* values) const override {
        return predicates_->NotIn(n, values);
    }

    TargetBitmap
    Range(const int64_t& value, index::CompareOp op) const override {
        return predicates_->Range(value, op);
    }

    TargetBitmap
    Range(const int64_t& lo,
          bool lo_inc,
          const int64_t& hi,
          bool hi_inc) const override {
        return predicates_->Range(lo, lo_inc, hi, hi_inc);
    }

    TargetBitmap
    IsNull() const override {
        return nulls_->IsNull();
    }

    TargetBitmap
    IsNotNull() const override {
        return nulls_->IsNotNull();
    }

    std::optional<int64_t>
    Lookup(int64_t offset) const override {
        AssertInfo(values_ != nullptr, "value reader is unavailable");
        return values_->Lookup(offset);
    }

    void
    Gather(const int64_t* offsets,
           int64_t count,
           const std::function<void(int64_t, const int64_t*, bool)>& out)
        const override {
        AssertInfo(values_ != nullptr, "value reader is unavailable");
        values_->Gather(offsets, count, out);
    }

 private:
    index::IIndexReaderBasePtr reader_;
    const index::IScalarPredicateReader<int64_t>* predicates_;
    const index::INullReader* nulls_;
    const index::IScalarValueReader<int64_t>* values_;
};

}  // namespace

TEST(Expr, TestNonNullableOrdinaryVectorNullExprCoversAllDTypes) {
    constexpr int64_t N = 8;
    constexpr int64_t dim = 16;
    const std::vector<DataType> vector_types = {
        DataType::VECTOR_FLOAT,
        DataType::VECTOR_BINARY,
        DataType::VECTOR_FLOAT16,
        DataType::VECTOR_BFLOAT16,
        DataType::VECTOR_SPARSE_U32_F32,
        DataType::VECTOR_INT8,
    };

    for (auto data_type : vector_types) {
        auto schema = std::make_shared<Schema>();
        auto i64_fid = schema->AddDebugField("id", DataType::INT64);
        auto vector_fid = schema->AddDebugField(
            "embedding", data_type, dim, knowhere::metric::L2, false);
        schema->set_primary_field_id(i64_fid);

        auto sealed_segment = CreateSealedSegment(schema);
        FixedVector<bool> all_valid(N, true);
        AssertVectorNullExprMatches(
            sealed_segment.get(), vector_fid, false, all_valid, N, data_type);
    }
}

TEST(Expr, TestArraySubscriptMissingElementIsUnknown) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto struct_array_fid = schema->AddDebugArrayField(
        "structA[price_array]", DataType::INT64, false);
    schema->set_primary_field_id(i64_fid);

    constexpr int N = 3;
    auto raw_data = DataGen(schema, N, 42, 0, 1, 2);
    std::vector<std::vector<int64_t>> rows = {{1, 2}, {}, {3}};
    SetInt64ArrayFieldData(raw_data, long_array_fid, rows);
    SetInt64ArrayFieldData(raw_data, struct_array_fid, rows);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    auto offset = seg->PreInsert(N);
    seg->Insert(offset,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                std::make_shared<InsertRecordProto>(*raw_data.raw_));
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ASSERT_NE(seg_promote, nullptr);

    struct Case {
        std::string name;
        milvus::expr::TypedExprPtr expr;
        std::vector<bool> bits;
        std::vector<bool> valid;
        std::vector<bool> not_bits;
        std::vector<bool> not_valid;
    };

    auto make_cases = [](const expr::ColumnInfo& column) {
        std::vector<proto::plan::GenericValue> term_values{Int64Value(2)};
        return std::vector<Case>{
            {"equal",
             std::make_shared<expr::UnaryRangeFilterExpr>(
                 column, proto::plan::OpType::Equal, Int64Value(2)),
             {true, false, false},
             {true, false, false},
             {false, false, false},
             {true, false, false}},
            {"not_equal",
             std::make_shared<expr::UnaryRangeFilterExpr>(
                 column, proto::plan::OpType::NotEqual, Int64Value(2)),
             {false, false, false},
             {true, false, false},
             {true, false, false},
             {true, false, false}},
            {"binary_range",
             std::make_shared<expr::BinaryRangeFilterExpr>(
                 column, Int64Value(1), Int64Value(3), false, false),
             {true, false, false},
             {true, false, false},
             {false, false, false},
             {true, false, false}},
            {"term",
             std::make_shared<expr::TermFilterExpr>(column, term_values),
             {true, false, false},
             {true, false, false},
             {false, false, false},
             {true, false, false}},
            // An empty set is FALSE even where the element is missing.
            {"term_empty",
             std::make_shared<expr::TermFilterExpr>(
                 column, std::vector<proto::plan::GenericValue>{}),
             {false, false, false},
             {true, true, true},
             {true, true, true},
             {true, true, true}},
            {"arith",
             std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
                 column,
                 proto::plan::OpType::Equal,
                 proto::plan::ArithOpType::Add,
                 Int64Value(3),
                 Int64Value(1)),
             {true, false, false},
             {true, false, false},
             {false, false, false},
             {true, false, false}},
        };
    };

    std::vector<std::pair<std::string, FieldId>> fields = {
        {"array", long_array_fid},
        {"struct_array", struct_array_fid},
    };
    for (const auto& [field_name, field_id] : fields) {
        auto column =
            expr::ColumnInfo(field_id, DataType::ARRAY, DataType::INT64, {"1"});
        for (const auto& testcase : make_cases(column)) {
            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, testcase.expr);
            auto vec = milvus::test::gen_filter_res(
                plan.get(), seg_promote, N, MAX_TIMESTAMP);
            AssertColumnVector(vec,
                               testcase.bits,
                               testcase.valid,
                               field_name + " " + testcase.name);

            exec::OffsetVector offsets = {1, 0, 2};
            auto offset_vec = milvus::test::gen_filter_res(
                plan.get(), seg_promote, N, MAX_TIMESTAMP, &offsets);
            AssertColumnVector(
                offset_vec,
                {testcase.bits[1], testcase.bits[0], testcase.bits[2]},
                {testcase.valid[1], testcase.valid[0], testcase.valid[2]},
                field_name + " " + testcase.name + " offsets");

            auto not_expr = std::make_shared<expr::LogicalUnaryExpr>(
                expr::LogicalUnaryExpr::OpType::LogicalNot, testcase.expr);
            auto not_plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, not_expr);
            auto not_vec = milvus::test::gen_filter_res(
                not_plan.get(), seg_promote, N, MAX_TIMESTAMP);
            AssertColumnVector(not_vec,
                               testcase.not_bits,
                               testcase.not_valid,
                               "not " + field_name + " " + testcase.name);

            auto final =
                ExecuteQueryExpr(not_plan, seg_promote, N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            for (int i = 0; i < N; ++i) {
                EXPECT_EQ(final[i],
                          testcase.not_bits[i] && testcase.not_valid[i])
                    << "not " << field_name << " " << testcase.name << ", row "
                    << i;
            }
        }
    }
}

TEST(Expr, TestArrayElementPredicateWithoutNestedPathThrows) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    constexpr int N = 2;
    auto raw_data = DataGen(schema, N, 42, 0, 1, 2);
    SetInt64ArrayFieldData(raw_data, long_array_fid, {{1, 2}, {3, 4}});

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    auto offset = seg->PreInsert(N);
    seg->Insert(offset,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                std::make_shared<InsertRecordProto>(*raw_data.raw_));
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ASSERT_NE(seg_promote, nullptr);

    auto whole_array_column =
        expr::ColumnInfo(long_array_fid, DataType::ARRAY, DataType::INT64);
    std::vector<proto::plan::GenericValue> term_values{Int64Value(2)};
    std::vector<std::pair<std::string, expr::TypedExprPtr>> cases = {
        {"unary",
         std::make_shared<expr::UnaryRangeFilterExpr>(
             whole_array_column, proto::plan::OpType::Equal, Int64Value(2))},
        {"binary_range",
         std::make_shared<expr::BinaryRangeFilterExpr>(
             whole_array_column, Int64Value(1), Int64Value(3), false, false)},
        {"term",
         std::make_shared<expr::TermFilterExpr>(whole_array_column,
                                                term_values)},
        {"arith",
         std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
             whole_array_column,
             proto::plan::OpType::Equal,
             proto::plan::ArithOpType::Add,
             Int64Value(3),
             Int64Value(1))},
    };

    for (const auto& [name, typed_expr] : cases) {
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           typed_expr);
        EXPECT_ANY_THROW({
            auto vec = milvus::test::gen_filter_res(
                plan.get(), seg_promote, N, MAX_TIMESTAMP);
            (void)vec;
        }) << name;
    }
}

TEST(Expr, ElementNullableStructChildElementLevelUsesRawData) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto child = schema->AddDebugArrayField(
        "items[child]", DataType::INT64, false, true);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);
    auto* growing = dynamic_cast<SegmentGrowingImpl*>(segment.get());
    ASSERT_NE(growing, nullptr);

    auto column = expr::ColumnInfo(child, DataType::ARRAY, DataType::INT64);
    column.element_level_ = true;
    auto logical = std::make_shared<expr::UnaryRangeFilterExpr>(
        column, proto::plan::OpType::Equal, Int64Value(1));
    exec::PhyUnaryRangeFilterExpr physical(
        {}, logical, "element-nullable child", nullptr, growing, 0, 64, 0);
    EXPECT_EQ(physical.GetExecPath(), exec::ExprExecPath::RawData);
}

TEST(Expr, TestArrayRange) {
    std::vector<std::tuple<std::string,
                           std::string,
                           std::function<bool(milvus::Array & array)>>>
        testcases = {
            // binary_range_expr: 1 < long_array[0] < 10000
            {"1 < long_array[0] < 10000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return 1 < val && val < 10000;
             }},
            // binary_range_expr: 1 < long_array[1024] < 10000
            {"1 < long_array[1024] < 10000",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data_unchecked<int64_t>(1024);
                 return 1 < val && val < 10000;
             }},
            // binary_range_expr: 1 <= long_array[0] < 10000
            {"1 <= long_array[0] < 10000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return 1 <= val && val < 10000;
             }},
            // binary_range_expr: 1 <= long_array[1024] < 10000
            {"1 <= long_array[1024] < 10000",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data_unchecked<int64_t>(1024);
                 return 1 <= val && val < 10000;
             }},
            // binary_range_expr: 1 < long_array[0] <= 10000
            {"1 < long_array[0] <= 10000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return 1 < val && val <= 10000;
             }},
            // binary_range_expr: 1 < long_array[1024] <= 10000
            {"1 < long_array[1024] <= 10000",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data_unchecked<int64_t>(1024);
                 return 1 < val && val <= 10000;
             }},
            // binary_range_expr: 1 <= long_array[0] <= 10000
            {"1 <= long_array[0] <= 10000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return 1 <= val && val <= 10000;
             }},
            // binary_range_expr: 1 <= long_array[1024] <= 10000
            {"1 <= long_array[1024] <= 10000",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data_unchecked<int64_t>(1024);
                 return 1 <= val && val <= 10000;
             }},
            // binary_range_expr: "aaa" <= string_array[0] <= "zzz"
            {R"("aaa" <= string_array[0] <= "zzz")",
             "string",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<std::string_view>(0);
                 return "aaa" <= val && val <= "zzz";
             }},
            // binary_range_expr: 1.1 <= double_array[0] <= 2048.12
            {"1.1 <= double_array[0] <= 2048.12",
             "float",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<double>(0);
                 return 1.1 <= val && val <= 2048.12;
             }},
            // unary_range_expr: long_array[0] >= 10000
            {"long_array[0] >= 10000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val >= 10000;
             }},
            // unary_range_expr: long_array[0] > 2000
            {"long_array[0] > 2000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val > 2000;
             }},
            // unary_range_expr: long_array[0] <= 2000
            {"long_array[0] <= 2000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val <= 2000;
             }},
            // unary_range_expr: long_array[0] < 2000
            {"long_array[0] < 2000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val < 2000;
             }},
            // unary_range_expr: long_array[0] == 2000
            {"long_array[0] == 2000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val == 2000;
             }},
            // unary_range_expr: long_array[0] != 2000
            {"long_array[0] != 2000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val != 2000;
             }},
            // unary_range_expr: bool_array[0] == false
            {"bool_array[0] == false",
             "bool",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<bool>(0);
                 return !val;
             }},
            // unary_range_expr: string_array[0] == "abc"
            {R"(string_array[0] == "abc")",
             "string",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<std::string_view>(0);
                 return val == "abc";
             }},
            // unary_range_expr: double_array[0] == 2.2
            {"double_array[0] == 2.2",
             "float",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<double>(0);
                 return val == 2.2;
             }},
            // unary_range_expr: double_array[1024] == 2.2
            {"double_array[1024] == 2.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data_unchecked<double>(1024);
                 return val == 2.2;
             }},
            // unary_range_expr: double_array[1024] != 2.2
            {"double_array[1024] != 2.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data_unchecked<double>(1024);
                 return val != 2.2;
             }},
            // unary_range_expr: double_array[1024] >= 2.2
            {"double_array[1024] >= 2.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data_unchecked<double>(1024);
                 return val >= 2.2;
             }},
            // unary_range_expr: double_array[1024] > 2.2
            {"double_array[1024] > 2.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data_unchecked<double>(1024);
                 return val > 2.2;
             }},
            // unary_range_expr: double_array[1024] <= 2.2
            {"double_array[1024] <= 2.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data_unchecked<double>(1024);
                 return val <= 2.2;
             }},
            // unary_range_expr: double_array[1024] < 2.2
            {"double_array[1024] < 2.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data_unchecked<double>(1024);
                 return val < 2.2;
             }},

        };
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto bool_array_fid =
        schema->AddDebugField("bool_array", DataType::ARRAY, DataType::BOOL);
    auto string_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    auto float_array_fid =
        schema->AddDebugField("double_array", DataType::ARRAY, DataType::FLOAT);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarFieldProto>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        auto new_bool_array_col =
            raw_data.get_col<ScalarFieldProto>(bool_array_fid);
        auto new_string_array_col =
            raw_data.get_col<ScalarFieldProto>(string_array_fid);
        auto new_float_array_col =
            raw_data.get_col<ScalarFieldProto>(float_array_fid);

        array_cols["long"].insert(array_cols["long"].end(),
                                  new_long_array_col.begin(),
                                  new_long_array_col.end());
        array_cols["bool"].insert(array_cols["bool"].end(),
                                  new_bool_array_col.begin(),
                                  new_bool_array_col.end());
        array_cols["string"].insert(array_cols["string"].end(),
                                    new_string_array_col.begin(),
                                    new_string_array_col.end());
        array_cols["float"].insert(array_cols["float"].end(),
                                   new_float_array_col.begin(),
                                   new_float_array_col.end());

        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*raw_data.raw_));
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ScopedSchemaHandle schema_handle(*schema);
    for (auto [expr, array_type, ref_func] : testcases) {
        auto plan_str = schema_handle.ParseSearch(
            expr, "fakevec", 10, "L2", R"({"nprobe": 10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols[array_type][i]);
            auto ref = ref_func(array);
            ASSERT_EQ(ans, ref);
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref);
            }
        }
    }
}

TEST(Expr, TestArrayEqual) {
    std::vector<
        std::tuple<std::string, std::function<bool(std::vector<int64_t>)>>>
        testcases = {
            // unary_range_expr: long_array == [1, 2, 3]
            {"long_array == [1, 2, 3]",
             [](std::vector<int64_t> v) {
                 if (v.size() != 3) {
                     return false;
                 }
                 for (int i = 0; i < 3; ++i) {
                     if (v[i] != i + 1) {
                         return false;
                     }
                 }
                 return true;
             }},
            // unary_range_expr: long_array != [1, 2, 3]
            {"long_array != [1, 2, 3]",
             [](std::vector<int64_t> v) {
                 if (v.size() != 3) {
                     return true;
                 }
                 for (int i = 0; i < 3; ++i) {
                     if (v[i] != i + 1) {
                         return true;
                     }
                 }
                 return false;
             }},
        };
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<ScalarFieldProto> long_array_col;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter, 0, 1, 3);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        long_array_col.insert(long_array_col.end(),
                              new_long_array_col.begin(),
                              new_long_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*raw_data.raw_));
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ScopedSchemaHandle schema_handle(*schema);
    for (auto [expr, ref_func] : testcases) {
        auto plan_str = schema_handle.ParseSearch(
            expr, "fakevec", 10, "L2", R"({"nprobe": 10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(long_array_col[i]);
            std::vector<int64_t> array_values(array.length());
            for (int j = 0; j < array.length(); ++j) {
                array_values.push_back(array.get_data_unchecked<int64_t>(j));
            }
            auto ref = ref_func(array_values);
            ASSERT_EQ(ans, ref);
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref);
            }
        }
    }
}

static void
CheckArrayIndexEqualityReadsOnlyCandidates(const std::string& index_type,
                                           Config params = Config::object()) {
    constexpr int64_t N = 1536;
    for (bool nested : {false, true}) {
        for (bool mmap : {false, true}) {
            for (bool sparse : {false, true}) {
                SCOPED_TRACE(fmt::format(
                    "nested={}, mmap={}, sparse={}", nested, mmap, sparse));
                auto schema = std::make_shared<Schema>();
                const auto pk = schema->AddDebugField("id", DataType::INT64);
                const auto field_name = nested ? "structA[array]" : "array";
                const auto field = schema->AddDebugField(
                    field_name, DataType::ARRAY, DataType::INT64, true);
                schema->set_primary_field_id(pk);
                auto raw_data = DataGen(schema, N);
                const std::vector<std::vector<int64_t>> patterns{
                    {1, 2, 1}, {1, 1, 2}, {1, 2}, {1, 2, 1, 1}, {}, {4, 5}};
                std::vector<std::vector<int64_t>> rows(N);
                std::vector<bool> validity(N, true);
                for (int64_t i = 0; i < N; ++i) {
                    rows[i] = sparse ? std::vector<int64_t>{4, 5}
                                     : patterns[i % patterns.size()];
                    validity[i] = i % 11 != 0;
                }
                if (sparse) {
                    rows[1] = {1, 2, 1};
                    rows[N / 2] = {1, 1, 2};
                    rows[N - 1] = {1, 2, 1};
                }
                ArrayEqualityCountingSegment segment(
                    schema,
                    empty_index_meta,
                    SegcoreConfig::default_config(),
                    kSegmentID + nested * 4 + mmap * 2 + sparse);
                LoadGeneratedDataIntoSegment(
                    raw_data, &segment, mmap, {field.get()});

                FixedVector<Array> arrays;
                std::vector<uint8_t> valid_bitmap((N + 7) / 8, 0);
                for (int64_t i = 0; i < N; ++i) {
                    ScalarFieldProto row;
                    auto* elements = row.mutable_long_data();
                    for (auto value : rows[i]) {
                        elements->add_data(value);
                    }
                    arrays.emplace_back(row);
                    if (validity[i]) {
                        valid_bitmap[i >> 3] |= 1 << (i & 7);
                    }
                }
                auto field_data = storage::CreateFieldData(
                    DataType::ARRAY, DataType::INT64, true);
                field_data->FillFieldData(
                    arrays.data(), valid_bitmap.data(), N, 0);
                // Different physical chunk sizes exercise global candidate
                // offsets on both sides of the boundary.
                constexpr int64_t split = 513;
                auto first = storage::CreateFieldData(
                    DataType::ARRAY, DataType::INT64, true);
                auto second = storage::CreateFieldData(
                    DataType::ARRAY, DataType::INT64, true);
                first->FillFieldData(
                    arrays.data(), valid_bitmap.data(), split, 0);
                second->FillFieldData(arrays.data() + split,
                                      valid_bitmap.data(),
                                      N - split,
                                      split);
                auto load_info = PrepareSingleFieldInsertBinlog(
                    kCollectionID,
                    kPartitionID,
                    kSegmentID,
                    field.get(),
                    {first, second},
                    storage::RemoteChunkManagerSingleton::GetInstance()
                        .GetRemoteChunkManager(),
                    mmap ? "./data/mmap-test" : "");
                segment.LoadFieldData(load_info);
                ASSERT_EQ(segment.num_chunk_data(field), 2);
                if (nested) {
                    ASSERT_NE(segment.GetStructElementOffsets(field), nullptr);
                }
                auto opened = test::expr_index::BuildIndex(field,
                                                           DataType::ARRAY,
                                                           index_type,
                                                           {field_data},
                                                           params,
                                                           DataType::INT64,
                                                           nested);
                if (index_type == index::HYBRID_INDEX_TYPE ||
                    index_type == index::ASCENDING_SORT) {
                    ASSERT_EQ(opened.family, index::families::kSort);
                    ASSERT_EQ(opened.caps.value_lookup, nested);
                }
                auto counting = std::make_unique<ArrayEqualityCountingIndex>(
                    std::move(opened.reader));
                auto* index_ptr = counting.get();
                opened.reader = std::move(counting);
                test::expr_index::InstallIndex(segment,
                                               field,
                                               DataType::ARRAY,
                                               std::move(opened),
                                               DataType::INT64);
                ASSERT_TRUE(segment.HasIndex(field));

                const std::vector<std::vector<int64_t>> targets{
                    {1, 2, 1}, {9, 9}, {}};
                for (const auto& target : targets) {
                    for (bool reverse : {false, true}) {
                        proto::plan::GenericValue value;
                        auto* array = value.mutable_array_val();
                        array->set_element_type(proto::schema::DataType::Int64);
                        array->set_same_type(true);
                        for (auto element : target) {
                            array->add_array()->set_int64_val(element);
                        }
                        auto expression =
                            std::make_shared<expr::UnaryRangeFilterExpr>(
                                expr::ColumnInfo(field,
                                                 DataType::ARRAY,
                                                 DataType::INT64,
                                                 {},
                                                 true),
                                reverse ? proto::plan::NotEqual
                                        : proto::plan::Equal,
                                value);
                        auto query_config = std::make_shared<exec::QueryConfig>(
                            std::unordered_map<std::string, std::string>{
                                {exec::QueryConfig::kExprEvalBatchSize,
                                 "128"}});
                        auto query_context =
                            std::make_shared<exec::QueryContext>(
                                DEAFULT_QUERY_ID,
                                &segment,
                                N,
                                MAX_TIMESTAMP,
                                0,
                                0,
                                query::PlanOptions(),
                                query_config);
                        exec::ExecContext exec_context(query_context.get());
                        exec::ExprSet exprs({expression}, &exec_context);
                        exec::EvalCtx eval_ctx(&exec_context);
                        segment.materialized_array_rows = 0;
                        segment.data_resource_reads = 0;
                        index_ptr->callback_calls = 0;
                        int64_t processed = 0;
                        while (processed < N) {
                            std::vector<VectorPtr> results;
                            exprs.Eval(0, 1, true, eval_ctx, results);
                            ASSERT_EQ(results.size(), 1);
                            ASSERT_NE(results[0], nullptr);
                            auto result = milvus::test::GetColumnVectorForTest(
                                results[0]);
                            ASSERT_GT(result->size(), 0);
                            ASSERT_LE(result->size(), 128);
                            BitsetTypeView bits(result->GetRawData(),
                                                result->size());
                            BitsetTypeView valid(result->GetValidRawData(),
                                                 result->size());
                            for (int64_t i = 0; i < result->size(); ++i) {
                                const auto row = processed + i;
                                EXPECT_EQ(valid[i], validity[row]) << row;
                                EXPECT_EQ(bits[i],
                                          validity[row] &&
                                              ((rows[row] == target) ^ reverse))
                                    << row;
                            }
                            processed += result->size();
                        }
                        ASSERT_EQ(processed, N);
                        if (!target.empty()) {
                            // Both the index and exact array recheck execute;
                            // later expression batches reuse the cached bitmap.
                            EXPECT_GT(index_ptr->callback_calls, 0);
                            EXPECT_EQ(segment.data_resource_reads,
                                      target.front() == 1 ? 1 : 0);
                            EXPECT_EQ(segment.materialized_array_rows, 0);
                        }
                    }
                }
            }
        }
    }
}

TEST(Expr, TestArrayIndexEqualityReadsOnlyCandidates) {
    CheckArrayIndexEqualityReadsOnlyCandidates(index::INVERTED_INDEX_TYPE);
}

TEST(Expr, TestArraySortedIndexEqualityReadsOnlyCandidates) {
    CheckArrayIndexEqualityReadsOnlyCandidates(
        index::HYBRID_INDEX_TYPE,
        {{index::SCALAR_INDEX_ENGINE_VERSION, kArrayHybridStlSortMinVersion},
         {index::ENABLE_ARRAY_SORT_INDEX, true},
         {index::BITMAP_INDEX_CARDINALITY_LIMIT, 1}});
}

TEST(Expr, TestArrayExplicitSortedIndexEqualityReadsOnlyCandidates) {
    CheckArrayIndexEqualityReadsOnlyCandidates(
        index::ASCENDING_SORT,
        {{index::SCALAR_INDEX_ENGINE_VERSION, kArrayHybridStlSortMinVersion}});
}

TEST(Expr, TestArrayNullExpr) {
    std::vector<std::tuple<std::string, std::function<bool(bool)>>> testcases =
        {
            // null_expr: long_array is null
            {"long_array is null", [](bool v) { return !v; }},
        };
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid = schema->AddDebugField(
        "long_array", DataType::ARRAY, DataType::INT64, true);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::vector<ScalarFieldProto> long_array_col;
    int num_iters = 1;
    FixedVector<bool> valid_data;

    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter, 0, 1, 3);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        long_array_col.insert(long_array_col.end(),
                              new_long_array_col.begin(),
                              new_long_array_col.end());
        auto new_valid_col = raw_data.get_col_valid(long_array_fid);
        valid_data.insert(
            valid_data.end(), new_valid_col.begin(), new_valid_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*raw_data.raw_));
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ScopedSchemaHandle schema_handle(*schema);
    for (auto [expr, ref_func] : testcases) {
        auto plan_str = schema_handle.ParseSearch(
            expr, "fakevec", 10, "L2", R"({"nprobe": 10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);
        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto valid = valid_data[i];
            auto ref = ref_func(valid);
            ASSERT_EQ(ans, ref);
        }
    }
}

TEST(Expr, TestArrayNullExprWithBitmapIndex) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid = schema->AddDebugField(
        "long_array", DataType::ARRAY, DataType::INT64, true);
    schema->set_primary_field_id(i64_fid);

    constexpr int N = 128;
    auto raw_data = DataGen(schema, N, 44, 0, 1, 3);
    auto valid_data = raw_data.get_col_valid(long_array_fid);
    auto long_array_col = raw_data.get_col<ScalarFieldProto>(long_array_fid);

    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    FixedVector<Array> arrays;
    arrays.reserve(N);
    std::vector<uint8_t> valid_bitmap((N + 7) / 8, 0);
    for (int i = 0; i < N; ++i) {
        arrays.emplace_back(long_array_col[i]);
        if (valid_data[i]) {
            valid_bitmap[i >> 3] |= 1 << (i & 0x07);
        }
    }

    auto field_data =
        storage::CreateFieldData(DataType::ARRAY, DataType::INT64, true);
    field_data->FillFieldData(arrays.data(), valid_bitmap.data(), N, 0);

    auto bitmap_index = test::expr_index::BuildIndex(long_array_fid,
                                                     DataType::ARRAY,
                                                     index::BITMAP_INDEX_TYPE,
                                                     {field_data},
                                                     Config::object(),
                                                     DataType::INT64);
    ASSERT_EQ(bitmap_index.reader->CoordDomain(), index::Domain::Row);
    ASSERT_FALSE(bitmap_index.caps.nested);
    ASSERT_EQ(bitmap_index.reader->Count(), N);
    test::expr_index::InstallIndex(*segment,
                                   long_array_fid,
                                   DataType::ARRAY,
                                   std::move(bitmap_index),
                                   DataType::INT64);

    auto null_expr = std::make_shared<expr::NullExpr>(
        expr::ColumnInfo(
            long_array_fid, DataType::ARRAY, DataType::INT64, {}, true),
        proto::plan::NullExpr_NullOp_IsNull);
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, null_expr);

    auto final = ExecuteQueryExpr(plan, segment.get(), N, MAX_TIMESTAMP);
    ASSERT_EQ(final.size(), N);
    for (int i = 0; i < N; ++i) {
        ASSERT_EQ(final[i], !valid_data[i]) << "row " << i;
    }

    milvus::exec::OffsetVector offsets;
    offsets.reserve(N / 2);
    for (int i = 0; i < N; ++i) {
        if (i % 2 == 0) {
            offsets.emplace_back(i);
        }
    }
    auto col_vec = milvus::test::gen_filter_res(
        plan.get(), segment.get(), N, MAX_TIMESTAMP, &offsets);
    BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
    ASSERT_EQ(view.size(), offsets.size());
    for (int i = 0; i < offsets.size(); ++i) {
        ASSERT_EQ(view[i], !valid_data[offsets[i]])
            << "offset row " << offsets[i];
    }
}

TEST(Expr, TestStructArrayParentNullExprUsesRepresentativeSubField) {
    auto schema = std::make_shared<Schema>();
    auto fakevec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto profile_history_fid = schema->AddDebugField(
        "profile[history]", DataType::ARRAY, DataType::INT64, true);
    schema->set_primary_field_id(i64_fid);

    constexpr int N = 128;
    auto raw_data = DataGen(schema, N, 43, 0, 1, 2);
    auto valid_data = raw_data.get_col_valid(profile_history_fid);
    auto profile_history_col =
        raw_data.get_col<ScalarFieldProto>(profile_history_fid);

    auto growing = CreateGrowingSegment(schema, empty_index_meta);
    auto offset = growing->PreInsert(N);
    growing->Insert(offset,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*raw_data.raw_));
    auto growing_segment = dynamic_cast<SegmentGrowingImpl*>(growing.get());
    ASSERT_NE(growing_segment, nullptr);

    FixedVector<Array> arrays;
    arrays.reserve(N);
    std::vector<uint8_t> valid_bitmap((N + 7) / 8, 0);
    for (int i = 0; i < N; ++i) {
        arrays.emplace_back(profile_history_col[i]);
        if (valid_data[i]) {
            valid_bitmap[i >> 3] |= 1 << (i & 0x07);
        }
    }

    auto field_data =
        storage::CreateFieldData(DataType::ARRAY, DataType::INT64, true);
    field_data->FillFieldData(arrays.data(), valid_bitmap.data(), N, 0);

    auto cm = storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto sealed_segment = CreateSealedSegment(schema);
    auto field_data_info =
        PrepareSingleFieldInsertBinlog(kCollectionID,
                                       kPartitionID,
                                       kSegmentID,
                                       profile_history_fid.get(),
                                       {field_data},
                                       cm);
    sealed_segment->LoadFieldData(field_data_info);

    auto make_plan = [&](proto::plan::NullExpr_NullOp op) {
        auto null_expr = std::make_shared<expr::NullExpr>(
            expr::ColumnInfo(profile_history_fid,
                             DataType::ARRAY,
                             DataType::INT64,
                             {},
                             true),
            op);
        return std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                      null_expr);
    };

    std::vector<
        std::pair<proto::plan::NullExpr_NullOp, std::function<bool(bool)>>>
        testcases = {
            {proto::plan::NullExpr_NullOp_IsNull,
             [](bool valid) { return !valid; }},
            {proto::plan::NullExpr_NullOp_IsNotNull,
             [](bool valid) { return valid; }},
        };

    for (auto [op, ref_func] : testcases) {
        std::array<const SegmentInternalInterface*, 2> segments = {
            static_cast<const SegmentInternalInterface*>(growing_segment),
            static_cast<const SegmentInternalInterface*>(sealed_segment.get())};
        for (auto* segment : segments) {
            auto plan = make_plan(op);
            auto final = ExecuteQueryExpr(plan, segment, N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            for (int i = 0; i < N; ++i) {
                ASSERT_EQ(final[i], ref_func(valid_data[i]))
                    << "segment type " << segment->type() << ", row " << i;
            }

            milvus::exec::OffsetVector offsets;
            offsets.reserve(N / 2);
            for (int i = 0; i < N; ++i) {
                if (i % 2 == 0) {
                    offsets.emplace_back(i);
                }
            }
            auto col_vec = milvus::test::gen_filter_res(
                plan.get(), segment, N, MAX_TIMESTAMP, &offsets);
            BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
            ASSERT_EQ(view.size(), offsets.size());
            for (int i = 0; i < offsets.size(); ++i) {
                ASSERT_EQ(view[i], ref_func(valid_data[offsets[i]]))
                    << "segment type " << segment->type() << ", offset row "
                    << offsets[i];
            }
        }
    }

    (void)fakevec_fid;
}

TEST(Expr, TestFloatVectorNullExpr) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto vector_fid = schema->AddDebugField(
        "embedding", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2, true);
    schema->set_primary_field_id(i64_fid);

    constexpr int64_t N = 128;
    constexpr int64_t dim = 16;
    auto raw_data = DataGen(schema, N, 42, 0, 1, 10, 1, false, true, false, 50);
    auto valid_data = raw_data.get_col_valid(vector_fid);

    auto growing = CreateGrowingSegment(schema, empty_index_meta);
    auto offset = growing->PreInsert(N);
    growing->Insert(offset,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*raw_data.raw_));
    auto growing_segment = dynamic_cast<SegmentGrowingImpl*>(growing.get());
    ASSERT_NE(growing_segment, nullptr);
    AssertSegmentFieldValidDataByOffsets(
        growing_segment,
        vector_fid,
        {-1, 0, 1, N - 1, N},
        {false, valid_data[0], valid_data[1], valid_data[N - 1], false});

    auto valid_count = CountValidRows(valid_data);
    auto vectors = MakeCompactFloatVectors(valid_count, dim);
    auto valid_bitmap = BuildValidBitmap(valid_data);
    auto field_data = storage::CreateFieldData(
        DataType::VECTOR_FLOAT, DataType::NONE, true, dim);
    field_data->FillFieldData(vectors.data(), valid_bitmap.data(), N, 0);

    auto cm = storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto sealed_segment = CreateSealedSegment(schema);
    auto field_data_info = PrepareSingleFieldInsertBinlog(kCollectionID,
                                                          kPartitionID,
                                                          kSegmentID,
                                                          vector_fid.get(),
                                                          {field_data},
                                                          cm);
    sealed_segment->LoadFieldData(field_data_info);
    ASSERT_TRUE(sealed_segment->HasFieldData(vector_fid));

    std::array<const SegmentInternalInterface*, 2> segments = {
        static_cast<const SegmentInternalInterface*>(growing_segment),
        static_cast<const SegmentInternalInterface*>(sealed_segment.get())};
    for (auto* segment : segments) {
        AssertVectorNullExprMatches(segment, vector_fid, true, valid_data, N);
    }
}

TEST(Expr, TestFloatVectorNullExprUsesGrowingIndexValidity) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto vector_fid = schema->AddDebugField(
        "embedding", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2, true);
    schema->set_primary_field_id(i64_fid);

    constexpr int64_t N = 64;
    constexpr int64_t dim = 16;

    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    InterimIndexConfigForTest interim_config;
    interim_config.chunk_rows = 64;
    interim_config.nlist = 1;
    interim_config.nprobe = 1;
    interim_config.dense_vector_interim_index_type =
        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC;
    ApplyInterimIndexConfigForTest(interim_config, config);
    config.set_build_ratio(0.1F);

    auto growing = CreateGrowingSegment(
        schema, MakeFloatVectorIndexMeta(vector_fid, dim, 100), 1, config);
    auto raw_data = DataGen(schema, N, 42, 0, 1, 10, 1, false, true, false, 20);
    auto valid_data = raw_data.get_col_valid(vector_fid);

    auto offset = growing->PreInsert(N);
    growing->Insert(offset,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*raw_data.raw_));
    auto growing_segment = dynamic_cast<SegmentGrowingImpl*>(growing.get());
    ASSERT_NE(growing_segment, nullptr);
    auto index_pin = growing_segment->PinGrowingIndex(vector_fid);
    ASSERT_TRUE(index_pin);
    ASSERT_GE(index_pin.CoveredRowEnd(), N);
    const auto* vec_index =
        dynamic_cast<const index::IVectorReader*>(&index_pin.Reader());
    ASSERT_NE(vec_index, nullptr);
    ASSERT_TRUE(vec_index->HasValidData());

    AssertSegmentFieldValidDataByOffsets(growing_segment,
                                         vector_fid,
                                         {-1, 0, 1, 2, 17, 18, N - 1, N},
                                         {false,
                                          valid_data[0],
                                          valid_data[1],
                                          valid_data[2],
                                          valid_data[17],
                                          valid_data[18],
                                          valid_data[N - 1],
                                          false});

    AssertVectorNullExprMatches(
        growing_segment, vector_fid, true, valid_data, N);
}

TEST(Expr, TestFloatVectorNullExprConjunctSkipMovesCursor) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto vector_fid = schema->AddDebugField(
        "embedding", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2, true);
    schema->set_primary_field_id(i64_fid);

    constexpr int64_t N = 64;
    auto raw_data =
        DataGen(schema, N, 42, 0, 1, 10, 1, false, false, false, 17);
    auto valid_data = raw_data.get_col_valid(vector_fid);
    ASSERT_FALSE(valid_data[0]);
    ASSERT_TRUE(valid_data[17]);

    auto growing = CreateGrowingSegment(schema, empty_index_meta);
    auto offset = growing->PreInsert(N);
    growing->Insert(offset,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*raw_data.raw_));
    auto growing_segment = dynamic_cast<SegmentGrowingImpl*>(growing.get());
    ASSERT_NE(growing_segment, nullptr);

    auto gate_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(i64_fid, DataType::INT64, {}, false),
        proto::plan::OpType::GreaterEqual,
        Int64Value(17));
    auto vector_not_null = MakeVectorNullExpr(
        vector_fid, true, proto::plan::NullExpr_NullOp_IsNotNull);
    auto and_expr = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::And, gate_expr, vector_not_null);
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, and_expr);

    auto final = ExecuteQueryExprWithBatchSize(
        plan, growing_segment, N, MAX_TIMESTAMP, 17);
    ASSERT_EQ(final.size(), N);
    for (int64_t i = 0; i < N; ++i) {
        ASSERT_EQ(final[i], i >= 17 && valid_data[i]) << "row " << i;
    }
}

TEST(Expr, TestFloatVectorNullExprAllNull) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto vector_fid = schema->AddDebugField(
        "embedding", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2, true);
    schema->set_primary_field_id(i64_fid);

    constexpr int64_t N = 128;
    constexpr int64_t dim = 16;
    auto raw_data =
        DataGen(schema, N, 42, 0, 1, 10, 1, false, true, false, 100);
    auto valid_data = raw_data.get_col_valid(vector_fid);
    ASSERT_EQ(CountValidRows(valid_data), 0);

    auto growing = CreateGrowingSegment(schema, empty_index_meta);
    auto offset = growing->PreInsert(N);
    growing->Insert(offset,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*raw_data.raw_));
    auto growing_segment = dynamic_cast<SegmentGrowingImpl*>(growing.get());
    ASSERT_NE(growing_segment, nullptr);

    auto valid_bitmap = BuildValidBitmap(valid_data);
    auto vectors = MakeCompactFloatVectors(0, dim);
    auto field_data = storage::CreateFieldData(
        DataType::VECTOR_FLOAT, DataType::NONE, true, dim);
    field_data->FillFieldData(vectors.data(), valid_bitmap.data(), N, 0);

    auto cm = storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto sealed_segment = CreateSealedSegment(schema);
    auto field_data_info = PrepareSingleFieldInsertBinlog(kCollectionID,
                                                          kPartitionID,
                                                          kSegmentID,
                                                          vector_fid.get(),
                                                          {field_data},
                                                          cm);
    sealed_segment->LoadFieldData(field_data_info);
    ASSERT_TRUE(sealed_segment->HasFieldData(vector_fid));

    std::array<const SegmentInternalInterface*, 2> segments = {
        static_cast<const SegmentInternalInterface*>(growing_segment),
        static_cast<const SegmentInternalInterface*>(sealed_segment.get())};
    for (auto* segment : segments) {
        AssertVectorNullExprMatches(segment, vector_fid, true, valid_data, N);
    }
}

TEST(Expr, TestFloatVectorNullExprUsesIndexValidityWithoutFieldData) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto vector_fid = schema->AddDebugField(
        "embedding", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2, true);
    schema->set_primary_field_id(i64_fid);

    constexpr int64_t N = 128;
    constexpr int64_t dim = 16;
    FixedVector<bool> valid_data(N);
    for (int64_t i = 0; i < N; ++i) {
        valid_data[i] = i % 3 != 0;
    }

    auto assert_index_only_case = [&](const FixedVector<bool>& expected_valid,
                                      const std::string& cache_key) {
        auto sealed_segment = MakeSealedVectorIndexOnlySegment(
            schema, vector_fid, expected_valid, dim, cache_key);
        ASSERT_TRUE(sealed_segment->HasIndex(vector_fid));
        ASSERT_FALSE(sealed_segment->HasFieldData(vector_fid));

        AssertVectorNullExprMatches(
            sealed_segment.get(), vector_fid, true, expected_valid, N);
        AssertSegmentFieldValidDataByOffsets(sealed_segment.get(),
                                             vector_fid,
                                             {-1, 0, 1, 2, N - 1, N},
                                             {false,
                                              expected_valid[0],
                                              expected_valid[1],
                                              expected_valid[2],
                                              expected_valid[N - 1],
                                              false});
    };

    assert_index_only_case(valid_data, "vector_null_expr_mixed");

    FixedVector<bool> all_null_valid_data(N, false);
    assert_index_only_case(all_null_valid_data, "vector_null_expr_all_null");

    FixedVector<bool> all_valid_data(N, true);
    assert_index_only_case(all_valid_data, "vector_null_expr_all_valid");
}

TEST(Expr, TestNonNullableFloatVectorNullExpr) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto vector_fid = schema->AddDebugField(
        "embedding", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2, false);
    schema->set_primary_field_id(i64_fid);

    constexpr int64_t N = 128;
    FixedVector<bool> all_valid(N, true);
    auto raw_data = DataGen(schema, N, 42, 0, 1, 10, 1, false, true, false, 0);

    auto growing = CreateGrowingSegment(schema, empty_index_meta);
    auto offset = growing->PreInsert(N);
    growing->Insert(offset,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*raw_data.raw_));
    auto growing_segment = dynamic_cast<SegmentGrowingImpl*>(growing.get());
    ASSERT_NE(growing_segment, nullptr);

    auto sealed_segment = CreateSealedSegment(schema);

    std::array<const SegmentInternalInterface*, 2> segments = {
        static_cast<const SegmentInternalInterface*>(growing_segment),
        static_cast<const SegmentInternalInterface*>(sealed_segment.get())};
    for (auto* segment : segments) {
        AssertVectorNullExprMatches(segment, vector_fid, false, all_valid, N);
    }
}

TEST(Expr, TestVectorArrayNullExpr) {
    auto schema = std::make_shared<Schema>();
    auto fakevec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto vector_array_fid =
        schema->AddDebugVectorArrayField("array_float_vector",
                                         DataType::VECTOR_FLOAT,
                                         4,
                                         knowhere::metric::L2,
                                         true);
    schema->set_primary_field_id(i64_fid);

    constexpr int N = 128;
    auto raw_data = DataGen(schema, N, 42, 0, 1, 2);
    auto valid_data = raw_data.get_col_valid(vector_array_fid);
    auto vector_array_col =
        raw_data.get_col<VectorFieldProto>(vector_array_fid);

    auto growing = CreateGrowingSegment(schema, empty_index_meta);
    auto offset = growing->PreInsert(N);
    growing->Insert(offset,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*raw_data.raw_));
    auto growing_segment = dynamic_cast<SegmentGrowingImpl*>(growing.get());
    ASSERT_NE(growing_segment, nullptr);

    std::vector<uint8_t> valid_bitmap((N + 7) / 8, 0);
    std::vector<milvus::VectorArray> vector_arrays;
    vector_arrays.reserve(N);
    for (int i = 0; i < N; ++i) {
        if (valid_data[i]) {
            valid_bitmap[i >> 3] |= 1 << (i & 0x07);
            vector_arrays.emplace_back(vector_array_col[i], false);
        }
    }

    auto field_data = storage::CreateFieldData(
        DataType::VECTOR_ARRAY, DataType::VECTOR_FLOAT, true, 4);
    field_data->FillFieldData(vector_arrays.data(), valid_bitmap.data(), N, 0);

    auto cm = storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto sealed_segment = CreateSealedSegment(schema);
    auto field_data_info =
        PrepareSingleFieldInsertBinlog(kCollectionID,
                                       kPartitionID,
                                       kSegmentID,
                                       vector_array_fid.get(),
                                       {field_data},
                                       cm);
    sealed_segment->LoadFieldData(field_data_info);

    auto make_plan = [&](proto::plan::NullExpr_NullOp op) {
        auto null_expr = std::make_shared<expr::NullExpr>(
            expr::ColumnInfo(vector_array_fid,
                             DataType::VECTOR_ARRAY,
                             DataType::VECTOR_FLOAT,
                             {},
                             true),
            op);
        return std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                      null_expr);
    };

    std::vector<
        std::pair<proto::plan::NullExpr_NullOp, std::function<bool(bool)>>>
        testcases = {
            {proto::plan::NullExpr_NullOp_IsNull,
             [](bool valid) { return !valid; }},
            {proto::plan::NullExpr_NullOp_IsNotNull,
             [](bool valid) { return valid; }},
        };

    for (auto [op, ref_func] : testcases) {
        std::array<const SegmentInternalInterface*, 2> segments = {
            static_cast<const SegmentInternalInterface*>(growing_segment),
            static_cast<const SegmentInternalInterface*>(sealed_segment.get())};
        for (auto* segment : segments) {
            auto plan = make_plan(op);
            auto final = ExecuteQueryExpr(plan, segment, N, MAX_TIMESTAMP);
            EXPECT_EQ(final.size(), N);
            for (int i = 0; i < N; ++i) {
                ASSERT_EQ(final[i], ref_func(valid_data[i]))
                    << "segment type " << segment->type() << ", row " << i;
            }

            milvus::exec::OffsetVector offsets;
            offsets.reserve(N / 2);
            for (int i = 0; i < N; ++i) {
                if (i % 2 == 0) {
                    offsets.emplace_back(i);
                }
            }
            auto col_vec = milvus::test::gen_filter_res(
                plan.get(), segment, N, MAX_TIMESTAMP, &offsets);
            BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
            ASSERT_EQ(view.size(), offsets.size());
            for (int i = 0; i < offsets.size(); ++i) {
                ASSERT_EQ(view[i], ref_func(valid_data[offsets[i]]))
                    << "segment type " << segment->type() << ", offset row "
                    << offsets[i];
            }
        }
    }

    (void)fakevec_fid;
}

TEST(Expr, TestVectorArrayLengthExpr) {
    auto schema = std::make_shared<Schema>();
    auto fakevec_fid = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto vector_array_fid =
        schema->AddDebugVectorArrayField("array_float_vector",
                                         DataType::VECTOR_FLOAT,
                                         4,
                                         knowhere::metric::L2,
                                         true);
    schema->set_primary_field_id(i64_fid);

    constexpr int N = 128;
    constexpr int kArrayLen = 2;
    auto raw_data = DataGen(schema, N, 42, 0, 1, kArrayLen);
    auto valid_data = raw_data.get_col_valid(vector_array_fid);
    auto vector_array_col =
        raw_data.get_col<VectorFieldProto>(vector_array_fid);

    auto growing = CreateGrowingSegment(schema, empty_index_meta);
    auto offset = growing->PreInsert(N);
    growing->Insert(offset,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*raw_data.raw_));
    auto growing_segment = dynamic_cast<SegmentGrowingImpl*>(growing.get());
    ASSERT_NE(growing_segment, nullptr);

    std::vector<uint8_t> valid_bitmap((N + 7) / 8, 0);
    std::vector<milvus::VectorArray> vector_arrays;
    vector_arrays.reserve(N);
    for (int i = 0; i < N; ++i) {
        if (valid_data[i]) {
            valid_bitmap[i >> 3] |= 1 << (i & 0x07);
            vector_arrays.emplace_back(vector_array_col[i], false);
        }
    }

    auto field_data = storage::CreateFieldData(
        DataType::VECTOR_ARRAY, DataType::VECTOR_FLOAT, true, 4);
    field_data->FillFieldData(vector_arrays.data(), valid_bitmap.data(), N, 0);

    auto cm = storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto sealed_segment = CreateSealedSegment(schema);
    auto field_data_info =
        PrepareSingleFieldInsertBinlog(kCollectionID,
                                       kPartitionID,
                                       kSegmentID,
                                       vector_array_fid.get(),
                                       {field_data},
                                       cm);
    sealed_segment->LoadFieldData(field_data_info);

    auto make_plan = [&](proto::plan::OpType op, int64_t target) {
        proto::plan::GenericValue value;
        value.set_int64_val(target);
        proto::plan::GenericValue right_operand;
        right_operand.set_int64_val(0);
        auto expr = std::make_shared<milvus::expr::BinaryArithOpEvalRangeExpr>(
            milvus::expr::ColumnInfo(vector_array_fid,
                                     DataType::VECTOR_ARRAY,
                                     DataType::VECTOR_FLOAT,
                                     {},
                                     true),
            op,
            proto::plan::ArithOpType::ArrayLength,
            value,
            right_operand);
        return std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                      expr);
    };

    struct Testcase {
        proto::plan::OpType op;
        int64_t target;
        std::function<bool(bool)> ref_func;
    };
    std::vector<Testcase> testcases = {
        {proto::plan::OpType::Equal,
         kArrayLen,
         [](bool valid) { return valid; }},
        {proto::plan::OpType::NotEqual,
         kArrayLen + 1,
         [](bool valid) { return valid; }},
        {proto::plan::OpType::GreaterThan,
         kArrayLen - 1,
         [](bool valid) { return valid; }},
        {proto::plan::OpType::GreaterEqual,
         kArrayLen,
         [](bool valid) { return valid; }},
        {proto::plan::OpType::LessThan,
         kArrayLen,
         [](bool valid) { return false; }},
        {proto::plan::OpType::LessEqual,
         kArrayLen,
         [](bool valid) { return valid; }},
    };

    for (const auto& testcase : testcases) {
        std::array<const SegmentInternalInterface*, 2> segments = {
            static_cast<const SegmentInternalInterface*>(growing_segment),
            static_cast<const SegmentInternalInterface*>(sealed_segment.get())};
        for (auto* segment : segments) {
            auto plan = make_plan(testcase.op, testcase.target);
            auto final = ExecuteQueryExpr(plan, segment, N, MAX_TIMESTAMP);
            EXPECT_EQ(final.size(), N);
            for (int i = 0; i < N; ++i) {
                ASSERT_EQ(final[i], testcase.ref_func(valid_data[i]))
                    << "segment type " << segment->type() << ", row " << i;
            }

            milvus::exec::OffsetVector offsets;
            offsets.reserve(N / 2);
            for (int i = 0; i < N; ++i) {
                if (i % 2 == 0) {
                    offsets.emplace_back(i);
                }
            }
            auto col_vec = milvus::test::gen_filter_res(
                plan.get(), segment, N, MAX_TIMESTAMP, &offsets);
            BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
            ASSERT_EQ(view.size(), offsets.size());
            for (int i = 0; i < offsets.size(); ++i) {
                ASSERT_EQ(view[i], testcase.ref_func(valid_data[offsets[i]]))
                    << "segment type " << segment->type() << ", offset row "
                    << offsets[i];
            }
        }
    }

    (void)fakevec_fid;
}

namespace {

bool
CompareArrayLength(proto::plan::OpType op, int64_t length, int64_t target) {
    switch (op) {
        case proto::plan::OpType::Equal:
            return length == target;
        case proto::plan::OpType::NotEqual:
            return length != target;
        case proto::plan::OpType::GreaterThan:
            return length > target;
        case proto::plan::OpType::GreaterEqual:
            return length >= target;
        case proto::plan::OpType::LessThan:
            return length < target;
        case proto::plan::OpType::LessEqual:
            return length <= target;
        default:
            return false;
    }
}

const std::vector<proto::plan::OpType> kArrayLengthOps = {
    proto::plan::OpType::Equal,
    proto::plan::OpType::NotEqual,
    proto::plan::OpType::GreaterThan,
    proto::plan::OpType::GreaterEqual,
    proto::plan::OpType::LessThan,
    proto::plan::OpType::LessEqual,
};

}  // namespace

// array_length on a nullable scalar ARRAY field (and a struct sub-field
// sharing the struct's offsets) is served straight from IArrayOffsets: the
// result must be bit-identical to the raw-data semantics (NULL row ->
// res=false/valid=false; empty [] is a valid length-0 array) over sealed AND
// growing segments, all compare ops incl. the N=0 boundary, sequential
// batches crossing the 8192-row batch boundary, and offset-input mode.
TEST(Expr, TestArrayLengthExprServedFromChunks) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid = schema->AddDebugField(
        "long_array", DataType::ARRAY, DataType::INT64, true);
    auto struct_array_fid = schema->AddDebugField(
        "structA[history]", DataType::ARRAY, DataType::INT64, true);
    schema->set_primary_field_id(i64_fid);

    // NULL, [], [1], [1,2], [1,2,3] repeating. N spans two default-size
    // (8192) execution batches so the sequential fast path advances the
    // data cursor across a batch boundary.
    constexpr int N = 10000;
    std::vector<std::optional<std::vector<int64_t>>> rows(N);
    for (int i = 0; i < N; ++i) {
        switch (i % 5) {
            case 0:
                rows[i] = std::nullopt;
                break;
            case 1:
                rows[i] = std::vector<int64_t>{};
                break;
            case 2:
                rows[i] = std::vector<int64_t>{1};
                break;
            case 3:
                rows[i] = std::vector<int64_t>{1, 2};
                break;
            default:
                rows[i] = std::vector<int64_t>{1, 2, 3};
                break;
        }
    }

    auto raw_data = DataGen(schema, N, 42, 0, 1, 2);
    SetNullableInt64ArrayFieldData(raw_data, long_array_fid, rows);
    SetNullableInt64ArrayFieldData(raw_data, struct_array_fid, rows);

    auto growing = CreateGrowingSegment(schema, empty_index_meta);
    auto offset = growing->PreInsert(N);
    growing->Insert(offset,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*raw_data.raw_));
    auto growing_segment = dynamic_cast<SegmentGrowingImpl*>(growing.get());
    ASSERT_NE(growing_segment, nullptr);

    auto sealed_segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    // Unordered offset input covering NULL, empty and valued rows.
    milvus::exec::OffsetVector offset_input;
    for (int i = N - 1; i >= 0; i -= 3) {
        offset_input.emplace_back(i);
    }

    const std::vector<int64_t> targets = {0, 2};
    std::vector<std::pair<std::string, FieldId>> fields = {
        {"long_array", long_array_fid},
        {"struct_array", struct_array_fid},
    };
    for (const auto& [field_name, fid] : fields) {
        for (auto op : kArrayLengthOps) {
            for (auto target : targets) {
                std::vector<bool> expected_bits(N);
                std::vector<bool> expected_valid(N);
                for (int i = 0; i < N; ++i) {
                    bool valid = rows[i].has_value();
                    expected_valid[i] = valid;
                    expected_bits[i] =
                        valid &&
                        CompareArrayLength(
                            op, static_cast<int64_t>(rows[i]->size()), target);
                }

                auto arith_expr =
                    std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
                        expr::ColumnInfo(
                            fid, DataType::ARRAY, DataType::INT64, {}, true),
                        op,
                        proto::plan::ArithOpType::ArrayLength,
                        Int64Value(target),
                        Int64Value(0));
                auto plan = std::make_shared<plan::FilterBitsNode>(
                    DEFAULT_PLANNODE_ID, arith_expr);
                std::string label = field_name + " op " +
                                    std::to_string(static_cast<int>(op)) +
                                    " target " + std::to_string(target);

                std::array<const SegmentInternalInterface*, 2> segments = {
                    static_cast<const SegmentInternalInterface*>(
                        growing_segment),
                    static_cast<const SegmentInternalInterface*>(
                        sealed_segment.get())};
                for (auto* segment : segments) {
                    auto seg_label =
                        label + " segment " +
                        std::to_string(static_cast<int>(segment->type()));

                    // gen_filter_res performs a single Eval, i.e. the first
                    // execution batch; full-N coverage (incl. the cursor
                    // advancing across the batch boundary) comes from
                    // ExecuteQueryExpr below, which drives the batched loop.
                    auto vec = milvus::test::gen_filter_res(
                        plan.get(), segment, N, MAX_TIMESTAMP);
                    ASSERT_GT(vec->size(), 0) << seg_label;
                    ASSERT_LE(vec->size(), N) << seg_label;
                    BitsetTypeView seq_bits(vec->GetRawData(), vec->size());
                    BitsetTypeView seq_valid(vec->GetValidRawData(),
                                             vec->size());
                    for (size_t i = 0; i < vec->size(); ++i) {
                        ASSERT_EQ(seq_bits[i], expected_bits[i])
                            << seg_label << ", row " << i;
                        ASSERT_EQ(seq_valid[i], expected_valid[i])
                            << seg_label << ", row " << i;
                    }

                    auto final =
                        ExecuteQueryExpr(plan, segment, N, MAX_TIMESTAMP);
                    ASSERT_EQ(final.size(), N) << seg_label;
                    for (int i = 0; i < N; ++i) {
                        ASSERT_EQ(final[i], expected_bits[i])
                            << seg_label << ", row " << i;
                    }

                    auto col_vec = milvus::test::gen_filter_res(
                        plan.get(), segment, N, MAX_TIMESTAMP, &offset_input);
                    BitsetTypeView bits(col_vec->GetRawData(), col_vec->size());
                    BitsetTypeView valid(col_vec->GetValidRawData(),
                                         col_vec->size());
                    ASSERT_EQ(bits.size(), offset_input.size()) << seg_label;
                    for (size_t j = 0; j < offset_input.size(); ++j) {
                        ASSERT_EQ(bits[j], expected_bits[offset_input[j]])
                            << seg_label << ", offset row " << offset_input[j];
                        ASSERT_EQ(valid[j], expected_valid[offset_input[j]])
                            << seg_label << ", offset row " << offset_input[j];
                    }
                }
            }
        }
    }
}

TEST(Expr, PraseArrayContainsExpr) {
    // Test expressions for array_contains operations
    std::vector<const char*> exprs{
        // json_contains_expr: array_contains(array, 1)
        "array_contains(array, 1)",
        // json_contains_expr: array_contains_all(array, [1, 2, 3])
        "array_contains_all(array, [1, 2, 3])",
        // json_contains_expr: array_contains_any(array, [1, 2, 3])
        "array_contains_any(array, [1, 2, 3])",
    };

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    schema->AddField(FieldName("array"),
                     FieldId(101),
                     DataType::ARRAY,
                     DataType::INT64,
                     false,
                     false);
    ScopedSchemaHandle schema_handle(*schema);

    for (auto& expr : exprs) {
        auto plan_str = schema_handle.ParseSearch(
            expr, "fakevec", 10, "L2", R"({"nprobe": 10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
    }
}

template <typename T>
struct ArrayTestcase {
    std::vector<T> term;
    std::vector<std::string> nested_path;
};

TEST(Expr, TestArrayContains) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto int_array_fid =
        schema->AddDebugField("int_array", DataType::ARRAY, DataType::INT8);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto bool_array_fid =
        schema->AddDebugField("bool_array", DataType::ARRAY, DataType::BOOL);
    auto float_array_fid =
        schema->AddDebugField("float_array", DataType::ARRAY, DataType::FLOAT);
    auto double_array_fid = schema->AddDebugField(
        "double_array", DataType::ARRAY, DataType::DOUBLE);
    auto string_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarFieldProto>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_int_array_col =
            raw_data.get_col<ScalarFieldProto>(int_array_fid);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        auto new_bool_array_col =
            raw_data.get_col<ScalarFieldProto>(bool_array_fid);
        auto new_float_array_col =
            raw_data.get_col<ScalarFieldProto>(float_array_fid);
        auto new_double_array_col =
            raw_data.get_col<ScalarFieldProto>(double_array_fid);
        auto new_string_array_col =
            raw_data.get_col<ScalarFieldProto>(string_array_fid);

        array_cols["int"].insert(array_cols["int"].end(),
                                 new_int_array_col.begin(),
                                 new_int_array_col.end());
        array_cols["long"].insert(array_cols["long"].end(),
                                  new_long_array_col.begin(),
                                  new_long_array_col.end());
        array_cols["bool"].insert(array_cols["bool"].end(),
                                  new_bool_array_col.begin(),
                                  new_bool_array_col.end());
        array_cols["float"].insert(array_cols["float"].end(),
                                   new_float_array_col.begin(),
                                   new_float_array_col.end());
        array_cols["double"].insert(array_cols["double"].end(),
                                    new_double_array_col.begin(),
                                    new_double_array_col.end());
        array_cols["string"].insert(array_cols["string"].end(),
                                    new_string_array_col.begin(),
                                    new_string_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*raw_data.raw_));
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    std::vector<ArrayTestcase<bool>> bool_testcases{{{true, true}, {}},
                                                    {{false, false}, {}}};

    for (auto testcase : bool_testcases) {
        auto check = [&](const std::vector<bool>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) !=
                    values.end()) {
                    return true;
                }
            }
            return false;
        };
        std::vector<proto::plan::GenericValue> values;
        for (const auto& val : testcase.term) {
            proto::plan::GenericValue gen_val;
            gen_val.set_bool_val(val);
            values.push_back(gen_val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(bool_array_fid, DataType::ARRAY, DataType::BOOL),
            proto::plan::JSONContainsExpr_JSONOp_Contains,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["bool"][i]);
            std::vector<bool> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data_unchecked<bool>(j));
            }
            ASSERT_EQ(ans, check(res)) << "@" << i;
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res)) << "@" << i;
            }
        }
    }

    std::vector<ArrayTestcase<double>> double_testcases{
        {{1.123, 10.34}, {"double"}},
        {{10.34, 100.234}, {"double"}},
        {{100.234, 1000.4546}, {"double"}},
        {{1000.4546, 1.123}, {"double"}},
        {{1000.4546, 10.34}, {"double"}},
        {{1.123, 100.234}, {"double"}},
    };

    for (auto testcase : double_testcases) {
        auto check = [&](const std::vector<double>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) !=
                    values.end()) {
                    return true;
                }
            }
            return false;
        };

        std::vector<proto::plan::GenericValue> values;
        for (const auto& val : testcase.term) {
            proto::plan::GenericValue gen_val;
            gen_val.set_float_val(val);
            values.push_back(gen_val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(
                double_array_fid, DataType::ARRAY, DataType::DOUBLE),
            proto::plan::JSONContainsExpr_JSONOp_Contains,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["double"][i]);
            std::vector<double> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data_unchecked<double>(j));
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }

    for (auto testcase : double_testcases) {
        auto check = [&](const std::vector<float>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) !=
                    values.end()) {
                    return true;
                }
            }
            return false;
        };
        std::vector<proto::plan::GenericValue> values;
        for (const auto& val : testcase.term) {
            proto::plan::GenericValue gen_val;
            gen_val.set_float_val(val);
            values.push_back(gen_val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(float_array_fid, DataType::ARRAY, DataType::FLOAT),
            proto::plan::JSONContainsExpr_JSONOp_Contains,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["float"][i]);
            std::vector<float> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data_unchecked<float>(j));
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }

    std::vector<ArrayTestcase<int64_t>> testcases{
        {{1, 10}, {"int"}},
        {{10, 100}, {"int"}},
        {{100, 1000}, {"int"}},
        {{1000, 10}, {"int"}},
        {{2, 4, 6, 8, 10}, {"int"}},
        {{1, 2, 3, 4, 5}, {"int"}},
    };

    for (auto testcase : testcases) {
        auto check = [&](const std::vector<int64_t>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };

        std::vector<proto::plan::GenericValue> values;
        for (const auto& val : testcase.term) {
            proto::plan::GenericValue gen_val;
            gen_val.set_int64_val(val);
            values.push_back(gen_val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(int_array_fid, DataType::ARRAY, DataType::INT8),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["int"][i]);
            std::vector<int64_t> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data_unchecked<int64_t>(j));
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }

    for (auto testcase : testcases) {
        auto check = [&](const std::vector<int64_t>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };

        std::vector<proto::plan::GenericValue> values;
        for (const auto& val : testcase.term) {
            proto::plan::GenericValue gen_val;
            gen_val.set_int64_val(val);
            values.push_back(gen_val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(long_array_fid, DataType::ARRAY, DataType::INT64),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["long"][i]);
            std::vector<int64_t> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data_unchecked<int64_t>(j));
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }

    std::vector<ArrayTestcase<std::string>> testcases_string = {
        {{"1sads", "10dsf"}, {"string"}},
        {{"10dsf", "100"}, {"string"}},
        {{"100", "10dsf", "1sads"}, {"string"}},
        {{"100ddfdsssdfdsfsd0", "100"}, {"string"}},
    };

    for (auto testcase : testcases_string) {
        auto check = [&](const std::vector<std::string_view>& values) {
            for (auto const& e : testcase.term) {
                if (std::find(values.begin(), values.end(), e) ==
                    values.end()) {
                    return false;
                }
            }
            return true;
        };

        std::vector<proto::plan::GenericValue> values;
        for (const auto& val : testcase.term) {
            proto::plan::GenericValue gen_val;
            gen_val.set_string_val(val);
            values.push_back(gen_val);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(
                string_array_fid, DataType::ARRAY, DataType::VARCHAR),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
            true,
            values);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["string"][i]);
            std::vector<std::string_view> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data_unchecked<std::string_view>(j));
            }
            ASSERT_EQ(ans, check(res));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], check(res));
            }
        }
    }
}

// Behavior coverage for ExecArrayContains across single- and multi-target
// invocations and across INT64 / VARCHAR element types. Exercises the typed
// cached set path (no MultiElement / variant round-trip).
TEST(Expr, TestArrayContainsTargetCoverage) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto string_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarFieldProto>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        auto new_string_array_col =
            raw_data.get_col<ScalarFieldProto>(string_array_fid);
        array_cols["long"].insert(array_cols["long"].end(),
                                  new_long_array_col.begin(),
                                  new_long_array_col.end());
        array_cols["string"].insert(array_cols["string"].end(),
                                    new_string_array_col.begin(),
                                    new_string_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*raw_data.raw_));
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    // --- Single-target Contains on INT64 ---
    {
        int64_t target = 10;
        auto check = [target](const std::vector<int64_t>& values) {
            return std::find(values.begin(), values.end(), target) !=
                   values.end();
        };
        proto::plan::GenericValue gen_val;
        gen_val.set_int64_val(target);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(long_array_fid, DataType::ARRAY, DataType::INT64),
            proto::plan::JSONContainsExpr_JSONOp_Contains,
            true,
            std::vector<proto::plan::GenericValue>{gen_val});
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final_bits =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final_bits.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto array = milvus::Array(array_cols["long"][i]);
            std::vector<int64_t> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data_unchecked<int64_t>(j));
            }
            ASSERT_EQ(final_bits[i], check(res)) << "@" << i;
        }
    }

    // --- Single-target Contains on VARCHAR ---
    {
        std::string target = "1sads";
        auto check = [&target](const std::vector<std::string_view>& values) {
            return std::find(values.begin(), values.end(), target) !=
                   values.end();
        };
        proto::plan::GenericValue gen_val;
        gen_val.set_string_val(target);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(
                string_array_fid, DataType::ARRAY, DataType::VARCHAR),
            proto::plan::JSONContainsExpr_JSONOp_Contains,
            true,
            std::vector<proto::plan::GenericValue>{gen_val});
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final_bits =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final_bits.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto array = milvus::Array(array_cols["string"][i]);
            std::vector<std::string_view> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data_unchecked<std::string_view>(j));
            }
            ASSERT_EQ(final_bits[i], check(res)) << "@" << i;
        }
    }

    // --- ContainsAny with a one-element target list ---
    {
        int64_t target = 100;
        auto check = [target](const std::vector<int64_t>& values) {
            return std::find(values.begin(), values.end(), target) !=
                   values.end();
        };
        proto::plan::GenericValue gen_val;
        gen_val.set_int64_val(target);
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(long_array_fid, DataType::ARRAY, DataType::INT64),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            std::vector<proto::plan::GenericValue>{gen_val});
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final_bits =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final_bits.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto array = milvus::Array(array_cols["long"][i]);
            std::vector<int64_t> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data_unchecked<int64_t>(j));
            }
            ASSERT_EQ(final_bits[i], check(res)) << "@" << i;
        }
    }

    // --- Multi-target ContainsAny on INT64 (no element should match all
    //     three targets, but each row may match any of them). ---
    {
        std::vector<int64_t> targets = {10, 100, 1000};
        auto check = [&targets](const std::vector<int64_t>& values) {
            for (auto t : targets) {
                if (std::find(values.begin(), values.end(), t) !=
                    values.end()) {
                    return true;
                }
            }
            return false;
        };
        std::vector<proto::plan::GenericValue> vals;
        for (auto t : targets) {
            proto::plan::GenericValue gv;
            gv.set_int64_val(t);
            vals.push_back(gv);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(long_array_fid, DataType::ARRAY, DataType::INT64),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            vals);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final_bits =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final_bits.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto array = milvus::Array(array_cols["long"][i]);
            std::vector<int64_t> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data_unchecked<int64_t>(j));
            }
            ASSERT_EQ(final_bits[i], check(res)) << "@" << i;
        }
    }

    // --- Multi-target ContainsAny on VARCHAR. ---
    {
        std::vector<std::string> targets = {"1sads", "10dsf", "100"};
        auto check = [&targets](const std::vector<std::string_view>& values) {
            for (auto const& t : targets) {
                if (std::find(values.begin(), values.end(), t) !=
                    values.end()) {
                    return true;
                }
            }
            return false;
        };
        std::vector<proto::plan::GenericValue> vals;
        for (const auto& t : targets) {
            proto::plan::GenericValue gv;
            gv.set_string_val(t);
            vals.push_back(gv);
        }
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(
                string_array_fid, DataType::ARRAY, DataType::VARCHAR),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            vals);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        BitsetType final_bits =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final_bits.size(), N * num_iters);

        for (int i = 0; i < N * num_iters; ++i) {
            auto array = milvus::Array(array_cols["string"][i]);
            std::vector<std::string_view> res;
            for (int j = 0; j < array.length(); ++j) {
                res.push_back(array.get_data_unchecked<std::string_view>(j));
            }
            ASSERT_EQ(final_bits[i], check(res)) << "@" << i;
        }
    }
}

TEST(Expr, TestArrayContainsFloatLiteralCastsToElementType) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto float_array_fid =
        schema->AddDebugField("float_array", DataType::ARRAY, DataType::FLOAT);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    constexpr int N = 3;
    auto raw_data = DataGen(schema, N, 42, 0, 1, 3);

    constexpr double target_1 = 492710.03520180425;
    constexpr double target_2 = 38241.68284883746;
    bool replaced_array_field = false;
    for (int i = 0; i < raw_data.raw_->fields_data_size(); ++i) {
        auto* field_data = raw_data.raw_->mutable_fields_data(i);
        if (field_data->field_id() != float_array_fid.get()) {
            continue;
        }

        auto* arrays =
            field_data->mutable_scalars()->mutable_array_data()->mutable_data();
        arrays->Clear();

        auto* matched = arrays->Add();
        matched->mutable_float_data()->add_data(static_cast<float>(target_1));
        matched->mutable_float_data()->add_data(static_cast<float>(target_2));

        auto* close_but_different = arrays->Add();
        close_but_different->mutable_float_data()->add_data(
            static_cast<float>(target_1) + 1.0F);
        close_but_different->mutable_float_data()->add_data(
            static_cast<float>(target_2) + 1.0F);

        arrays->Add();
        replaced_array_field = true;
        break;
    }
    ASSERT_TRUE(replaced_array_field);

    seg->PreInsert(N);
    seg->Insert(0,
                N,
                raw_data.row_ids_.data(),
                raw_data.timestamps_.data(),
                std::make_shared<InsertRecordProto>(*raw_data.raw_));
    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ASSERT_NE(seg_promote, nullptr);

    auto make_float_value = [](double target) {
        proto::plan::GenericValue value;
        value.set_float_val(target);
        return value;
    };

    auto make_values = [&](std::vector<double> targets) {
        std::vector<proto::plan::GenericValue> values;
        values.reserve(targets.size());
        for (auto target : targets) {
            values.push_back(make_float_value(target));
        }
        return values;
    };

    auto expect_only_first_match =
        [&](const std::shared_ptr<plan::FilterBitsNode>& plan) {
            auto result = ExecuteQueryExpr(plan, seg_promote, N, MAX_TIMESTAMP);
            ASSERT_EQ(result.size(), N);
            EXPECT_TRUE(result[0]);
            EXPECT_FALSE(result[1]);
            EXPECT_FALSE(result[2]);
        };

    auto check = [&](proto::plan::JSONContainsExpr_JSONOp op,
                     std::vector<double> targets) {
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(float_array_fid, DataType::ARRAY, DataType::FLOAT),
            op,
            true,
            make_values(std::move(targets)));
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        expect_only_first_match(plan);
    };

    check(proto::plan::JSONContainsExpr_JSONOp_Contains, {target_1});
    check(proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
          {target_1, target_2});
    check(proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
          {target_1, target_2});

    auto element_expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(
            float_array_fid, DataType::ARRAY, DataType::FLOAT, {"0"}),
        proto::plan::OpType::Equal,
        make_float_value(target_1));
    expect_only_first_match(std::make_shared<plan::FilterBitsNode>(
        DEFAULT_PLANNODE_ID, element_expr));

    auto array_value = make_float_value(0);
    auto* array = array_value.mutable_array_val();
    array->set_same_type(true);
    array->set_element_type(proto::schema::DataType::Float);
    array->add_array()->set_float_val(target_1);
    array->add_array()->set_float_val(target_2);

    auto array_expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(float_array_fid, DataType::ARRAY, DataType::FLOAT),
        proto::plan::OpType::Equal,
        array_value);
    expect_only_first_match(std::make_shared<plan::FilterBitsNode>(
        DEFAULT_PLANNODE_ID, array_expr));
}

TEST(Expr, TestArrayContainsEmptyValues) {
    auto schema = std::make_shared<Schema>();
    auto int_array_fid =
        schema->AddDebugField("int_array", DataType::ARRAY, DataType::INT8);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto bool_array_fid =
        schema->AddDebugField("bool_array", DataType::ARRAY, DataType::BOOL);
    auto float_array_fid =
        schema->AddDebugField("float_array", DataType::ARRAY, DataType::FLOAT);
    auto double_array_fid = schema->AddDebugField(
        "double_array", DataType::ARRAY, DataType::DOUBLE);
    auto string_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    schema->set_primary_field_id(schema->AddDebugField("id", DataType::INT64));
    std::vector<FieldId> fields = {
        int_array_fid,
        long_array_fid,
        bool_array_fid,
        float_array_fid,
        double_array_fid,
        string_array_fid,
    };

    auto dummy_seg = CreateGrowingSegment(schema, empty_index_meta);

    int N = 1000;
    std::vector<int> age_col;
    int num_iters = 100;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        dummy_seg->PreInsert(N);
        dummy_seg->Insert(iter * N,
                          N,
                          raw_data.row_ids_.data(),
                          raw_data.timestamps_.data(),
                          std::make_shared<InsertRecordProto>(*raw_data.raw_));
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(dummy_seg.get());
    std::vector<proto::plan::GenericValue> empty_values;

    for (auto field_id : fields) {
        auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
            expr::ColumnInfo(field_id, DataType::ARRAY),
            proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
            true,
            empty_values);

        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);
        for (int i = 0; i < N * num_iters; ++i) {
            ASSERT_EQ(final[i], false);
        }
    }
}

TEST(Expr, TestArrayBinaryArith) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto int_array_fid =
        schema->AddDebugField("int_array", DataType::ARRAY, DataType::INT8);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto float_array_fid =
        schema->AddDebugField("float_array", DataType::ARRAY, DataType::FLOAT);
    auto double_array_fid = schema->AddDebugField(
        "double_array", DataType::ARRAY, DataType::DOUBLE);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarFieldProto>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_int_array_col =
            raw_data.get_col<ScalarFieldProto>(int_array_fid);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        auto new_float_array_col =
            raw_data.get_col<ScalarFieldProto>(float_array_fid);
        auto new_double_array_col =
            raw_data.get_col<ScalarFieldProto>(double_array_fid);

        array_cols["int"].insert(array_cols["int"].end(),
                                 new_int_array_col.begin(),
                                 new_int_array_col.end());
        array_cols["long"].insert(array_cols["long"].end(),
                                  new_long_array_col.begin(),
                                  new_long_array_col.end());
        array_cols["float"].insert(array_cols["float"].end(),
                                   new_float_array_col.begin(),
                                   new_float_array_col.end());
        array_cols["double"].insert(array_cols["double"].end(),
                                    new_double_array_col.begin(),
                                    new_double_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*raw_data.raw_));
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ScopedSchemaHandle schema_handle(*schema);

    std::vector<std::tuple<std::string,
                           std::string,
                           std::function<bool(milvus::Array & array)>>>
        testcases = {
            // int_array[0] + 2 == 5
            {"int_array[0] + 2 == 5",
             "int",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val + 2 == 5;
             }},
            // int_array[0] + 2 != 5
            {"int_array[0] + 2 != 5",
             "int",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val + 2 != 5;
             }},
            // int_array[0] + 2 > 5
            {"int_array[0] + 2 > 5",
             "int",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val + 2 > 5;
             }},
            // int_array[0] + 2 >= 5
            {"int_array[0] + 2 >= 5",
             "int",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val + 2 >= 5;
             }},
            // int_array[0] + 2 < 5
            {"int_array[0] + 2 < 5",
             "int",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val + 2 < 5;
             }},
            // int_array[0] + 2 <= 5
            {"int_array[0] + 2 <= 5",
             "int",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val + 2 <= 5;
             }},
            // long_array[0] - 1 == 144
            {"long_array[0] - 1 == 144",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val - 1 == 144;
             }},
            // long_array[0] - 1 != 144
            {"long_array[0] - 1 != 144",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val - 1 != 144;
             }},
            // long_array[0] - 1 > 144
            {"long_array[0] - 1 > 144",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val - 1 > 144;
             }},
            // long_array[0] - 1 >= 144
            {"long_array[0] - 1 >= 144",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val - 1 >= 144;
             }},
            // long_array[0] - 1 < 144
            {"long_array[0] - 1 < 144",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val - 1 < 144;
             }},
            // long_array[0] - 1 <= 144
            {"long_array[0] - 1 <= 144",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val - 1 <= 144;
             }},
            // float_array[0] + 2.2 == 133.2
            {"float_array[0] + 2.2 == 133.2",
             "float",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<double>(0);
                 return val + 2.2 == 133.2;
             }},
            // float_array[0] + 2.2 != 133.2
            {"float_array[0] + 2.2 != 133.2",
             "float",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<double>(0);
                 return val + 2.2 != 133.2;
             }},
            // double_array[0] - 11.1 == 125.7
            {"double_array[0] - 11.1 == 125.7",
             "double",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<double>(0);
                 return val - 11.1 == 125.7;
             }},
            // double_array[0] - 11.1 != 125.7
            {"double_array[0] - 11.1 != 125.7",
             "double",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<double>(0);
                 return val - 11.1 != 125.7;
             }},
            // long_array[0] * 2 == 8
            {"long_array[0] * 2 == 8",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val * 2 == 8;
             }},
            // long_array[0] * 2 != 20
            {"long_array[0] * 2 != 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val * 2 != 20;
             }},
            // long_array[0] * 2 > 20
            {"long_array[0] * 2 > 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val * 2 > 20;
             }},
            // long_array[0] * 2 >= 20
            {"long_array[0] * 2 >= 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val * 2 >= 20;
             }},
            // long_array[0] * 2 < 20
            {"long_array[0] * 2 < 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val * 2 < 20;
             }},
            // long_array[0] * 2 <= 20
            {"long_array[0] * 2 <= 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val * 2 <= 20;
             }},
            // long_array[0] / 2 == 8
            {"long_array[0] / 2 == 8",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val / 2 == 8;
             }},
            // long_array[0] / 2 != 20
            {"long_array[0] / 2 != 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val / 2 != 20;
             }},
            // long_array[0] / 2 > 20
            {"long_array[0] / 2 > 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val / 2 > 20;
             }},
            // long_array[0] / 2 >= 20
            {"long_array[0] / 2 >= 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val / 2 >= 20;
             }},
            // long_array[0] / 2 < 20
            {"long_array[0] / 2 < 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val / 2 < 20;
             }},
            // long_array[0] / 2 <= 20
            {"long_array[0] / 2 <= 20",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val / 2 <= 20;
             }},
            // long_array[0] % 3 == 0
            {"long_array[0] % 3 == 0",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val % 3 == 0;
             }},
            // long_array[0] % 3 != 2
            {"long_array[0] % 3 != 2",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val % 3 != 2;
             }},
            // long_array[0] % 3 > 2
            {"long_array[0] % 3 > 2",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val % 3 > 2;
             }},
            // long_array[0] % 3 >= 2
            {"long_array[0] % 3 >= 2",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val % 3 >= 2;
             }},
            // long_array[0] % 3 < 2
            {"long_array[0] % 3 < 2",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val % 3 < 2;
             }},
            // long_array[0] % 3 <= 2
            {"long_array[0] % 3 <= 2",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val % 3 <= 2;
             }},
            // Bitwise AND/OR/XOR over an array integer element. long_array
            // values span a wide range, so OR/XOR clauses use a mid-range
            // threshold to keep the result mixed; AND clauses test low bits.
            {"(long_array[0] & 1) == 0",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return (val & 1) == 0;
             }},
            {"(long_array[0] & 1) != 0",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return (val & 1) != 0;
             }},
            {"(long_array[0] & 8) == 8",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return (val & 8) == 8;
             }},
            {"(long_array[0] | 4) < 5000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return (val | 4) < 5000;
             }},
            {"(long_array[0] ^ 15) < 5000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return (val ^ 15) < 5000;
             }},
            // Shift / bitwise-NOT over an array integer element. ~ is rewritten
            // to (x ^ -1), exercising the same BitXor array path.
            {"(long_array[0] >> 1) < 5000",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return (int64_t(val) >> 1) < 5000;
             }},
            {"(long_array[0] >> 2) >= 0",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return (int64_t(val) >> 2) >= 0;
             }},
            {"~long_array[0] != 0",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return (~int64_t(val)) != 0;
             }},
            // float_array[1024] + 2.2 == 133.2
            {"float_array[1024] + 2.2 == 133.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data_unchecked<double>(1024);
                 return val + 2.2 == 133.2;
             }},
            // float_array[1024] + 2.2 != 133.2
            {"float_array[1024] + 2.2 != 133.2",
             "float",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data_unchecked<double>(1024);
                 return val + 2.2 != 133.2;
             }},
            // double_array[1024] - 11.1 == 125.7
            {"double_array[1024] - 11.1 == 125.7",
             "double",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data_unchecked<double>(1024);
                 return val - 11.1 == 125.7;
             }},
            // double_array[1024] - 11.1 != 125.7
            {"double_array[1024] - 11.1 != 125.7",
             "double",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data_unchecked<double>(1024);
                 return val - 11.1 != 125.7;
             }},
            // long_array[1024] * 2 == 8
            {"long_array[1024] * 2 == 8",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data_unchecked<int64_t>(1024);
                 return val * 2 == 8;
             }},
            // long_array[1024] * 2 != 20
            {"long_array[1024] * 2 != 20",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data_unchecked<int64_t>(1024);
                 return val * 2 != 20;
             }},
            // long_array[1024] / 2 == 8
            {"long_array[1024] / 2 == 8",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data_unchecked<int64_t>(1024);
                 return val / 2 == 8;
             }},
            // long_array[1024] / 2 != 20
            {"long_array[1024] / 2 != 20",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data_unchecked<int64_t>(1024);
                 return val / 2 != 20;
             }},
            // long_array[1024] % 3 == 0
            {"long_array[1024] % 3 == 0",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data_unchecked<int64_t>(1024);
                 return val % 3 == 0;
             }},
            // long_array[1024] % 3 != 2
            {"long_array[1024] % 3 != 2",
             "long",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data_unchecked<int64_t>(1024);
                 return val % 3 != 2;
             }},
            // array_length(int_array) == 10
            {"array_length(int_array) == 10",
             "int",
             [](milvus::Array& array) { return array.length() == 10; }},
            // array_length(int_array) != 8
            {"array_length(int_array) != 8",
             "int",
             [](milvus::Array& array) { return array.length() != 8; }},
            // array_length(int_array) > 8
            {"array_length(int_array) > 8",
             "int",
             [](milvus::Array& array) { return array.length() > 8; }},
            // array_length(int_array) >= 8
            {"array_length(int_array) >= 8",
             "int",
             [](milvus::Array& array) { return array.length() >= 8; }},
            // array_length(int_array) < 8
            {"array_length(int_array) < 8",
             "int",
             [](milvus::Array& array) { return array.length() < 8; }},
            // array_length(int_array) <= 8
            {"array_length(int_array) <= 8",
             "int",
             [](milvus::Array& array) { return array.length() <= 8; }},
        };

    for (auto [expr, array_type, ref_func] : testcases) {
        auto plan_str = schema_handle.ParseSearch(
            expr, "fakevec", 10, "L2", R"({"nprobe": 10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols[array_type][i]);
            auto ref = ref_func(array);
            ASSERT_EQ(ans, ref);
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref);
            }
        }
    }
}

template <typename T>
struct UnaryRangeTestcase {
    milvus::OpType op_type;
    T value;
    std::vector<std::string> nested_path;
    std::function<bool(milvus::Array&)> check_func;
};

TEST(Expr, TestArrayStringMatch) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto string_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarFieldProto>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_string_array_col =
            raw_data.get_col<ScalarFieldProto>(string_array_fid);
        array_cols["string"].insert(array_cols["string"].end(),
                                    new_string_array_col.begin(),
                                    new_string_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*raw_data.raw_));
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    std::vector<UnaryRangeTestcase<std::string>> prefix_testcases{
        {OpType::PrefixMatch,
         "abc",
         {"0"},
         [](milvus::Array& array) {
             return PrefixMatch(array.get_data_unchecked<std::string_view>(0),
                                "abc");
         }},
        {OpType::PrefixMatch,
         "def",
         {"1"},
         [](milvus::Array& array) {
             return PrefixMatch(array.get_data_unchecked<std::string_view>(1),
                                "def");
         }},
        {OpType::PrefixMatch,
         "def",
         {"1024"},
         [](milvus::Array& array) {
             if (array.length() <= 1024) {
                 return false;
             }
             return PrefixMatch(
                 array.get_data_unchecked<std::string_view>(1024), "def");
         }},
    };
    //vector_anns:<field_id:201 predicates:<unary_range_expr:<column_info:<field_id:131 data_type:Array nested_path:"0" element_type:VarChar > op:PrefixMatch value:<string_val:"abc" > > > query_info:<> placeholder_tag:"$0" >
    for (auto& testcase : prefix_testcases) {
        proto::plan::GenericValue value;
        value.set_string_val(testcase.value);
        auto expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
            milvus::expr::ColumnInfo(
                string_array_fid, DataType::ARRAY, testcase.nested_path),
            testcase.op_type,
            value,
            std::vector<proto::plan::GenericValue>{});
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["string"][i]);
            ASSERT_EQ(ans, testcase.check_func(array));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], testcase.check_func(array));
            }
        }
    }
}

TEST(Expr, TestArrayInTerm) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    auto bool_array_fid =
        schema->AddDebugField("bool_array", DataType::ARRAY, DataType::BOOL);
    auto float_array_fid =
        schema->AddDebugField("float_array", DataType::ARRAY, DataType::FLOAT);
    auto string_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarFieldProto>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        auto new_bool_array_col =
            raw_data.get_col<ScalarFieldProto>(bool_array_fid);
        auto new_float_array_col =
            raw_data.get_col<ScalarFieldProto>(float_array_fid);
        auto new_string_array_col =
            raw_data.get_col<ScalarFieldProto>(string_array_fid);
        array_cols["long"].insert(array_cols["long"].end(),
                                  new_long_array_col.begin(),
                                  new_long_array_col.end());
        array_cols["bool"].insert(array_cols["bool"].end(),
                                  new_bool_array_col.begin(),
                                  new_bool_array_col.end());
        array_cols["float"].insert(array_cols["float"].end(),
                                   new_float_array_col.begin(),
                                   new_float_array_col.end());
        array_cols["string"].insert(array_cols["string"].end(),
                                    new_string_array_col.begin(),
                                    new_string_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*raw_data.raw_));
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());
    ScopedSchemaHandle schema_handle(*schema);

    std::vector<std::tuple<std::string,
                           std::string,
                           std::function<bool(milvus::Array & array)>>>
        testcases = {
            // term_expr: long_array[0] in [1, 2, 3]
            {"long_array[0] in [1, 2, 3]",
             "long",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<int64_t>(0);
                 return val == 1 || val == 2 || val == 3;
             }},
            // term_expr: long_array[0] in [] (empty list)
            {"long_array[0] in []",
             "long",
             [](milvus::Array& array) { return false; }},
            // term_expr: bool_array[0] in [false, false]
            {"bool_array[0] in [false, false]",
             "bool",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<bool>(0);
                 return !val;
             }},
            // term_expr: bool_array[0] in [] (empty list)
            {"bool_array[0] in []",
             "bool",
             [](milvus::Array& array) { return false; }},
            // term_expr: float_array[0] in [1.23, 124.31]
            {"float_array[0] in [1.23, 124.31]",
             "float",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<double>(0);
                 return val == 1.23 || val == 124.31;
             }},
            // term_expr: float_array[0] in [] (empty list)
            {"float_array[0] in []",
             "float",
             [](milvus::Array& array) { return false; }},
            // term_expr: string_array[0] in ["abc", "idhgf1s"]
            {R"(string_array[0] in ["abc", "idhgf1s"])",
             "string",
             [](milvus::Array& array) {
                 auto val = array.get_data_unchecked<std::string_view>(0);
                 return val == "abc" || val == "idhgf1s";
             }},
            // term_expr: string_array[0] in [] (empty list)
            {R"(string_array[0] in [])",
             "string",
             [](milvus::Array& array) { return false; }},
            // term_expr: string_array[1024] in ["abc", "idhgf1s"]
            {R"(string_array[1024] in ["abc", "idhgf1s"])",
             "string",
             [](milvus::Array& array) {
                 if (array.length() <= 1024) {
                     return false;
                 }
                 auto val = array.get_data_unchecked<std::string_view>(1024);
                 return val == "abc" || val == "idhgf1s";
             }},
            // A subscript beyond int is out of bounds on every row.
            {"long_array[2147483648] in [1, 2, 3]",
             "long",
             [](milvus::Array& array) { return false; }},
            {"long_array[2147483648] in []",
             "long",
             [](milvus::Array& array) { return false; }},
        };

    for (auto [expr, array_type, ref_func] : testcases) {
        auto plan_str = schema_handle.ParseSearch(
            expr, "fakevec", 10, "L2", R"({"nprobe": 10})", 3);
        auto plan =
            CreateSearchPlanByExpr(schema, plan_str.data(), plan_str.size());
        BitsetType final;
        final = ExecuteQueryExpr(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0],
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan->plan_node_->plannodes_->sources()[0]->sources()[0].get(),
            seg_promote,
            N * num_iters,
            MAX_TIMESTAMP,
            &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols[array_type][i]);
            ASSERT_EQ(ans, ref_func(array));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], ref_func(array));
            }
        }
    }
}

TEST(Expr, TestTermInArray) {
    auto schema = std::make_shared<Schema>();
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    schema->set_primary_field_id(i64_fid);

    auto seg = CreateGrowingSegment(schema, empty_index_meta);
    int N = 1000;
    std::map<std::string, std::vector<ScalarFieldProto>> array_cols;
    int num_iters = 1;
    for (int iter = 0; iter < num_iters; ++iter) {
        auto raw_data = DataGen(schema, N, iter);
        auto new_long_array_col =
            raw_data.get_col<ScalarFieldProto>(long_array_fid);
        array_cols["long"].insert(array_cols["long"].end(),
                                  new_long_array_col.begin(),
                                  new_long_array_col.end());
        seg->PreInsert(N);
        seg->Insert(iter * N,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*raw_data.raw_));
    }

    auto seg_promote = dynamic_cast<SegmentGrowingImpl*>(seg.get());

    struct TermTestCases {
        std::vector<int64_t> values;
        std::vector<std::string> nested_path;
        std::function<bool(milvus::Array&)> check_func;
    };
    std::vector<TermTestCases> testcases = {
        {{100},
         {},
         [](milvus::Array& array) {
             for (int i = 0; i < array.length(); ++i) {
                 auto val = array.get_data_unchecked<int64_t>(i);
                 if (val == 100) {
                     return true;
                 }
             }
             return false;
         }},
        {{1024},
         {},
         [](milvus::Array& array) {
             for (int i = 0; i < array.length(); ++i) {
                 auto val = array.get_data_unchecked<int64_t>(i);
                 if (val == 1024) {
                     return true;
                 }
             }
             return false;
         }},
    };

    for (auto& testcase : testcases) {
        std::vector<proto::plan::GenericValue> values;
        for (auto& v : testcase.values) {
            proto::plan::GenericValue val;
            val.set_int64_val(v);
            values.emplace_back(val);
        }
        auto expr = std::make_shared<milvus::expr::TermFilterExpr>(
            milvus::expr::ColumnInfo(
                long_array_fid, DataType::ARRAY, testcase.nested_path),
            values,
            true);
        BitsetType final;
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        final =
            ExecuteQueryExpr(plan, seg_promote, N * num_iters, MAX_TIMESTAMP);
        EXPECT_EQ(final.size(), N * num_iters);

        // specify some offsets and do scalar filtering on these offsets
        milvus::exec::OffsetVector offsets;
        offsets.reserve(N * num_iters / 2);
        for (auto i = 0; i < N * num_iters; ++i) {
            if (i % 2 == 0) {
                offsets.emplace_back(i);
            }
        }
        auto col_vec = milvus::test::gen_filter_res(
            plan.get(), seg_promote, N * num_iters, MAX_TIMESTAMP, &offsets);
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        EXPECT_EQ(view.size(), N * num_iters / 2);

        for (int i = 0; i < N * num_iters; ++i) {
            auto ans = final[i];
            auto array = milvus::Array(array_cols["long"][i]);
            ASSERT_EQ(ans, testcase.check_func(array));
            if (i % 2 == 0) {
                ASSERT_EQ(view[int(i / 2)], testcase.check_func(array));
            }
        }
    }
}

TEST(Expr, TestArrayContainsForStruct) {
    // Step 1: Prepare schema with array field
    int dim = 4;
    auto schema = std::make_shared<Schema>();
    auto vec_fid = schema->AddDebugVectorArrayField("structA[array_float_vec]",
                                                    DataType::VECTOR_FLOAT,
                                                    dim,
                                                    knowhere::metric::L2);
    auto int_array_fid = schema->AddDebugArrayField(
        "structA[price_array]", DataType::INT32, false);

    auto int64_fid = schema->AddDebugField("id", DataType::INT64);
    schema->set_primary_field_id(int64_fid);

    size_t N = 500;
    int array_len = 3;

    // Step 2: Generate test data
    auto raw_data = DataGen(schema, N, 42, 0, 1, array_len);

    // Use random values between 1-20 for array elements
    std::mt19937 rng(42);  // Fixed seed for reproducibility
    std::uniform_int_distribution<int> dist(1, 20);

    // Track which rows contain value 5 for verification
    std::set<int> rows_containing_5;

    for (int i = 0; i < raw_data.raw_->fields_data_size(); i++) {
        auto* field_data = raw_data.raw_->mutable_fields_data(i);
        if (field_data->field_id() == int_array_fid.get()) {
            field_data->mutable_scalars()
                ->mutable_array_data()
                ->mutable_data()
                ->Clear();

            for (int row = 0; row < N; row++) {
                auto* array_data = field_data->mutable_scalars()
                                       ->mutable_array_data()
                                       ->mutable_data()
                                       ->Add();

                for (int elem = 0; elem < array_len; elem++) {
                    int value = dist(rng);  // Random value between 1-20
                    array_data->mutable_int_data()->mutable_data()->Add(value);
                    if (value == 5) {
                        rows_containing_5.insert(row);
                    }
                }
            }
            break;
        }
    }

    // Step 3: Create sealed segment with field data
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    // Step 4: Load vector index for element-level search
    auto array_vec_values = raw_data.get_col<VectorFieldProto>(vec_fid);

    // DataGen generates VECTOR_ARRAY with data in float_vector (flattened),
    // not in vector_array (nested structure)
    std::vector<float> vector_data(dim * N * array_len);
    for (int i = 0; i < N; i++) {
        const auto& float_vec = array_vec_values[i].float_vector().data();
        // float_vec contains array_len * dim floats
        for (int j = 0; j < array_len * dim; j++) {
            vector_data[i * array_len * dim + j] = float_vec[j];
        }
    }

    // For element-level search, index all elements (N * array_len vectors)
    auto indexing = GenVecIndexing(N * array_len,
                                   dim,
                                   vector_data.data(),
                                   knowhere::IndexEnum::INDEX_HNSW);
    LoadIndexInfo load_index_info;
    load_index_info.field_id = vec_fid.get();
    load_index_info.field_type = DataType::VECTOR_ARRAY;
    load_index_info.element_type = DataType::VECTOR_FLOAT;
    load_index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    load_index_info.num_rows = N;
    load_index_info.index_size = indexing->MemoryUsage();
    SetTestIndexMetadata(
        load_index_info, *indexing, index::families::kVectorMem);
    load_index_info.index_params = GenIndexParams(indexing.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(indexing));
    load_index_info.index_params["metric_type"] = knowhere::metric::L2;
    segment->LoadIndex(load_index_info);

    int topK = 5;

    ScopedSchemaHandle schema_handle(*schema);

    // Step 5a: Test with array_contains_any filter
    {
        std::string expr = "array_contains_any(structA[price_array], [5])";

        auto plan_bytes = schema_handle.ParseSearch(
            expr, "structA[array_float_vec]", topK, "L2", R"({"ef": 50})", 3);
        auto plan = CreateSearchPlanByExpr(
            schema, plan_bytes.data(), plan_bytes.size());
        ASSERT_NE(plan, nullptr);

        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw =
            CreatePlaceholderGroup(num_queries, dim, seed, true);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);

        // Verify results
        ASSERT_NE(search_result, nullptr);

        // Array contains is a regular filter, not element-level search
        ASSERT_FALSE(search_result->seg_offsets_.empty());

        // Should have topK results per query
        ASSERT_LE(search_result->seg_offsets_.size(),
                  static_cast<size_t>(topK * num_queries));

        for (size_t i = 0; i < search_result->seg_offsets_.size(); i++) {
            int64_t doc_id = search_result->seg_offsets_[i];
            float distance = search_result->distances_[i];

            // Verify the doc's array contains value 5 using our tracked set
            ASSERT_TRUE(rows_containing_5.count(doc_id) > 0)
                << "Result doc_id " << doc_id << " should contain value 5";
        }

        // Verify distances are sorted (ascending for L2)
        for (size_t i = 1; i < search_result->distances_.size(); ++i) {
            ASSERT_LE(search_result->distances_[i - 1],
                      search_result->distances_[i])
                << "Distances should be sorted in ascending order";
        }
    }

    // Step 5b: Test with array_contains_all filter
    // contains_all requires the array to contain ALL given elements
    {
        std::string expr = "array_contains_all(structA[price_array], [5, 10])";

        auto plan_bytes = schema_handle.ParseSearch(
            expr, "structA[array_float_vec]", topK, "L2", R"({"ef": 50})", 3);
        auto plan = CreateSearchPlanByExpr(
            schema, plan_bytes.data(), plan_bytes.size());
        ASSERT_NE(plan, nullptr);

        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw =
            CreatePlaceholderGroup(num_queries, dim, seed, true);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);

        ASSERT_NE(search_result, nullptr);

        // Verify each result's array contains BOTH 5 and 10
        auto array_col =
            raw_data.get_col(int_array_fid)->scalars().array_data().data();
        for (size_t i = 0; i < search_result->seg_offsets_.size(); i++) {
            int64_t doc_id = search_result->seg_offsets_[i];
            float distance = search_result->distances_[i];

            auto& arr = array_col[doc_id].int_data().data();
            bool has_5 = std::find(arr.begin(), arr.end(), 5) != arr.end();
            bool has_10 = std::find(arr.begin(), arr.end(), 10) != arr.end();
            ASSERT_TRUE(has_5 && has_10) << "Result doc_id " << doc_id
                                         << " should contain both 5 and 10";
        }

        for (size_t i = 1; i < search_result->distances_.size(); ++i) {
            ASSERT_LE(search_result->distances_[i - 1],
                      search_result->distances_[i])
                << "Distances should be sorted in ascending order";
        }
    }

    // Step 6: Test with scalar index on price_array field
    {
        // Materialize the same per-row arrays; the builder publishes element
        // coordinates and the segment retains the offsets used for projection.
        auto array_col =
            raw_data.get_col(int_array_fid)->scalars().array_data().data();
        std::vector<Array> arrays;
        arrays.reserve(N);
        for (size_t i = 0; i < N; i++) {
            arrays.emplace_back(array_col[i]);
        }
        auto field = std::make_shared<FieldData<Array>>(DataType::ARRAY, false);
        field->FillFieldData(arrays.data(), arrays.size());
        auto arr_index =
            test::expr_index::BuildIndex(int_array_fid,
                                         DataType::ARRAY,
                                         index::INVERTED_INDEX_TYPE,
                                         {field},
                                         Config::object(),
                                         DataType::INT32,
                                         true);
        test::expr_index::InstallIndex(*segment,
                                       int_array_fid,
                                       DataType::ARRAY,
                                       std::move(arr_index),
                                       DataType::INT32);

        // Now search with index
        std::string expr = "array_contains_any(structA[price_array], [5])";

        auto plan_bytes = schema_handle.ParseSearch(
            expr, "structA[array_float_vec]", topK, "L2", R"({"ef": 50})", 3);
        auto plan = CreateSearchPlanByExpr(
            schema, plan_bytes.data(), plan_bytes.size());
        ASSERT_NE(plan, nullptr);

        auto num_queries = 1;
        auto seed = 1024;
        auto ph_group_raw =
            CreatePlaceholderGroup(num_queries, dim, seed, true);
        auto ph_group =
            ParsePlaceholderGroup(plan.get(), ph_group_raw.SerializeAsString());

        auto search_result =
            segment->Search(plan.get(), ph_group.get(), 1L << 63);

        // Verify results with index
        ASSERT_NE(search_result, nullptr);
        ASSERT_FALSE(search_result->seg_offsets_.empty());
        ASSERT_LE(search_result->seg_offsets_.size(),
                  static_cast<size_t>(topK * num_queries));

        for (size_t i = 0; i < search_result->seg_offsets_.size(); i++) {
            int64_t doc_id = search_result->seg_offsets_[i];
            float distance = search_result->distances_[i];

            // Verify the doc's array contains value 5
            ASSERT_TRUE(rows_containing_5.count(doc_id) > 0)
                << "Result doc_id " << doc_id
                << " should contain value 5 (with index)";
        }

        // Verify distances are sorted (ascending for L2)
        for (size_t i = 1; i < search_result->distances_.size(); ++i) {
            ASSERT_LE(search_result->distances_[i - 1],
                      search_result->distances_[i])
                << "Distances should be sorted in ascending order (with index)";
        }
    }
}

// The index path converts the term list only when the index result is not
// cached. Evaluating a sealed segment larger than one batch, without the
// single-pass optimization, checks that every later batch sees the same,
// deduplicated terms as the first one.
static void
CheckArrayContainsIndexPathAcrossBatches(const std::string& index_type,
                                         Config params = Config::object()) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto str_array_fid = schema->AddDebugField(
        "string_array", DataType::ARRAY, DataType::VARCHAR);
    schema->set_primary_field_id(i64_fid);

    // more than two default-size (8192) batches
    constexpr int N = 20000;
    auto raw_data = DataGen(schema, N, 42, 0, 1, 3);
    auto str_array_col = raw_data.get_col<ScalarFieldProto>(str_array_fid);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    FixedVector<Array> arrays;
    arrays.reserve(N);
    for (int i = 0; i < N; ++i) {
        arrays.emplace_back(str_array_col[i]);
    }
    auto field_data =
        storage::CreateFieldData(DataType::ARRAY, DataType::VARCHAR, false);
    field_data->FillFieldData(arrays.data(), N);

    // Build, serialize and reload through the production reader contracts.
    auto opened_index = test::expr_index::BuildIndex(str_array_fid,
                                                     DataType::ARRAY,
                                                     index_type,
                                                     {field_data},
                                                     params,
                                                     DataType::VARCHAR);
    if (index_type == index::HYBRID_INDEX_TYPE ||
        index_type == index::ASCENDING_SORT) {
        ASSERT_EQ(opened_index.family, index::families::kSort);
    }
    ASSERT_EQ(opened_index.reader->CoordDomain(), index::Domain::Row);
    ASSERT_FALSE(opened_index.caps.nested);
    ASSERT_EQ(opened_index.reader->Count(), N);
    test::expr_index::InstallIndex(*segment,
                                   str_array_fid,
                                   DataType::ARRAY,
                                   std::move(opened_index),
                                   DataType::VARCHAR);

    auto row_values = [&](int i) {
        auto array = milvus::Array(str_array_col[i]);
        std::vector<std::string> res;
        for (int j = 0; j < array.length(); ++j) {
            res.emplace_back(array.get_data_unchecked<std::string_view>(j));
        }
        return res;
    };
    auto contains = [](const std::vector<std::string>& values,
                       const std::string& t) {
        return std::find(values.begin(), values.end(), t) != values.end();
    };

    // Targets: every distinct element of the first rows; each is listed
    // twice in the expression so the converted list must deduplicate.
    std::vector<std::string> targets;
    std::unordered_set<std::string> seen;
    for (int i = 0; i < 300; ++i) {
        for (auto& v : row_values(i)) {
            if (seen.insert(v).second) {
                targets.push_back(v);
            }
        }
    }
    ASSERT_GT(targets.size(), 1);

    auto run =
        [&](proto::plan::JSONContainsExpr_JSONOp op,
            const std::vector<std::string>& terms,
            const std::function<bool(const std::vector<std::string>&)>& check) {
            std::vector<proto::plan::GenericValue> vals;
            for (const auto& t : terms) {
                proto::plan::GenericValue gv;
                gv.set_string_val(t);
                vals.push_back(gv);
                vals.push_back(gv);
            }
            auto expr = std::make_shared<milvus::expr::JsonContainsExpr>(
                expr::ColumnInfo(
                    str_array_fid, DataType::ARRAY, DataType::VARCHAR),
                op,
                true,
                vals);

            auto evaluation =
                milvus::test::EvalExprInBatches(expr, segment.get(), N);
            ASSERT_GT(evaluation.batch_sizes.size(), 2);
            ASSERT_EQ(evaluation.result->size(), N);
            BitsetTypeView bits(evaluation.result->GetRawData(), N);
            int matched = 0;
            for (int i = 0; i < N; ++i) {
                bool expected = check(row_values(i));
                matched += expected;
                ASSERT_EQ(bits[i], expected) << "row " << i;
            }
            ASSERT_GT(matched, 0);
            ASSERT_LT(matched, N);
        };

    run(proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
        targets,
        [&](const std::vector<std::string>& values) {
            for (const auto& t : targets) {
                if (contains(values, t)) {
                    return true;
                }
            }
            return false;
        });

    std::vector<std::string> all_terms{targets[0], targets[1]};
    run(proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
        all_terms,
        [&](const std::vector<std::string>& values) {
            return contains(values, all_terms[0]) &&
                   contains(values, all_terms[1]);
        });
}

TEST(Expr, TestArrayContainsIndexPathAcrossBatches) {
    CheckArrayContainsIndexPathAcrossBatches(index::BITMAP_INDEX_TYPE);
}

TEST(Expr, TestArraySortedContainsIndexPathAcrossBatches) {
    CheckArrayContainsIndexPathAcrossBatches(
        index::HYBRID_INDEX_TYPE,
        {{index::SCALAR_INDEX_ENGINE_VERSION, kArrayHybridStlSortMinVersion},
         {index::ENABLE_ARRAY_SORT_INDEX, true},
         {index::BITMAP_INDEX_CARDINALITY_LIMIT, 1}});
}

TEST(Expr, TestArrayExplicitSortedContainsIndexPathAcrossBatches) {
    CheckArrayContainsIndexPathAcrossBatches(
        index::ASCENDING_SORT,
        {{index::SCALAR_INDEX_ENGINE_VERSION, kArrayHybridStlSortMinVersion}});
}

// A sealed segment with two chunks: the chunk-length path must resolve rows
// across the chunk boundary for both the sequential and the offset-input
// paths, and agree with the expected lengths row by row.
TEST(Expr, TestArrayLengthExprAcrossChunks) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid = schema->AddDebugField(
        "long_array", DataType::ARRAY, DataType::INT64, true);
    schema->set_primary_field_id(i64_fid);

    constexpr int N = 5000;
    auto make_rows = [](int shift) {
        std::vector<std::optional<std::vector<int64_t>>> rows(N);
        for (int i = 0; i < N; ++i) {
            if ((i + shift) % 7 == 0) {
                continue;  // NULL
            }
            rows[i] = std::vector<int64_t>((i + shift) % 4, 1);
        }
        return rows;
    };
    auto rows_a = make_rows(0);
    auto rows_b = make_rows(3);
    auto data_a = DataGen(schema, N, 42, 0, 1, 2);
    auto data_b = DataGen(schema, N, 43, N, 1, 2);
    SetNullableInt64ArrayFieldData(data_a, long_array_fid, rows_a);
    SetNullableInt64ArrayFieldData(data_b, long_array_fid, rows_b);
    auto segment = CreateTwoChunkSealed(schema, data_a, data_b);
    ASSERT_EQ(segment->num_chunk_data(long_array_fid), 2);

    std::vector<std::optional<std::vector<int64_t>>> rows(rows_a);
    rows.insert(rows.end(), rows_b.begin(), rows_b.end());
    const int64_t total = rows.size();

    milvus::exec::OffsetVector offset_input;
    for (int64_t i = total - 1; i >= 0; i -= 5) {
        offset_input.emplace_back(i);
    }

    for (auto op : kArrayLengthOps) {
        for (int64_t target : {0, 2}) {
            auto expr = std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
                expr::ColumnInfo(
                    long_array_fid, DataType::ARRAY, DataType::INT64, {}, true),
                op,
                proto::plan::ArithOpType::ArrayLength,
                Int64Value(target),
                Int64Value(0));
            auto expected = [&](int64_t i) {
                return rows[i].has_value() &&
                       CompareArrayLength(
                           op, static_cast<int64_t>(rows[i]->size()), target);
            };
            auto label = "op " + std::to_string(static_cast<int>(op)) +
                         " target " + std::to_string(target);

            auto batched =
                milvus::test::EvalExprInBatches(expr, segment.get(), total);
            ASSERT_EQ(batched.result->size(), total) << label;
            BitsetTypeView bits(batched.result->GetRawData(), total);
            BitsetTypeView valid(batched.result->GetValidRawData(), total);
            for (int64_t i = 0; i < total; ++i) {
                ASSERT_EQ(bits[i], expected(i)) << label << ", row " << i;
                ASSERT_EQ(valid[i], rows[i].has_value())
                    << label << ", row " << i;
            }

            auto plan = std::make_shared<plan::FilterBitsNode>(
                DEFAULT_PLANNODE_ID, expr);
            auto final =
                ExecuteQueryExpr(plan, segment.get(), total, MAX_TIMESTAMP);
            for (int64_t i = 0; i < total; ++i) {
                ASSERT_EQ(final[i], expected(i)) << label << ", row " << i;
            }

            auto col_vec = milvus::test::gen_filter_res(
                plan.get(), segment.get(), total, MAX_TIMESTAMP, &offset_input);
            BitsetTypeView obits(col_vec->GetRawData(), col_vec->size());
            ASSERT_EQ(obits.size(), offset_input.size());
            for (size_t k = 0; k < offset_input.size(); ++k) {
                ASSERT_EQ(obits[k], expected(offset_input[k]))
                    << label << ", offset row " << offset_input[k];
            }
        }
    }
}

// An ARRAY length predicate on a sealed segment no longer forces the plan
// out of single-pass execution.
TEST(Expr, TestArrayLengthExprCanExecuteAllAtOnce) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("id", DataType::INT64);
    auto long_array_fid =
        schema->AddDebugField("long_array", DataType::ARRAY, DataType::INT64);
    schema->set_primary_field_id(i64_fid);
    constexpr int N = 100;
    auto raw_data = DataGen(schema, N, 42);
    auto sealed = CreateSealedWithFieldDataLoaded(schema, raw_data);
    auto growing = CreateGrowingSegment(schema, empty_index_meta);
    growing->PreInsert(N);
    growing->Insert(0,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*raw_data.raw_));

    auto expr = std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
        expr::ColumnInfo(long_array_fid, DataType::ARRAY, DataType::INT64),
        proto::plan::OpType::Equal,
        proto::plan::ArithOpType::ArrayLength,
        Int64Value(0),
        Int64Value(0));
    EXPECT_TRUE(milvus::test::CanExprExecuteAllAtOnce(expr, sealed.get(), N));
    EXPECT_FALSE(milvus::test::CanExprExecuteAllAtOnce(expr, growing.get(), N));
}

namespace {

// Rows of the struct `s` used by the element-nullable row filter tests.
// Row 3 is a NULL struct row in every sub-field.
//   s[b]    (Array<Int64>, element nullable):
//           [1, NULL, 3], [], [NULL], NULL, [5, 6]
//   s[name] (Array<VarChar>, element nullable):
//           ["ab", NULL], [], [NULL], NULL, ["ac"]
//   s[tags] (Array<Array<Int64>>, inner Arrays and leaves nullable):
//           [[1, 2], [NULL, 4]], [], [NULL, [7]], NULL, [[5], [6, 6]]
//   s[vec]  (ArrayOfVector, element nullable):
//           [v, NULL, v], [], [NULL], NULL, [v, v]
// And a top-level nullable Array<Int64> `arr` (elements never null):
//           [1, 2], [], [3], NULL, [5, 6]
constexpr int64_t kElementNullRows = 5;
constexpr int64_t kElementNullVecDim = 4;

struct ElementNullableStruct {
    SchemaPtr schema;
    FieldId pk;
    FieldId b;
    FieldId name;
    FieldId tags;
    FieldId vec;
    FieldId arr;
    std::unique_ptr<InsertRecordProto> insert;
};

// Expected per-row result: 1 TRUE, 0 FALSE, -1 UNKNOWN.
using ElementNullExpected = std::vector<int>;

ElementNullableStruct
BuildElementNullableStruct() {
    ElementNullableStruct out;
    auto schema = std::make_shared<Schema>();
    out.pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(out.pk);
    out.arr = schema->AddDebugArrayField("arr", DataType::INT64, true, false);
    out.b = schema->AddDebugArrayField("s[b]", DataType::INT64, true, true);
    out.name =
        schema->AddDebugArrayField("s[name]", DataType::VARCHAR, true, true);
    out.vec = schema->AddDebugVectorArrayField("s[vec]",
                                               DataType::VECTOR_FLOAT,
                                               kElementNullVecDim,
                                               knowhere::metric::L2,
                                               true,
                                               true);
    out.tags = FieldId(out.vec.get() + 100);
    proto::schema::TypeSchema tags_type;
    tags_type.set_nullable(true);
    auto* inner = tags_type.mutable_array_element();
    inner->set_nullable(true);
    inner->mutable_array_element()->set_leaf_type(
        proto::schema::DataType::Int64);
    inner->mutable_array_element()->set_nullable(true);
    schema->AddField(FieldMeta(FieldName("s[tags]"),
                               out.tags,
                               DataType::ARRAY,
                               DataType::ARRAY,
                               true,
                               true,
                               std::nullopt,
                               std::string{},
                               LOCAL_FORMAT_RAW,
                               std::make_optional(std::move(tags_type))));
    out.schema = schema;

    const std::vector<bool> row_valid = {true, true, true, false, true};
    auto insert = std::make_unique<InsertRecordProto>();
    insert->set_num_rows(kElementNullRows);

    auto* pk = insert->add_fields_data();
    pk->set_field_id(out.pk.get());
    pk->set_type(proto::schema::DataType::Int64);
    for (int64_t i = 0; i < kElementNullRows; ++i) {
        pk->mutable_scalars()->mutable_long_data()->add_data(i);
    }

    auto add_array_field = [&](FieldId id, proto::schema::DataType element) {
        auto* field = insert->add_fields_data();
        field->set_field_id(id.get());
        field->set_type(proto::schema::DataType::Array);
        for (bool valid : row_valid) {
            field->mutable_scalars()->add_valid_data(valid);
        }
        auto* rows = field->mutable_scalars()->mutable_array_data();
        rows->set_element_type(element);
        return rows;
    };

    // Null slots carry a zero payload so a predicate that ignores element
    // validity would match them.
    using Leaves = std::vector<std::optional<int64_t>>;
    auto fill_long = [](ScalarFieldProto* row, const Leaves& leaves) {
        auto* data = row->mutable_long_data();
        for (const auto& leaf : leaves) {
            data->add_data(leaf.value_or(0));
            row->add_valid_data(leaf.has_value());
        }
    };
    auto* arr_rows = add_array_field(out.arr, proto::schema::DataType::Int64);
    for (const auto& row :
         std::vector<std::vector<int64_t>>{{1, 2}, {}, {3}, {}, {5, 6}}) {
        auto* data = arr_rows->add_data()->mutable_long_data();
        for (auto value : row) {
            data->add_data(value);
        }
    }

    auto* b_rows = add_array_field(out.b, proto::schema::DataType::Int64);
    for (const auto& row : std::vector<Leaves>{
             {1, std::nullopt, 3}, {}, {std::nullopt}, {}, {5, 6}}) {
        fill_long(b_rows->add_data(), row);
    }

    using Strings = std::vector<std::optional<std::string>>;
    auto* name_rows =
        add_array_field(out.name, proto::schema::DataType::VarChar);
    for (const auto& row : std::vector<Strings>{
             {"ab", std::nullopt}, {}, {std::nullopt}, {}, {"ac"}}) {
        auto* proto_row = name_rows->add_data();
        auto* data = proto_row->mutable_string_data();
        for (const auto& leaf : row) {
            data->add_data(leaf.value_or(""));
            proto_row->add_valid_data(leaf.has_value());
        }
    }

    using Inner = std::optional<Leaves>;
    auto* tags_rows = add_array_field(out.tags, proto::schema::DataType::Array);
    for (const auto& row : std::vector<std::vector<Inner>>{
             {Leaves{1, 2}, Leaves{std::nullopt, 4}},
             {},
             {std::nullopt, Leaves{7}},
             {},
             {Leaves{5}, Leaves{6, 6}}}) {
        auto* proto_row = tags_rows->add_data();
        auto* children = proto_row->mutable_array_data();
        children->set_element_type(proto::schema::DataType::Int64);
        for (const auto& child : row) {
            auto* proto_child = children->add_data();
            if (child.has_value()) {
                fill_long(proto_child, *child);
            } else {
                proto_child->mutable_long_data();
            }
            proto_row->add_valid_data(child.has_value());
        }
    }

    auto* vec = insert->add_fields_data();
    vec->set_field_id(out.vec.get());
    vec->set_type(proto::schema::DataType::ArrayOfVector);
    vec->mutable_vectors()->set_dim(kElementNullVecDim);
    auto* vec_rows = vec->mutable_vectors()->mutable_vector_array();
    vec_rows->set_dim(kElementNullVecDim);
    vec_rows->set_element_type(proto::schema::DataType::FloatVector);
    for (const auto& elements : std::vector<std::vector<bool>>{
             {true, false, true}, {}, {false}, {}, {true, true}}) {
        auto* row = vec_rows->add_data();
        row->set_dim(kElementNullVecDim);
        row->mutable_float_vector();
        for (bool valid : elements) {
            row->add_valid_data(valid);
            if (valid) {
                for (int64_t d = 0; d < kElementNullVecDim; ++d) {
                    row->mutable_float_vector()->add_data(1.0f);
                }
            }
        }
    }
    for (bool valid : row_valid) {
        vec->mutable_vectors()->add_valid_data(valid);
    }
    out.insert = std::move(insert);
    return out;
}

std::unique_ptr<SegmentGrowing>
LoadElementNullableGrowing(const ElementNullableStruct& data) {
    auto growing = CreateGrowingSegment(
        data.schema, empty_index_meta, 1, SegcoreConfig::default_config());
    std::vector<int64_t> row_ids(kElementNullRows);
    std::vector<Timestamp> timestamps(kElementNullRows);
    std::iota(row_ids.begin(), row_ids.end(), 0);
    std::iota(timestamps.begin(), timestamps.end(), 0);
    const auto offset = growing->PreInsert(kElementNullRows);
    growing->Insert(offset,
                    kElementNullRows,
                    row_ids.data(),
                    timestamps.data(),
                    std::make_shared<InsertRecordProto>(*data.insert));
    return growing;
}

// Native-list fields load only from Storage V2/V3, so write the rows as a
// V3 segment in two-row batches.
std::unique_ptr<SegmentSealed>
LoadElementNullableSealed(const ElementNullableStruct& data,
                          const std::string& segment_path) {
    proto::schema::CollectionSchema storage_proto;
    for (auto [id, name] : {std::pair{RowFieldID, "RowID"},
                            std::pair{TimestampFieldID, "Timestamp"}}) {
        auto* field = storage_proto.add_fields();
        field->set_fieldid(id.get());
        field->set_name(name);
        field->set_data_type(proto::schema::DataType::Int64);
    }
    const auto user_proto = data.schema->ToProto();
    for (const auto& field : user_proto.fields()) {
        *storage_proto.add_fields() = field;
    }
    auto schema = Schema::ParseFrom(storage_proto);

    auto build_int64 = [](int64_t base) {
        arrow::Int64Builder builder;
        for (int64_t i = 0; i < kElementNullRows; ++i) {
            AssertInfo(builder.Append(base + i).ok(), "append int64");
        }
        std::shared_ptr<arrow::Array> output;
        AssertInfo(builder.Finish(&output).ok(), "finish int64");
        return output;
    };
    std::unordered_map<FieldId, std::shared_ptr<arrow::Array>> columns = {
        {RowFieldID, build_int64(0)}, {TimestampFieldID, build_int64(0)}};
    for (const auto& field_data : data.insert->fields_data()) {
        const FieldId id(field_data.field_id());
        auto converted = FieldDataToArrow(std::to_string(id.get()),
                                          field_data,
                                          kElementNullRows,
                                          false,
                                          nullptr,
                                          &(*schema)[id]);
        AssertInfo(converted.ok(),
                   "convert field {}: {}",
                   id.get(),
                   converted.status().ToString());
        columns[id] = converted->second;
    }
    auto loon_schema = schema->ConvertToLoonArrowSchema();
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    for (int64_t offset = 0; offset < kElementNullRows; offset += 2) {
        const auto length = std::min<int64_t>(2, kElementNullRows - offset);
        arrow::ArrayVector sliced;
        for (const auto& id : schema->get_field_ids()) {
            sliced.push_back(columns.at(id)->Slice(offset, length));
        }
        batches.push_back(
            arrow::RecordBatch::Make(loon_schema, length, sliced));
    }
    const auto version =
        test::WriteRecordBatchesToV3(schema,
                                     segment_path,
                                     batches,
                                     test::GenerateColumnGroupPattern(schema));

    proto::segcore::SegmentLoadInfo load_info;
    load_info.set_collectionid(1);
    load_info.set_partitionid(2);
    load_info.set_segmentid(3);
    load_info.set_storageversion(STORAGE_V3);
    load_info.set_num_of_rows(kElementNullRows);
    load_info.set_manifest_path("{\"base_path\":\"" + segment_path +
                                "\",\"ver\":" + std::to_string(version) + "}");
    load_info.set_insert_channel("element-nullable-row-filter-test");
    auto sealed = CreateSealedSegment(schema, empty_index_meta, 3);
    sealed->SetLoadInfo(load_info);
    tracer::TraceContext trace_ctx;
    sealed->Load(trace_ctx, nullptr);
    return sealed;
}

// Evaluate expr on the batch path, the offsets path and under NOT, and check
// UNKNOWN stays UNKNOWN under NOT while TRUE/FALSE flip.
void
CheckElementNullExpr(SegmentInternalInterface* segment,
                     const expr::TypedExprPtr& typed_expr,
                     const ElementNullExpected& expected,
                     const std::string& label) {
    ASSERT_EQ(expected.size(), kElementNullRows) << label;
    auto check = [&](const ColumnVectorPtr& vec,
                     const std::vector<int64_t>& rows,
                     bool negated,
                     const std::string& path) {
        ASSERT_EQ(vec->size(), rows.size()) << label << " " << path;
        BitsetTypeView bits(vec->GetRawData(), vec->size());
        BitsetTypeView valid(vec->GetValidRawData(), vec->size());
        for (size_t i = 0; i < rows.size(); ++i) {
            const auto want = expected[rows[i]];
            const auto where =
                label + " " + path + ", row " + std::to_string(rows[i]);
            if (want < 0) {
                EXPECT_FALSE(valid[i]) << where;
                EXPECT_FALSE(bits[i]) << where;
            } else {
                EXPECT_TRUE(valid[i]) << where;
                EXPECT_EQ(bits[i], (want == 1) != negated) << where;
            }
        }
    };
    const std::vector<int64_t> all_rows = {0, 1, 2, 3, 4};
    auto not_expr = std::make_shared<expr::LogicalUnaryExpr>(
        expr::LogicalUnaryExpr::OpType::LogicalNot, typed_expr);
    for (bool negated : {false, true}) {
        auto plan = std::make_shared<plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID,
            negated ? expr::TypedExprPtr(not_expr) : typed_expr);
        const std::string tag = negated ? "NOT " : "";
        check(milvus::test::gen_filter_res(
                  plan.get(), segment, kElementNullRows, MAX_TIMESTAMP),
              all_rows,
              negated,
              tag + "batch");

        exec::OffsetVector offsets = {4, 0, 3, 2, 1};
        check(
            milvus::test::gen_filter_res(
                plan.get(), segment, kElementNullRows, MAX_TIMESTAMP, &offsets),
            {4, 0, 3, 2, 1},
            negated,
            tag + "offsets");

        // Two-row batches cross chunk boundaries on the batch path.
        const auto saved_batch_size = EXEC_EVAL_EXPR_BATCH_SIZE.load();
        EXEC_EVAL_EXPR_BATCH_SIZE.store(2);
        auto final =
            ExecuteQueryExpr(plan, segment, kElementNullRows, MAX_TIMESTAMP);
        EXEC_EVAL_EXPR_BATCH_SIZE.store(saved_batch_size);
        ASSERT_EQ(final.size(), kElementNullRows) << label;
        for (int64_t i = 0; i < kElementNullRows; ++i) {
            const bool want =
                expected[i] >= 0 && ((expected[i] == 1) != negated);
            EXPECT_EQ(final[i], want)
                << label << " " << tag << "filter, row " << i;
        }
    }
}

proto::plan::GenericValue
ElementNullInt(int64_t value) {
    proto::plan::GenericValue val;
    val.set_int64_val(value);
    return val;
}

proto::plan::GenericValue
ElementNullString(const std::string& value) {
    proto::plan::GenericValue val;
    val.set_string_val(value);
    return val;
}

proto::plan::GenericValue
ElementNullIntArray(const std::vector<int64_t>& values) {
    proto::plan::GenericValue val;
    auto* array = val.mutable_array_val();
    array->set_same_type(true);
    array->set_element_type(proto::schema::DataType::Int64);
    for (auto value : values) {
        array->add_array()->set_int64_val(value);
    }
    return val;
}

proto::plan::GenericValue
ElementNullStringArray(const std::vector<std::string>& values) {
    proto::plan::GenericValue val;
    auto* array = val.mutable_array_val();
    array->set_same_type(true);
    array->set_element_type(proto::schema::DataType::VarChar);
    for (const auto& value : values) {
        array->add_array()->set_string_val(value);
    }
    return val;
}

void
RunElementNullableRowFilters(SegmentInternalInterface* segment,
                             const ElementNullableStruct& data) {
    struct Case {
        std::string name;
        expr::TypedExprPtr expr;
        ElementNullExpected expected;
    };
    auto column =
        [](FieldId id, DataType element, std::vector<std::string> path = {}) {
            return expr::ColumnInfo(
                id, DataType::ARRAY, element, std::move(path), true);
        };
    auto unary = [](expr::ColumnInfo col,
                    proto::plan::OpType op,
                    proto::plan::GenericValue val) {
        return std::make_shared<expr::UnaryRangeFilterExpr>(col, op, val);
    };
    auto contains = [](expr::ColumnInfo col,
                       proto::plan::JSONContainsExpr_JSONOp op,
                       std::vector<proto::plan::GenericValue> vals) {
        return std::make_shared<expr::JsonContainsExpr>(col, op, true, vals);
    };
    auto length = [](expr::ColumnInfo col, int64_t len) {
        return std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
            col,
            proto::plan::OpType::Equal,
            proto::plan::ArithOpType::ArrayLength,
            ElementNullInt(len),
            ElementNullInt(0));
    };
    using proto::plan::OpType;
    constexpr auto kContains = proto::plan::JSONContainsExpr_JSONOp_Contains;
    constexpr auto kAny = proto::plan::JSONContainsExpr_JSONOp_ContainsAny;
    constexpr auto kAll = proto::plan::JSONContainsExpr_JSONOp_ContainsAll;
    const auto b = column(data.b, DataType::INT64);
    const auto b0 = column(data.b, DataType::INT64, {"0"});
    const auto b1 = column(data.b, DataType::INT64, {"1"});
    const auto name = column(data.name, DataType::VARCHAR);
    const auto name0 = column(data.name, DataType::VARCHAR, {"0"});
    const auto name1 = column(data.name, DataType::VARCHAR, {"1"});
    const auto tags0 = column(data.tags, DataType::INT64, {"0"});
    const auto tags1 = column(data.tags, DataType::INT64, {"1"});
    const auto vec = expr::ColumnInfo(
        data.vec, DataType::VECTOR_ARRAY, DataType::VECTOR_FLOAT, {}, true);
    // Subscripts beyond int (and beyond int64) or negative name no element.
    const auto b_huge = column(data.b, DataType::INT64, {"2147483648"});
    const auto b_beyond_int64 =
        column(data.b, DataType::INT64, {"99999999999999999999"});
    const auto b_negative = column(data.b, DataType::INT64, {"-1"});
    const auto tags_huge = column(data.tags, DataType::INT64, {"2147483648"});
    const auto arr0 = column(data.arr, DataType::INT64, {"0"});
    const auto arr1 = column(data.arr, DataType::INT64, {"1"});
    const auto arr_huge = column(data.arr, DataType::INT64, {"2147483648"});
    const ElementNullExpected all_unknown = {-1, -1, -1, -1, -1};
    const ElementNullExpected all_false = {0, 0, 0, 0, 0};
    auto term = [](expr::ColumnInfo col,
                   std::vector<proto::plan::GenericValue> vals) {
        return std::make_shared<expr::TermFilterExpr>(col, std::move(vals));
    };

    const std::vector<Case> cases = {
        // C7: null elements match nothing; only the NULL row is UNKNOWN.
        {"contains(b, 3)",
         contains(b, kContains, {ElementNullInt(3)}),
         {1, 0, 0, -1, 0}},
        {"contains(b, 0)",
         contains(b, kContains, {ElementNullInt(0)}),
         {0, 0, 0, -1, 0}},
        {"contains_any(b, [0, 6])",
         contains(b, kAny, {ElementNullInt(0), ElementNullInt(6)}),
         {0, 0, 0, -1, 1}},
        {"contains_all(b, [1, 3])",
         contains(b, kAll, {ElementNullInt(1), ElementNullInt(3)}),
         {1, 0, 0, -1, 0}},
        {"contains_all(b, [0, 1])",
         contains(b, kAll, {ElementNullInt(0), ElementNullInt(1)}),
         {0, 0, 0, -1, 0}},
        {"contains_all(b, [])", contains(b, kAll, {}), {1, 1, 1, -1, 1}},
        {"contains(name, \"\")",
         contains(name, kContains, {ElementNullString("")}),
         {0, 0, 0, -1, 0}},
        // array_length counts null elements.
        {"length(b) == 3", length(b, 3), {1, 0, 0, -1, 0}},
        {"length(b) == 1", length(b, 1), {0, 0, 1, -1, 0}},
        {"length(b) == 0", length(b, 0), {0, 1, 0, -1, 0}},
        {"length(vec) == 3", length(vec, 3), {1, 0, 0, -1, 0}},
        {"length(vec) == 1", length(vec, 1), {0, 0, 1, -1, 0}},
        {"length(vec) == 0", length(vec, 0), {0, 1, 0, -1, 0}},
        // C3: a null or missing element is UNKNOWN.
        {"b[0] > 0",
         unary(b0, OpType::GreaterThan, ElementNullInt(0)),
         {1, -1, -1, -1, 1}},
        {"b[1] == 0",
         unary(b1, OpType::Equal, ElementNullInt(0)),
         {-1, -1, -1, -1, 0}},
        {"b[1] != 0",
         unary(b1, OpType::NotEqual, ElementNullInt(0)),
         {-1, -1, -1, -1, 1}},
        {"b[1] <= 6",
         unary(b1, OpType::LessEqual, ElementNullInt(6)),
         {-1, -1, -1, -1, 1}},
        {"b[1] in [0, 6]",
         std::make_shared<expr::TermFilterExpr>(
             b1,
             std::vector<proto::plan::GenericValue>{ElementNullInt(0),
                                                    ElementNullInt(6)}),
         {-1, -1, -1, -1, 1}},
        {"0 <= b[1] <= 6",
         std::make_shared<expr::BinaryRangeFilterExpr>(
             b1, ElementNullInt(0), ElementNullInt(6), true, true),
         {-1, -1, -1, -1, 1}},
        {"b[1] + 1 == 1",
         std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
             b1,
             OpType::Equal,
             proto::plan::ArithOpType::Add,
             ElementNullInt(1),
             ElementNullInt(1)),
         {-1, -1, -1, -1, 0}},
        {"name[0] like \"a%\"",
         unary(name0, OpType::Match, ElementNullString("a%")),
         {1, -1, -1, -1, 1}},
        {"name[1] like \"%\"",
         unary(name1, OpType::Match, ElementNullString("%")),
         {-1, -1, -1, -1, -1}},
        {"name[0] prefix \"a\"",
         unary(name0, OpType::PrefixMatch, ElementNullString("a")),
         {1, -1, -1, -1, 1}},
        {"name[1] == \"\"",
         unary(name1, OpType::Equal, ElementNullString("")),
         {-1, -1, -1, -1, -1}},
        // 4.6.1: an Array holding a null never equals a literal.
        {"b == [5, 6]",
         unary(b, OpType::Equal, ElementNullIntArray({5, 6})),
         {0, 0, 0, -1, 1}},
        {"b == [1, 0, 3]",
         unary(b, OpType::Equal, ElementNullIntArray({1, 0, 3})),
         {0, 0, 0, -1, 0}},
        {"b == [0]",
         unary(b, OpType::Equal, ElementNullIntArray({0})),
         {0, 0, 0, -1, 0}},
        {"b == []",
         unary(b, OpType::Equal, ElementNullIntArray({})),
         {0, 1, 0, -1, 0}},
        {"b != [5, 6]",
         unary(b, OpType::NotEqual, ElementNullIntArray({5, 6})),
         {1, 1, 1, -1, 0}},
        {"name == [\"ac\"]",
         unary(name, OpType::Equal, ElementNullStringArray({"ac"})),
         {0, 0, 0, -1, 1}},
        {"name != [\"ab\", \"\"]",
         unary(name, OpType::NotEqual, ElementNullStringArray({"ab", ""})),
         {1, 1, 1, -1, 1}},
        // 4.2.2: s[i][tags] is the inner Array of struct element i; a
        // missing or null one is UNKNOWN, its leaves follow C7/4.6.1/length.
        {"tags[0] == [1, 2]",
         unary(tags0, OpType::Equal, ElementNullIntArray({1, 2})),
         {1, -1, -1, -1, 0}},
        {"tags[1] == [0, 4]",
         unary(tags1, OpType::Equal, ElementNullIntArray({0, 4})),
         {0, -1, 0, -1, 0}},
        {"tags[1] != [7]",
         unary(tags1, OpType::NotEqual, ElementNullIntArray({7})),
         {1, -1, 0, -1, 1}},
        {"contains(tags[1], 4)",
         contains(tags1, kContains, {ElementNullInt(4)}),
         {1, -1, 0, -1, 0}},
        {"contains(tags[1], 0)",
         contains(tags1, kContains, {ElementNullInt(0)}),
         {0, -1, 0, -1, 0}},
        {"contains_any(tags[0], [2, 5])",
         contains(tags0, kAny, {ElementNullInt(2), ElementNullInt(5)}),
         {1, -1, -1, -1, 1}},
        {"contains_all(tags[1], [6])",
         contains(tags1, kAll, {ElementNullInt(6)}),
         {0, -1, 0, -1, 1}},
        {"contains_all(tags[0], [])",
         contains(tags0, kAll, {}),
         {1, -1, -1, -1, 1}},
        {"contains_any(tags[0], [])",
         contains(tags0, kAny, {}),
         {0, -1, -1, -1, 0}},
        {"length(tags[1]) == 2", length(tags1, 2), {1, -1, 0, -1, 1}},
        {"length(tags[0]) == 1", length(tags0, 1), {0, -1, -1, -1, 1}},
        // An empty set is FALSE on every row, including a null row, a
        // missing element and a null element, so NOT makes it TRUE.
        {"b[0] in []", term(b0, {}), all_false},
        {"b[1] in []", term(b1, {}), all_false},
        {"b[2147483648] in []", term(b_huge, {}), all_false},
        // The same on a top-level Array. A non-empty set still makes a
        // missing element or a null row UNKNOWN.
        {"arr[0] in []", term(arr0, {}), all_false},
        {"arr[1] in []", term(arr1, {}), all_false},
        {"arr[2147483648] in []", term(arr_huge, {}), all_false},
        {"arr[0] in [3]", term(arr0, {ElementNullInt(3)}), {0, -1, 1, -1, 0}},
        {"arr[1] in [2, 6]",
         term(arr1, {ElementNullInt(2), ElementNullInt(6)}),
         {1, -1, -1, -1, 1}},
        // C3 / 4.2.2 with a subscript beyond int: out of bounds, UNKNOWN.
        {"b[2147483648] > 0",
         unary(b_huge, OpType::GreaterThan, ElementNullInt(0)),
         all_unknown},
        {"b[99999999999999999999] == 0",
         unary(b_beyond_int64, OpType::Equal, ElementNullInt(0)),
         all_unknown},
        {"b[-1] > 0",
         unary(b_negative, OpType::GreaterThan, ElementNullInt(0)),
         all_unknown},
        {"b[2147483648] in [0, 6]",
         term(b_huge, {ElementNullInt(0), ElementNullInt(6)}),
         all_unknown},
        {"0 <= b[2147483648] <= 6",
         std::make_shared<expr::BinaryRangeFilterExpr>(
             b_huge, ElementNullInt(0), ElementNullInt(6), true, true),
         all_unknown},
        {"b[2147483648] + 1 == 1",
         std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
             b_huge,
             OpType::Equal,
             proto::plan::ArithOpType::Add,
             ElementNullInt(1),
             ElementNullInt(1)),
         all_unknown},
        {"contains(tags[2147483648], 4)",
         contains(tags_huge, kContains, {ElementNullInt(4)}),
         all_unknown},
        {"contains_all(tags[2147483648], [])",
         contains(tags_huge, kAll, {}),
         all_unknown},
        {"length(tags[2147483648]) == 0", length(tags_huge, 0), all_unknown},
        {"tags[2147483648] == [1, 2]",
         unary(tags_huge, OpType::Equal, ElementNullIntArray({1, 2})),
         all_unknown},
        {"tags[2147483648] != [1, 2]",
         unary(tags_huge, OpType::NotEqual, ElementNullIntArray({1, 2})),
         all_unknown},
    };
    for (const auto& c : cases) {
        CheckElementNullExpr(segment, c.expr, c.expected, c.name);
    }

    // Operators outside this scope stay rejected.
    const std::vector<std::pair<std::string, expr::TypedExprPtr>> rejected = {
        {"1 in b (is_in_field)",
         std::make_shared<expr::TermFilterExpr>(
             b,
             std::vector<proto::plan::GenericValue>{ElementNullInt(1)},
             true)},
        {"contains(tags, 1)",
         contains(column(data.tags, DataType::INT64),
                  kContains,
                  {ElementNullInt(1)})},
        {"tags == [1]",
         unary(column(data.tags, DataType::INT64),
               OpType::Equal,
               ElementNullIntArray({1}))},
        {"tags[0] > 1", unary(tags0, OpType::GreaterThan, ElementNullInt(1))},
        {"tags[0] in [1]",
         std::make_shared<expr::TermFilterExpr>(
             tags0, std::vector<proto::plan::GenericValue>{ElementNullInt(1)})},
    };
    for (const auto& [label, typed_expr] : rejected) {
        auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                           typed_expr);
        EXPECT_THROW(
            {
                auto vec = milvus::test::gen_filter_res(
                    plan.get(), segment, kElementNullRows, MAX_TIMESTAMP);
                (void)vec;
            },
            SegcoreError)
            << label;
    }
}

// Same rows through the Go plan parser. The parser resolves `s[i][x]` only
// for sub-fields of a registered struct, so group the `s[...]` fields into
// struct `s` before registering the schema.
void
RunElementNullableParsedRowFilters(SegmentInternalInterface* segment,
                                   const ElementNullableStruct& data) {
    auto flat = data.schema->ToProto();
    proto::schema::CollectionSchema parser_schema;
    parser_schema.set_name("element_nullable_row_filter");
    auto* s = parser_schema.add_struct_array_fields();
    s->set_fieldid(data.tags.get() + 1);
    s->set_name("s");
    s->set_nullable(true);
    for (const auto& field : flat.fields()) {
        if (field.name().rfind("s[", 0) == 0) {
            *s->add_fields() = field;
        } else {
            *parser_schema.add_fields() = field;
        }
    }
    std::string schema_bytes;
    parser_schema.SerializeToString(&schema_bytes);
    auto handle = planparserv2::PlanParser::RegisterSchema(
        std::vector<uint8_t>(schema_bytes.begin(), schema_bytes.end()));
    auto unregister = folly::makeGuard(
        [handle] { planparserv2::PlanParser::UnregisterSchema(handle); });

    auto parse = [&](const std::string& text) -> expr::TypedExprPtr {
        const auto bytes = planparserv2::PlanParser::Parse(handle, text);
        auto plan =
            CreateRetrievePlanByExpr(data.schema, bytes.data(), bytes.size());
        AssertInfo(plan != nullptr, "failed to parse {}", text);
        const auto& sources = plan->plan_node_->plannodes_->sources();
        AssertInfo(sources.size() == 1, "unexpected plan shape for {}", text);
        auto* filter =
            dynamic_cast<plan::FilterBitsNode*>(sources.front().get());
        AssertInfo(filter != nullptr, "no filter node for {}", text);
        return filter->filter();
    };

    const std::vector<std::pair<std::string, ElementNullExpected>> cases = {
        // C3
        {"s[0][b] > 0", {1, -1, -1, -1, 1}},
        {"s[1][b] == 0", {-1, -1, -1, -1, 0}},
        {"s[1][b] in [0, 6]", {-1, -1, -1, -1, 1}},
        {"s[0][name] like \"a%\"", {1, -1, -1, -1, 1}},
        // C7
        {"array_contains(s[b], 3)", {1, 0, 0, -1, 0}},
        {"array_contains(s[b], 0)", {0, 0, 0, -1, 0}},
        {"array_contains_any(s[b], [0, 6])", {0, 0, 0, -1, 1}},
        {"array_contains_all(s[b], [1, 3])", {1, 0, 0, -1, 0}},
        // 4.6.1
        {"s[b] == [5, 6]", {0, 0, 0, -1, 1}},
        {"s[b] != [5, 6]", {1, 1, 1, -1, 0}},
        {"s[b] == [1, 0, 3]", {0, 0, 0, -1, 0}},
        // array_length
        {"array_length(s[b]) == 1", {0, 0, 1, -1, 0}},
        {"array_length(s[vec]) == 3", {1, 0, 0, -1, 0}},
        // 4.2.2
        {"s[0][tags] == [1, 2]", {1, -1, -1, -1, 0}},
        {"s[1][tags] != [7]", {1, -1, 0, -1, 1}},
        {"array_contains_any(s[0][tags], [2, 5])", {1, -1, -1, -1, 1}},
        {"array_contains(s[1][tags], 0)", {0, -1, 0, -1, 0}},
        {"array_length(s[1][tags]) == 2", {1, -1, 0, -1, 1}},
        // An empty set is FALSE on every row
        {"s[0][b] in []", {0, 0, 0, 0, 0}},
        {"s[1][b] in []", {0, 0, 0, 0, 0}},
        {"s[2147483648][b] in []", {0, 0, 0, 0, 0}},
        {"arr[1] in []", {0, 0, 0, 0, 0}},
        {"arr[2147483648] in []", {0, 0, 0, 0, 0}},
        {"arr[1] in [2, 6]", {1, -1, -1, -1, 1}},
        // Subscripts beyond int / int64: out of bounds, UNKNOWN
        {"s[2147483648][b] > 0", {-1, -1, -1, -1, -1}},
        {"s[99999999999999999999][b] == 0", {-1, -1, -1, -1, -1}},
        {"array_contains(s[2147483648][tags], 4)", {-1, -1, -1, -1, -1}},
        {"array_length(s[2147483648][tags]) == 0", {-1, -1, -1, -1, -1}},
        {"s[2147483648][tags] == [1, 2]", {-1, -1, -1, -1, -1}},
    };
    for (const auto& [text, expected] : cases) {
        CheckElementNullExpr(segment, parse(text), expected, "parsed " + text);
        // NOT from the parser: TRUE/FALSE flip, UNKNOWN stays.
        ElementNullExpected negated;
        for (auto value : expected) {
            negated.push_back(value < 0 ? -1 : 1 - value);
        }
        CheckElementNullExpr(segment,
                             parse("not (" + text + ")"),
                             negated,
                             "parsed not (" + text + ")");
    }
}

}  // namespace

TEST(Expr, ElementNullableRowFilterGrowing) {
    auto data = BuildElementNullableStruct();
    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    config.set_chunk_rows(2);
    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();
    const auto saved_growing_mmap = mmap_config.GetEnableGrowingMmap();
    auto mmap_guard = folly::makeGuard([&mmap_config, saved_growing_mmap] {
        mmap_config.growing_enable_mmap = saved_growing_mmap;
    });
    for (bool enable_mmap : {false, true}) {
        SCOPED_TRACE(enable_mmap ? "growing mmap" : "growing");
        mmap_config.growing_enable_mmap = enable_mmap;
        auto growing = LoadElementNullableGrowing(data);
        ASSERT_GT(growing->num_chunk(data.b), 1);
        RunElementNullableRowFilters(growing.get(), data);
        RunElementNullableParsedRowFilters(growing.get(), data);
    }
}

TEST(Expr, ElementNullableRowFilterSealed) {
    auto data = BuildElementNullableStruct();
    const auto segment_path =
        (std::filesystem::temp_directory_path() /
         ("milvus_element_null_row_filter_" +
          std::to_string(
              std::chrono::steady_clock::now().time_since_epoch().count())))
            .string();
    std::filesystem::remove_all(segment_path);
    auto directory_guard = folly::makeGuard(
        [&segment_path] { std::filesystem::remove_all(segment_path); });
    auto sealed = LoadElementNullableSealed(data, segment_path);
    RunElementNullableRowFilters(sealed.get(), data);
    RunElementNullableParsedRowFilters(sealed.get(), data);
}

// Element-level filters (MATCH_* / element_filter) on element-nullable struct
// sub-fields. Struct `s`:
//   a     Array<Int64>, elements never null
//   b     Array<Int64>, element nullable
//   i8    Array<Int8>, element nullable
//   name  Array<VarChar>, element nullable
//   tags  Array<Array<Int64>>, inner Arrays and leaves nullable
//   vec   ArrayOfVector, element nullable
//   pv    ArrayOfVector, elements never null
// and, only in the reopened segments, three sub-fields added to the struct
// after the rows were written, so every existing element of them is null:
//   lang  Array<VarChar>, lnum Array<Int64>, ltags Array<Array<Int64>>.
// Null slots carry a zero / "" payload, so a predicate that ignores element
// validity would match them.
namespace {

constexpr int64_t kElemVecDim = 4;

// Three-valued result: 1 TRUE, 0 FALSE, -1 UNKNOWN.
using Tv = int;
constexpr Tv kTvTrue = 1;
constexpr Tv kTvFalse = 0;
constexpr Tv kTvUnknown = -1;

Tv
TvNot(Tv v) {
    return v == kTvUnknown ? kTvUnknown : 1 - v;
}

Tv
TvAnd(Tv x, Tv y) {
    if (x == kTvFalse || y == kTvFalse) {
        return kTvFalse;
    }
    return x == kTvUnknown || y == kTvUnknown ? kTvUnknown : kTvTrue;
}

Tv
TvOr(Tv x, Tv y) {
    if (x == kTvTrue || y == kTvTrue) {
        return kTvTrue;
    }
    return x == kTvUnknown || y == kTvUnknown ? kTvUnknown : kTvFalse;
}

// A predicate on a nullable value is UNKNOWN when the value is null.
template <typename T, typename Pred>
Tv
TvOf(const std::optional<T>& value, Pred pred) {
    return value.has_value() ? Tv(pred(*value)) : kTvUnknown;
}

Tv
TvBool(bool value) {
    return value ? kTvTrue : kTvFalse;
}

using ElemLeaves = std::vector<std::optional<int64_t>>;

struct ElemValue {
    int64_t a = 0;
    std::optional<int64_t> b;
    std::optional<int64_t> i8;
    std::optional<std::string> name;
    std::optional<ElemLeaves> tags;
    bool vec = false;
};

// One struct row; nullopt is a NULL struct row.
using ElemRow = std::optional<std::vector<ElemValue>>;

struct ElementLevelFixture {
    // Schema version 1, without the added sub-fields.
    SchemaPtr schema;
    FieldId pk, a, b, i8, name, tags, vec, pv;
    // Added in schema version 2.
    FieldId lang, lnum, ltags;
    std::vector<ElemRow> rows;
    std::unique_ptr<InsertRecordProto> insert;
    // Logical element ids: values[id], and the row / in-row index of id.
    std::vector<ElemValue> values;
    std::vector<int64_t> elem_row;
    std::vector<int64_t> elem_index;
    // First element id of each row, plus the total at the end.
    std::vector<int64_t> row_begin;

    int64_t
    num_rows() const {
        return static_cast<int64_t>(rows.size());
    }

    int64_t
    num_elements() const {
        return static_cast<int64_t>(values.size());
    }
};

proto::schema::TypeSchema
NestedNullableInt64Type() {
    proto::schema::TypeSchema type;
    type.set_nullable(true);
    auto* inner = type.mutable_array_element();
    inner->set_nullable(true);
    inner->mutable_array_element()->set_leaf_type(
        proto::schema::DataType::Int64);
    inner->mutable_array_element()->set_nullable(true);
    return type;
}

ElementLevelFixture
BuildElementLevelFixture(std::vector<ElemRow> rows) {
    ElementLevelFixture f;
    auto schema = std::make_shared<Schema>();
    f.pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(f.pk);
    f.a = schema->AddDebugArrayField("s[a]", DataType::INT64, true, false);
    f.b = schema->AddDebugArrayField("s[b]", DataType::INT64, true, true);
    f.i8 = schema->AddDebugArrayField("s[i8]", DataType::INT8, true, true);
    f.name =
        schema->AddDebugArrayField("s[name]", DataType::VARCHAR, true, true);
    f.vec = schema->AddDebugVectorArrayField("s[vec]",
                                             DataType::VECTOR_FLOAT,
                                             kElemVecDim,
                                             knowhere::metric::L2,
                                             true,
                                             true);
    f.pv = schema->AddDebugVectorArrayField("s[pv]",
                                            DataType::VECTOR_FLOAT,
                                            kElemVecDim,
                                            knowhere::metric::L2,
                                            true,
                                            false);
    f.tags = FieldId(f.pv.get() + 100);
    schema->AddField(FieldMeta(FieldName("s[tags]"),
                               f.tags,
                               DataType::ARRAY,
                               DataType::ARRAY,
                               true,
                               true,
                               std::nullopt,
                               std::string{},
                               LOCAL_FORMAT_RAW,
                               std::make_optional(NestedNullableInt64Type())));
    f.lang = FieldId(f.pv.get() + 101);
    f.lnum = FieldId(f.pv.get() + 102);
    f.ltags = FieldId(f.pv.get() + 103);
    schema->set_schema_version(1);
    f.schema = schema;
    f.rows = std::move(rows);

    for (int64_t r = 0; r < f.num_rows(); ++r) {
        f.row_begin.push_back(f.num_elements());
        if (!f.rows[r].has_value()) {
            continue;
        }
        for (size_t j = 0; j < f.rows[r]->size(); ++j) {
            f.values.push_back((*f.rows[r])[j]);
            f.elem_row.push_back(r);
            f.elem_index.push_back(static_cast<int64_t>(j));
        }
    }
    f.row_begin.push_back(f.num_elements());

    auto insert = std::make_unique<InsertRecordProto>();
    insert->set_num_rows(f.num_rows());
    auto* pk = insert->add_fields_data();
    pk->set_field_id(f.pk.get());
    pk->set_type(proto::schema::DataType::Int64);
    for (int64_t r = 0; r < f.num_rows(); ++r) {
        pk->mutable_scalars()->mutable_long_data()->add_data(r);
    }

    auto add_array_field = [&](FieldId id, proto::schema::DataType element) {
        auto* field = insert->add_fields_data();
        field->set_field_id(id.get());
        field->set_type(proto::schema::DataType::Array);
        for (const auto& row : f.rows) {
            field->mutable_scalars()->add_valid_data(row.has_value());
        }
        auto* data = field->mutable_scalars()->mutable_array_data();
        data->set_element_type(element);
        return data;
    };
    auto* a_rows = add_array_field(f.a, proto::schema::DataType::Int64);
    auto* b_rows = add_array_field(f.b, proto::schema::DataType::Int64);
    auto* i8_rows = add_array_field(f.i8, proto::schema::DataType::Int8);
    auto* name_rows = add_array_field(f.name, proto::schema::DataType::VarChar);
    auto* tags_rows = add_array_field(f.tags, proto::schema::DataType::Array);
    for (const auto& row : f.rows) {
        auto* a_row = a_rows->add_data();
        a_row->mutable_long_data();
        auto* b_row = b_rows->add_data();
        b_row->mutable_long_data();
        auto* i8_row = i8_rows->add_data();
        i8_row->mutable_int_data();
        auto* name_row = name_rows->add_data();
        name_row->mutable_string_data();
        auto* tags_row = tags_rows->add_data();
        tags_row->mutable_array_data()->set_element_type(
            proto::schema::DataType::Int64);
        if (!row.has_value()) {
            continue;
        }
        for (const auto& e : *row) {
            a_row->mutable_long_data()->add_data(e.a);
            b_row->mutable_long_data()->add_data(e.b.value_or(0));
            b_row->add_valid_data(e.b.has_value());
            i8_row->mutable_int_data()->add_data(
                static_cast<int32_t>(e.i8.value_or(0)));
            i8_row->add_valid_data(e.i8.has_value());
            name_row->mutable_string_data()->add_data(e.name.value_or(""));
            name_row->add_valid_data(e.name.has_value());
            auto* child = tags_row->mutable_array_data()->add_data();
            child->mutable_long_data();
            if (e.tags.has_value()) {
                for (const auto& leaf : *e.tags) {
                    child->mutable_long_data()->add_data(leaf.value_or(0));
                    child->add_valid_data(leaf.has_value());
                }
            }
            tags_row->add_valid_data(e.tags.has_value());
        }
    }

    auto add_vector_field = [&](FieldId id, bool element_nullable) {
        auto* vec = insert->add_fields_data();
        vec->set_field_id(id.get());
        vec->set_type(proto::schema::DataType::ArrayOfVector);
        vec->mutable_vectors()->set_dim(kElemVecDim);
        auto* vec_rows = vec->mutable_vectors()->mutable_vector_array();
        vec_rows->set_dim(kElemVecDim);
        vec_rows->set_element_type(proto::schema::DataType::FloatVector);
        for (const auto& row : f.rows) {
            auto* proto_row = vec_rows->add_data();
            proto_row->set_dim(kElemVecDim);
            proto_row->mutable_float_vector();
            if (!row.has_value()) {
                continue;
            }
            for (const auto& e : *row) {
                const bool valid = !element_nullable || e.vec;
                if (element_nullable) {
                    proto_row->add_valid_data(valid);
                }
                if (valid) {
                    for (int64_t d = 0; d < kElemVecDim; ++d) {
                        proto_row->mutable_float_vector()->add_data(1.0f);
                    }
                }
            }
        }
        for (const auto& row : f.rows) {
            vec->mutable_vectors()->add_valid_data(row.has_value());
        }
    };
    add_vector_field(f.vec, true);
    add_vector_field(f.pv, false);
    f.insert = std::move(insert);
    return f;
}

// The main data set. Rows 1 (empty) and 2 (NULL) share a two-row batch
// that has no element.
ElementLevelFixture
BuildElementLevelMatrixFixture() {
    using L = ElemLeaves;
    const auto null = std::nullopt;
    std::vector<ElemRow> rows = {
        std::vector<ElemValue>{
            {1, null, 1, "ab", L{1, 2}, true},
            {2, 3, null, null, null, false},
            {1, 1, 5, "ac", L{null, 4}, true},
        },
        std::vector<ElemValue>{},
        null,
        std::vector<ElemValue>{
            {1, 3, null, "b", L{}, false},
            {3, null, 2, null, L{null}, true},
        },
        std::vector<ElemValue>{
            {1, null, null, null, null, false},
            {2, null, null, null, null, false},
        },
        std::vector<ElemValue>{
            {5, 7, 7, "abc", L{7, 7}, true},
        },
        std::vector<ElemValue>{
            {1, 1, 1, "aa", L{1}, true},
            {1, null, 3, "ba", L{2, null}, false},
            {2, 2, null, "ab", L{}, true},
        },
    };
    return BuildElementLevelFixture(std::move(rows));
}

// The C18 truth table rows, on real element-nullable data:
//   b = [3, NULL], [NULL, NULL], [], [NULL, NULL, 5], [3, 5, NULL], [3, 5],
//   s = [{a: 1, b: NULL}, {a: 2, b: 3}].
ElementLevelFixture
BuildElementLevelC18Fixture() {
    const auto null = std::nullopt;
    auto b_only = [](std::vector<std::optional<int64_t>> values) {
        std::vector<ElemValue> row;
        for (auto value : values) {
            ElemValue e;
            e.b = value;
            row.push_back(e);
        }
        return ElemRow(std::move(row));
    };
    ElemValue first;
    first.a = 1;
    ElemValue second;
    second.a = 2;
    second.b = 3;
    std::vector<ElemRow> rows = {
        b_only({3, null}),
        b_only({null, null}),
        b_only({}),
        b_only({null, null, 5}),
        b_only({3, 5, null}),
        b_only({3, 5}),
        ElemRow(std::vector<ElemValue>{first, second}),
    };
    return BuildElementLevelFixture(std::move(rows));
}

// Schema version 2: `base` plus the sub-fields added to `s` after the rows
// were written.
SchemaPtr
WithAddedSubFields(const Schema& base, const ElementLevelFixture& f) {
    auto next = Schema::ParseFrom(base.ToProto());
    next->AddField(FieldName("s[lang]"),
                   f.lang,
                   DataType::ARRAY,
                   DataType::VARCHAR,
                   true,
                   true);
    next->AddField(FieldName("s[lnum]"),
                   f.lnum,
                   DataType::ARRAY,
                   DataType::INT64,
                   true,
                   true);
    next->AddField(FieldMeta(FieldName("s[ltags]"),
                             f.ltags,
                             DataType::ARRAY,
                             DataType::ARRAY,
                             true,
                             true,
                             std::nullopt,
                             std::string{},
                             LOCAL_FORMAT_RAW,
                             std::make_optional(NestedNullableInt64Type())));
    next->set_schema_version(base.get_schema_version() + 1);
    return next;
}

std::unique_ptr<SegmentGrowing>
LoadElementLevelGrowing(const ElementLevelFixture& f) {
    auto growing = CreateGrowingSegment(
        f.schema, empty_index_meta, 1, SegcoreConfig::default_config());
    std::vector<int64_t> row_ids(f.num_rows());
    std::vector<Timestamp> timestamps(f.num_rows());
    std::iota(row_ids.begin(), row_ids.end(), 0);
    std::iota(timestamps.begin(), timestamps.end(), 0);
    const auto offset = growing->PreInsert(f.num_rows());
    growing->Insert(offset,
                    f.num_rows(),
                    row_ids.data(),
                    timestamps.data(),
                    std::make_shared<InsertRecordProto>(*f.insert));
    return growing;
}

// Native-list fields load only from Storage V2/V3: write the rows as a V3
// segment in batches of batch_rows, one chunk per batch.
std::unique_ptr<SegmentSealed>
LoadElementLevelSealed(const ElementLevelFixture& f,
                       const std::string& segment_path,
                       int64_t batch_rows = 2) {
    proto::schema::CollectionSchema storage_proto;
    for (auto [id, name] : {std::pair{RowFieldID, "RowID"},
                            std::pair{TimestampFieldID, "Timestamp"}}) {
        auto* field = storage_proto.add_fields();
        field->set_fieldid(id.get());
        field->set_name(name);
        field->set_data_type(proto::schema::DataType::Int64);
    }
    const auto user_proto = f.schema->ToProto();
    for (const auto& field : user_proto.fields()) {
        *storage_proto.add_fields() = field;
    }
    auto schema = Schema::ParseFrom(storage_proto);
    schema->set_schema_version(f.schema->get_schema_version());

    const auto rows = f.num_rows();
    auto build_int64 = [rows]() {
        arrow::Int64Builder builder;
        for (int64_t i = 0; i < rows; ++i) {
            AssertInfo(builder.Append(i).ok(), "append int64");
        }
        std::shared_ptr<arrow::Array> output;
        AssertInfo(builder.Finish(&output).ok(), "finish int64");
        return output;
    };
    std::unordered_map<FieldId, std::shared_ptr<arrow::Array>> columns = {
        {RowFieldID, build_int64()}, {TimestampFieldID, build_int64()}};
    for (const auto& field_data : f.insert->fields_data()) {
        const FieldId id(field_data.field_id());
        auto converted = FieldDataToArrow(std::to_string(id.get()),
                                          field_data,
                                          rows,
                                          false,
                                          nullptr,
                                          &(*schema)[id]);
        AssertInfo(converted.ok(),
                   "convert field {}: {}",
                   id.get(),
                   converted.status().ToString());
        columns[id] = converted->second;
    }
    auto loon_schema = schema->ConvertToLoonArrowSchema();
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    for (int64_t offset = 0; offset < rows; offset += batch_rows) {
        const auto length = std::min<int64_t>(batch_rows, rows - offset);
        arrow::ArrayVector sliced;
        for (const auto& id : schema->get_field_ids()) {
            sliced.push_back(columns.at(id)->Slice(offset, length));
        }
        batches.push_back(
            arrow::RecordBatch::Make(loon_schema, length, sliced));
    }
    // One commit per batch, so each batch lands in its own file and chunk.
    int64_t version = 0;
    for (const auto& batch : batches) {
        version = test::WriteRecordBatchesToV3(
            schema,
            segment_path,
            {batch},
            test::GenerateColumnGroupPattern(schema));
    }

    proto::segcore::SegmentLoadInfo load_info;
    load_info.set_collectionid(1);
    load_info.set_partitionid(2);
    load_info.set_segmentid(3);
    load_info.set_storageversion(STORAGE_V3);
    load_info.set_num_of_rows(rows);
    load_info.set_manifest_path("{\"base_path\":\"" + segment_path +
                                "\",\"ver\":" + std::to_string(version) + "}");
    load_info.set_insert_channel("element-level-null-test");
    auto sealed = CreateSealedSegment(schema, empty_index_meta, 3);
    sealed->SetLoadInfo(load_info);
    // One cell per written batch; the default target merges them.
    const auto cell_target = storagev2translator::GetCellTargetSizeBytes();
    storagev2translator::SetCellTargetSizeBytes(1);
    auto restore_cell_target = folly::makeGuard([cell_target] {
        storagev2translator::SetCellTargetSizeBytes(cell_target);
    });
    tracer::TraceContext trace_ctx;
    sealed->Load(trace_ctx, nullptr);
    return sealed;
}

std::string
ElementLevelSegmentPath(const std::string& tag) {
    return (std::filesystem::temp_directory_path() /
            ("milvus_element_level_null_" + tag + "_" +
             std::to_string(
                 std::chrono::steady_clock::now().time_since_epoch().count())))
        .string();
}

// Evaluate an element-level expression over all rows in two-row batches,
// crossing chunk boundaries and the zero-element batch.
ColumnVectorPtr
EvalElementBatches(SegmentInternalInterface* segment,
                   const expr::TypedExprPtr& typed_expr,
                   int64_t num_rows,
                   int64_t num_elements) {
    const auto saved_batch_size = EXEC_EVAL_EXPR_BATCH_SIZE.load();
    EXEC_EVAL_EXPR_BATCH_SIZE.store(2);
    auto restore = folly::makeGuard([saved_batch_size] {
        EXEC_EVAL_EXPR_BATCH_SIZE.store(saved_batch_size);
    });
    auto query_context = std::make_shared<exec::QueryContext>(
        DEAFULT_QUERY_ID, segment, num_rows, MAX_TIMESTAMP);
    exec::ExecContext exec_context(query_context.get());
    exec::ExprSet expr_set({typed_expr}, &exec_context);
    exec::EvalCtx eval_ctx(&exec_context);
    TargetBitmap res;
    TargetBitmap valid;
    for (int64_t batch = 0; batch < num_rows; batch += 2) {
        std::vector<VectorPtr> results;
        expr_set.Eval(0, 1, true, eval_ctx, results);
        AssertInfo(results.size() == 1 && results[0] != nullptr,
                   "element batch {} returned no result",
                   batch);
        auto vec = test::GetColumnVectorForTest(results[0]);
        res.append(TargetBitmapView(vec->GetRawData(), vec->size()));
        valid.append(TargetBitmapView(vec->GetValidRawData(), vec->size()));
    }
    AssertInfo(static_cast<int64_t>(res.size()) == num_elements,
               "two-row batches covered {} elements, expected {}",
               res.size(),
               num_elements);
    return std::make_shared<ColumnVector>(std::move(res), std::move(valid));
}

void
ExpectTv(Tv want, bool bit, bool valid, const std::string& where) {
    if (want == kTvUnknown) {
        EXPECT_FALSE(valid) << where << ": want UNKNOWN";
        EXPECT_FALSE(bit) << where << ": want UNKNOWN";
    } else {
        EXPECT_TRUE(valid) << where << ": want " << want;
        EXPECT_EQ(bit, want == kTvTrue) << where;
    }
}

// Check an element-level predicate element by element: one batch, two-row
// batches, and element-id input in ascending, scrambled and partial order,
// each also under NOT.
void
CheckElementLevelExpr(SegmentInternalInterface* segment,
                      const ElementLevelFixture& f,
                      const expr::TypedExprPtr& typed_expr,
                      const std::vector<Tv>& expected,
                      const std::string& label) {
    const auto n = f.num_elements();
    ASSERT_EQ(static_cast<int64_t>(expected.size()), n) << label;
    std::vector<int32_t> ascending(n);
    std::iota(ascending.begin(), ascending.end(), 0);
    std::vector<int32_t> scrambled;
    for (int64_t lo = 0, hi = n - 1; lo <= hi; ++lo, --hi) {
        scrambled.push_back(static_cast<int32_t>(hi));
        if (lo != hi) {
            scrambled.push_back(static_cast<int32_t>(lo));
        }
    }
    std::vector<int32_t> partial;
    for (int64_t id = n - 1; id >= 0; id -= 3) {
        partial.push_back(static_cast<int32_t>(id));
    }

    auto check = [&](const ColumnVectorPtr& vec,
                     const std::vector<int32_t>& ids,
                     bool negated,
                     const std::string& path) {
        ASSERT_EQ(vec->size(), ids.size()) << label << " " << path;
        BitsetTypeView bits(vec->GetRawData(), vec->size());
        BitsetTypeView valid(vec->GetValidRawData(), vec->size());
        for (size_t i = 0; i < ids.size(); ++i) {
            const auto id = ids[i];
            const auto want = negated ? TvNot(expected[id]) : expected[id];
            ExpectTv(want,
                     bits[i],
                     valid[i],
                     label + " " + path + ", row " +
                         std::to_string(f.elem_row[id]) + " element " +
                         std::to_string(f.elem_index[id]));
        }
    };

    for (bool negated : {false, true}) {
        expr::TypedExprPtr target = typed_expr;
        if (negated) {
            target = std::make_shared<expr::LogicalUnaryExpr>(
                expr::LogicalUnaryExpr::OpType::LogicalNot, typed_expr);
        }
        const std::string tag = negated ? "NOT " : "";
        plan::FilterBitsNode node(DEFAULT_PLANNODE_ID, target);
        if (n > 0) {
            check(test::gen_filter_res(
                      &node, segment, f.num_rows(), MAX_TIMESTAMP),
                  ascending,
                  negated,
                  tag + "batch");
        }
        check(EvalElementBatches(segment, target, f.num_rows(), n),
              ascending,
              negated,
              tag + "two-row batches");
        for (const auto& [ids, path] : {std::pair{ascending, "ascending ids"},
                                        std::pair{scrambled, "scrambled ids"},
                                        std::pair{partial, "partial ids"}}) {
            if (ids.empty()) {
                continue;
            }
            exec::OffsetVector offsets(ids.begin(), ids.end());
            check(test::gen_filter_res(
                      &node, segment, f.num_rows(), MAX_TIMESTAMP, &offsets),
                  ids,
                  negated,
                  tag + path);
        }
    }
}

// Check a row-level predicate on the batch path, on row-offset input and
// through the query pipeline in two-row batches, each also under NOT.
void
CheckRowLevelExpr(SegmentInternalInterface* segment,
                  int64_t num_rows,
                  const expr::TypedExprPtr& typed_expr,
                  const std::vector<Tv>& expected,
                  const std::string& label) {
    ASSERT_EQ(static_cast<int64_t>(expected.size()), num_rows) << label;
    std::vector<int64_t> all_rows(num_rows);
    std::iota(all_rows.begin(), all_rows.end(), 0);
    std::vector<int64_t> reversed(all_rows.rbegin(), all_rows.rend());
    auto check = [&](const ColumnVectorPtr& vec,
                     const std::vector<int64_t>& rows,
                     bool negated,
                     const std::string& path) {
        ASSERT_EQ(vec->size(), rows.size()) << label << " " << path;
        BitsetTypeView bits(vec->GetRawData(), vec->size());
        BitsetTypeView valid(vec->GetValidRawData(), vec->size());
        for (size_t i = 0; i < rows.size(); ++i) {
            const auto want =
                negated ? TvNot(expected[rows[i]]) : expected[rows[i]];
            ExpectTv(want,
                     bits[i],
                     valid[i],
                     label + " " + path + ", row " + std::to_string(rows[i]));
        }
    };
    auto not_expr = std::make_shared<expr::LogicalUnaryExpr>(
        expr::LogicalUnaryExpr::OpType::LogicalNot, typed_expr);
    for (bool negated : {false, true}) {
        auto plan = std::make_shared<plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID,
            negated ? expr::TypedExprPtr(not_expr) : typed_expr);
        const std::string tag = negated ? "NOT " : "";
        check(
            test::gen_filter_res(plan.get(), segment, num_rows, MAX_TIMESTAMP),
            all_rows,
            negated,
            tag + "batch");
        exec::OffsetVector offsets(reversed.begin(), reversed.end());
        check(test::gen_filter_res(
                  plan.get(), segment, num_rows, MAX_TIMESTAMP, &offsets),
              reversed,
              negated,
              tag + "offsets");

        const auto saved_batch_size = EXEC_EVAL_EXPR_BATCH_SIZE.load();
        EXEC_EVAL_EXPR_BATCH_SIZE.store(2);
        auto final = ExecuteQueryExpr(plan, segment, num_rows, MAX_TIMESTAMP);
        EXEC_EVAL_EXPR_BATCH_SIZE.store(saved_batch_size);
        ASSERT_EQ(static_cast<int64_t>(final.size()), num_rows) << label;
        for (int64_t r = 0; r < num_rows; ++r) {
            const auto want = negated ? TvNot(expected[r]) : expected[r];
            EXPECT_EQ(final[r], want == kTvTrue)
                << label << " " << tag << "filter, row " << r;
        }
    }
}

// Row results of MATCH_<type>(s, P, threshold) from P's element results:
// an UNKNOWN element is a miss (C18), a NULL struct row is UNKNOWN.
std::vector<Tv>
MatchRowExpected(const ElementLevelFixture& f,
                 const std::vector<Tv>& elements,
                 expr::MatchType type,
                 int64_t threshold) {
    std::vector<Tv> rows;
    for (int64_t r = 0; r < f.num_rows(); ++r) {
        if (!f.rows[r].has_value()) {
            rows.push_back(kTvUnknown);
            continue;
        }
        const auto begin = f.row_begin[r];
        const auto count = f.row_begin[r + 1] - begin;
        int64_t hits = 0;
        for (int64_t id = begin; id < begin + count; ++id) {
            hits += elements[id] == kTvTrue;
        }
        bool match = false;
        switch (type) {
            case expr::MatchType::MatchAny:
                match = hits > 0;
                break;
            case expr::MatchType::MatchAll:
                match = hits == count;
                break;
            case expr::MatchType::MatchLeast:
                match = hits >= threshold;
                break;
            case expr::MatchType::MatchMost:
                match = hits <= threshold;
                break;
            case expr::MatchType::MatchExact:
                match = hits == threshold;
                break;
            default:
                ThrowInfo(UnexpectedError,
                          "unexpected match type {}",
                          static_cast<int>(type));
        }
        rows.push_back(TvBool(match));
    }
    return rows;
}

// (row, in-row element index) of the elements an element_filter retrieve
// selects, optionally behind a row-level predicate.
std::set<std::pair<int64_t, int64_t>>
RetrieveElementHits(SegmentInternalInterface* segment,
                    const ElementLevelFixture& f,
                    const expr::TypedExprPtr& element_expr,
                    const expr::TypedExprPtr& doc_expr) {
    auto plan =
        std::make_unique<query::RetrievePlan>(segment->get_schema_snapshot());
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    std::vector<plan::PlanNodePtr> sources;
    if (doc_expr != nullptr) {
        sources = {
            std::make_shared<plan::FilterBitsNode>("1001", doc_expr, sources)};
    }
    auto mvcc = std::make_shared<plan::MvccNode>("1002", sources);
    plan->plan_node_->plannodes_ =
        std::make_shared<plan::ElementFilterBitsNode>(
            "1003", element_expr, "s", std::vector<plan::PlanNodePtr>{mvcc});
    plan->plan_node_->limit_ = 1 << 20;
    plan->field_ids_ = {f.pk};
    auto result = segment->Retrieve(
        nullptr, plan.get(), MAX_TIMESTAMP, DEFAULT_MAX_OUTPUT_SIZE, false);
    std::set<std::pair<int64_t, int64_t>> hits;
    if (result == nullptr) {
        ADD_FAILURE() << "element_filter retrieve returned no result";
        return hits;
    }
    EXPECT_EQ(result->element_indices_size(), result->offset_size());
    for (int i = 0; i < result->offset_size(); ++i) {
        for (auto index : result->element_indices(i).indices()) {
            hits.emplace(result->offset(i), index);
        }
    }
    return hits;
}

struct ElementLevelCase {
    std::string name;
    expr::TypedExprPtr expr;
    // Expected result on one element. The sub-fields added in schema
    // version 2 are null on every element.
    std::function<Tv(const ElemValue&)> oracle;
    bool uses_added_fields = false;
};

// Check one element-level predicate P everywhere it is consumed:
// - element by element (CheckElementLevelExpr), also as NOT P;
// - MATCH_ANY / ALL / LEAST(2) / MOST(1) / EXACT(1) over P and over NOT P,
//   each also under an outer NOT;
// - element_filter over P and NOT P, with and without a row predicate.
void
CheckElementLevelEverywhere(SegmentInternalInterface* segment,
                            const ElementLevelFixture& f,
                            const std::string& name,
                            const expr::TypedExprPtr& typed_expr,
                            const std::vector<Tv>& expected) {
    CheckElementLevelExpr(segment, f, typed_expr, expected, name);

    using MT = expr::MatchType;
    const std::vector<std::pair<MT, int64_t>> match_types = {
        {MT::MatchAny, 0},
        {MT::MatchAll, 0},
        {MT::MatchLeast, 2},
        {MT::MatchMost, 1},
        {MT::MatchExact, 1}};
    auto pk_column = expr::ColumnInfo(f.pk, DataType::INT64);
    proto::plan::GenericValue two;
    two.set_int64_val(2);
    auto doc_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        pk_column, proto::plan::OpType::GreaterThan, two);
    for (bool inner_not : {false, true}) {
        expr::TypedExprPtr predicate = typed_expr;
        std::vector<Tv> elements = expected;
        if (inner_not) {
            predicate = std::make_shared<expr::LogicalUnaryExpr>(
                expr::LogicalUnaryExpr::OpType::LogicalNot, typed_expr);
            for (auto& value : elements) {
                value = TvNot(value);
            }
        }
        const std::string pred_name = inner_not ? "!(" + name + ")" : name;
        for (const auto& [type, threshold] : match_types) {
            auto match = std::make_shared<expr::MatchExpr>(
                "s", type, threshold, predicate);
            CheckRowLevelExpr(segment,
                              f.num_rows(),
                              match,
                              MatchRowExpected(f, elements, type, threshold),
                              "MATCH_" + proto::plan::MatchType_Name(type) +
                                  "(" + std::to_string(threshold) + ", " +
                                  pred_name + ")");
        }
        for (bool with_doc : {false, true}) {
            std::set<std::pair<int64_t, int64_t>> want;
            for (int64_t id = 0; id < f.num_elements(); ++id) {
                if (elements[id] == kTvTrue &&
                    (!with_doc || f.elem_row[id] > 2)) {
                    want.emplace(f.elem_row[id], f.elem_index[id]);
                }
            }
            EXPECT_EQ(RetrieveElementHits(
                          segment, f, predicate, with_doc ? doc_expr : nullptr),
                      want)
                << "element_filter(s, " << pred_name << ")"
                << (with_doc ? " behind pk > 2" : "");
        }
    }
}

// Builders for element-level operands and predicates.
struct ElementLevelExprs {
    static expr::ColumnInfo
    Column(FieldId id, DataType type, DataType element) {
        expr::ColumnInfo column(id, type, element, {}, true);
        column.element_level_ = true;
        return column;
    }
    // `$[x]` on a scalar or vector sub-field.
    static expr::ColumnInfo
    Scalar(FieldId id, DataType type) {
        return Column(id, type, type);
    }
    // `$[tags]` in array_contains*: the leaf type.
    static expr::ColumnInfo
    NestedLeaf(FieldId id) {
        return Column(id, DataType::ARRAY, DataType::INT64);
    }
    // `$[tags]` in array_length and IS NULL.
    static expr::ColumnInfo
    Nested(FieldId id) {
        return Column(id, DataType::ARRAY, DataType::ARRAY);
    }

    static proto::plan::GenericValue
    Int(int64_t value) {
        proto::plan::GenericValue val;
        val.set_int64_val(value);
        return val;
    }
    static proto::plan::GenericValue
    Str(const std::string& value) {
        proto::plan::GenericValue val;
        val.set_string_val(value);
        return val;
    }

    static expr::TypedExprPtr
    Unary(const expr::ColumnInfo& col,
          proto::plan::OpType op,
          proto::plan::GenericValue val) {
        return std::make_shared<expr::UnaryRangeFilterExpr>(col, op, val);
    }
    static expr::TypedExprPtr
    Range(const expr::ColumnInfo& col,
          proto::plan::GenericValue lower,
          proto::plan::GenericValue upper,
          bool lower_inclusive,
          bool upper_inclusive) {
        return std::make_shared<expr::BinaryRangeFilterExpr>(
            col, lower, upper, lower_inclusive, upper_inclusive);
    }
    static expr::TypedExprPtr
    Term(const expr::ColumnInfo& col,
         std::vector<proto::plan::GenericValue> vals) {
        return std::make_shared<expr::TermFilterExpr>(col, std::move(vals));
    }
    static expr::TypedExprPtr
    Arith(const expr::ColumnInfo& col,
          proto::plan::ArithOpType arith,
          int64_t right,
          proto::plan::OpType op,
          int64_t value) {
        return std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
            col, op, arith, Int(value), Int(right));
    }
    static expr::TypedExprPtr
    Contains(const expr::ColumnInfo& col,
             proto::plan::JSONContainsExpr_JSONOp op,
             std::vector<proto::plan::GenericValue> vals) {
        return std::make_shared<expr::JsonContainsExpr>(col, op, true, vals);
    }
    static expr::TypedExprPtr
    Length(const expr::ColumnInfo& col, proto::plan::OpType op, int64_t value) {
        return std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
            col, op, proto::plan::ArithOpType::ArrayLength, Int(value), Int(0));
    }
    static expr::TypedExprPtr
    IsNull(const expr::ColumnInfo& col) {
        return std::make_shared<expr::NullExpr>(
            col, proto::plan::NullExpr_NullOp_IsNull);
    }
    static expr::TypedExprPtr
    IsNotNull(const expr::ColumnInfo& col) {
        return std::make_shared<expr::NullExpr>(
            col, proto::plan::NullExpr_NullOp_IsNotNull);
    }
    static expr::TypedExprPtr
    And(const expr::TypedExprPtr& left, const expr::TypedExprPtr& right) {
        return std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::And, left, right);
    }
    static expr::TypedExprPtr
    Or(const expr::TypedExprPtr& left, const expr::TypedExprPtr& right) {
        return std::make_shared<expr::LogicalBinaryExpr>(
            expr::LogicalBinaryExpr::OpType::Or, left, right);
    }
    static expr::TypedExprPtr
    Not(const expr::TypedExprPtr& input) {
        return std::make_shared<expr::LogicalUnaryExpr>(
            expr::LogicalUnaryExpr::OpType::LogicalNot, input);
    }
};

std::vector<ElementLevelCase>
BuildElementLevelCases(const ElementLevelFixture& f) {
    using E = ElementLevelExprs;
    using proto::plan::ArithOpType;
    using proto::plan::OpType;
    constexpr auto kContains = proto::plan::JSONContainsExpr_JSONOp_Contains;
    constexpr auto kAny = proto::plan::JSONContainsExpr_JSONOp_ContainsAny;
    constexpr auto kAll = proto::plan::JSONContainsExpr_JSONOp_ContainsAll;
    const auto a = E::Scalar(f.a, DataType::INT64);
    const auto b = E::Scalar(f.b, DataType::INT64);
    const auto i8 = E::Scalar(f.i8, DataType::INT8);
    const auto name = E::Scalar(f.name, DataType::VARCHAR);
    const auto tags_leaf = E::NestedLeaf(f.tags);
    const auto tags = E::Nested(f.tags);
    const auto vec = E::Scalar(f.vec, DataType::VECTOR_FLOAT);
    const auto pv = E::Scalar(f.pv, DataType::VECTOR_FLOAT);
    const auto lang = E::Scalar(f.lang, DataType::VARCHAR);
    const auto lnum = E::Scalar(f.lnum, DataType::INT64);
    const auto ltags_leaf = E::NestedLeaf(f.ltags);
    const auto ltags = E::Nested(f.ltags);

    auto b_is = [](int64_t v) {
        return [v](const ElemValue& e) {
            return TvOf(e.b, [v](int64_t x) { return x == v; });
        };
    };
    auto a_is = [](int64_t v) {
        return [v](const ElemValue& e) { return TvBool(e.a == v); };
    };
    // Leaves of a non-null inner Array that match, skipping null leaves.
    auto contains_leaf = [](const ElemValue& e, int64_t v) {
        for (const auto& leaf : *e.tags) {
            if (leaf.has_value() && *leaf == v) {
                return true;
            }
        }
        return false;
    };
    auto tags_tv = [](auto pred) {
        return [pred](const ElemValue& e) {
            return e.tags.has_value() ? TvBool(pred(e)) : kTvUnknown;
        };
    };
    auto unknown = [](const ElemValue&) { return kTvUnknown; };

    std::vector<ElementLevelCase> cases = {
        // Compare, including the payload a null slot carries.
        {"$[b] == 3", E::Unary(b, OpType::Equal, E::Int(3)), b_is(3)},
        {"$[b] == 0", E::Unary(b, OpType::Equal, E::Int(0)), b_is(0)},
        {"$[b] != 1",
         E::Unary(b, OpType::NotEqual, E::Int(1)),
         [](const ElemValue& e) {
             return TvOf(e.b, [](int64_t x) { return x != 1; });
         }},
        {"$[b] > 1",
         E::Unary(b, OpType::GreaterThan, E::Int(1)),
         [](const ElemValue& e) {
             return TvOf(e.b, [](int64_t x) { return x > 1; });
         }},
        {"$[b] <= 1",
         E::Unary(b, OpType::LessEqual, E::Int(1)),
         [](const ElemValue& e) {
             return TvOf(e.b, [](int64_t x) { return x <= 1; });
         }},
        {"$[i8] >= 3",
         E::Unary(i8, OpType::GreaterEqual, E::Int(3)),
         [](const ElemValue& e) {
             return TvOf(e.i8, [](int64_t x) { return x >= 3; });
         }},
        // Literals outside Int8 take the overflow shortcut.
        {"$[i8] < 1000",
         E::Unary(i8, OpType::LessThan, E::Int(1000)),
         [](const ElemValue& e) {
             return TvOf(e.i8, [](int64_t) { return true; });
         }},
        {"$[i8] == 1000",
         E::Unary(i8, OpType::Equal, E::Int(1000)),
         [](const ElemValue& e) {
             return TvOf(e.i8, [](int64_t) { return false; });
         }},
        {"$[i8] > -1000",
         E::Unary(i8, OpType::GreaterThan, E::Int(-1000)),
         [](const ElemValue& e) {
             return TvOf(e.i8, [](int64_t) { return true; });
         }},
        {"$[i8] != 1000",
         E::Unary(i8, OpType::NotEqual, E::Int(1000)),
         [](const ElemValue& e) {
             return TvOf(e.i8, [](int64_t) { return true; });
         }},
        {"$[name] == \"ab\"",
         E::Unary(name, OpType::Equal, E::Str("ab")),
         [](const ElemValue& e) {
             return TvOf(e.name,
                         [](const std::string& s) { return s == "ab"; });
         }},
        {"$[name] == \"\"",
         E::Unary(name, OpType::Equal, E::Str("")),
         [](const ElemValue& e) {
             return TvOf(e.name,
                         [](const std::string& s) { return s.empty(); });
         }},
        {"$[name] != \"ab\"",
         E::Unary(name, OpType::NotEqual, E::Str("ab")),
         [](const ElemValue& e) {
             return TvOf(e.name,
                         [](const std::string& s) { return s != "ab"; });
         }},
        {"$[name] > \"ab\"",
         E::Unary(name, OpType::GreaterThan, E::Str("ab")),
         [](const ElemValue& e) {
             return TvOf(e.name, [](const std::string& s) { return s > "ab"; });
         }},
        {"$[name] prefix \"a\"",
         E::Unary(name, OpType::PrefixMatch, E::Str("a")),
         [](const ElemValue& e) {
             return TvOf(e.name, [](const std::string& s) {
                 return s.rfind("a", 0) == 0;
             });
         }},
        {"$[name] postfix \"b\"",
         E::Unary(name, OpType::PostfixMatch, E::Str("b")),
         [](const ElemValue& e) {
             return TvOf(e.name, [](const std::string& s) {
                 return !s.empty() && s.back() == 'b';
             });
         }},
        {"$[name] inner \"b\"",
         E::Unary(name, OpType::InnerMatch, E::Str("b")),
         [](const ElemValue& e) {
             return TvOf(e.name, [](const std::string& s) {
                 return s.find('b') != std::string::npos;
             });
         }},
        {"$[name] like \"a_\"",
         E::Unary(name, OpType::Match, E::Str("a_")),
         [](const ElemValue& e) {
             return TvOf(e.name, [](const std::string& s) {
                 return s.size() == 2 && s[0] == 'a';
             });
         }},
        {"$[name] like \"%\"",
         E::Unary(name, OpType::Match, E::Str("%")),
         [](const ElemValue& e) {
             return TvOf(e.name, [](const std::string&) { return true; });
         }},
        {"$[name] regex \"^a.$\"",
         E::Unary(name, OpType::RegexMatch, E::Str("^a.$")),
         [](const ElemValue& e) {
             return TvOf(e.name, [](const std::string& s) {
                 return s.size() == 2 && s[0] == 'a';
             });
         }},
        {"$[a] == 1", E::Unary(a, OpType::Equal, E::Int(1)), a_is(1)},
        {"$[a] != 1",
         E::Unary(a, OpType::NotEqual, E::Int(1)),
         [](const ElemValue& e) { return TvBool(e.a != 1); }},
        // BinaryRange.
        {"1 <= $[b] <= 3",
         E::Range(b, E::Int(1), E::Int(3), true, true),
         [](const ElemValue& e) {
             return TvOf(e.b, [](int64_t x) { return 1 <= x && x <= 3; });
         }},
        {"1 < $[b] < 3",
         E::Range(b, E::Int(1), E::Int(3), false, false),
         [](const ElemValue& e) {
             return TvOf(e.b, [](int64_t x) { return 1 < x && x < 3; });
         }},
        {"-1000 < $[i8] < 3",
         E::Range(i8, E::Int(-1000), E::Int(3), false, false),
         [](const ElemValue& e) {
             return TvOf(e.i8, [](int64_t x) { return x < 3; });
         }},
        {"1000 < $[i8] < 2000",
         E::Range(i8, E::Int(1000), E::Int(2000), false, false),
         [](const ElemValue& e) {
             return TvOf(e.i8, [](int64_t) { return false; });
         }},
        {"-2000 < $[i8] < -1000",
         E::Range(i8, E::Int(-2000), E::Int(-1000), false, false),
         [](const ElemValue& e) {
             return TvOf(e.i8, [](int64_t) { return false; });
         }},
        {"\"a\" <= $[name] < \"b\"",
         E::Range(name, E::Str("a"), E::Str("b"), true, false),
         [](const ElemValue& e) {
             return TvOf(e.name, [](const std::string& s) {
                 return "a" <= s && s < "b";
             });
         }},
        {"0 <= $[a] <= 1",
         E::Range(a, E::Int(0), E::Int(1), true, true),
         [](const ElemValue& e) { return TvBool(0 <= e.a && e.a <= 1); }},
        // Term.
        {"$[b] in [1, 3]",
         E::Term(b, {E::Int(1), E::Int(3)}),
         [](const ElemValue& e) {
             return TvOf(e.b, [](int64_t x) { return x == 1 || x == 3; });
         }},
        {"$[b] in [0]", E::Term(b, {E::Int(0)}), b_is(0)},
        // An empty set is FALSE on a null element too.
        {"$[b] in []",
         E::Term(b, {}),
         [](const ElemValue&) { return TvBool(false); }},
        {"$[i8] in []",
         E::Term(i8, {}),
         [](const ElemValue&) { return TvBool(false); }},
        // An overflowing literal leaves the set empty after conversion; it
        // still matches nothing and a null element stays UNKNOWN.
        {"$[i8] in [1000]",
         E::Term(i8, {E::Int(1000)}),
         [](const ElemValue& e) {
             return TvOf(e.i8, [](int64_t) { return false; });
         }},
        {"$[i8] in [1, 1000]",
         E::Term(i8, {E::Int(1), E::Int(1000)}),
         [](const ElemValue& e) {
             return TvOf(e.i8, [](int64_t x) { return x == 1; });
         }},
        {"$[name] in [\"ab\", \"b\"]",
         E::Term(name, {E::Str("ab"), E::Str("b")}),
         [](const ElemValue& e) {
             return TvOf(e.name, [](const std::string& s) {
                 return s == "ab" || s == "b";
             });
         }},
        {"$[a] in [2, 3]",
         E::Term(a, {E::Int(2), E::Int(3)}),
         [](const ElemValue& e) { return TvBool(e.a == 2 || e.a == 3); }},
        // BinaryArithOpEvalRange.
        {"$[b] + 1 == 4",
         E::Arith(b, ArithOpType::Add, 1, OpType::Equal, 4),
         b_is(3)},
        {"$[b] * 2 > 3",
         E::Arith(b, ArithOpType::Mul, 2, OpType::GreaterThan, 3),
         [](const ElemValue& e) {
             return TvOf(e.b, [](int64_t x) { return x * 2 > 3; });
         }},
        {"$[b] - 1 != 0",
         E::Arith(b, ArithOpType::Sub, 1, OpType::NotEqual, 0),
         [](const ElemValue& e) {
             return TvOf(e.b, [](int64_t x) { return x - 1 != 0; });
         }},
        {"$[i8] % 2 == 1",
         E::Arith(i8, ArithOpType::Mod, 2, OpType::Equal, 1),
         [](const ElemValue& e) {
             return TvOf(e.i8, [](int64_t x) { return x % 2 == 1; });
         }},
        {"$[a] + 1 == 2",
         E::Arith(a, ArithOpType::Add, 1, OpType::Equal, 2),
         a_is(1)},
        // contains* on a nested Array: a null leaf matches nothing, a NULL
        // inner Array is UNKNOWN.
        {"array_contains($[tags], 4)",
         E::Contains(tags_leaf, kContains, {E::Int(4)}),
         tags_tv([contains_leaf](const ElemValue& e) {
             return contains_leaf(e, 4);
         })},
        {"array_contains($[tags], 0)",
         E::Contains(tags_leaf, kContains, {E::Int(0)}),
         tags_tv([contains_leaf](const ElemValue& e) {
             return contains_leaf(e, 0);
         })},
        {"array_contains_any($[tags], [1, 7])",
         E::Contains(tags_leaf, kAny, {E::Int(1), E::Int(7)}),
         tags_tv([contains_leaf](const ElemValue& e) {
             return contains_leaf(e, 1) || contains_leaf(e, 7);
         })},
        {"array_contains_all($[tags], [1, 2])",
         E::Contains(tags_leaf, kAll, {E::Int(1), E::Int(2)}),
         tags_tv([contains_leaf](const ElemValue& e) {
             return contains_leaf(e, 1) && contains_leaf(e, 2);
         })},
        {"array_contains_all($[tags], [])",
         E::Contains(tags_leaf, kAll, {}),
         tags_tv([](const ElemValue&) { return true; })},
        {"array_contains_any($[tags], [])",
         E::Contains(tags_leaf, kAny, {}),
         tags_tv([](const ElemValue&) { return false; })},
        // array_length counts null leaves; a NULL inner Array is UNKNOWN.
        {"array_length($[tags]) == 0",
         E::Length(tags, OpType::Equal, 0),
         tags_tv([](const ElemValue& e) { return e.tags->empty(); })},
        {"array_length($[tags]) == 1",
         E::Length(tags, OpType::Equal, 1),
         tags_tv([](const ElemValue& e) { return e.tags->size() == 1; })},
        {"array_length($[tags]) == 2",
         E::Length(tags, OpType::Equal, 2),
         tags_tv([](const ElemValue& e) { return e.tags->size() == 2; })},
        {"array_length($[tags]) > 0",
         E::Length(tags, OpType::GreaterThan, 0),
         tags_tv([](const ElemValue& e) { return !e.tags->empty(); })},
        // IS NULL / IS NOT NULL: only true or false.
        {"$[b] IS NULL",
         E::IsNull(b),
         [](const ElemValue& e) { return TvBool(!e.b.has_value()); }},
        {"$[b] IS NOT NULL",
         E::IsNotNull(b),
         [](const ElemValue& e) { return TvBool(e.b.has_value()); }},
        {"$[i8] IS NULL",
         E::IsNull(i8),
         [](const ElemValue& e) { return TvBool(!e.i8.has_value()); }},
        {"$[name] IS NOT NULL",
         E::IsNotNull(name),
         [](const ElemValue& e) { return TvBool(e.name.has_value()); }},
        {"$[a] IS NULL",
         E::IsNull(a),
         [](const ElemValue&) { return kTvFalse; }},
        {"$[a] IS NOT NULL",
         E::IsNotNull(a),
         [](const ElemValue&) { return kTvTrue; }},
        {"$[tags] IS NULL",
         E::IsNull(tags),
         [](const ElemValue& e) { return TvBool(!e.tags.has_value()); }},
        {"$[tags] IS NOT NULL",
         E::IsNotNull(tags),
         [](const ElemValue& e) { return TvBool(e.tags.has_value()); }},
        {"$[vec] IS NULL",
         E::IsNull(vec),
         [](const ElemValue& e) { return TvBool(!e.vec); }},
        {"$[vec] IS NOT NULL",
         E::IsNotNull(vec),
         [](const ElemValue& e) { return TvBool(e.vec); }},
        {"$[pv] IS NULL",
         E::IsNull(pv),
         [](const ElemValue&) { return kTvFalse; }},
        {"$[pv] IS NOT NULL",
         E::IsNotNull(pv),
         [](const ElemValue&) { return kTvTrue; }},
        // Sub-fields of one element with different null layouts.
        {"$[a] == 1 && $[b] == 3",
         E::And(E::Unary(a, OpType::Equal, E::Int(1)),
                E::Unary(b, OpType::Equal, E::Int(3))),
         [a_is, b_is](const ElemValue& e) {
             return TvAnd(a_is(1)(e), b_is(3)(e));
         }},
        {"$[a] == 1 || $[b] == 3",
         E::Or(E::Unary(a, OpType::Equal, E::Int(1)),
               E::Unary(b, OpType::Equal, E::Int(3))),
         [a_is, b_is](const ElemValue& e) {
             return TvOr(a_is(1)(e), b_is(3)(e));
         }},
        {"!($[b] == 3)",
         E::Not(E::Unary(b, OpType::Equal, E::Int(3))),
         [b_is](const ElemValue& e) { return TvNot(b_is(3)(e)); }},
        {"$[b] != 1 || $[b] IS NULL",
         E::Or(E::Unary(b, OpType::NotEqual, E::Int(1)), E::IsNull(b)),
         [](const ElemValue& e) {
             return TvOr(TvOf(e.b, [](int64_t x) { return x != 1; }),
                         TvBool(!e.b.has_value()));
         }},
        {"$[b] == 1 && $[i8] == 1",
         E::And(E::Unary(b, OpType::Equal, E::Int(1)),
                E::Unary(i8, OpType::Equal, E::Int(1))),
         [b_is](const ElemValue& e) {
             return TvAnd(b_is(1)(e),
                          TvOf(e.i8, [](int64_t x) { return x == 1; }));
         }},
        {"$[b] IS NULL && $[name] IS NOT NULL",
         E::And(E::IsNull(b), E::IsNotNull(name)),
         [](const ElemValue& e) {
             return TvBool(!e.b.has_value() && e.name.has_value());
         }},
        {"$[a] == 1 && $[vec] IS NULL",
         E::And(E::Unary(a, OpType::Equal, E::Int(1)), E::IsNull(vec)),
         [](const ElemValue& e) { return TvBool(e.a == 1 && !e.vec); }},
        {"($[b] > 0 || $[i8] > 0) && $[name] prefix \"a\"",
         E::And(E::Or(E::Unary(b, OpType::GreaterThan, E::Int(0)),
                      E::Unary(i8, OpType::GreaterThan, E::Int(0))),
                E::Unary(name, OpType::PrefixMatch, E::Str("a"))),
         [](const ElemValue& e) {
             return TvAnd(TvOr(TvOf(e.b, [](int64_t x) { return x > 0; }),
                               TvOf(e.i8, [](int64_t x) { return x > 0; })),
                          TvOf(e.name, [](const std::string& s) {
                              return s.rfind("a", 0) == 0;
                          }));
         }},
        {"array_contains($[tags], 4) || $[b] == 1",
         E::Or(E::Contains(tags_leaf, kContains, {E::Int(4)}),
               E::Unary(b, OpType::Equal, E::Int(1))),
         [contains_leaf, b_is](const ElemValue& e) {
             const auto left =
                 e.tags.has_value() ? TvBool(contains_leaf(e, 4)) : kTvUnknown;
             return TvOr(left, b_is(1)(e));
         }},
        {"!($[a] == 1 && $[b] IS NULL)",
         E::Not(E::And(E::Unary(a, OpType::Equal, E::Int(1)), E::IsNull(b))),
         [](const ElemValue& e) {
             return TvBool(!(e.a == 1 && !e.b.has_value()));
         }},
        // Sub-fields added after the rows were written: null on every
        // existing element.
        {"$[lang] == \"en\"",
         E::Unary(lang, OpType::Equal, E::Str("en")),
         unknown,
         true},
        {"$[lang] prefix \"e\"",
         E::Unary(lang, OpType::PrefixMatch, E::Str("e")),
         unknown,
         true},
        {"$[lang] in [\"en\"]", E::Term(lang, {E::Str("en")}), unknown, true},
        {"\"a\" < $[lang] < \"z\"",
         E::Range(lang, E::Str("a"), E::Str("z"), false, false),
         unknown,
         true},
        {"$[lang] IS NULL",
         E::IsNull(lang),
         [](const ElemValue&) { return kTvTrue; },
         true},
        {"$[lang] IS NOT NULL",
         E::IsNotNull(lang),
         [](const ElemValue&) { return kTvFalse; },
         true},
        {"$[lnum] + 1 == 1",
         E::Arith(lnum, ArithOpType::Add, 1, OpType::Equal, 1),
         unknown,
         true},
        {"0 <= $[lnum] <= 5",
         E::Range(lnum, E::Int(0), E::Int(5), true, true),
         unknown,
         true},
        {"$[lnum] in [0]", E::Term(lnum, {E::Int(0)}), unknown, true},
        {"$[lnum] IS NULL",
         E::IsNull(lnum),
         [](const ElemValue&) { return kTvTrue; },
         true},
        {"array_contains($[ltags], 0)",
         E::Contains(ltags_leaf, kContains, {E::Int(0)}),
         unknown,
         true},
        {"array_contains_all($[ltags], [])",
         E::Contains(ltags_leaf, kAll, {}),
         unknown,
         true},
        {"array_length($[ltags]) == 0",
         E::Length(ltags, OpType::Equal, 0),
         unknown,
         true},
        {"$[ltags] IS NULL",
         E::IsNull(ltags),
         [](const ElemValue&) { return kTvTrue; },
         true},
        {"$[a] == 1 && $[lang] == \"en\"",
         E::And(E::Unary(a, OpType::Equal, E::Int(1)),
                E::Unary(lang, OpType::Equal, E::Str("en"))),
         [](const ElemValue& e) { return e.a == 1 ? kTvUnknown : kTvFalse; },
         true},
        {"$[a] == 1 || $[lang] == \"en\"",
         E::Or(E::Unary(a, OpType::Equal, E::Int(1)),
               E::Unary(lang, OpType::Equal, E::Str("en"))),
         [](const ElemValue& e) { return e.a == 1 ? kTvTrue : kTvUnknown; },
         true},
        {"$[lang] IS NULL && $[b] IS NOT NULL",
         E::And(E::IsNull(lang), E::IsNotNull(b)),
         [](const ElemValue& e) { return TvBool(e.b.has_value()); },
         true},
    };

    // LIKE and regex share the element-level path and validity handling of
    // the comparisons above: a null element is UNKNOWN for every pattern,
    // including one that matches the "" payload a null slot carries.
    auto name_tv = [](auto pred) {
        return [pred](const ElemValue& e) { return TvOf(e.name, pred); };
    };
    auto ends_with = [](const std::string& s, const std::string& suffix) {
        return s.size() >= suffix.size() &&
               s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
    };
    auto has_b = [](const std::string& s) {
        return s.find('b') != std::string::npos;
    };
    std::vector<ElementLevelCase> pattern_cases = {
        // LIKE through the generic matcher (OpType::Match), the operator the
        // parser keeps for patterns it cannot rewrite.
        {"$[name] like \"%b\"",
         E::Unary(name, OpType::Match, E::Str("%b")),
         name_tv(
             [ends_with](const std::string& s) { return ends_with(s, "b"); })},
        {"$[name] like \"%b%\"",
         E::Unary(name, OpType::Match, E::Str("%b%")),
         name_tv(has_b)},
        {"$[name] like \"a%\"",
         E::Unary(name, OpType::Match, E::Str("a%")),
         name_tv([](const std::string& s) { return s.rfind("a", 0) == 0; })},
        {"$[name] like \"a%c\"",
         E::Unary(name, OpType::Match, E::Str("a%c")),
         name_tv([ends_with](const std::string& s) {
             return s.rfind("a", 0) == 0 && ends_with(s, "c");
         })},
        {"$[name] like \"_b\"",
         E::Unary(name, OpType::Match, E::Str("_b")),
         name_tv([](const std::string& s) {
             return s.size() == 2 && s[1] == 'b';
         })},
        {"$[name] like \"__\"",
         E::Unary(name, OpType::Match, E::Str("__")),
         name_tv([](const std::string& s) { return s.size() == 2; })},
        {"$[name] like \"_\"",
         E::Unary(name, OpType::Match, E::Str("_")),
         name_tv([](const std::string& s) { return s.size() == 1; })},
        {"$[name] like \"%a_\"",
         E::Unary(name, OpType::Match, E::Str("%a_")),
         name_tv([](const std::string& s) {
             return s.size() >= 2 && s[s.size() - 2] == 'a';
         })},
        {"$[name] like \"\"",
         E::Unary(name, OpType::Match, E::Str("")),
         name_tv([](const std::string& s) { return s.empty(); })},
        {"$[name] like \"zz%\"",
         E::Unary(name, OpType::Match, E::Str("zz%")),
         name_tv([](const std::string&) { return false; })},
        {"$[name] postfix \"\"",
         E::Unary(name, OpType::PostfixMatch, E::Str("")),
         name_tv([](const std::string&) { return true; })},
        // Regex is a partial (unanchored) match.
        {"$[name] =~ \"b\"",
         E::Unary(name, OpType::RegexMatch, E::Str("b")),
         name_tv(has_b)},
        {"$[name] =~ \"a$\"",
         E::Unary(name, OpType::RegexMatch, E::Str("a$")),
         name_tv(
             [ends_with](const std::string& s) { return ends_with(s, "a"); })},
        {"$[name] =~ \"^(aa|b)$\"",
         E::Unary(name, OpType::RegexMatch, E::Str("^(aa|b)$")),
         name_tv([](const std::string& s) { return s == "aa" || s == "b"; })},
        {"$[name] =~ \"^[ab]{3}\"",
         E::Unary(name, OpType::RegexMatch, E::Str("^[ab]{3}")),
         name_tv([](const std::string& s) {
             return s.size() >= 3 &&
                    std::all_of(s.begin(), s.begin() + 3, [](char c) {
                        return c == 'a' || c == 'b';
                    });
         })},
        {"$[name] =~ \".*\"",
         E::Unary(name, OpType::RegexMatch, E::Str(".*")),
         name_tv([](const std::string&) { return true; })},
        // Two sub-fields with different null layouts in one element.
        {"$[name] like \"a%\" && $[b] == 1",
         E::And(E::Unary(name, OpType::PrefixMatch, E::Str("a")),
                E::Unary(b, OpType::Equal, E::Int(1))),
         [b_is](const ElemValue& e) {
             return TvAnd(TvOf(e.name,
                               [](const std::string& s) {
                                   return s.rfind("a", 0) == 0;
                               }),
                          b_is(1)(e));
         }},
        {"$[name] like \"a_\" && $[b] == 1",
         E::And(E::Unary(name, OpType::Match, E::Str("a_")),
                E::Unary(b, OpType::Equal, E::Int(1))),
         [b_is](const ElemValue& e) {
             return TvAnd(TvOf(e.name,
                               [](const std::string& s) {
                                   return s.size() == 2 && s[0] == 'a';
                               }),
                          b_is(1)(e));
         }},
        {"$[name] like \"%b%\" || $[b] == 1",
         E::Or(E::Unary(name, OpType::Match, E::Str("%b%")),
               E::Unary(b, OpType::Equal, E::Int(1))),
         [b_is, has_b](const ElemValue& e) {
             return TvOr(TvOf(e.name, has_b), b_is(1)(e));
         }},
        {"!($[name] =~ \"b\") && $[i8] > 0",
         E::And(E::Not(E::Unary(name, OpType::RegexMatch, E::Str("b"))),
                E::Unary(i8, OpType::GreaterThan, E::Int(0))),
         [has_b](const ElemValue& e) {
             return TvAnd(TvNot(TvOf(e.name, has_b)),
                          TvOf(e.i8, [](int64_t x) { return x > 0; }));
         }},
        {"$[name] =~ \"c$\" || $[name] IS NULL",
         E::Or(E::Unary(name, OpType::RegexMatch, E::Str("c$")),
               E::IsNull(name)),
         [ends_with](const ElemValue& e) {
             return TvOr(TvOf(e.name,
                              [ends_with](const std::string& s) {
                                  return ends_with(s, "c");
                              }),
                         TvBool(!e.name.has_value()));
         }},
        // Patterns on a sub-field added after the rows were written.
        {"$[lang] like \"e_\"",
         E::Unary(lang, OpType::Match, E::Str("e_")),
         unknown,
         true},
        {"$[lang] like \"%\"",
         E::Unary(lang, OpType::Match, E::Str("%")),
         unknown,
         true},
        {"$[lang] =~ \"en\"",
         E::Unary(lang, OpType::RegexMatch, E::Str("en")),
         unknown,
         true},
        {"$[a] == 1 && $[lang] like \"%n\"",
         E::And(E::Unary(a, OpType::Equal, E::Int(1)),
                E::Unary(lang, OpType::Match, E::Str("%n"))),
         [](const ElemValue& e) { return e.a == 1 ? kTvUnknown : kTvFalse; },
         true},
    };
    cases.insert(cases.end(), pattern_cases.begin(), pattern_cases.end());
    return cases;
}

void
RunElementLevelCases(SegmentInternalInterface* segment,
                     const ElementLevelFixture& f,
                     bool with_added_fields) {
    for (const auto& c : BuildElementLevelCases(f)) {
        if (c.uses_added_fields && !with_added_fields) {
            continue;
        }
        SCOPED_TRACE(c.name);
        std::vector<Tv> expected;
        for (const auto& value : f.values) {
            expected.push_back(c.oracle(value));
        }
        CheckElementLevelEverywhere(segment, f, c.name, c.expr, expected);
    }
}

// Run the matrix on growing (heap and mmap) and sealed segments, each also
// after a schema change that adds sub-fields to `s`.
void
RunElementLevelMatrix(const ElementLevelFixture& f,
                      const std::function<void(SegmentInternalInterface*,
                                               bool with_added_fields,
                                               const std::string& kind)>& run) {
    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    config.set_chunk_rows(2);
    auto& mmap_config = storage::MmapManager::GetInstance().GetMmapConfig();
    const auto saved_growing_mmap = mmap_config.GetEnableGrowingMmap();
    auto mmap_guard = folly::makeGuard([&mmap_config, saved_growing_mmap] {
        mmap_config.growing_enable_mmap = saved_growing_mmap;
    });
    for (bool enable_mmap : {false, true}) {
        mmap_config.growing_enable_mmap = enable_mmap;
        const std::string kind = enable_mmap ? "growing mmap" : "growing";
        {
            SCOPED_TRACE(kind);
            auto growing = LoadElementLevelGrowing(f);
            ASSERT_GT(growing->num_chunk(f.b), 1);
            run(growing.get(), false, kind);
        }
        {
            SCOPED_TRACE(kind + " with added sub-fields");
            auto growing = LoadElementLevelGrowing(f);
            growing->Reopen(WithAddedSubFields(*f.schema, f));
            run(growing.get(), true, kind + " with added sub-fields");
        }
    }
    mmap_config.growing_enable_mmap = saved_growing_mmap;

    const auto segment_path = ElementLevelSegmentPath("sealed");
    std::filesystem::remove_all(segment_path);
    auto directory_guard = folly::makeGuard(
        [&segment_path] { std::filesystem::remove_all(segment_path); });
    {
        SCOPED_TRACE("sealed");
        auto sealed = LoadElementLevelSealed(f, segment_path);
        ASSERT_GT(sealed->num_chunk(f.b), 1);
        run(sealed.get(), false, "sealed");
    }
    {
        SCOPED_TRACE("sealed with added sub-fields");
        auto sealed = LoadElementLevelSealed(f, segment_path + "_added");
        auto added_guard = folly::makeGuard([&segment_path] {
            std::filesystem::remove_all(segment_path + "_added");
        });
        sealed->Reopen(WithAddedSubFields(*sealed->get_schema_snapshot(), f));
        run(sealed.get(), true, "sealed with added sub-fields");
    }
}

}  // namespace

TEST(Expr, ElementLevelFilterOnElementNullableSubFields) {
    auto f = BuildElementLevelMatrixFixture();
    ASSERT_EQ(f.num_elements(), 11);
    RunElementLevelMatrix(f,
                          [&](SegmentInternalInterface* segment,
                              bool with_added_fields,
                              const std::string&) {
                              RunElementLevelCases(
                                  segment, f, with_added_fields);
                          });
}

// The C18 truth table (feature map, 语义约定) rerun on real element-nullable
// data instead of injected element bitmaps: an UNKNOWN element is a miss.
TEST(Expr, ElementLevelMatchC18TruthTableOnElementNullableData) {
    auto f = BuildElementLevelC18Fixture();
    using E = ElementLevelExprs;
    using MT = expr::MatchType;
    using proto::plan::OpType;
    const auto a = E::Scalar(f.a, DataType::INT64);
    const auto b = E::Scalar(f.b, DataType::INT64);
    auto b_gt0 = E::Unary(b, OpType::GreaterThan, E::Int(0));
    auto match = [](MT type, int64_t threshold, expr::TypedExprPtr pred) {
        return std::make_shared<expr::MatchExpr>("s", type, threshold, pred);
    };
    // Rows: [3, NULL], [NULL, NULL], [], [NULL, NULL, 5], [3, 5, NULL],
    // [3, 5], [{a: 1, b: NULL}, {a: 2, b: 3}].
    const std::vector<
        std::tuple<std::string, expr::TypedExprPtr, std::vector<Tv>>>
        table = {
            {"MATCH_ANY($[b] > 0)",
             match(MT::MatchAny, 0, b_gt0),
             {1, 0, 0, 1, 1, 1, 1}},
            {"MATCH_ALL($[b] > 0)",
             match(MT::MatchAll, 0, b_gt0),
             {0, 0, 1, 0, 0, 1, 0}},
            {"MATCH_LEAST($[b] > 0, 2)",
             match(MT::MatchLeast, 2, b_gt0),
             {0, 0, 0, 0, 1, 1, 0}},
            {"MATCH_MOST($[b] > 0, 1)",
             match(MT::MatchMost, 1, b_gt0),
             {1, 1, 1, 1, 0, 0, 1}},
            {"MATCH_EXACT($[b] > 0, 1)",
             match(MT::MatchExact, 1, b_gt0),
             {1, 0, 0, 1, 0, 0, 1}},
            {"MATCH_ANY($[a] == 1 && $[b] == 3)",
             match(MT::MatchAny,
                   0,
                   E::And(E::Unary(a, OpType::Equal, E::Int(1)),
                          E::Unary(b, OpType::Equal, E::Int(3)))),
             {0, 0, 0, 0, 0, 0, 0}},
            {"MATCH_ANY($[a] == 1 || $[b] == 3)",
             match(MT::MatchAny,
                   0,
                   E::Or(E::Unary(a, OpType::Equal, E::Int(1)),
                         E::Unary(b, OpType::Equal, E::Int(3)))),
             {1, 0, 0, 0, 1, 1, 1}},
            {"MATCH_ALL($[b] != 1)",
             match(MT::MatchAll, 0, E::Unary(b, OpType::NotEqual, E::Int(1))),
             {0, 0, 1, 0, 0, 1, 0}},
            {"MATCH_ANY(!($[b] == 3))",
             match(MT::MatchAny,
                   0,
                   E::Not(E::Unary(b, OpType::Equal, E::Int(3)))),
             {0, 0, 0, 1, 1, 1, 0}},
            // Spelling out the null check makes a null element a hit.
            {"MATCH_ALL($[b] != 1 || $[b] IS NULL)",
             match(
                 MT::MatchAll,
                 0,
                 E::Or(E::Unary(b, OpType::NotEqual, E::Int(1)), E::IsNull(b))),
             {1, 1, 1, 1, 1, 1, 1}},
        };
    RunElementLevelMatrix(
        f,
        [&](SegmentInternalInterface* segment,
            bool with_added_fields,
            const std::string&) {
            for (const auto& [label, typed_expr, expected] : table) {
                CheckRowLevelExpr(
                    segment, f.num_rows(), typed_expr, expected, label);
            }
            RunElementLevelCases(segment, f, with_added_fields);
        });
}

namespace {

// A nested (element-domain) inverted index on `s[a]`, whose elements are
// never null. A NULL struct row has no elements.
void
InstallNestedIndexOnA(SegmentSealed& segment, const ElementLevelFixture& f) {
    std::vector<Array> arrays;
    for (const auto& row : f.rows) {
        proto::schema::ScalarField values;
        auto* integers = values.mutable_long_data();
        if (row.has_value()) {
            for (const auto& e : *row) {
                integers->add_data(e.a);
            }
        }
        arrays.emplace_back(values);
    }
    auto field = std::make_shared<FieldData<Array>>(DataType::ARRAY, false);
    field->FillFieldData(arrays.data(), arrays.size());
    auto opened = test::expr_index::BuildIndex(f.a,
                                               DataType::ARRAY,
                                               index::INVERTED_INDEX_TYPE,
                                               {field},
                                               Config::object(),
                                               DataType::INT64,
                                               true);
    test::expr_index::InstallIndex(
        segment, f.a, DataType::ARRAY, std::move(opened), DataType::INT64);
}

exec::ExprExecPath
ElementLevelUnaryExecPath(SegmentInternalInterface* segment,
                          const expr::ColumnInfo& column) {
    auto logical = std::make_shared<expr::UnaryRangeFilterExpr>(
        column, proto::plan::OpType::Equal, ElementLevelExprs::Int(1));
    exec::PhyUnaryRangeFilterExpr physical({},
                                           logical,
                                           "element-level unary",
                                           nullptr,
                                           segment,
                                           segment->get_row_count(),
                                           64,
                                           0);
    return physical.GetExecPath();
}

}  // namespace

// One predicate mixes a sub-field served by a nested index (`$[a]`) with
// element-nullable sub-fields read from raw data; results stay aligned by
// element id.
TEST(Expr, ElementLevelMixedNestedIndexAndElementNullable) {
    auto f = BuildElementLevelMatrixFixture();
    const auto segment_path = ElementLevelSegmentPath("nested_index");
    std::filesystem::remove_all(segment_path);
    auto directory_guard = folly::makeGuard(
        [&segment_path] { std::filesystem::remove_all(segment_path); });
    auto sealed = LoadElementLevelSealed(f, segment_path);
    InstallNestedIndexOnA(*sealed, f);
    ASSERT_EQ(
        ElementLevelUnaryExecPath(
            sealed.get(), ElementLevelExprs::Scalar(f.a, DataType::INT64)),
        exec::ExprExecPath::ScalarIndex);
    ASSERT_EQ(
        ElementLevelUnaryExecPath(
            sealed.get(), ElementLevelExprs::Scalar(f.b, DataType::INT64)),
        exec::ExprExecPath::RawData);
    RunElementLevelCases(sealed.get(), f, false);
}

namespace {

// Element predicates written as filter strings and parsed by the Go plan
// parser, including `$[x] IS NULL`.
struct ParsedElementLevelCase {
    std::string text;
    std::function<Tv(const ElemValue&)> oracle;
    bool uses_added_fields = false;
};

std::vector<ParsedElementLevelCase>
BuildParsedElementLevelCases() {
    auto b_tv = [](auto pred) {
        return [pred](const ElemValue& e) { return TvOf(e.b, pred); };
    };
    auto b_is_null = [](const ElemValue& e) {
        return TvBool(!e.b.has_value());
    };
    auto name_tv = [](auto pred) {
        return [pred](const ElemValue& e) { return TvOf(e.name, pred); };
    };
    auto contains_leaf = [](const ElemValue& e, int64_t v) {
        for (const auto& leaf : *e.tags) {
            if (leaf.has_value() && *leaf == v) {
                return true;
            }
        }
        return false;
    };
    return {
        {"$[b] IS NULL", b_is_null},
        {"$[b] is not null",
         [](const ElemValue& e) { return TvBool(e.b.has_value()); }},
        {"not ($[b] IS NULL)",
         [](const ElemValue& e) { return TvBool(e.b.has_value()); }},
        {"$[i8] IS NULL",
         [](const ElemValue& e) { return TvBool(!e.i8.has_value()); }},
        {"$[name] IS NOT NULL",
         [](const ElemValue& e) { return TvBool(e.name.has_value()); }},
        {"$[a] IS NULL", [](const ElemValue&) { return kTvFalse; }},
        {"$[a] IS NOT NULL", [](const ElemValue&) { return kTvTrue; }},
        {"$[tags] IS NULL",
         [](const ElemValue& e) { return TvBool(!e.tags.has_value()); }},
        {"$[tags] IS NOT NULL",
         [](const ElemValue& e) { return TvBool(e.tags.has_value()); }},
        {"$[vec] IS NULL", [](const ElemValue& e) { return TvBool(!e.vec); }},
        {"$[vec] IS NOT NULL",
         [](const ElemValue& e) { return TvBool(e.vec); }},
        {"$[pv] IS NULL", [](const ElemValue&) { return kTvFalse; }},
        {"$[b] == 3", b_tv([](int64_t x) { return x == 3; })},
        {"$[b] != 1 || $[b] IS NULL",
         [](const ElemValue& e) {
             return TvOr(TvOf(e.b, [](int64_t x) { return x != 1; }),
                         TvBool(!e.b.has_value()));
         }},
        {"$[a] == 1 && $[b] == 3",
         [](const ElemValue& e) {
             return TvAnd(TvBool(e.a == 1),
                          TvOf(e.b, [](int64_t x) { return x == 3; }));
         }},
        {"$[a] == 1 || $[b] == 3",
         [](const ElemValue& e) {
             return TvOr(TvBool(e.a == 1),
                         TvOf(e.b, [](int64_t x) { return x == 3; }));
         }},
        {"!($[b] == 3)",
         [](const ElemValue& e) {
             return TvNot(TvOf(e.b, [](int64_t x) { return x == 3; }));
         }},
        {"$[b] IS NULL && $[name] IS NOT NULL",
         [](const ElemValue& e) {
             return TvBool(!e.b.has_value() && e.name.has_value());
         }},
        {"$[a] == 1 && $[vec] IS NULL",
         [](const ElemValue& e) { return TvBool(e.a == 1 && !e.vec); }},
        {"$[i8] < 1000",
         [](const ElemValue& e) {
             return TvOf(e.i8, [](int64_t) { return true; });
         }},
        {"1000 < $[i8] < 2000",
         [](const ElemValue& e) {
             return TvOf(e.i8, [](int64_t) { return false; });
         }},
        {"$[name] like \"a%\"",
         [](const ElemValue& e) {
             return TvOf(e.name, [](const std::string& s) {
                 return s.rfind("a", 0) == 0;
             });
         }},
        {"$[b] in [1, 3]", b_tv([](int64_t x) { return x == 1 || x == 3; })},
        {"$[b] not in [1, 3]",
         b_tv([](int64_t x) { return x != 1 && x != 3; })},
        {"$[b] + 1 == 4", b_tv([](int64_t x) { return x == 3; })},
        {"array_contains($[tags], 4)",
         [contains_leaf](const ElemValue& e) {
             return e.tags.has_value() ? TvBool(contains_leaf(e, 4))
                                       : kTvUnknown;
         }},
        {"array_contains_all($[tags], [])",
         [](const ElemValue& e) {
             return e.tags.has_value() ? kTvTrue : kTvUnknown;
         }},
        {"array_length($[tags]) == 1",
         [](const ElemValue& e) {
             return e.tags.has_value() ? TvBool(e.tags->size() == 1)
                                       : kTvUnknown;
         }},
        {"$[lang] IS NULL", [](const ElemValue&) { return kTvTrue; }, true},
        {"$[lang] IS NOT NULL",
         [](const ElemValue&) { return kTvFalse; },
         true},
        {"$[ltags] IS NULL", [](const ElemValue&) { return kTvTrue; }, true},
        {"$[a] == 1 && $[lang] == \"en\"",
         [](const ElemValue& e) { return e.a == 1 ? kTvUnknown : kTvFalse; },
         true},
        // LIKE and regex: the parser rewrites simple LIKE patterns to
        // prefix / postfix / inner matches and keeps the rest as Match.
        {"$[name] like \"%b\"", name_tv([](const std::string& s) {
             return !s.empty() && s.back() == 'b';
         })},
        {"$[name] like \"%b%\"", name_tv([](const std::string& s) {
             return s.find('b') != std::string::npos;
         })},
        {"$[name] like \"a_\"", name_tv([](const std::string& s) {
             return s.size() == 2 && s[0] == 'a';
         })},
        {"$[name] like \"_b%\"", name_tv([](const std::string& s) {
             return s.size() >= 2 && s[1] == 'b';
         })},
        {"not ($[name] like \"a%\")",
         name_tv([](const std::string& s) { return s.rfind("a", 0) != 0; })},
        {"$[name] =~ \"b\"", name_tv([](const std::string& s) {
             return s.find('b') != std::string::npos;
         })},
        {"$[name] =~ \"^a.$\"", name_tv([](const std::string& s) {
             return s.size() == 2 && s[0] == 'a';
         })},
        {"$[name] =~ \"^a\"",
         name_tv([](const std::string& s) { return s.rfind("a", 0) == 0; })},
        {"$[name] !~ \"b\"", name_tv([](const std::string& s) {
             return s.find('b') == std::string::npos;
         })},
        {"$[name] like \"a%\" && $[b] == 1",
         [](const ElemValue& e) {
             return TvAnd(TvOf(e.name,
                               [](const std::string& s) {
                                   return s.rfind("a", 0) == 0;
                               }),
                          TvOf(e.b, [](int64_t x) { return x == 1; }));
         }},
        {"$[name] =~ \"c$\" || $[b] IS NULL",
         [](const ElemValue& e) {
             return TvOr(TvOf(e.name,
                              [](const std::string& s) {
                                  return !s.empty() && s.back() == 'c';
                              }),
                         TvBool(!e.b.has_value()));
         }},
        {"$[lang] like \"e%\"",
         [](const ElemValue&) { return kTvUnknown; },
         true},
        {"$[lang] =~ \"en\"",
         [](const ElemValue&) { return kTvUnknown; },
         true},
    };
}

void
RunParsedElementLevelCases(SegmentInternalInterface* segment,
                           const ElementLevelFixture& f,
                           bool with_added_fields) {
    auto schema = segment->get_schema_snapshot();
    ScopedSchemaHandle handle(*schema);
    for (const auto& c : BuildParsedElementLevelCases()) {
        if (c.uses_added_fields && !with_added_fields) {
            continue;
        }
        SCOPED_TRACE("parsed " + c.text);
        std::vector<Tv> expected;
        for (const auto& value : f.values) {
            expected.push_back(c.oracle(value));
        }
        // element_filter: the element predicate, checked everywhere.
        const auto bytes = handle.Parse("element_filter(s, " + c.text + ")");
        auto plan =
            CreateRetrievePlanByExpr(schema, bytes.data(), bytes.size());
        ASSERT_NE(plan, nullptr);
        auto* element_filter = dynamic_cast<plan::ElementFilterBitsNode*>(
            plan->plan_node_->plannodes_.get());
        ASSERT_NE(element_filter, nullptr);
        CheckElementLevelEverywhere(segment,
                                    f,
                                    "parsed " + c.text,
                                    element_filter->element_filter(),
                                    expected);

        // MATCH_* written in the filter string.
        using MT = expr::MatchType;
        for (const auto& [type, spelled, threshold] :
             std::vector<std::tuple<MT, std::string, int64_t>>{
                 {MT::MatchAny, "match_any(s, " + c.text + ")", 0},
                 {MT::MatchAll, "match_all(s, " + c.text + ")", 0},
                 {MT::MatchLeast,
                  "match_least(s, " + c.text + ", threshold=2)",
                  2},
                 {MT::MatchMost,
                  "match_most(s, " + c.text + ", threshold=1)",
                  1},
                 {MT::MatchExact,
                  "match_exact(s, " + c.text + ", threshold=1)",
                  1}}) {
            const auto match_bytes = handle.Parse(spelled);
            auto match_plan = CreateRetrievePlanByExpr(
                schema, match_bytes.data(), match_bytes.size());
            ASSERT_NE(match_plan, nullptr) << spelled;
            const auto& sources = match_plan->plan_node_->plannodes_->sources();
            ASSERT_EQ(sources.size(), 1) << spelled;
            auto* filter =
                dynamic_cast<plan::FilterBitsNode*>(sources.front().get());
            ASSERT_NE(filter, nullptr) << spelled;
            CheckRowLevelExpr(segment,
                              f.num_rows(),
                              filter->filter(),
                              MatchRowExpected(f, expected, type, threshold),
                              "parsed " + spelled);
        }
    }
}

}  // namespace

TEST(Expr, ElementLevelFilterOnElementNullableSubFieldsParsed) {
    auto f = BuildElementLevelMatrixFixture();
    RunElementLevelMatrix(f,
                          [&](SegmentInternalInterface* segment,
                              bool with_added_fields,
                              const std::string&) {
                              RunParsedElementLevelCases(
                                  segment, f, with_added_fields);
                          });
}

// element_filter evaluates only the elements of the rows that pass the row
// predicate when few rows pass (offset mode, under 1% of the rows). Element
// validity must hold there too.
TEST(Expr, ElementFilterOffsetModeOnElementNullableSubFields) {
    constexpr int64_t kRows = 1000;
    std::vector<ElemRow> rows;
    for (int64_t r = 0; r < kRows; ++r) {
        ElemValue first;
        first.a = 1;
        if (r % 3 != 0) {
            first.b = r % 4;
        }
        ElemValue second;
        second.a = 2;
        if (r % 2 == 0) {
            second.b = 1;
        }
        rows.emplace_back(std::vector<ElemValue>{first, second});
    }
    auto f = BuildElementLevelFixture(std::move(rows));
    using E = ElementLevelExprs;
    const auto b = E::Scalar(f.b, DataType::INT64);
    const std::vector<std::tuple<std::string,
                                 expr::TypedExprPtr,
                                 std::function<Tv(const ElemValue&)>>>
        cases = {
            {"$[b] != 1",
             E::Unary(b, proto::plan::OpType::NotEqual, E::Int(1)),
             [](const ElemValue& e) {
                 return TvOf(e.b, [](int64_t x) { return x != 1; });
             }},
            {"!($[b] != 1)",
             E::Not(E::Unary(b, proto::plan::OpType::NotEqual, E::Int(1))),
             [](const ElemValue& e) {
                 return TvOf(e.b, [](int64_t x) { return x == 1; });
             }},
            {"$[b] IS NULL",
             E::IsNull(b),
             [](const ElemValue& e) { return TvBool(!e.b.has_value()); }},
            {"$[b] != 1 || $[b] IS NULL",
             E::Or(E::Unary(b, proto::plan::OpType::NotEqual, E::Int(1)),
                   E::IsNull(b)),
             [](const ElemValue& e) {
                 return TvBool(!e.b.has_value() || *e.b != 1);
             }},
        };
    // pk in [3, 501, 998]: 0.3% of the rows pass.
    const std::set<int64_t> selected_rows = {3, 501, 998};
    std::vector<proto::plan::GenericValue> pks;
    for (auto r : selected_rows) {
        pks.push_back(E::Int(r));
    }
    auto doc_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(f.pk, DataType::INT64), pks);

    auto check = [&](SegmentInternalInterface* segment) {
        for (const auto& [label, typed_expr, oracle] : cases) {
            std::set<std::pair<int64_t, int64_t>> want;
            for (int64_t id = 0; id < f.num_elements(); ++id) {
                if (selected_rows.count(f.elem_row[id]) > 0 &&
                    oracle(f.values[id]) == kTvTrue) {
                    want.emplace(f.elem_row[id], f.elem_index[id]);
                }
            }
            EXPECT_EQ(RetrieveElementHits(segment, f, typed_expr, doc_expr),
                      want)
                << "pk in [3, 501, 998] && element_filter(s, " << label << ")";
        }
    };
    {
        SCOPED_TRACE("growing");
        auto& config = SegcoreConfig::default_config();
        ScopedSegcoreConfigRestore config_restore(config);
        config.set_chunk_rows(64);
        auto growing = LoadElementLevelGrowing(f);
        check(growing.get());
    }
    {
        SCOPED_TRACE("sealed");
        const auto segment_path = ElementLevelSegmentPath("offset_mode");
        std::filesystem::remove_all(segment_path);
        auto directory_guard = folly::makeGuard(
            [&segment_path] { std::filesystem::remove_all(segment_path); });
        auto sealed = LoadElementLevelSealed(f, segment_path, 250);
        ASSERT_GT(sealed->num_chunk(f.b), 1);
        check(sealed.get());
    }
}

namespace {

// Struct sub-field output. Every output column of a struct sub-field has one
// row per output row, so the sub-fields of a struct stay aligned. A sub-field
// added after rows were written reads on those rows as a null row where the
// struct row is null and otherwise as one null element per element of the
// row, in the format the stored sub-fields use.

using google::protobuf::util::MessageDifferencer;

// s[lvec]: an element-nullable vector sub-field added in schema version 2.
FieldId
AddedVectorSubField(const ElementLevelFixture& f) {
    return FieldId(f.pv.get() + 104);
}

// WithAddedSubFields (s[lang], s[lnum], s[ltags]) plus s[lvec].
SchemaPtr
WithAddedOutputSubFields(const Schema& base, const ElementLevelFixture& f) {
    auto next = WithAddedSubFields(base, f);
    next->AddField(FieldMeta(FieldName("s[lvec]"),
                             AddedVectorSubField(f),
                             DataType::VECTOR_ARRAY,
                             DataType::VECTOR_FLOAT,
                             kElemVecDim,
                             knowhere::metric::L2,
                             true,
                             true));
    return next;
}

// Each added sub-field and the stored sibling of the same type.
std::vector<std::pair<FieldId, FieldId>>
AddedAndSiblingSubFields(const ElementLevelFixture& f) {
    return {{f.lnum, f.b},
            {f.lang, f.name},
            {f.ltags, f.tags},
            {AddedVectorSubField(f), f.vec}};
}

std::vector<FieldId>
AllOutputFields(const ElementLevelFixture& f) {
    return {f.pk,
            f.a,
            f.b,
            f.i8,
            f.name,
            f.tags,
            f.vec,
            f.pv,
            f.lang,
            f.lnum,
            f.ltags,
            AddedVectorSubField(f)};
}

// The fixture row an output row holds, its pk, and whether the added
// sub-fields were written with values for it.
struct OutputRow {
    int64_t row;
    int64_t pk;
    bool added_written;
};

// Growing segments hold the fixture rows in batches; batch k has
// pk = row + kOutputPkStride * k.
constexpr int64_t kOutputPkStride = 100;

// The fixture rows as one insert message with pk = row + pk_base. With
// `with_added`, s[lnum], s[lang], s[ltags] and s[lvec] carry the values of
// s[b], s[name], s[tags] and s[vec], as rows written after the sub-fields were
// added. Without, the message lacks them, as from a proxy that predates the
// change.
std::shared_ptr<InsertRecordProto>
OutputInsertBatch(const ElementLevelFixture& f,
                  int64_t pk_base,
                  bool with_added) {
    auto insert = std::make_shared<InsertRecordProto>(*f.insert);
    std::vector<DataArray> added;
    for (auto& field : *insert->mutable_fields_data()) {
        if (field.field_id() == f.pk.get()) {
            for (auto& pk : *field.mutable_scalars()
                                 ->mutable_long_data()
                                 ->mutable_data()) {
                pk += pk_base;
            }
        }
        if (!with_added) {
            continue;
        }
        for (const auto& [added_id, sibling] : AddedAndSiblingSubFields(f)) {
            if (field.field_id() == sibling.get()) {
                added.push_back(field);
                added.back().set_field_id(added_id.get());
            }
        }
    }
    for (auto& field : added) {
        *insert->add_fields_data() = std::move(field);
    }
    return insert;
}

void
InsertOutputBatch(SegmentGrowing* growing,
                  const ElementLevelFixture& f,
                  int64_t batch,
                  bool with_added) {
    const auto n = f.num_rows();
    std::vector<int64_t> row_ids(n);
    std::vector<Timestamp> timestamps(n);
    std::iota(row_ids.begin(), row_ids.end(), batch * n);
    std::iota(timestamps.begin(),
              timestamps.end(),
              static_cast<Timestamp>(batch * n));
    const auto offset = growing->PreInsert(n);
    growing->Insert(offset,
                    n,
                    row_ids.data(),
                    timestamps.data(),
                    OutputInsertBatch(f, batch * kOutputPkStride, with_added));
}

// Segment offset -> expected output row. Batch 0 is the fixture as loaded;
// later batches of a growing segment repeat the fixture rows.
OutputRow
ExpectedOutputRow(const ElementLevelFixture& f,
                  int64_t offset,
                  int64_t first_written_batch) {
    const auto batch = offset / f.num_rows();
    const auto row = offset % f.num_rows();
    return {row, row + batch * kOutputPkStride, batch >= first_written_batch};
}

std::vector<OutputRow>
ExpectedOutputRows(const ElementLevelFixture& f,
                   const std::vector<int64_t>& offsets,
                   int64_t first_written_batch) {
    std::vector<OutputRow> rows;
    for (auto offset : offsets) {
        rows.push_back(ExpectedOutputRow(f, offset, first_written_batch));
    }
    return rows;
}

int
OutputRowCount(const FieldMeta& meta, const DataArray& column) {
    return meta.get_data_type() == DataType::VECTOR_ARRAY
               ? column.vectors().vector_array().data_size()
               : column.scalars().array_data().data_size();
}

// Logical element count of row i. Element-nullable scalar rows are dense:
// one payload value per element, null or not.
int64_t
OutputElementCount(const FieldMeta& meta,
                   const DataArray& column,
                   int i,
                   const std::string& where) {
    if (meta.get_data_type() == DataType::VECTOR_ARRAY) {
        const auto& row = column.vectors().vector_array().data(i);
        if (meta.is_element_nullable()) {
            return row.valid_data_size();
        }
        return row.float_vector().data_size() / kElemVecDim;
    }
    const auto& row = column.scalars().array_data().data(i);
    int64_t count = 0;
    switch (meta.get_element_type()) {
        case DataType::ARRAY:
            count = row.array_data().data_size();
            break;
        case DataType::INT64:
            count = row.long_data().data_size();
            break;
        case DataType::INT8:
            count = row.int_data().data_size();
            break;
        case DataType::VARCHAR:
            count = row.string_data().data_size();
            break;
        default:
            ADD_FAILURE() << where << ": unexpected element type";
    }
    if (meta.is_element_nullable()) {
        EXPECT_EQ(row.valid_data_size(), count)
            << where << ": element validity must cover the dense payload";
    }
    return count;
}

int
ScalarPayloadCount(const proto::schema::ScalarField& row) {
    switch (row.data_case()) {
        case proto::schema::ScalarField::kBoolData:
            return row.bool_data().data_size();
        case proto::schema::ScalarField::kIntData:
            return row.int_data().data_size();
        case proto::schema::ScalarField::kLongData:
            return row.long_data().data_size();
        case proto::schema::ScalarField::kStringData:
            return row.string_data().data_size();
        case proto::schema::ScalarField::kArrayData:
            return row.array_data().data_size();
        case proto::schema::ScalarField::DATA_NOT_SET:
            return 0;
        default:
            ADD_FAILURE() << "unexpected payload " << row.ShortDebugString();
            return -1;
    }
}

// A null struct row: an empty VectorField with the dim set, or a ScalarField
// without values. A stored row is unset when segcore wrote it (column read,
// backfill) and typed when the insert message sent a typed placeholder;
// `require_unset` asks for the former.
void
ExpectNullStructRow(const FieldMeta& meta,
                    const DataArray& column,
                    int i,
                    bool require_unset,
                    const std::string& where) {
    if (meta.get_data_type() == DataType::VECTOR_ARRAY) {
        const auto& row = column.vectors().vector_array().data(i);
        EXPECT_EQ(row.dim(), kElemVecDim) << where;
        EXPECT_TRUE(row.has_float_vector()) << where;
        EXPECT_EQ(row.float_vector().data_size(), 0) << where;
        EXPECT_EQ(row.valid_data_size(), 0) << where;
        return;
    }
    const auto& row = column.scalars().array_data().data(i);
    EXPECT_EQ(ScalarPayloadCount(row), 0)
        << where << ": " << row.ShortDebugString();
    EXPECT_EQ(row.valid_data_size(), 0) << where;
    if (require_unset) {
        EXPECT_EQ(row.data_case(), proto::schema::ScalarField::DATA_NOT_SET)
            << where << ": " << row.ShortDebugString();
    }
}

// A valid row of an added sub-field written before the sub-field existed:
// n null elements. Scalars are dense zero values, a null inner Array is an
// empty leaf-typed ScalarField, and vectors have no physical values.
void
ExpectNullElementsRow(const FieldMeta& meta,
                      const DataArray& column,
                      int i,
                      int64_t n,
                      const std::string& where) {
    auto expect_all_null = [&](const google::protobuf::RepeatedField<bool>& v) {
        ASSERT_EQ(v.size(), n) << where;
        for (bool valid : v) {
            EXPECT_FALSE(valid) << where;
        }
    };
    if (meta.get_data_type() == DataType::VECTOR_ARRAY) {
        const auto& row = column.vectors().vector_array().data(i);
        EXPECT_EQ(row.dim(), kElemVecDim) << where;
        EXPECT_TRUE(row.has_float_vector()) << where;
        EXPECT_EQ(row.float_vector().data_size(), 0) << where;
        expect_all_null(row.valid_data());
        return;
    }
    const auto& row = column.scalars().array_data().data(i);
    expect_all_null(row.valid_data());
    switch (meta.get_element_type()) {
        case DataType::INT64:
            ASSERT_TRUE(row.has_long_data()) << where;
            ASSERT_EQ(row.long_data().data_size(), n) << where;
            for (auto value : row.long_data().data()) {
                EXPECT_EQ(value, 0) << where;
            }
            break;
        case DataType::VARCHAR:
            ASSERT_TRUE(row.has_string_data()) << where;
            ASSERT_EQ(row.string_data().data_size(), n) << where;
            for (const auto& value : row.string_data().data()) {
                EXPECT_TRUE(value.empty()) << where;
            }
            break;
        case DataType::ARRAY:
            ASSERT_TRUE(row.has_array_data()) << where;
            EXPECT_EQ(row.array_data().element_type(),
                      proto::schema::DataType::Int64)
                << where;
            ASSERT_EQ(row.array_data().data_size(), n) << where;
            for (const auto& inner : row.array_data().data()) {
                EXPECT_TRUE(inner.has_long_data()) << where;
                EXPECT_EQ(inner.long_data().data_size(), 0) << where;
                EXPECT_EQ(inner.valid_data_size(), 0) << where;
            }
            break;
        default:
            ADD_FAILURE() << where << ": unexpected added sub-field type";
    }
}

bool
AllElementsNull(const FieldMeta& meta, const DataArray& column, int i) {
    const auto& valid =
        meta.get_data_type() == DataType::VECTOR_ARRAY
            ? column.vectors().vector_array().data(i).valid_data()
            : column.scalars().array_data().data(i).valid_data();
    return std::none_of(
        valid.begin(), valid.end(), [](bool element) { return element; });
}

std::string
OutputRowDebugString(const FieldMeta& meta, const DataArray& column, int i) {
    return meta.get_data_type() == DataType::VECTOR_ARRAY
               ? column.vectors().vector_array().data(i).ShortDebugString()
               : column.scalars().array_data().data(i).ShortDebugString();
}

bool
OutputRowsEqual(const FieldMeta& meta,
                const DataArray& lhs,
                int lhs_row,
                const DataArray& rhs,
                int rhs_row) {
    if (meta.get_data_type() == DataType::VECTOR_ARRAY) {
        return MessageDifferencer::Equals(
            lhs.vectors().vector_array().data(lhs_row),
            rhs.vectors().vector_array().data(rhs_row));
    }
    return MessageDifferencer::Equals(lhs.scalars().array_data().data(lhs_row),
                                      rhs.scalars().array_data().data(rhs_row));
}

// Checks retrieve or search output columns at the proto level: every column
// has one row per output row, row validity in ScalarField / VectorField
// valid_data and, after the sync segcore runs before results go to Go, in
// FieldData.valid_data; each sub-field row has the struct row's element
// count; null struct rows are empty placeholders; and each added sub-field
// row is null elements (history rows) or equals its sibling.
void
ExpectStructOutput(const Schema& schema,
                   const ElementLevelFixture& f,
                   const std::map<FieldId, const DataArray*>& columns,
                   const std::vector<OutputRow>& rows,
                   const std::string& where) {
    const auto n = static_cast<int>(rows.size());
    for (const auto& [id, column] : columns) {
        const auto& meta = schema[id];
        const auto field_where = where + ", " + meta.get_name().get();
        ASSERT_NE(column, nullptr) << field_where;
        EXPECT_EQ(column->field_id(), id.get()) << field_where;
        if (id == f.pk) {
            ASSERT_EQ(column->scalars().long_data().data_size(), n)
                << field_where;
            for (int i = 0; i < n; ++i) {
                EXPECT_EQ(column->scalars().long_data().data(i), rows[i].pk)
                    << field_where << ", output row " << i;
            }
            continue;
        }
        const bool is_vector = meta.get_data_type() == DataType::VECTOR_ARRAY;
        if (is_vector) {
            ASSERT_TRUE(column->vectors().has_vector_array()) << field_where;
            EXPECT_EQ(column->vectors().dim(), kElemVecDim) << field_where;
            EXPECT_EQ(column->vectors().vector_array().dim(), kElemVecDim)
                << field_where;
            EXPECT_EQ(column->vectors().vector_array().element_type(),
                      proto::schema::DataType::FloatVector)
                << field_where;
        } else {
            ASSERT_TRUE(column->scalars().has_array_data()) << field_where;
            EXPECT_EQ(
                column->scalars().array_data().element_type(),
                static_cast<proto::schema::DataType>(meta.get_element_type()))
                << field_where;
        }
        ASSERT_EQ(OutputRowCount(meta, *column), n) << field_where;
        DataArray synced = *column;
        SyncFieldDataRowValidData(&synced);
        const auto& field_valid = is_vector ? synced.vectors().valid_data()
                                            : synced.scalars().valid_data();
        ASSERT_EQ(synced.valid_data_size(), n) << field_where;
        ASSERT_EQ(field_valid.size(), n) << field_where;
        for (int i = 0; i < n; ++i) {
            const auto& r = f.rows[rows[i].row];
            const auto row_where = field_where + ", output row " +
                                   std::to_string(i) + " (fixture row " +
                                   std::to_string(rows[i].row) + ")";
            EXPECT_EQ(synced.valid_data(i), r.has_value()) << row_where;
            EXPECT_EQ(field_valid.Get(i), r.has_value()) << row_where;
            if (!r.has_value()) {
                ExpectNullStructRow(meta, *column, i, false, row_where);
                continue;
            }
            EXPECT_EQ(OutputElementCount(meta, *column, i, row_where),
                      static_cast<int64_t>(r->size()))
                << row_where;
        }
    }

    for (const auto& [added, sibling] : AddedAndSiblingSubFields(f)) {
        auto added_it = columns.find(added);
        if (added_it == columns.end()) {
            continue;
        }
        const auto& meta = schema[added];
        auto sibling_it = columns.find(sibling);
        for (int i = 0; i < n; ++i) {
            const auto& r = f.rows[rows[i].row];
            const auto row_where = where + ", " + meta.get_name().get() +
                                   ", output row " + std::to_string(i) +
                                   " (fixture row " +
                                   std::to_string(rows[i].row) + ")";
            if (!rows[i].added_written) {
                if (r.has_value()) {
                    ExpectNullElementsRow(
                        meta, *added_it->second, i, r->size(), row_where);
                } else {
                    ExpectNullStructRow(
                        meta, *added_it->second, i, true, row_where);
                }
            }
            if (sibling_it == columns.end()) {
                continue;
            }
            // Where the sibling row is empty or all null, or the added
            // sub-field was written with the sibling's values, the two rows
            // must be identical. (A NULL struct row is checked above: its
            // placeholder carries no values either way.)
            const bool same = rows[i].added_written ||
                              (r.has_value() &&
                               AllElementsNull(meta, *sibling_it->second, i));
            if (same) {
                EXPECT_TRUE(OutputRowsEqual(
                    meta, *added_it->second, i, *sibling_it->second, i))
                    << row_where << "\n added:   "
                    << OutputRowDebugString(meta, *added_it->second, i)
                    << "\n sibling: "
                    << OutputRowDebugString(meta, *sibling_it->second, i);
            }
        }
    }
}

std::map<FieldId, const DataArray*>
OutputColumns(const proto::segcore::RetrieveResults& result) {
    std::map<FieldId, const DataArray*> columns;
    for (const auto& field : result.fields_data()) {
        columns[FieldId(field.field_id())] = &field;
    }
    return columns;
}

std::map<FieldId, const DataArray*>
OutputColumns(const std::map<FieldId, std::unique_ptr<DataArray>>& fields) {
    std::map<FieldId, const DataArray*> columns;
    for (const auto& [id, field] : fields) {
        columns[id] = field.get();
    }
    return columns;
}

std::unique_ptr<query::RetrievePlan>
OutputRetrievePlan(const SchemaPtr& schema,
                   const std::vector<FieldId>& fields) {
    auto plan = std::make_unique<query::RetrievePlan>(schema);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->limit_ = 1 << 20;
    plan->field_ids_ = fields;
    plan->access_entries_ = fields;
    return plan;
}

// One segment under test: `schema` is the newest schema (with the added
// sub-fields); `first_written_batch` is the first batch whose rows were
// written with the added sub-fields.
struct OutputSegment {
    std::string kind;
    SegmentInternalInterface* segment;
    SchemaPtr schema;
    int64_t num_rows;
    int64_t first_written_batch;
};

// The output fields of a query on the whole struct, and two partial ones
// (like clips[b] + clips[tags], with one added sub-field each).
std::vector<std::vector<FieldId>>
OutputFieldSets(const ElementLevelFixture& f) {
    return {AllOutputFields(f),
            {f.b, f.ltags},
            {f.pk, f.lang, AddedVectorSubField(f)}};
}

// Offsets to read: all rows, a scrambled order, and subsets that hold only
// the NULL struct row, the empty and NULL rows, and a few scattered rows.
std::vector<std::vector<int64_t>>
OutputOffsetSets(int64_t num_rows) {
    std::vector<int64_t> all(num_rows);
    std::iota(all.begin(), all.end(), 0);
    std::vector<int64_t> scrambled;
    for (int64_t lo = 0, hi = num_rows - 1; lo <= hi; ++lo, --hi) {
        scrambled.push_back(hi);
        if (lo != hi) {
            scrambled.push_back(lo);
        }
    }
    std::vector<int64_t> scattered;
    for (int64_t offset = num_rows - 1; offset >= 0; offset -= 3) {
        scattered.push_back(offset);
    }
    return {all, scrambled, {2}, {1, 2}, scattered};
}

// Retrieve by offsets: the second phase of a query, and the only phase when
// the segment's rows need no merge.
void
CheckRetrieveByOffsets(const OutputSegment& s, const ElementLevelFixture& f) {
    for (const auto& fields : OutputFieldSets(f)) {
        auto plan = OutputRetrievePlan(s.schema, fields);
        for (const auto& offsets : OutputOffsetSets(s.num_rows)) {
            const auto where = s.kind + ", retrieve by " +
                               std::to_string(offsets.size()) + " offsets, " +
                               std::to_string(fields.size()) + " fields";
            auto result = s.segment->Retrieve(nullptr,
                                              plan.get(),
                                              offsets.data(),
                                              offsets.size(),
                                              folly::CancellationToken());
            ASSERT_NE(result, nullptr) << where;
            ASSERT_EQ(result->fields_data_size(),
                      static_cast<int>(fields.size()))
                << where;
            ExpectStructOutput(
                *s.schema,
                f,
                OutputColumns(*result),
                ExpectedOutputRows(f, offsets, s.first_written_batch),
                where);
        }
    }
}

// Search output: FillTargetEntry on the hits' row offsets, which come in
// distance order and repeat a row hit through several of its elements.
std::map<FieldId, std::unique_ptr<DataArray>>
FillSearchOutput(const OutputSegment& s,
                 const std::vector<FieldId>& fields,
                 const std::vector<int64_t>& offsets) {
    query::Plan plan(s.schema);
    plan.target_entries_ = fields;
    plan.access_entries_ = fields;
    SearchResult result;
    result.seg_offsets_ = offsets;
    result.distances_.resize(offsets.size(), 0.0f);
    s.segment->FillTargetEntry(&plan, result);
    return std::move(result.output_fields_data_);
}

void
CheckSearchOutput(const OutputSegment& s, const ElementLevelFixture& f) {
    std::vector<int64_t> hits;
    for (int64_t offset = s.num_rows - 1; offset >= 0; offset -= 2) {
        hits.push_back(offset);
        hits.push_back((offset * 5 + 3) % s.num_rows);
    }
    for (const auto& fields : OutputFieldSets(f)) {
        const auto where = s.kind + ", search output, " +
                           std::to_string(fields.size()) + " fields";
        auto output = FillSearchOutput(s, fields, hits);
        ASSERT_EQ(output.size(), fields.size()) << where;
        ExpectStructOutput(*s.schema,
                           f,
                           OutputColumns(output),
                           ExpectedOutputRows(f, hits, s.first_written_batch),
                           where);
    }
}

struct FilteredOutput {
    std::string mode;
    // offset / element_indices of the filter, and the output columns.
    std::unique_ptr<proto::segcore::RetrieveResults> result;
};

// Runs a filtered query: in two passes as a query over several segments does
// (the filter with pk only, then the output fields by offset), and also in
// one pass when the segment has every output field. A one-pass Retrieve on a
// segment that lacks an output field throws in its output-size estimate
// (get_field_avg_size looks the field up in the segment's schema); in
// production LazyCheckSchema reopens the segment first.
std::vector<FilteredOutput>
RunFilteredRetrieve(const OutputSegment& s,
                    const ElementLevelFixture& f,
                    const std::function<plan::PlanNodePtr()>& make_nodes,
                    const std::vector<FieldId>& fields) {
    auto run = [&](const std::vector<FieldId>& plan_fields) {
        auto plan = OutputRetrievePlan(s.schema, plan_fields);
        plan->plan_node_->plannodes_ = make_nodes();
        return s.segment->Retrieve(
            nullptr, plan.get(), MAX_TIMESTAMP, DEFAULT_MAX_OUTPUT_SIZE, false);
    };
    std::vector<FilteredOutput> outputs;
    if (std::all_of(fields.begin(), fields.end(), [&](FieldId id) {
            return s.segment->is_field_exist(id);
        })) {
        outputs.push_back({"one pass", run(fields)});
    }
    auto first = run({f.pk});
    std::vector<int64_t> offsets(first->offset().begin(),
                                 first->offset().end());
    auto plan = OutputRetrievePlan(s.schema, fields);
    auto second = s.segment->Retrieve(nullptr,
                                      plan.get(),
                                      offsets.data(),
                                      offsets.size(),
                                      folly::CancellationToken());
    *second->mutable_offset() = first->offset();
    second->set_element_level(first->element_level());
    *second->mutable_element_indices() = first->element_indices();
    outputs.push_back({"two passes", std::move(second)});
    return outputs;
}

// A filtered query: the rows come from the filter.
void
CheckFilteredRetrieve(const OutputSegment& s, const ElementLevelFixture& f) {
    using E = ElementLevelExprs;
    std::vector<int64_t> wanted;
    std::vector<proto::plan::GenericValue> pks;
    for (int64_t offset = 0; offset < s.num_rows; ++offset) {
        if (offset % 3 != 1) {
            wanted.push_back(offset);
            pks.push_back(
                E::Int(ExpectedOutputRow(f, offset, s.first_written_batch).pk));
        }
    }
    auto filter = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(f.pk, DataType::INT64), pks);
    auto make_nodes = [&]() -> plan::PlanNodePtr {
        std::vector<plan::PlanNodePtr> sources = {
            std::make_shared<plan::FilterBitsNode>(
                "1001", filter, std::vector<plan::PlanNodePtr>{})};
        return std::make_shared<plan::MvccNode>("1002", sources);
    };
    for (const auto& fields : OutputFieldSets(f)) {
        for (auto& [mode, result] :
             RunFilteredRetrieve(s, f, make_nodes, fields)) {
            const auto where = s.kind + ", filtered retrieve in " + mode +
                               ", " + std::to_string(fields.size()) + " fields";
            ASSERT_NE(result, nullptr) << where;
            std::vector<int64_t> offsets(result->offset().begin(),
                                         result->offset().end());
            EXPECT_EQ(offsets, wanted) << where;
            ExpectStructOutput(
                *s.schema,
                f,
                OutputColumns(*result),
                ExpectedOutputRows(f, offsets, s.first_written_batch),
                where);
        }
    }
}

// Checks element_filter(s, $[b] IS NULL) with output fields: rows come back
// whole, with the hit elements' logical positions (null elements keep their
// slot) in element_indices, and every sub-field row has an element at each
// of them.
void
CheckElementFilterResult(const OutputSegment& s,
                         const ElementLevelFixture& f,
                         const proto::segcore::RetrieveResults& result,
                         const std::string& where) {
    ASSERT_TRUE(result.element_level()) << where;
    ASSERT_EQ(result.element_indices_size(), result.offset_size()) << where;

    std::map<int64_t, std::vector<int32_t>> want;
    for (int64_t offset = 0; offset < s.num_rows; ++offset) {
        const auto& r = f.rows[offset % f.num_rows()];
        if (!r.has_value()) {
            continue;
        }
        for (size_t j = 0; j < r->size(); ++j) {
            if (!(*r)[j].b.has_value()) {
                want[offset].push_back(static_cast<int32_t>(j));
            }
        }
    }
    std::map<int64_t, std::vector<int32_t>> got;
    std::vector<int64_t> offsets;
    for (int i = 0; i < result.offset_size(); ++i) {
        offsets.push_back(result.offset(i));
        auto& indices = got[result.offset(i)];
        indices.assign(result.element_indices(i).indices().begin(),
                       result.element_indices(i).indices().end());
    }
    EXPECT_EQ(got, want) << where;
    const auto rows = ExpectedOutputRows(f, offsets, s.first_written_batch);
    const auto columns = OutputColumns(result);
    ExpectStructOutput(*s.schema, f, columns, rows, where);

    // The hit element is the null element at that position of every
    // element-nullable sub-field, the added ones included.
    for (int i = 0; i < result.offset_size(); ++i) {
        for (auto index : result.element_indices(i).indices()) {
            const auto hit_where = where + ", output row " + std::to_string(i) +
                                   ", element " + std::to_string(index);
            for (auto id : {f.b, f.lnum, f.lang, f.ltags}) {
                const auto& row =
                    columns.at(id)->scalars().array_data().data(i);
                ASSERT_LT(index, row.valid_data_size()) << hit_where;
                if (id == f.b || id == f.lnum || !rows[i].added_written) {
                    EXPECT_FALSE(row.valid_data(index)) << hit_where;
                }
            }
            for (auto id : {f.vec, AddedVectorSubField(f)}) {
                const auto& row =
                    columns.at(id)->vectors().vector_array().data(i);
                ASSERT_LT(index, row.valid_data_size()) << hit_where;
            }
        }
    }
}

void
CheckElementFilterOutput(const OutputSegment& s, const ElementLevelFixture& f) {
    using E = ElementLevelExprs;
    const auto b = E::Scalar(f.b, DataType::INT64);
    auto make_nodes = [&]() -> plan::PlanNodePtr {
        auto mvcc = std::make_shared<plan::MvccNode>(
            "1002", std::vector<plan::PlanNodePtr>{});
        return std::make_shared<plan::ElementFilterBitsNode>(
            "1003", E::IsNull(b), "s", std::vector<plan::PlanNodePtr>{mvcc});
    };
    for (auto& [mode, result] :
         RunFilteredRetrieve(s, f, make_nodes, AllOutputFields(f))) {
        CheckElementFilterResult(
            s,
            f,
            *result,
            s.kind + ", element_filter($[b] IS NULL) in " + mode);
    }
}

void
CheckStructOutput(const OutputSegment& s, const ElementLevelFixture& f) {
    SCOPED_TRACE(s.kind);
    CheckRetrieveByOffsets(s, f);
    CheckSearchOutput(s, f);
    CheckFilteredRetrieve(s, f);
    CheckElementFilterOutput(s, f);
}

// Output of `lhs` and `rhs` for the same rows must be identical: an added
// sub-field read without a column (the segment predates it) and read from
// the backfilled column after Reopen, or from a growing and a sealed segment.
void
ExpectSameOutput(const OutputSegment& lhs,
                 const OutputSegment& rhs,
                 const std::vector<FieldId>& fields,
                 const std::vector<int64_t>& offsets) {
    auto lhs_plan = OutputRetrievePlan(lhs.schema, fields);
    auto rhs_plan = OutputRetrievePlan(rhs.schema, fields);
    auto lhs_result = lhs.segment->Retrieve(nullptr,
                                            lhs_plan.get(),
                                            offsets.data(),
                                            offsets.size(),
                                            folly::CancellationToken());
    auto rhs_result = rhs.segment->Retrieve(nullptr,
                                            rhs_plan.get(),
                                            offsets.data(),
                                            offsets.size(),
                                            folly::CancellationToken());
    ASSERT_EQ(lhs_result->fields_data_size(), rhs_result->fields_data_size());
    for (int i = 0; i < lhs_result->fields_data_size(); ++i) {
        DataArray lhs_field = lhs_result->fields_data(i);
        DataArray rhs_field = rhs_result->fields_data(i);
        SyncFieldDataRowValidData(&lhs_field);
        SyncFieldDataRowValidData(&rhs_field);
        EXPECT_TRUE(MessageDifferencer::Equals(lhs_field, rhs_field))
            << lhs.kind << " vs " << rhs.kind << ", field "
            << lhs_field.field_id() << "\n " << lhs.kind << ": "
            << lhs_field.ShortDebugString() << "\n " << rhs.kind << ": "
            << rhs_field.ShortDebugString();
    }
}

// The C++ search reduce: per-segment output gathered by FillTargetEntry and
// merged into the final hit order by MergeDataArray, as
// search_result_export_c.cpp does.
void
CheckMergedSearchOutput(const std::vector<const OutputSegment*>& segments,
                        const ElementLevelFixture& f) {
    // (segment, offset) in final hit order, alternating segments.
    std::vector<std::pair<size_t, int64_t>> hits;
    for (int64_t k = 0; k < 12; ++k) {
        const auto s = static_cast<size_t>(k) % segments.size();
        hits.emplace_back(s, (k * 5 + 2) % segments[s]->num_rows);
    }
    const auto fields = AllOutputFields(f);
    std::vector<std::vector<int64_t>> offsets(segments.size());
    std::vector<std::vector<size_t>> positions(segments.size());
    for (size_t pos = 0; pos < hits.size(); ++pos) {
        offsets[hits[pos].first].push_back(hits[pos].second);
        positions[hits[pos].first].push_back(pos);
    }
    std::vector<std::map<FieldId, std::unique_ptr<DataArray>>> outputs;
    for (size_t s = 0; s < segments.size(); ++s) {
        outputs.push_back(FillSearchOutput(*segments[s], fields, offsets[s]));
    }
    const auto& schema = *segments.front()->schema;
    std::vector<MergeBase> bases(hits.size());
    for (size_t s = 0; s < segments.size(); ++s) {
        for (size_t i = 0; i < positions[s].size(); ++i) {
            bases[positions[s][i]] = MergeBase(&outputs[s], i);
        }
    }
    std::map<FieldId, std::unique_ptr<DataArray>> merged;
    for (auto id : fields) {
        merged[id] = MergeDataArray(bases, schema[id]);
    }
    std::vector<OutputRow> rows;
    for (const auto& [s, offset] : hits) {
        rows.push_back(
            ExpectedOutputRow(f, offset, segments[s]->first_written_batch));
    }
    std::string kinds;
    for (const auto* s : segments) {
        kinds += (kinds.empty() ? "" : " + ") + s->kind;
    }
    ExpectStructOutput(
        schema, f, OutputColumns(merged), rows, "merged search of " + kinds);
}

}  // namespace

// 7.1.4: a sealed segment written before s[lang], s[lnum], s[ltags] and
// s[lvec] were added. Read with the newer plan schema and no Reopen, the
// added sub-fields have no column (CreateNullStructSubFieldDataArray); after
// Reopen they are read from the backfilled column. Both must give dense rows
// that line up with the stored sub-fields, including on the NULL struct row.
TEST(StructSubFieldOutput, SealedSegmentWrittenBeforeSubFieldsWereAdded) {
    auto f = BuildElementLevelMatrixFixture();
    const auto segment_path = ElementLevelSegmentPath("output");
    std::filesystem::remove_all(segment_path);
    std::filesystem::remove_all(segment_path + "_reopened");
    auto directory_guard = folly::makeGuard([&segment_path] {
        std::filesystem::remove_all(segment_path);
        std::filesystem::remove_all(segment_path + "_reopened");
    });

    auto history = LoadElementLevelSealed(f, segment_path);
    ASSERT_GT(history->num_chunk(f.b), 1);
    const OutputSegment absent{
        "sealed, sub-fields absent",
        history.get(),
        WithAddedOutputSubFields(*history->get_schema_snapshot(), f),
        f.num_rows(),
        1};
    for (auto [added, sibling] : AddedAndSiblingSubFields(f)) {
        ASSERT_FALSE(history->is_field_exist(added));
    }
    CheckStructOutput(absent, f);

    auto reopened_segment =
        LoadElementLevelSealed(f, segment_path + "_reopened");
    reopened_segment->Reopen(
        WithAddedOutputSubFields(*reopened_segment->get_schema_snapshot(), f));
    const OutputSegment reopened{"sealed, reopened",
                                 reopened_segment.get(),
                                 reopened_segment->get_schema_snapshot(),
                                 f.num_rows(),
                                 1};
    for (auto [added, sibling] : AddedAndSiblingSubFields(f)) {
        ASSERT_TRUE(reopened_segment->is_field_exist(added));
    }
    CheckStructOutput(reopened, f);

    for (const auto& offsets : OutputOffsetSets(f.num_rows())) {
        ExpectSameOutput(absent, reopened, AllOutputFields(f), offsets);
    }
}

// 7.1.4 on growing segments, with chunks of two rows:
// - "absent": rows inserted with the old schema, read with the newer plan
//   schema before the segment is reopened;
// - "mixed": the same rows backfilled by Reopen (batch 0), then a batch from
//   a proxy that predates the change (no added sub-fields in the message,
//   batch 1), then a batch written with values (batch 2).
// A sealed segment from before the change is merged with the mixed one as
// the C++ search reduce does.
TEST(StructSubFieldOutput, GrowingSegmentWrittenBeforeSubFieldsWereAdded) {
    auto f = BuildElementLevelMatrixFixture();
    auto& config = SegcoreConfig::default_config();
    ScopedSegcoreConfigRestore config_restore(config);
    config.set_chunk_rows(2);

    auto history = LoadElementLevelGrowing(f);
    ASSERT_GT(history->num_chunk(f.b), 1);
    const OutputSegment absent{"growing, sub-fields absent",
                               history.get(),
                               WithAddedOutputSubFields(*f.schema, f),
                               f.num_rows(),
                               1};
    for (auto [added, sibling] : AddedAndSiblingSubFields(f)) {
        ASSERT_FALSE(history->is_field_exist(added));
    }
    CheckStructOutput(absent, f);

    auto mixed_segment = LoadElementLevelGrowing(f);
    mixed_segment->Reopen(WithAddedOutputSubFields(*f.schema, f));
    InsertOutputBatch(mixed_segment.get(), f, 1, false);
    InsertOutputBatch(mixed_segment.get(), f, 2, true);
    const OutputSegment mixed{"growing, reopened and written",
                              mixed_segment.get(),
                              mixed_segment->get_schema_snapshot(),
                              3 * f.num_rows(),
                              2};
    CheckStructOutput(mixed, f);

    for (const auto& offsets : OutputOffsetSets(f.num_rows())) {
        ExpectSameOutput(absent, mixed, AllOutputFields(f), offsets);
    }

    const auto segment_path = ElementLevelSegmentPath("output_merge");
    std::filesystem::remove_all(segment_path);
    auto directory_guard = folly::makeGuard(
        [&segment_path] { std::filesystem::remove_all(segment_path); });
    auto sealed_history = LoadElementLevelSealed(f, segment_path);
    const OutputSegment sealed_absent{
        "sealed, sub-fields absent",
        sealed_history.get(),
        WithAddedOutputSubFields(*sealed_history->get_schema_snapshot(), f),
        f.num_rows(),
        1};
    {
        // Only the added sub-fields: a stored NULL struct row of the growing
        // segment keeps the typed placeholder of the fixture's insert message.
        SCOPED_TRACE("growing vs sealed");
        std::vector<FieldId> added_fields;
        for (const auto& [added, sibling] : AddedAndSiblingSubFields(f)) {
            added_fields.push_back(added);
        }
        for (const auto& offsets : OutputOffsetSets(f.num_rows())) {
            ExpectSameOutput(absent, sealed_absent, added_fields, offsets);
        }
    }
    CheckMergedSearchOutput({&sealed_absent, &mixed}, f);
    CheckMergedSearchOutput({&mixed, &absent, &sealed_absent}, f);
}
