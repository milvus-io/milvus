// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICEN_SE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRAN_TIES OR CON_DITION_S OF AN_Y KIN_D, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>
#include <atomic>
#include <regex>

#include "cachinglayer/Manager.h"
#include "common/Common.h"
#include "exec/QueryContext.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "pb/plan.pb.h"
#include "index/InvertedIndexTantivy.h"
#include "common/Schema.h"

#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "query/PlanProto.h"
#include "query/ExecPlanNodeVisitor.h"

#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;

template <typename T>
SchemaPtr
GenTestSchema() {
    auto schema_ = std::make_shared<Schema>();
    schema_->AddDebugField(
        "fvec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk = schema_->AddDebugField("pk", DataType::INT64);
    schema_->set_primary_field_id(pk);

    if constexpr (std::is_same_v<T, bool>) {
        schema_->AddDebugArrayField("array", DataType::BOOL, false);
    } else if constexpr (std::is_same_v<T, int8_t>) {
        schema_->AddDebugArrayField("array", DataType::INT8, false);
    } else if constexpr (std::is_same_v<T, int16_t>) {
        schema_->AddDebugArrayField("array", DataType::INT16, false);
    } else if constexpr (std::is_same_v<T, int32_t>) {
        schema_->AddDebugArrayField("array", DataType::INT32, false);
    } else if constexpr (std::is_same_v<T, int64_t>) {
        schema_->AddDebugArrayField("array", DataType::INT64, false);
    } else if constexpr (std::is_same_v<T, float>) {
        schema_->AddDebugArrayField("array", DataType::FLOAT, false);
    } else if constexpr (std::is_same_v<T, double>) {
        schema_->AddDebugArrayField("array", DataType::DOUBLE, false);
    } else if constexpr (std::is_same_v<T, std::string>) {
        schema_->AddDebugArrayField("array", DataType::VARCHAR, false);
    }

    return schema_;
}

template <typename T>
class ArrayInvertedIndexTest : public ::testing::Test {
 public:
    void
    SetUp() override {
        schema_ = GenTestSchema<T>();
        N_ = 3000;
        uint64_t seed = 19190504;
        auto raw_data = DataGen(schema_, N_, seed);
        auto array_col =
            raw_data.get_col(schema_->get_field_id(FieldName("array")))
                ->scalars()
                .array_data()
                .data();
        for (size_t i = 0; i < N_; i++) {
            boost::container::vector<T> array;
            if constexpr (std::is_same_v<T, bool>) {
                for (size_t j = 0; j < array_col[i].bool_data().data_size();
                     j++) {
                    array.push_back(array_col[i].bool_data().data(j));
                }
            } else if constexpr (std::is_same_v<T, int64_t>) {
                for (size_t j = 0; j < array_col[i].long_data().data_size();
                     j++) {
                    array.push_back(array_col[i].long_data().data(j));
                }
            } else if constexpr (std::is_integral_v<T>) {
                for (size_t j = 0; j < array_col[i].int_data().data_size();
                     j++) {
                    array.push_back(array_col[i].int_data().data(j));
                }
            } else if constexpr (std::is_floating_point_v<T>) {
                for (size_t j = 0; j < array_col[i].float_data().data_size();
                     j++) {
                    array.push_back(array_col[i].float_data().data(j));
                }
            } else if constexpr (std::is_same_v<T, std::string>) {
                for (size_t j = 0; j < array_col[i].string_data().data_size();
                     j++) {
                    array.push_back(array_col[i].string_data().data(j));
                }
            }
            vec_of_array_.push_back(array);
        }
        seg_ = CreateSealedWithFieldDataLoaded(schema_, raw_data);
        LoadInvertedIndex();
    }

    void
    TearDown() override {
    }

    void
    LoadInvertedIndex() {
        auto index = std::make_unique<index::InvertedIndexTantivy<T>>();
        Config cfg;
        cfg["is_array"] = true;
        index->BuildWithRawDataForUT(N_, vec_of_array_.data(), cfg);
        LoadIndexInfo info{};
        info.field_id = schema_->get_field_id(FieldName("array")).get();
        info.index_params = GenIndexParams(index.get());
        info.cache_index = CreateTestCacheIndex("test", std::move(index));
        seg_->LoadIndex(info);
    }

 public:
    SchemaPtr schema_;
    SegmentSealedUPtr seg_;
    int64_t N_;
    std::vector<boost::container::vector<T>> vec_of_array_;
};

TYPED_TEST_SUITE_P(ArrayInvertedIndexTest);

TYPED_TEST_P(ArrayInvertedIndexTest, ArrayContainsAny) {
    const auto& meta = this->schema_->operator[](FieldName("array"));
    auto column_info = test::GenColumnInfo(
        meta.get_id().get(),
        static_cast<proto::schema::DataType>(meta.get_data_type()),
        false,
        false,
        static_cast<proto::schema::DataType>(meta.get_element_type()));
    auto contains_expr = std::make_unique<proto::plan::JSONContainsExpr>();
    contains_expr->set_allocated_column_info(column_info);
    contains_expr->set_op(proto::plan::JSONContainsExpr_JSONOp::
                              JSONContainsExpr_JSONOp_ContainsAny);
    contains_expr->set_elements_same_type(true);
    for (const auto& elem : this->vec_of_array_[0]) {
        auto t = test::GenGenericValue(elem);
        contains_expr->mutable_elements()->AddAllocated(t);
    }
    auto expr = test::GenExpr();
    expr->set_allocated_json_contains_expr(contains_expr.release());

    auto parser = ProtoParser(this->schema_);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);

    auto segpromote = dynamic_cast<ChunkedSegmentSealedImpl*>(this->seg_.get());
    BitsetType final;
    final = ExecuteQueryExpr(parsed, segpromote, this->N_, MAX_TIMESTAMP);

    std::unordered_set<TypeParam> elems(this->vec_of_array_[0].begin(),
                                        this->vec_of_array_[0].end());
    auto ref = [this, &elems](size_t offset) -> bool {
        std::unordered_set<TypeParam> row(this->vec_of_array_[offset].begin(),
                                          this->vec_of_array_[offset].end());
        if (elems.empty()) {
            return false;
        }

        for (const auto& elem : elems) {
            if (row.find(elem) != row.end()) {
                return true;
            }
        }
        return false;
    };
    ASSERT_EQ(final.size(), this->N_);
    for (size_t i = 0; i < this->N_; i++) {
        ASSERT_EQ(final[i], ref(i)) << "i: " << i << ", final[i]: " << final[i]
                                    << ", ref(i): " << ref(i);
    }
}

TYPED_TEST_P(ArrayInvertedIndexTest, ArrayContainsAll) {
    const auto& meta = this->schema_->operator[](FieldName("array"));
    auto column_info = test::GenColumnInfo(
        meta.get_id().get(),
        static_cast<proto::schema::DataType>(meta.get_data_type()),
        false,
        false,
        static_cast<proto::schema::DataType>(meta.get_element_type()));
    auto contains_expr = std::make_unique<proto::plan::JSONContainsExpr>();
    contains_expr->set_allocated_column_info(column_info);
    contains_expr->set_op(proto::plan::JSONContainsExpr_JSONOp::
                              JSONContainsExpr_JSONOp_ContainsAll);
    contains_expr->set_elements_same_type(true);
    for (const auto& elem : this->vec_of_array_[0]) {
        auto t = test::GenGenericValue(elem);
        contains_expr->mutable_elements()->AddAllocated(t);
    }
    auto expr = test::GenExpr();
    expr->set_allocated_json_contains_expr(contains_expr.release());

    auto parser = ProtoParser(this->schema_);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);

    auto segpromote = dynamic_cast<ChunkedSegmentSealedImpl*>(this->seg_.get());
    BitsetType final;
    final = ExecuteQueryExpr(parsed, segpromote, this->N_, MAX_TIMESTAMP);

    std::unordered_set<TypeParam> elems(this->vec_of_array_[0].begin(),
                                        this->vec_of_array_[0].end());
    auto ref = [this, &elems](size_t offset) -> bool {
        std::unordered_set<TypeParam> row(this->vec_of_array_[offset].begin(),
                                          this->vec_of_array_[offset].end());
        if (elems.empty()) {
            return true;
        }

        for (const auto& elem : elems) {
            if (row.find(elem) == row.end()) {
                return false;
            }
        }
        return true;
    };
    ASSERT_EQ(final.size(), this->N_);
    for (size_t i = 0; i < this->N_; i++) {
        ASSERT_EQ(final[i], ref(i)) << "i: " << i << ", final[i]: " << final[i]
                                    << ", ref(i): " << ref(i);
    }
}

TYPED_TEST_P(ArrayInvertedIndexTest, ArrayEqual) {
    if (std::is_floating_point_v<TypeParam>) {
        GTEST_SKIP() << "not accurate to perform equal comparison on floating "
                        "point number";
    }

    const auto& meta = this->schema_->operator[](FieldName("array"));
    auto column_info = test::GenColumnInfo(
        meta.get_id().get(),
        static_cast<proto::schema::DataType>(meta.get_data_type()),
        false,
        false,
        static_cast<proto::schema::DataType>(meta.get_element_type()));
    auto unary_range_expr = std::make_unique<proto::plan::UnaryRangeExpr>();
    unary_range_expr->set_allocated_column_info(column_info);
    unary_range_expr->set_op(proto::plan::OpType::Equal);
    auto arr = new proto::plan::GenericValue;
    arr->mutable_array_val()->set_element_type(
        static_cast<proto::schema::DataType>(meta.get_element_type()));
    arr->mutable_array_val()->set_same_type(true);
    for (const auto& elem : this->vec_of_array_[0]) {
        auto e = test::GenGenericValue(elem);
        arr->mutable_array_val()->mutable_array()->AddAllocated(e);
    }
    unary_range_expr->set_allocated_value(arr);
    auto expr = test::GenExpr();
    expr->set_allocated_unary_range_expr(unary_range_expr.release());

    auto parser = ProtoParser(this->schema_);
    auto typed_expr = parser.ParseExprs(*expr);
    auto parsed =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, typed_expr);

    auto segpromote = dynamic_cast<ChunkedSegmentSealedImpl*>(this->seg_.get());
    BitsetType final;
    final = ExecuteQueryExpr(parsed, segpromote, this->N_, MAX_TIMESTAMP);

    auto ref = [this](size_t offset) -> bool {
        if (this->vec_of_array_[0].size() !=
            this->vec_of_array_[offset].size()) {
            return false;
        }
        auto size = this->vec_of_array_[0].size();
        for (size_t i = 0; i < size; i++) {
            if (this->vec_of_array_[0][i] != this->vec_of_array_[offset][i]) {
                return false;
            }
        }
        return true;
    };
    ASSERT_EQ(final.size(), this->N_);
    for (size_t i = 0; i < this->N_; i++) {
        ASSERT_EQ(final[i], ref(i)) << "i: " << i << ", final[i]: " << final[i]
                                    << ", ref(i): " << ref(i);
    }
}

using ElementType = testing::
    Types<bool, int8_t, int16_t, int32_t, int64_t, float, double, std::string>;

REGISTER_TYPED_TEST_CASE_P(ArrayInvertedIndexTest,
                           ArrayContainsAny,
                           ArrayContainsAll,
                           ArrayEqual);

INSTANTIATE_TYPED_TEST_SUITE_P(Naive, ArrayInvertedIndexTest, ElementType);

namespace {

ColumnVectorPtr
EvalFilterInBatches(const expr::TypedExprPtr& logical_expr,
                    const SegmentInternalInterface* segment,
                    int64_t active_count) {
    auto query_context = std::make_shared<exec::QueryContext>(
        DEAFULT_QUERY_ID, segment, active_count, MAX_TIMESTAMP);
    exec::ExecContext exec_context(query_context.get());
    auto compiled =
        exec::CompileExpressions({logical_expr}, &exec_context, {}, false);
    AssertInfo(compiled.size() == 1,
               "expected one compiled expression, got {}",
               compiled.size());

    exec::EvalCtx eval_context(&exec_context);
    TargetBitmap combined_result;
    TargetBitmap combined_validity;
    int64_t processed_rows = 0;
    while (processed_rows < active_count) {
        VectorPtr result;
        compiled[0]->Eval(eval_context, result);
        AssertInfo(result != nullptr,
                   "expression evaluation stopped after {} of {} rows",
                   processed_rows,
                   active_count);
        auto column = std::dynamic_pointer_cast<ColumnVector>(result);
        AssertInfo(column != nullptr && column->IsBitmap(),
                   "expected bitmap column result");
        const auto batch_size = column->size();
        AssertInfo(
            batch_size > 0 && processed_rows + batch_size <= active_count,
            "invalid expression batch size {} after {} of {} rows",
            batch_size,
            processed_rows,
            active_count);
        combined_result.append(
            TargetBitmapView(column->GetRawData(), batch_size));
        combined_validity.append(
            TargetBitmapView(column->GetValidRawData(), batch_size));
        processed_rows += batch_size;
    }
    return std::make_shared<ColumnVector>(std::move(combined_result),
                                          std::move(combined_validity));
}

class NullableInt64ArrayInvertedIndex
    : public index::InvertedIndexTantivy<int64_t> {
 public:
    explicit NullableInt64ArrayInvertedIndex(
        std::shared_ptr<std::atomic<size_t>> query_count)
        : query_count_(std::move(query_count)) {
    }

    void
    SetNullOffsets(std::vector<size_t> offsets) {
        null_offset_ = std::move(offsets);
    }

    void
    InApplyCallback(size_t n,
                    const int64_t* values,
                    const std::function<void(size_t)>& callback) override {
        query_count_->fetch_add(1, std::memory_order_relaxed);
        index::InvertedIndexTantivy<int64_t>::InApplyCallback(
            n, values, callback);
    }

 private:
    std::shared_ptr<std::atomic<size_t>> query_count_;
};

struct ExprBatchSizeGuard {
    explicit ExprBatchSizeGuard(int64_t batch_size)
        : old_batch_size_(EXEC_EVAL_EXPR_BATCH_SIZE.exchange(batch_size)) {
    }

    ~ExprBatchSizeGuard() {
        EXEC_EVAL_EXPR_BATCH_SIZE.store(old_batch_size_);
    }

    int64_t old_batch_size_;
};

proto::plan::GenericValue
MakeInt64ArrayValue(const std::vector<int64_t>& values) {
    proto::plan::GenericValue value;
    auto* array = value.mutable_array_val();
    array->set_element_type(proto::schema::DataType::Int64);
    array->set_same_type(true);
    for (auto element : values) {
        array->add_array()->set_int64_val(element);
    }
    return value;
}

}  // namespace

TEST(ArrayInvertedIndexRegression,
     ArrayNotEqualUsesIndexCandidatesAsRowLevelPrefilter) {
    const std::vector<std::vector<int64_t>> arrays = {
        {1, 1, 2},     // equal
        {},            // null
        {1, 2},        // missing duplicate
        {},            // null
        {1, 1, 2, 3},  // extra element
        {},            // null
        {1, 2, 1},     // different order
        {},            // null
        {1, 1, 1, 2},  // different duplicate count
        {},            // null
        {1, 3},        // missing indexed element
        {},            // null
        {1, 1, 2},     // equal in the second raw data chunk
        {},            // null
    };
    const auto row_count = static_cast<int64_t>(arrays.size());
    std::vector<bool> expected_validity(row_count);
    std::vector<bool> expected_values(row_count);
    const std::vector<int64_t> literal = {1, 1, 2};
    for (int64_t row = 0; row < row_count; ++row) {
        const bool valid = row % 2 == 0;
        expected_validity[row] = valid;
        expected_values[row] = valid && arrays[row] != literal;
    }

    ExprBatchSizeGuard batch_size_guard(4);
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fvec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto array_fid = schema->AddDebugArrayField("array", DataType::INT64, true);
    schema->set_primary_field_id(pk_fid);

    auto raw_data = DataGen(schema, row_count, 42, 0, 1, 3);
    proto::schema::FieldData* array_field = nullptr;
    for (auto& field : *raw_data.raw_->mutable_fields_data()) {
        if (field.field_id() == array_fid.get()) {
            array_field = &field;
            break;
        }
    }
    ASSERT_NE(array_field, nullptr);
    auto* array_data = array_field->mutable_scalars()->mutable_array_data();
    ASSERT_EQ(array_data->data_size(), row_count);
    auto* valid_data = array_field->mutable_valid_data();
    ASSERT_EQ(valid_data->size(), row_count);

    std::vector<boost::container::vector<int64_t>> index_arrays;
    index_arrays.reserve(row_count);
    std::vector<size_t> null_offsets;
    for (int64_t row = 0; row < row_count; ++row) {
        const bool valid = expected_validity[row];
        valid_data->at(row) = valid;

        auto* values =
            array_data->mutable_data(row)->mutable_long_data()->mutable_data();
        values->Clear();
        values->Add(arrays[row].begin(), arrays[row].end());

        if (valid) {
            index_arrays.emplace_back(arrays[row].begin(), arrays[row].end());
        } else {
            index_arrays.emplace_back();
            null_offsets.push_back(row);
        }
    }

    auto raw_segment = CreateSealedWithFieldDataLoaded(schema, raw_data);
    auto indexed_segment = CreateSealedWithFieldDataLoaded(schema, raw_data);

    // Keep the scalar index field-wide, but reload the indexed segment's raw
    // ARRAY data as two chunks. This matches the 2.6 sealed layout and verifies
    // that index callback offsets are interpreted as global row offsets.
    auto full_array_field = segcore::CreateFieldDataFromDataArray(
        row_count, array_field, schema->operator[](array_fid));
    auto* full_array_data =
        static_cast<const milvus::Array*>(full_array_field->Data());
    auto* full_valid_data = full_array_field->ValidData();
    std::vector<FieldDataPtr> array_chunks;
    const int64_t first_chunk_size = row_count / 2;
    for (const auto [offset, size] : std::vector<std::pair<int64_t, int64_t>>{
             {0, first_chunk_size},
             {first_chunk_size, row_count - first_chunk_size}}) {
        auto chunk = std::make_shared<FieldData<milvus::Array>>(
            DataType::ARRAY, true, size);
        chunk->FillFieldData(
            full_array_data + offset, full_valid_data, size, offset);
        array_chunks.emplace_back(std::move(chunk));
    }
    indexed_segment->DropFieldData(array_fid);
    auto cm = storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto array_load_info = PrepareSingleFieldInsertBinlog(
        9101, 9102, 9103, array_fid.get(), array_chunks, cm);
    indexed_segment->LoadFieldData(array_load_info);
    ASSERT_EQ(indexed_segment->num_chunk_data(array_fid), 2);

    auto logical_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(array_fid, DataType::ARRAY, DataType::INT64, {}, true),
        proto::plan::OpType::NotEqual,
        MakeInt64ArrayValue(literal),
        std::vector<proto::plan::GenericValue>{});
    auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                       logical_expr);

    auto index_query_count = std::make_shared<std::atomic<size_t>>(0);
    auto index =
        std::make_unique<NullableInt64ArrayInvertedIndex>(index_query_count);
    Config config;
    config["is_array"] = true;
    index->BuildWithRawDataForUT(row_count, index_arrays.data(), config);
    index->SetNullOffsets(null_offsets);

    LoadIndexInfo load_info{};
    load_info.field_id = array_fid.get();
    load_info.field_type = DataType::ARRAY;
    load_info.element_type = DataType::INT64;
    load_info.index_params = GenIndexParams(index.get());
    load_info.cache_index =
        CreateTestCacheIndex("array_not_equal_row_level", std::move(index));
    indexed_segment->LoadIndex(load_info);

    auto raw_result =
        EvalFilterInBatches(logical_expr, raw_segment.get(), row_count);
    auto indexed_result =
        EvalFilterInBatches(logical_expr, indexed_segment.get(), row_count);
    EXPECT_GT(index_query_count->load(std::memory_order_relaxed), 0);

    TargetBitmapView raw_values(raw_result->GetRawData(), row_count);
    TargetBitmapView raw_validity(raw_result->GetValidRawData(), row_count);
    TargetBitmapView indexed_values(indexed_result->GetRawData(), row_count);
    TargetBitmapView indexed_validity(indexed_result->GetValidRawData(),
                                      row_count);
    for (int64_t row = 0; row < row_count; ++row) {
        EXPECT_EQ(indexed_validity[row], expected_validity[row])
            << "row=" << row;
        EXPECT_EQ(indexed_validity[row], raw_validity[row]) << "row=" << row;
        EXPECT_EQ(indexed_values[row], expected_values[row]) << "row=" << row;
        EXPECT_EQ(indexed_values[row], raw_values[row]) << "row=" << row;
    }

    auto equal_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(array_fid, DataType::ARRAY, DataType::INT64, {}, true),
        proto::plan::OpType::Equal,
        MakeInt64ArrayValue(literal),
        std::vector<proto::plan::GenericValue>{});
    auto raw_equal =
        EvalFilterInBatches(equal_expr, raw_segment.get(), row_count);
    auto indexed_equal =
        EvalFilterInBatches(equal_expr, indexed_segment.get(), row_count);
    TargetBitmapView raw_equal_values(raw_equal->GetRawData(), row_count);
    TargetBitmapView indexed_equal_values(indexed_equal->GetRawData(),
                                          row_count);
    TargetBitmapView indexed_equal_validity(indexed_equal->GetValidRawData(),
                                            row_count);
    for (int64_t row = 0; row < row_count; ++row) {
        const auto expected_equal =
            expected_validity[row] && arrays[row] == literal;
        EXPECT_EQ(indexed_equal_validity[row], expected_validity[row])
            << "equal row=" << row;
        EXPECT_EQ(indexed_equal_values[row], expected_equal)
            << "equal row=" << row;
        EXPECT_EQ(indexed_equal_values[row], raw_equal_values[row])
            << "equal row=" << row;
    }

    exec::OffsetVector offsets = {0, 1, 2, 4, 6, 8, 10, 12, 13};
    auto raw_offset_result = milvus::test::gen_filter_res(
        plan.get(), raw_segment.get(), row_count, MAX_TIMESTAMP, &offsets);
    auto indexed_offset_result = milvus::test::gen_filter_res(
        plan.get(), indexed_segment.get(), row_count, MAX_TIMESTAMP, &offsets);
    TargetBitmapView raw_offset_values(raw_offset_result->GetRawData(),
                                       offsets.size());
    TargetBitmapView raw_offset_validity(raw_offset_result->GetValidRawData(),
                                         offsets.size());
    TargetBitmapView indexed_offset_values(indexed_offset_result->GetRawData(),
                                           offsets.size());
    TargetBitmapView indexed_offset_validity(
        indexed_offset_result->GetValidRawData(), offsets.size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        const auto row = offsets[i];
        EXPECT_EQ(indexed_offset_validity[i], expected_validity[row])
            << "offset row=" << row;
        EXPECT_EQ(indexed_offset_validity[i], raw_offset_validity[i])
            << "offset row=" << row;
        EXPECT_EQ(indexed_offset_values[i], expected_values[row])
            << "offset row=" << row;
        EXPECT_EQ(indexed_offset_values[i], raw_offset_values[i])
            << "offset row=" << row;
    }

    auto mixed_literal = MakeInt64ArrayValue({1});
    mixed_literal.mutable_array_val()->add_array()->set_float_val(1.5);
    mixed_literal.mutable_array_val()->set_same_type(false);
    auto wrong_type_literal = MakeInt64ArrayValue({});
    wrong_type_literal.mutable_array_val()->add_array()->set_float_val(1.5);
    std::vector<proto::plan::GenericValue> fallback_literals;
    fallback_literals.emplace_back(std::move(mixed_literal));
    fallback_literals.emplace_back(std::move(wrong_type_literal));
    for (auto& fallback_literal : fallback_literals) {
        auto fallback_not_equal = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(
                array_fid, DataType::ARRAY, DataType::INT64, {}, true),
            proto::plan::OpType::NotEqual,
            std::move(fallback_literal),
            std::vector<proto::plan::GenericValue>{});
        auto query_count = index_query_count->load(std::memory_order_relaxed);
        auto raw_fallback = EvalFilterInBatches(
            fallback_not_equal, raw_segment.get(), row_count);
        auto indexed_fallback = EvalFilterInBatches(
            fallback_not_equal, indexed_segment.get(), row_count);
        EXPECT_EQ(index_query_count->load(std::memory_order_relaxed),
                  query_count);
        TargetBitmapView raw_fallback_values(raw_fallback->GetRawData(),
                                             row_count);
        TargetBitmapView raw_fallback_validity(raw_fallback->GetValidRawData(),
                                               row_count);
        TargetBitmapView indexed_mixed_values(indexed_fallback->GetRawData(),
                                              row_count);
        TargetBitmapView indexed_mixed_validity(
            indexed_fallback->GetValidRawData(), row_count);
        for (int64_t row = 0; row < row_count; ++row) {
            EXPECT_EQ(indexed_mixed_validity[row], expected_validity[row])
                << "fallback row=" << row;
            EXPECT_EQ(indexed_mixed_validity[row], raw_fallback_validity[row])
                << "fallback row=" << row;
            EXPECT_EQ(indexed_mixed_values[row], expected_validity[row])
                << "fallback row=" << row;
            EXPECT_EQ(indexed_mixed_values[row], raw_fallback_values[row])
                << "fallback row=" << row;
        }
    }
}
