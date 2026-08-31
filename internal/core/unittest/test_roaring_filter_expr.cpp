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
#include <roaring/roaring64map.hh>

#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "common/IndexMeta.h"
#include "common/RoaringMembership.h"
#include "common/Schema.h"
#include "common/Utils.h"
#include "common/Types.h"
#include "exec/QueryContext.h"
#include "exec/expression/ConjunctExpr.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "exec/expression/LogicalUnaryExpr.h"
#include "exec/expression/RoaringFilterExpr.h"
#include "expr/ITypeExpr.h"
#include "index/BitmapIndex.h"
#include "index/ScalarIndexSort.h"
#include "pb/plan.pb.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/PlanProto.h"
#include "rescores/BoostScoreRunner.h"
#include "rescores/Scorer.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

namespace milvus {
namespace {

void
PutU16(std::string& out, size_t offset, uint16_t value) {
    out[offset] = static_cast<char>(value & 0xff);
    out[offset + 1] = static_cast<char>((value >> 8) & 0xff);
}

void
PutU64(std::string& out, size_t offset, uint64_t value) {
    for (int i = 0; i < 8; ++i) {
        out[offset + i] = static_cast<char>((value >> (8 * i)) & 0xff);
    }
}

std::string
BuildMrb1(const std::vector<int64_t>& values) {
    roaring::Roaring64Map bitmap;
    for (auto value : values) {
        bitmap.add(static_cast<uint64_t>(value));
    }
    bitmap.runOptimize();

    std::string body(bitmap.getSizeInBytes(true), '\0');
    EXPECT_EQ(bitmap.write(body.data(), true), body.size());

    std::string blob(RoaringMembership::kHeaderSize + body.size(), '\0');
    std::memcpy(blob.data(),
                RoaringMembership::kMagic.data(),
                RoaringMembership::kMagic.size());
    PutU16(blob, 4, RoaringMembership::kVersion);
    PutU16(blob, 6, RoaringMembership::kFormatPortableRoaring64);
    PutU64(blob, 8, bitmap.cardinality());
    PutU64(blob, 16, body.size());
    PutU64(blob, 24, 0);
    std::memcpy(
        blob.data() + RoaringMembership::kHeaderSize, body.data(), body.size());
    return blob;
}

ErrorCode
CatchCode(const std::function<void()>& fn) {
    try {
        fn();
    } catch (const SegcoreError& error) {
        return error.get_error_code();
    }
    return ErrorCode::Success;
}

proto::plan::Expr
MakeExpr(FieldId field_id, DataType data_type, const std::string& blob) {
    proto::plan::Expr expr_pb;
    auto* roaring_expr = expr_pb.mutable_roaring_filter_expr();
    auto* column = roaring_expr->mutable_column_info();
    column->set_field_id(field_id.get());
    column->set_data_type(static_cast<proto::schema::DataType>(data_type));
    roaring_expr->set_bitmap_blob(blob);
    return expr_pb;
}

TEST(RoaringFilterExprTest, ParsesAllIntegerTypesAndPreservesMembership) {
    auto schema = std::make_shared<Schema>();
    const std::array<std::pair<FieldId, DataType>, 4> fields = {{
        {schema->AddDebugField("int8", DataType::INT8), DataType::INT8},
        {schema->AddDebugField("int16", DataType::INT16), DataType::INT16},
        {schema->AddDebugField("int32", DataType::INT32), DataType::INT32},
        {schema->AddDebugField("int64", DataType::INT64), DataType::INT64},
    }};
    const std::vector<int64_t> values = {std::numeric_limits<int64_t>::min(),
                                         -7,
                                         0,
                                         42,
                                         std::numeric_limits<int64_t>::max()};
    const auto blob = BuildMrb1(values);
    query::ProtoParser parser(schema);

    for (const auto& [field_id, data_type] : fields) {
        auto parsed = parser.ParseExprs(MakeExpr(field_id, data_type, blob));
        auto roaring_expr =
            std::dynamic_pointer_cast<const expr::RoaringFilterExpr>(parsed);

        ASSERT_NE(roaring_expr, nullptr);
        EXPECT_EQ(roaring_expr->column_.field_id_, field_id);
        EXPECT_EQ(roaring_expr->column_.data_type_, data_type);
        ASSERT_NE(roaring_expr->membership_, nullptr);
        EXPECT_EQ(roaring_expr->membership_->cardinality(), values.size());
        EXPECT_EQ(roaring_expr->membership_->serialized_size(),
                  blob.size() - RoaringMembership::kHeaderSize);
        for (auto value : values) {
            EXPECT_TRUE(roaring_expr->membership_->Contains(value));
        }
        EXPECT_FALSE(roaring_expr->membership_->Contains(43));
    }
}

TEST(RoaringFilterExprTest, RejectsUnsupportedAndMismatchedFieldTypes) {
    auto schema = std::make_shared<Schema>();
    auto double_field = schema->AddDebugField("double", DataType::DOUBLE);
    auto int64_field = schema->AddDebugField("int64", DataType::INT64);
    const auto blob = BuildMrb1({1, 2, 3});
    query::ProtoParser parser(schema);

    auto unsupported = MakeExpr(double_field, DataType::DOUBLE, blob);
    EXPECT_EQ(CatchCode([&] { parser.ParseExprs(unsupported); }),
              ErrorCode::ExprInvalid);

    auto mismatched = MakeExpr(int64_field, DataType::INT32, blob);
    EXPECT_EQ(CatchCode([&] { parser.ParseExprs(mismatched); }),
              ErrorCode::UnexpectedError);
}

TEST(RoaringFilterExprTest, RejectsMalformedMrb1DuringLogicalParsing) {
    auto schema = std::make_shared<Schema>();
    auto field_id = schema->AddDebugField("int64", DataType::INT64);
    query::ProtoParser parser(schema);
    auto malformed = MakeExpr(field_id, DataType::INT64, "MRB1");

    EXPECT_EQ(CatchCode([&] { parser.ParseExprs(malformed); }),
              ErrorCode::ExprInvalid);
}

TEST(RoaringFilterExprTest, ToStringSummarizesWithoutDumpingBlob) {
    const auto blob = BuildMrb1({-7, 0, 42});
    auto membership = RoaringMembership::Parse(blob);
    expr::RoaringFilterExpr roaring_expr(
        expr::ColumnInfo(FieldId(101), DataType::INT64), membership);

    const auto summary = roaring_expr.ToString();

    EXPECT_EQ(roaring_expr.membership_, membership);
    EXPECT_NE(summary.find("FieldId:101"), std::string::npos) << summary;
    EXPECT_NE(summary.find("PortableBodyBytes: " +
                           std::to_string(membership->serialized_size())),
              std::string::npos)
        << summary;
    EXPECT_NE(summary.find("Cardinality: 3"), std::string::npos) << summary;
    EXPECT_EQ(summary.find(blob.substr(0, 8)), std::string::npos)
        << "ToString must not contain raw MRB1 bytes";
}

template <typename T>
void
OverrideIntegerColumn(segcore::GeneratedData& data,
                      FieldId field_id,
                      const std::vector<T>& values) {
    ASSERT_EQ(values.size(), data.raw_->num_rows());
    for (int i = 0; i < data.raw_->fields_data_size(); ++i) {
        auto* field = data.raw_->mutable_fields_data(i);
        if (field->field_id() != field_id.get()) {
            continue;
        }
        if constexpr (std::is_same_v<T, int64_t>) {
            auto* column = field->mutable_scalars()->mutable_long_data();
            column->clear_data();
            for (auto value : values) {
                column->add_data(value);
            }
        } else {
            auto* column = field->mutable_scalars()->mutable_int_data();
            column->clear_data();
            for (auto value : values) {
                column->add_data(static_cast<int32_t>(value));
            }
        }
        return;
    }
    FAIL() << "integer field " << field_id.get()
           << " not found in generated data";
}

void
OverrideValidity(segcore::GeneratedData& data,
                 FieldId field_id,
                 const FixedVector<bool>& validity) {
    ASSERT_EQ(validity.size(), data.raw_->num_rows());
    for (int i = 0; i < data.raw_->fields_data_size(); ++i) {
        auto* field = data.raw_->mutable_fields_data(i);
        if (field->field_id() != field_id.get()) {
            continue;
        }
        // The 3.0 protobuf stores row validity directly on FieldData.
        auto* valid_data = field->mutable_valid_data();
        valid_data->Clear();
        for (bool valid : validity) {
            valid_data->Add(valid);
        }
        return;
    }
    FAIL() << "integer field " << field_id.get()
           << " not found in generated data";
}

template <typename T>
std::vector<T>
BuildControlledValues(size_t count) {
    std::vector<T> values(count);
    for (size_t i = 0; i < count; ++i) {
        values[i] = static_cast<T>(static_cast<int64_t>(i) - 15);
    }
    values[0] = std::numeric_limits<T>::min();
    values[1] = std::numeric_limits<T>::max();
    values[2] = static_cast<T>(-7);
    values[3] = static_cast<T>(0);
    values[4] = static_cast<T>(42);
    values[5] = static_cast<T>(-7);
    values[6] = static_cast<T>(-7);
    values[17] = static_cast<T>(42);
    return values;
}

template <typename T>
std::vector<int64_t>
WidenValues(const std::vector<T>& values) {
    std::vector<int64_t> widened;
    widened.reserve(values.size());
    for (auto value : values) {
        widened.push_back(static_cast<int64_t>(value));
    }
    return widened;
}

std::atomic<int64_t> g_roaring_reverse_lookup_calls{0};

class CountingInt64Index : public index::ScalarIndexSort<int64_t> {
 public:
    std::optional<int64_t>
    Reverse_Lookup(size_t offset) const override {
        ++g_roaring_reverse_lookup_calls;
        return index::ScalarIndexSort<int64_t>::Reverse_Lookup(offset);
    }
};

class RoaringFilterExprEvalTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        schema_ = std::make_shared<Schema>();
        schema_->AddDebugField(
            "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
        pk_fid_ = schema_->AddDebugField("pk", DataType::INT64);
        i8_fid_ = schema_->AddDebugField("int8", DataType::INT8, true);
        i16_fid_ = schema_->AddDebugField("int16", DataType::INT16, true);
        i32_fid_ = schema_->AddDebugField("int32", DataType::INT32, true);
        i64_fid_ = schema_->AddDebugField("int64", DataType::INT64, true);
        schema_->set_primary_field_id(pk_fid_);

        dataset_ = std::make_unique<segcore::GeneratedData>(
            segcore::DataGen(schema_, N));
        i8_values_ = BuildControlledValues<int8_t>(N);
        i16_values_ = BuildControlledValues<int16_t>(N);
        i32_values_ = BuildControlledValues<int32_t>(N);
        i64_values_ = BuildControlledValues<int64_t>(N);

        validity_.resize(N);
        for (size_t i = 0; i < N; ++i) {
            validity_[i] = i != 6 && i != 17;
        }

        OverrideIntegerColumn(*dataset_, i8_fid_, i8_values_);
        OverrideIntegerColumn(*dataset_, i16_fid_, i16_values_);
        OverrideIntegerColumn(*dataset_, i32_fid_, i32_values_);
        OverrideIntegerColumn(*dataset_, i64_fid_, i64_values_);
        for (auto field_id : {i8_fid_, i16_fid_, i32_fid_, i64_fid_}) {
            OverrideValidity(*dataset_, field_id, validity_);
        }
    }

    std::unique_ptr<segcore::SegmentGrowing>
    BuildGrowing() const {
        auto segment = segcore::CreateGrowingSegment(schema_, empty_index_meta);
        auto offset = segment->PreInsert(N);
        segment->Insert(offset,
                        N,
                        dataset_->row_ids_.data(),
                        dataset_->timestamps_.data(),
                        dataset_->raw_);
        return segment;
    }

    std::unique_ptr<segcore::SegmentSealed>
    BuildSealed() const {
        return CreateSealedWithFieldDataLoaded(schema_, *dataset_);
    }

    std::unique_ptr<segcore::SegmentSealed>
    BuildIndexOnlyStlSegment(bool counting = false) const {
        auto segment = CreateSealedWithFieldDataLoaded(
            schema_, *dataset_, false, {i64_fid_.get()});
        EXPECT_FALSE(segment->HasFieldData(i64_fid_));

        segcore::LoadIndexInfo index_info;
        index_info.field_id = i64_fid_.get();
        index_info.field_type = DataType::INT64;
        std::unique_ptr<index::ScalarIndex<int64_t>> scalar_index;
        if (counting) {
            scalar_index = std::make_unique<CountingInt64Index>();
        } else {
            scalar_index = index::CreateScalarIndexSort<int64_t>();
        }
        scalar_index->Build(N, i64_values_.data(), validity_.data());
        index_info.index_params = GenIndexParams(scalar_index.get());
        index_info.cache_index = CreateTestCacheIndex(
            counting ? "roaring-counting-index" : "roaring-stl-index",
            std::move(scalar_index));
        segment->LoadIndex(index_info);
        EXPECT_FALSE(segment->HasFieldData(i64_fid_));
        EXPECT_TRUE(segment->HasIndex(i64_fid_));
        return segment;
    }

    std::unique_ptr<segcore::SegmentSealed>
    BuildIndexOnlyBitmapSegment() const {
        auto segment = CreateSealedWithFieldDataLoaded(
            schema_, *dataset_, false, {i64_fid_.get()});
        EXPECT_FALSE(segment->HasFieldData(i64_fid_));

        segcore::LoadIndexInfo index_info;
        index_info.field_id = i64_fid_.get();
        index_info.field_type = DataType::INT64;
        auto bitmap_index = std::make_unique<index::BitmapIndex<int64_t>>();
        bitmap_index->Build(N, i64_values_.data(), validity_.data());
        EXPECT_FALSE(bitmap_index->SupportFastReverseLookup());
        index_info.index_params = GenIndexParams(bitmap_index.get());
        index_info.cache_index = CreateTestCacheIndex("roaring-bitmap-index",
                                                      std::move(bitmap_index));
        segment->LoadIndex(index_info);
        EXPECT_FALSE(segment->HasFieldData(i64_fid_));
        EXPECT_TRUE(segment->HasIndex(i64_fid_));
        return segment;
    }

    std::shared_ptr<expr::RoaringFilterExpr>
    MakeLogical(FieldId field_id,
                DataType data_type,
                const std::vector<int64_t>& members) const {
        return std::make_shared<expr::RoaringFilterExpr>(
            expr::ColumnInfo(field_id, data_type, {}, true),
            RoaringMembership::Parse(BuildMrb1(members)));
    }

    ColumnVectorPtr
    EvalPredicate(const segcore::SegmentInternalInterface* segment,
                  const expr::TypedExprPtr& logical,
                  exec::OffsetVector* offsets = nullptr) const {
        auto filter_node = std::make_shared<plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID, logical);
        return test::gen_filter_res(
            filter_node.get(), segment, N, MAX_TIMESTAMP, offsets);
    }

    ColumnVectorPtr
    AsColumnVector(const VectorPtr& result) const {
        // Boolean physical expressions return ColumnVector. Keep the cast
        // static, as in test_bloom_filter_expr.cpp: header-defined RTTI can
        // have distinct identities across all_tests and libmilvus_core.dylib
        // in the macOS shared-library build even though typeid names match.
        return std::static_pointer_cast<ColumnVector>(result);
    }

    ColumnVectorPtr
    EvalPhysical(const segcore::SegmentInternalInterface* segment,
                 const expr::TypedExprPtr& logical,
                 TargetBitmap bitmap_input,
                 exec::OffsetVector* offsets = nullptr) const {
        auto query_context = std::make_shared<exec::QueryContext>(
            DEAFULT_QUERY_ID, segment, N, MAX_TIMESTAMP);
        exec::ExecContext exec_context(query_context.get());
        auto compiled =
            exec::CompileExpressions({logical}, &exec_context, {}, false);
        EXPECT_EQ(compiled.size(), 1u);
        if (compiled.empty()) {
            return nullptr;
        }

        exec::EvalCtx eval_ctx(&exec_context, offsets);
        eval_ctx.set_bitmap_input(std::move(bitmap_input));
        VectorPtr result;
        compiled[0]->Eval(eval_ctx, result);
        return AsColumnVector(result);
    }

    void
    ExpectSameColumn(const ColumnVectorPtr& actual,
                     const ColumnVectorPtr& expected) const {
        ASSERT_NE(actual, nullptr);
        ASSERT_NE(expected, nullptr);
        ASSERT_EQ(actual->size(), expected->size());
        BitsetTypeView actual_result(actual->GetRawData(), actual->size());
        BitsetTypeView actual_valid(actual->GetValidRawData(), actual->size());
        BitsetTypeView expected_result(expected->GetRawData(),
                                       expected->size());
        BitsetTypeView expected_valid(expected->GetValidRawData(),
                                      expected->size());
        for (size_t i = 0; i < actual->size(); ++i) {
            EXPECT_EQ(actual_result[i], expected_result[i]) << "position " << i;
            EXPECT_EQ(actual_valid[i], expected_valid[i]) << "position " << i;
        }
    }

    template <typename T>
    void
    CheckExactMembership(const segcore::SegmentInternalInterface* segment,
                         FieldId field_id,
                         DataType data_type,
                         const std::vector<T>& values) const {
        const std::vector<int64_t> members = {
            static_cast<int64_t>(values[0]),
            static_cast<int64_t>(values[1]),
            static_cast<int64_t>(values[2]),
            static_cast<int64_t>(values[4]),
        };
        const std::unordered_set<int64_t> member_set(members.begin(),
                                                     members.end());
        auto column =
            EvalPredicate(segment, MakeLogical(field_id, data_type, members));
        ASSERT_NE(column, nullptr);
        ASSERT_EQ(column->size(), N);

        BitsetTypeView result(column->GetRawData(), N);
        BitsetTypeView valid(column->GetValidRawData(), N);
        for (size_t i = 0; i < N; ++i) {
            const bool expected =
                validity_[i] &&
                member_set.count(static_cast<int64_t>(values[i])) != 0;
            EXPECT_EQ(valid[i], validity_[i]) << "row " << i;
            EXPECT_EQ(result[i], expected)
                << "row " << i << " value=" << static_cast<int64_t>(values[i]);
        }
    }

    static constexpr size_t N = 32;

    SchemaPtr schema_;
    FieldId pk_fid_, i8_fid_, i16_fid_, i32_fid_, i64_fid_;
    std::unique_ptr<segcore::GeneratedData> dataset_;
    std::vector<int8_t> i8_values_;
    std::vector<int16_t> i16_values_;
    std::vector<int32_t> i32_values_;
    std::vector<int64_t> i64_values_;
    FixedVector<bool> validity_;
};

TEST_F(RoaringFilterExprEvalTest,
       CompilesDedicatedPhysicalExprAndForcesRawPath) {
    auto sealed = BuildSealed();

    segcore::LoadIndexInfo index_info;
    index_info.field_id = i64_fid_.get();
    index_info.field_type = DataType::INT64;
    auto index = index::CreateScalarIndexSort<int64_t>();
    index->Build(N, i64_values_.data(), validity_.data());
    index_info.index_params = GenIndexParams(index.get());
    index_info.cache_index =
        CreateTestCacheIndex("roaring-raw-path", std::move(index));
    sealed->LoadIndex(index_info);
    ASSERT_TRUE(sealed->HasFieldData(i64_fid_));
    ASSERT_TRUE(sealed->HasIndex(i64_fid_));

    auto logical = MakeLogical(i64_fid_, DataType::INT64, {-7, 42});
    auto query_context = std::make_shared<exec::QueryContext>(
        DEAFULT_QUERY_ID, sealed.get(), N, MAX_TIMESTAMP);
    exec::ExecContext exec_context(query_context.get());
    auto compiled =
        exec::CompileExpressions({logical}, &exec_context, {}, false);

    ASSERT_EQ(compiled.size(), 1u);
    EXPECT_EQ(compiled[0]->name(), "PhyRoaringFilterExpr");
    auto physical =
        std::dynamic_pointer_cast<exec::PhyRoaringFilterExpr>(compiled[0]);
    ASSERT_NE(physical, nullptr);
    EXPECT_FALSE(physical->UseIndexCursor());
    EXPECT_FALSE(physical->CanExecuteAllAtOnce());
    EXPECT_FALSE(physical->IsCacheable());
    ASSERT_TRUE(physical->GetColumnInfo().has_value());
    EXPECT_EQ(physical->GetColumnInfo()->field_id_, i64_fid_);
    EXPECT_EQ(physical->ToString(), logical->ToString());

    expr::TypedExprPtr negated = std::make_shared<expr::LogicalUnaryExpr>(
        expr::LogicalUnaryExpr::OpType::LogicalNot, logical);
    auto compiled_negated =
        exec::CompileExpressions({negated}, &exec_context, {}, false);
    ASSERT_EQ(compiled_negated.size(), 1u);
    EXPECT_FALSE(compiled_negated[0]->IsCacheable());

    auto membership = RoaringMembership::Parse(BuildMrb1({1}));
    auto unsupported = std::make_shared<expr::RoaringFilterExpr>(
        expr::ColumnInfo(i64_fid_, DataType::DOUBLE, {}, true), membership);
    EXPECT_EQ(CatchCode([&] {
                  exec::CompileExpressions(
                      {unsupported}, &exec_context, {}, false);
              }),
              ErrorCode::ExprInvalid);

    auto missing_membership = std::make_shared<expr::RoaringFilterExpr>(
        expr::ColumnInfo(i64_fid_, DataType::INT64, {}, true), nullptr);
    EXPECT_NE(CatchCode([&] {
                  exec::CompileExpressions(
                      {missing_membership}, &exec_context, {}, false);
              }),
              ErrorCode::Success);
}

TEST_F(RoaringFilterExprEvalTest,
       AllIntegerTypesGrowingAndSealedExactMembership) {
    auto growing = BuildGrowing();
    auto sealed = BuildSealed();
    for (const auto* segment :
         {static_cast<const segcore::SegmentInternalInterface*>(growing.get()),
          static_cast<const segcore::SegmentInternalInterface*>(
              sealed.get())}) {
        CheckExactMembership(segment, i8_fid_, DataType::INT8, i8_values_);
        CheckExactMembership(segment, i16_fid_, DataType::INT16, i16_values_);
        CheckExactMembership(segment, i32_fid_, DataType::INT32, i32_values_);
        CheckExactMembership(segment, i64_fid_, DataType::INT64, i64_values_);
    }
}

TEST_F(RoaringFilterExprEvalTest, NullableAndNotPreserveThreeValuedLogic) {
    auto growing = BuildGrowing();
    auto sealed = BuildSealed();
    const std::vector<int64_t> members = {
        i64_values_[2], i64_values_[6], i64_values_[12]};

    for (const auto* segment :
         {static_cast<const segcore::SegmentInternalInterface*>(growing.get()),
          static_cast<const segcore::SegmentInternalInterface*>(
              sealed.get())}) {
        auto logical = MakeLogical(i64_fid_, DataType::INT64, members);
        auto direct = EvalPredicate(segment, logical);
        ASSERT_NE(direct, nullptr);
        BitsetTypeView direct_result(direct->GetRawData(), N);
        BitsetTypeView direct_valid(direct->GetValidRawData(), N);

        auto negated = std::make_shared<expr::LogicalUnaryExpr>(
            expr::LogicalUnaryExpr::OpType::LogicalNot, logical);
        auto inverse = EvalPredicate(segment, negated);
        ASSERT_NE(inverse, nullptr);
        BitsetTypeView inverse_result(inverse->GetRawData(), N);
        BitsetTypeView inverse_valid(inverse->GetValidRawData(), N);

        const std::unordered_set<int64_t> member_set(members.begin(),
                                                     members.end());
        for (size_t i = 0; i < N; ++i) {
            const bool contains = member_set.count(i64_values_[i]) != 0;
            EXPECT_EQ(direct_valid[i], validity_[i]) << "row " << i;
            EXPECT_EQ(inverse_valid[i], validity_[i]) << "row " << i;
            EXPECT_EQ(direct_result[i], validity_[i] && contains)
                << "row " << i;
            EXPECT_EQ(inverse_result[i], validity_[i] && !contains)
                << "row " << i;
        }

        auto direct_plan = test::CreateRetrievePlanByExpr(logical);
        auto direct_final =
            query::ExecuteQueryExpr(direct_plan, segment, N, MAX_TIMESTAMP);
        auto inverse_plan = test::CreateRetrievePlanByExpr(negated);
        auto inverse_final =
            query::ExecuteQueryExpr(inverse_plan, segment, N, MAX_TIMESTAMP);
        ASSERT_FALSE(validity_[6]);
        EXPECT_FALSE(direct_final[6]);
        EXPECT_FALSE(inverse_final[6]);
    }
}

TEST_F(RoaringFilterExprEvalTest, NonContiguousOffsetInputGrowingAndSealed) {
    const std::vector<int64_t> members = {
        i64_values_[7], i64_values_[19], i64_values_[25]};
    const std::unordered_set<int64_t> member_set(members.begin(),
                                                 members.end());
    exec::OffsetVector offsets;
    for (auto offset : std::vector<int32_t>{25, 2, 7, 12, 19, 6, 29, 4, 18}) {
        offsets.emplace_back(offset);
    }

    auto growing = BuildGrowing();
    auto sealed = BuildSealed();
    for (const auto* segment :
         {static_cast<const segcore::SegmentInternalInterface*>(growing.get()),
          static_cast<const segcore::SegmentInternalInterface*>(
              sealed.get())}) {
        auto result = EvalPredicate(
            segment, MakeLogical(i64_fid_, DataType::INT64, members), &offsets);
        ASSERT_NE(result, nullptr);
        ASSERT_EQ(result->size(), offsets.size());
        BitsetTypeView bits(result->GetRawData(), result->size());
        BitsetTypeView valid(result->GetValidRawData(), result->size());

        bool differs_from_first_n = false;
        for (size_t i = 0; i < offsets.size(); ++i) {
            const auto row = static_cast<size_t>(offsets[i]);
            const bool expected =
                validity_[row] && member_set.count(i64_values_[row]) != 0;
            EXPECT_EQ(valid[i], validity_[row])
                << "candidate " << i << " row " << row;
            EXPECT_EQ(bits[i], expected) << "candidate " << i << " row " << row;

            const bool first_n_expected =
                validity_[i] && member_set.count(i64_values_[i]) != 0;
            differs_from_first_n |= expected != first_n_expected;
        }
        EXPECT_TRUE(differs_from_first_n);
    }
}

// roaring_match is supported inside ScoreFunction.filter, exactly like
// bloom_match. Parsing it is not enough: the production scorer path compiles
// the filter, decides native-vs-non-native, and evaluates it against the
// candidate offsets a segment actually produced -- which are non-contiguous,
// out of order, and may repeat. This drives that whole path through
// ComputeScorerScores and checks the resulting boosts, not the bitset.
TEST_F(RoaringFilterExprEvalTest, ScorerFilterBoostsOnlyMatchedNonNullRows) {
    // -7 lives at rows 2, 5, 6 and 8; 42 at rows 4 and 17. Rows 6 and 17 are
    // NULL, so a value match there must still yield no boost.
    const std::vector<int64_t> members = {-7, 42};
    const std::unordered_set<int64_t> member_set(members.begin(),
                                                 members.end());
    // Non-contiguous, out of order, with a repeat: row 4 appears twice.
    const std::vector<int32_t> offset_values = {
        17, 4, 31, 6, 8, 0, 5, 2, 30, 4};

    auto growing = BuildGrowing();
    auto sealed = BuildSealed();
    for (const auto* segment :
         {static_cast<const segcore::SegmentInternalInterface*>(growing.get()),
          static_cast<const segcore::SegmentInternalInterface*>(
              sealed.get())}) {
        auto scorer = std::make_shared<rescores::WeightScorer>(
            MakeLogical(i64_fid_, DataType::INT64, members), 2.0F);

        auto query_context = std::make_shared<exec::QueryContext>(
            "roaring_scorer_filter", segment, N, MAX_TIMESTAMP);
        OpContext op_context;
        query_context->set_op_context(&op_context);
        exec::ExecContext exec_context(query_context.get());

        // Guard: roaring_match consumes offset input, so the scorer must take
        // the native branch. Without this the assertions below would silently
        // degrade into testing the whole-segment bitset fallback instead.
        ASSERT_FALSE(
            rescores::ComputeNonNativeFilterBitset(&exec_context, scorer)
                .has_value());

        FixedVector<int32_t> offsets;
        for (auto offset : offset_values) {
            offsets.emplace_back(offset);
        }
        std::vector<std::optional<float>> scores(offsets.size(), std::nullopt);
        rescores::ComputeScorerScores(
            &exec_context, &op_context, segment, scorer, offsets, scores);

        ASSERT_EQ(scores.size(), offsets.size());
        bool saw_boost = false;
        bool saw_null_match = false;
        for (size_t i = 0; i < offsets.size(); ++i) {
            const auto row = static_cast<size_t>(offsets[i]);
            const bool value_matches = member_set.count(i64_values_[row]) != 0;
            const bool expected = validity_[row] && value_matches;
            saw_boost |= expected;
            saw_null_match |= value_matches && !validity_[row];
            if (expected) {
                ASSERT_TRUE(scores[i].has_value())
                    << "candidate " << i << " row " << row
                    << " must be boosted";
                EXPECT_FLOAT_EQ(scores[i].value(), 2.0F);
            } else {
                EXPECT_FALSE(scores[i].has_value())
                    << "candidate " << i << " row " << row
                    << " must not be boosted";
            }
        }
        // The offsets must actually exercise both interesting outcomes.
        EXPECT_TRUE(saw_boost);
        EXPECT_TRUE(saw_null_match);
    }
}

TEST_F(RoaringFilterExprEvalTest, BitmapInputPrunesByCandidatePosition) {
    auto growing = BuildGrowing();
    const auto all_values = WidenValues(i64_values_);
    auto logical = MakeLogical(i64_fid_, DataType::INT64, all_values);

    TargetBitmap contiguous_mask(N, false);
    for (size_t i = 0; i < N; ++i) {
        contiguous_mask[i] = i % 3 != 1;
    }
    auto contiguous =
        EvalPhysical(growing.get(), logical, std::move(contiguous_mask));
    ASSERT_NE(contiguous, nullptr);
    BitsetTypeView contiguous_result(contiguous->GetRawData(), N);
    BitsetTypeView contiguous_valid(contiguous->GetValidRawData(), N);
    for (size_t i = 0; i < N; ++i) {
        const bool active = i % 3 != 1;
        EXPECT_EQ(contiguous_valid[i], active ? validity_[i] : true)
            << "row " << i;
        EXPECT_EQ(contiguous_result[i], validity_[i] && i % 3 != 1)
            << "row " << i;
    }

    exec::OffsetVector offsets;
    for (auto offset : std::vector<int32_t>{25, 6, 2, 19, 17, 7, 29, 4, 12}) {
        offsets.emplace_back(offset);
    }
    TargetBitmap offset_mask(offsets.size(), false);
    for (size_t i = 0; i < offsets.size(); ++i) {
        offset_mask[i] = i % 2 == 0;
    }
    auto by_offset =
        EvalPhysical(growing.get(), logical, std::move(offset_mask), &offsets);
    ASSERT_NE(by_offset, nullptr);
    ASSERT_EQ(by_offset->size(), offsets.size());
    BitsetTypeView offset_result(by_offset->GetRawData(), by_offset->size());
    BitsetTypeView offset_valid(by_offset->GetValidRawData(),
                                by_offset->size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        const auto row = static_cast<size_t>(offsets[i]);
        const bool active = i % 2 == 0;
        EXPECT_EQ(offset_valid[i], active ? validity_[row] : true)
            << "candidate " << i << " row " << row;
        EXPECT_EQ(offset_result[i], validity_[row] && i % 2 == 0)
            << "candidate " << i << " row " << row;
    }

    EXPECT_ANY_THROW(
        EvalPhysical(growing.get(), logical, TargetBitmap(N - 1, true)));
}

TEST_F(RoaringFilterExprEvalTest, SealedWithoutRawFieldDataFailsClearly) {
    auto sealed = CreateSealedWithFieldDataLoaded(
        schema_, *dataset_, false, {i64_fid_.get()});
    ASSERT_FALSE(sealed->HasFieldData(i64_fid_));

    auto logical = MakeLogical(i64_fid_, DataType::INT64, {-7, 42});
    auto query_context = std::make_shared<exec::QueryContext>(
        DEAFULT_QUERY_ID, sealed.get(), N, MAX_TIMESTAMP);
    exec::ExecContext exec_context(query_context.get());
    auto compiled =
        exec::CompileExpressions({logical}, &exec_context, {}, false);
    ASSERT_EQ(compiled.size(), 1u);

    exec::EvalCtx eval_ctx(&exec_context);
    VectorPtr result;
    try {
        compiled[0]->Eval(eval_ctx, result);
        FAIL() << "index-only roaring_match without a usable index must fail";
    } catch (const SegcoreError& error) {
        // FieldNotLoaded, not UnexpectedError: raw data being absent is a
        // load/state condition rather than a bad request, so it must stay a
        // retriable System error. Matches PhyBloomFilterExpr.
        EXPECT_EQ(error.get_error_code(), ErrorCode::FieldNotLoaded);
        EXPECT_NE(
            std::string(error.what()).find("raw field data is not loaded"),
            std::string::npos)
            << error.what();
    }
}

TEST_F(RoaringFilterExprEvalTest,
       Int64SealedScalarIndexOnlyMatchesRawAndOffsets) {
    auto raw = BuildSealed();
    auto index_only = BuildIndexOnlyStlSegment();
    auto logical =
        MakeLogical(i64_fid_, DataType::INT64, {-7, 0, 42, i64_values_[1]});
    auto negated = std::make_shared<expr::LogicalUnaryExpr>(
        expr::LogicalUnaryExpr::OpType::LogicalNot, logical);

    ExpectSameColumn(EvalPredicate(index_only.get(), logical),
                     EvalPredicate(raw.get(), logical));
    ExpectSameColumn(EvalPredicate(index_only.get(), negated),
                     EvalPredicate(raw.get(), negated));

    exec::OffsetVector offsets{25, 6, 2, 19, 17, 7, 29, 4, 12};
    ExpectSameColumn(EvalPredicate(index_only.get(), logical, &offsets),
                     EvalPredicate(raw.get(), logical, &offsets));
    ExpectSameColumn(EvalPredicate(index_only.get(), negated, &offsets),
                     EvalPredicate(raw.get(), negated, &offsets));

    auto query_context = std::make_shared<exec::QueryContext>(
        DEAFULT_QUERY_ID, index_only.get(), N, MAX_TIMESTAMP);
    exec::ExecContext exec_context(query_context.get());
    auto compiled =
        exec::CompileExpressions({logical}, &exec_context, {}, false);
    ASSERT_EQ(compiled.size(), 1u);
    auto physical =
        std::dynamic_pointer_cast<exec::PhyRoaringFilterExpr>(compiled[0]);
    ASSERT_NE(physical, nullptr);
    EXPECT_TRUE(physical->UseIndexCursor());
    EXPECT_FALSE(physical->CanExecuteAllAtOnce());

    exec::EvalCtx offset_eval_ctx(&exec_context, &offsets);
    VectorPtr offset_result;
    compiled[0]->Eval(offset_eval_ctx, offset_result);
    ExpectSameColumn(AsColumnVector(offset_result),
                     EvalPredicate(raw.get(), logical, &offsets));

    exec::EvalCtx contiguous_eval_ctx(&exec_context);
    VectorPtr contiguous_result;
    compiled[0]->Eval(contiguous_eval_ctx, contiguous_result);
    ExpectSameColumn(AsColumnVector(contiguous_result),
                     EvalPredicate(raw.get(), logical));
}

TEST_F(RoaringFilterExprEvalTest,
       IndexOnlyBitmapInputPrunesReverseLookupsByCandidatePosition) {
    auto index_only = BuildIndexOnlyStlSegment(true);
    auto logical =
        MakeLogical(i64_fid_, DataType::INT64, WidenValues(i64_values_));
    exec::OffsetVector offsets{25, 17, 2, 19, 7, 6, 29, 4};
    TargetBitmap bitmap_input(offsets.size(), false);
    bitmap_input.set(1);
    bitmap_input.set(4);
    bitmap_input.set(6);
    const size_t expected_lookup_count = 3;

    g_roaring_reverse_lookup_calls.store(0);
    auto result = EvalPhysical(
        index_only.get(), logical, std::move(bitmap_input), &offsets);
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->size(), offsets.size());
    EXPECT_EQ(g_roaring_reverse_lookup_calls.load(), expected_lookup_count);

    BitsetTypeView bits(result->GetRawData(), result->size());
    BitsetTypeView valid(result->GetValidRawData(), result->size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        const bool active = i == 1 || i == 4 || i == 6;
        const auto row = static_cast<size_t>(offsets[i]);
        EXPECT_EQ(bits[i], active && validity_[row]) << "candidate " << i;
        EXPECT_EQ(valid[i], active ? validity_[row] : true)
            << "candidate " << i;
    }
    ASSERT_FALSE(validity_[17]);
    EXPECT_FALSE(bits[1]);
    EXPECT_FALSE(valid[1]);
    // Candidate 5 (row 6) is excluded by bitmap_input and NULL: it is never
    // reverse-looked-up and remains at the initial (false, valid).
    ASSERT_FALSE(validity_[6]);
    EXPECT_FALSE(bits[5]);
    EXPECT_TRUE(valid[5]);
}

TEST_F(RoaringFilterExprEvalTest,
       IndexOnlyContiguousExecutionIsBatchedAndAdvancesCursorOnce) {
    constexpr int64_t active_count = 18;
    constexpr int64_t batch_size = 7;
    auto index_only = BuildIndexOnlyStlSegment(true);
    const std::vector<int64_t> members = {-7, 0, 42, i64_values_[1]};
    const std::unordered_set<int64_t> member_set(members.begin(),
                                                 members.end());
    auto logical = MakeLogical(i64_fid_, DataType::INT64, members);
    auto query_config = std::make_shared<exec::QueryConfig>(
        std::unordered_map<std::string, std::string>{
            {exec::QueryConfig::kExprEvalBatchSize,
             std::to_string(batch_size)}});
    auto query_context =
        std::make_shared<exec::QueryContext>(DEAFULT_QUERY_ID,
                                             index_only.get(),
                                             active_count,
                                             MAX_TIMESTAMP,
                                             0,
                                             0,
                                             query::PlanOptions{},
                                             query_config);
    exec::ExecContext exec_context(query_context.get());
    auto compiled =
        exec::CompileExpressions({logical}, &exec_context, {}, false);
    ASSERT_EQ(compiled.size(), 1u);
    auto physical =
        std::dynamic_pointer_cast<exec::PhyRoaringFilterExpr>(compiled[0]);
    ASSERT_NE(physical, nullptr);
    EXPECT_TRUE(physical->UseIndexCursor());
    EXPECT_FALSE(physical->CanExecuteAllAtOnce());

    std::vector<bool> all_result;
    std::vector<bool> all_valid;
    exec::EvalCtx eval_ctx(&exec_context);
    g_roaring_reverse_lookup_calls.store(0);
    for (auto expected_size : {size_t{7}, size_t{7}, size_t{4}}) {
        VectorPtr result;
        compiled[0]->Eval(eval_ctx, result);
        auto column = AsColumnVector(result);
        ASSERT_NE(column, nullptr);
        ASSERT_EQ(column->size(), expected_size);
        BitsetTypeView bits(column->GetRawData(), column->size());
        BitsetTypeView valid(column->GetValidRawData(), column->size());
        for (size_t i = 0; i < column->size(); ++i) {
            all_result.push_back(bits[i]);
            all_valid.push_back(valid[i]);
        }
    }
    VectorPtr eof;
    compiled[0]->Eval(eval_ctx, eof);
    EXPECT_EQ(eof, nullptr);
    EXPECT_EQ(g_roaring_reverse_lookup_calls.load(), active_count);
    ASSERT_EQ(all_result.size(), active_count);
    for (size_t i = 0; i < active_count; ++i) {
        const bool expected =
            validity_[i] && member_set.count(i64_values_[i]) != 0;
        EXPECT_EQ(all_valid[i], validity_[i]) << "row " << i;
        EXPECT_EQ(all_result[i], expected) << "row " << i;
    }
}

TEST_F(RoaringFilterExprEvalTest,
       IndexOnlyConstructionSnapshotSurvivesRawFieldLoadBeforeFirstEval) {
    auto raw = BuildSealed();
    auto index_only = BuildIndexOnlyStlSegment();
    auto logical = MakeLogical(i64_fid_, DataType::INT64, {-7, 0, 42});
    auto query_context = std::make_shared<exec::QueryContext>(
        DEAFULT_QUERY_ID, index_only.get(), N, MAX_TIMESTAMP);
    exec::ExecContext exec_context(query_context.get());
    auto compiled =
        exec::CompileExpressions({logical}, &exec_context, {}, false);
    ASSERT_EQ(compiled.size(), 1u);

    std::vector<int64_t> excluded_fields = {RowFieldID.get(),
                                            TimestampFieldID.get()};
    for (const auto& field : schema_->get_fields()) {
        if (field.first.get() != i64_fid_.get()) {
            excluded_fields.push_back(field.first.get());
        }
    }
    LoadGeneratedDataIntoSegment(
        *dataset_, index_only.get(), false, excluded_fields);
    ASSERT_TRUE(index_only->HasFieldData(i64_fid_));
    ASSERT_TRUE(index_only->HasIndex(i64_fid_));

    auto physical =
        std::dynamic_pointer_cast<exec::PhyRoaringFilterExpr>(compiled[0]);
    ASSERT_NE(physical, nullptr);
    ASSERT_TRUE(physical->UseIndexCursor());
    exec::EvalCtx eval_ctx(&exec_context);
    VectorPtr result;
    compiled[0]->Eval(eval_ctx, result);
    ExpectSameColumn(AsColumnVector(result), EvalPredicate(raw.get(), logical));
}

TEST_F(RoaringFilterExprEvalTest, Int64SealedBitmapIndexOnlyRejected) {
    auto index_only = BuildIndexOnlyBitmapSegment();
    auto logical = MakeLogical(i64_fid_, DataType::INT64, {-7, 0, 42});
    auto query_context = std::make_shared<exec::QueryContext>(
        DEAFULT_QUERY_ID, index_only.get(), N, MAX_TIMESTAMP);
    exec::ExecContext exec_context(query_context.get());
    auto compiled =
        exec::CompileExpressions({logical}, &exec_context, {}, false);
    ASSERT_EQ(compiled.size(), 1u);
    auto physical =
        std::dynamic_pointer_cast<exec::PhyRoaringFilterExpr>(compiled[0]);
    ASSERT_NE(physical, nullptr);
    EXPECT_FALSE(physical->UseIndexCursor());

    exec::EvalCtx eval_ctx(&exec_context);
    VectorPtr result;
    try {
        compiled[0]->Eval(eval_ctx, result);
        FAIL() << "bare BITMAP index must not run slow reverse lookup";
    } catch (const SegcoreError& error) {
        // Same classification as the no-index case: raw data missing is a
        // load/state condition, so a retriable System error rather than a bad
        // request. Matches PhyBloomFilterExpr.
        EXPECT_EQ(error.get_error_code(), ErrorCode::FieldNotLoaded);
        const std::string message = error.what();
        EXPECT_NE(message.find("raw field data"), std::string::npos) << message;
        EXPECT_NE(message.find("offset cache"), std::string::npos) << message;
    }
}

TEST_F(RoaringFilterExprEvalTest, ReorderPlacesRoaringInMembershipTier) {
    auto sealed = BuildSealed();
    auto roaring = MakeLogical(i64_fid_, DataType::INT64, {-7, 0, 42});
    proto::plan::GenericValue selector_value;
    selector_value.set_int64_val(-7);
    auto selector = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(i32_fid_, DataType::INT32, {}, true),
        proto::plan::OpType::Equal,
        selector_value,
        std::vector<proto::plan::GenericValue>{});
    auto conjunction = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::And, roaring, selector);

    auto query_context = std::make_shared<exec::QueryContext>(
        DEAFULT_QUERY_ID, sealed.get(), N, MAX_TIMESTAMP);
    exec::ExecContext exec_context(query_context.get());
    auto compiled =
        exec::CompileExpressions({conjunction}, &exec_context, {}, false);
    ASSERT_EQ(compiled.size(), 1u);
    auto physical =
        std::dynamic_pointer_cast<exec::PhyConjunctFilterExpr>(compiled[0]);
    ASSERT_NE(physical, nullptr);
    EXPECT_EQ(physical->GetReorder(), (std::vector<size_t>{1, 0}));
}

TEST_F(RoaringFilterExprEvalTest,
       ConjunctPrunesIndexOnlyRoaringAndMatchesRawBaseline) {
    auto raw = BuildSealed();
    auto index_only = BuildIndexOnlyStlSegment(true);
    auto roaring = MakeLogical(i64_fid_, DataType::INT64, {-7, 42});
    proto::plan::GenericValue selector_value;
    selector_value.set_int64_val(-7);
    auto selector = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(i32_fid_, DataType::INT32, {}, true),
        proto::plan::OpType::Equal,
        selector_value,
        std::vector<proto::plan::GenericValue>{});
    auto conjunction = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::And, roaring, selector);

    auto eval_conjunction =
        [&](const segcore::SegmentInternalInterface* segment,
            bool count_lookups) {
            auto query_context = std::make_shared<exec::QueryContext>(
                DEAFULT_QUERY_ID, segment, N, MAX_TIMESTAMP);
            exec::ExecContext exec_context(query_context.get());
            auto compiled = exec::CompileExpressions(
                {conjunction}, &exec_context, {}, false);
            EXPECT_EQ(compiled.size(), 1u);
            if (compiled.empty()) {
                return ColumnVectorPtr{};
            }
            auto physical =
                std::dynamic_pointer_cast<exec::PhyConjunctFilterExpr>(
                    compiled[0]);
            EXPECT_NE(physical, nullptr);
            if (physical != nullptr) {
                EXPECT_EQ(physical->GetReorder(), (std::vector<size_t>{1, 0}));
            }
            if (count_lookups) {
                g_roaring_reverse_lookup_calls.store(0);
            }
            exec::EvalCtx eval_ctx(&exec_context);
            VectorPtr result;
            compiled[0]->Eval(eval_ctx, result);
            return AsColumnVector(result);
        };

    auto expected = eval_conjunction(raw.get(), false);
    auto actual = eval_conjunction(index_only.get(), true);
    ExpectSameColumn(actual, expected);

    size_t selector_active_rows = 0;
    for (size_t i = 0; i < N; ++i) {
        // 3.0 preserves strict three-valued logic here: UNKNOWN rows must
        // remain active because UNKNOWN AND FALSE becomes FALSE. The later
        // null-rejecting optimization from #51182 is not part of this branch.
        selector_active_rows += !validity_[i] || i32_values_[i] == -7;
    }
    ASSERT_GT(selector_active_rows, 0u);
    ASSERT_LT(selector_active_rows, N);
    EXPECT_EQ(g_roaring_reverse_lookup_calls.load(), selector_active_rows);
}

}  // namespace
}  // namespace milvus
