// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "gtest/gtest.h"
#include "segcore/UpdatesRecord.h"
#include "common/Common.h"
#include "exec/expression/ExprCache.h"
#include "exec/expression/ExprBatchTestUtils.h"
#include "query/PlanProto.h"
#include "test_utils/GenExprProto.h"
#include "segcore/ChunkedSegmentSealedImpl.h"

#include "test_utils/DataGen.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

namespace milvus::segcore {
namespace {
using ExpectedValue = std::variant<std::monostate, int64_t, std::string>;
class PkUpdateExpression : public ::testing::TestWithParam<bool> {
 protected:
    static constexpr int64_t kRows = 9000;
    void
    SetUp() override {
        cache_ = exec::ExprResCacheManager::IsEnabled();
        exec::ExprResCacheManager::SetEnabled(
            true);  // Updated leaves must bypass it.
        prefetch_ = ENABLE_DRIVER_PREFETCH.exchange(false);
        batch_ = EXEC_EVAL_EXPR_BATCH_SIZE.exchange(8192);
        schema_ = std::make_shared<Schema>();
        pk_ = schema_->AddDebugField("pk", DataType::INT64);
        age_ = schema_->AddDebugField("age", DataType::INT64, true);
        name_ = schema_->AddDebugField("name", DataType::VARCHAR, true);
        schema_->set_primary_field_id(pk_);
        auto data = DataGen(schema_, kRows, 42);
        std::fill(data.timestamps_.begin(), data.timestamps_.end(), 1);
        std::vector<int64_t> ages;
        std::vector<std::string> names;
        for (int64_t i = 0; i < kRows; ++i) {
            ages.push_back(i % 100);
            names.push_back(i % 2 ? "alpha" : "other");
        }
        for (auto& field : *data.raw_->mutable_fields_data()) {
            field.clear_valid_data();
            if (field.field_id() != pk_.get())
                for (int64_t i = 0; i < kRows; ++i) field.add_valid_data(true);
            if (field.field_id() == name_.get()) {
                auto* values = field.mutable_scalars()->mutable_string_data();
                values->clear_data();
                for (const auto& v : names) values->add_data(v);
            } else {
                auto* values = field.mutable_scalars()->mutable_long_data();
                values->clear_data();
                for (int64_t i = 0; i < kRows; ++i)
                    values->add_data(field.field_id() == pk_.get() ? i
                                                                   : ages[i]);
            }
        }
        segment_ = std::shared_ptr<ChunkedSegmentSealedImpl>(
            dynamic_cast<ChunkedSegmentSealedImpl*>(
                CreateSealedSegment(
                    schema_, nullptr, GetParam() ? 78001 : 78002)
                    .release()));
        LoadGeneratedDataIntoSegment(
            data,
            segment_.get(),
            false,
            GetParam() ? std::vector<int64_t>{age_.get(), name_.get()}
                       : std::vector<int64_t>{});
        if (GetParam()) {
            LoadSorted(age_, ages);
            LoadSorted(name_, names);
            ASSERT_FALSE(segment_->HasFieldData(age_));
            ASSERT_FALSE(segment_->HasFieldData(name_));
        }
        mutations_ = {{1, 100, 18, "beta"},
                      {2, 100, 21, "alpha"},
                      {8192, 100, 180, "alpha"},
                      {1, 120, 35, "other"},
                      {3, 120, std::monostate{}, std::monostate{}},
                      {1, 160, 18, "alpha"}};
        for (const auto& m : mutations_) {
            IdArray ids;
            ids.mutable_int_id()->add_data(m.offset);
            auto assign = [](FieldId field, const ExpectedValue& value) {
                UpdateAssignment a{field, {}};
                if (auto integer = std::get_if<int64_t>(&value))
                    a.literal.mutable_scalar_value()->set_long_data(*integer);
                else if (auto text = std::get_if<std::string>(&value))
                    a.literal.mutable_scalar_value()->set_string_data(*text);
                else
                    a.literal.set_null_value(google::protobuf::NULL_VALUE);
                return a;
            };
            segment_->ApplyUpdates(
                ids, m.ts, 0, {assign(age_, m.age), assign(name_, m.name)});
        }
    }
    void
    TearDown() override {
        segment_.reset();
        EXEC_EVAL_EXPR_BATCH_SIZE.store(batch_);
        ENABLE_DRIVER_PREFETCH.store(prefetch_);
        exec::ExprResCacheManager::SetEnabled(cache_);
    }
    template <typename T>
    void
    LoadSorted(FieldId field, const std::vector<T>& values) {
        auto built = BuildTestScalarIndex<T>(
            index::families::kSort, values.size(), values.data());
        LoadIndexInfo info{};
        info.field_id = field.get();
        info.field_type = (*schema_)[field].get_data_type();
        info.index_params = GenIndexParams(built.get());
        SetTestIndexMetadata(info, *built, index::families::kSort);
        info.index = CreateTestIndex("ts-vector-segment", std::move(built));
        segment_->LoadIndex(info);
    }
    void
    Column(proto::plan::ColumnInfo* col, FieldId field) const {
        col->set_field_id(field.get());
        col->set_data_type(static_cast<proto::schema::DataType>(
            (*schema_)[field].get_data_type()));
        col->set_nullable(true);
    }
    proto::plan::Expr
    Predicate(int kind) const {
        proto::plan::Expr expr;
        if (kind == 0 || kind == 5) {
            auto* e = expr.mutable_unary_range_expr();
            Column(e->mutable_column_info(), age_);
            e->set_op(proto::plan::Equal);
            e->mutable_value()->set_int64_val(kind == 0 ? 18 : 180);
        } else if (kind == 1) {
            auto* and_expr = expr.mutable_binary_expr();
            and_expr->set_op(proto::plan::BinaryExpr::LogicalAnd);
            auto* range = and_expr->mutable_left()->mutable_binary_range_expr();
            Column(range->mutable_column_info(), age_);
            range->set_lower_inclusive(true);
            range->set_upper_inclusive(false);
            range->mutable_lower_value()->set_int64_val(10);
            range->mutable_upper_value()->set_int64_val(30);
            auto* term = and_expr->mutable_right()->mutable_term_expr();
            Column(term->mutable_column_info(), name_);
            term->add_values()->set_string_val("alpha");
            term->add_values()->set_string_val("beta");
        } else if (kind == 2) {
            auto* e = expr.mutable_binary_arith_op_eval_range_expr();
            Column(e->mutable_column_info(), age_);
            e->set_arith_op(proto::plan::Mod);
            e->mutable_right_operand()->set_int64_val(7);
            e->set_op(proto::plan::Equal);
            e->mutable_value()->set_int64_val(0);
        } else if (kind == 6) {
            auto* e = expr.mutable_compare_expr();
            Column(e->mutable_left_column_info(), age_);
            Column(e->mutable_right_column_info(), pk_);
            e->set_op(proto::plan::GreaterThan);
        } else if (kind == 3) {
            auto* e = expr.mutable_null_expr();
            Column(e->mutable_column_info(), age_);
            e->set_op(proto::plan::NullExpr::IsNull);
        } else {
            auto* e = expr.mutable_unary_range_expr();
            Column(e->mutable_column_info(), name_);
            e->set_op(proto::plan::PrefixMatch);
            e->mutable_value()->set_string_val("a");
        }
        return expr;
    }
    std::pair<ExpectedValue, ExpectedValue>
    ValueAt(int64_t row, uint64_t ts) const {
        ExpectedValue age = row % 100;
        ExpectedValue name = std::string(row % 2 ? "alpha" : "other");
        for (const auto& m : mutations_)
            if (m.offset == row && m.ts <= ts) {
                age = m.age;
                name = m.name;
            }
        return {age, name};
    }
    bool
    Matches(int kind,
            const ExpectedValue& age,
            const ExpectedValue& name) const {
        auto a = std::get_if<int64_t>(&age);
        auto n = std::get_if<std::string>(&name);
        if (kind == 0)
            return a && *a == 18;
        if (kind == 5)
            return a && *a == 180;
        if (kind == 1)
            return a && *a >= 10 && *a < 30 && n &&
                   (*n == "alpha" || *n == "beta");
        if (kind == 2)
            return a && *a % 7 == 0;
        if (kind == 3)
            return !a;
        return n && n->starts_with("a");
    }
    void
    Verify(int kind, uint64_t ts) {
        proto::plan::PlanNode proto;
        *proto.mutable_query()->mutable_predicates() = Predicate(kind);
        proto.mutable_query()->set_limit(1000);
        auto plan = query::ProtoParser(schema_).CreateRetrievePlan(proto);
        auto result = segment_->Retrieve(nullptr,
                                         plan.get(),
                                         ts,
                                         std::numeric_limits<int64_t>::max(),
                                         false);
        std::vector<int64_t> expected;
        for (int64_t row = 0; row < kRows && expected.size() < 1000; ++row) {
            auto [age, name] = ValueAt(row, ts);
            if (kind == 6 ? (std::holds_alternative<int64_t>(age) &&
                             std::get<int64_t>(age) > row)
                          : Matches(kind, age, name))
                expected.push_back(row);
        }
        ASSERT_EQ(std::vector<int64_t>(result->offset().begin(),
                                       result->offset().end()),
                  expected);
    }
    struct Mutation {
        int64_t offset;
        uint64_t ts;
        ExpectedValue age;
        ExpectedValue name;
    };
    bool cache_, prefetch_;
    int64_t batch_;
    SchemaPtr schema_;
    FieldId pk_{0}, age_{0}, name_{0};
    std::vector<Mutation> mutations_;
    std::shared_ptr<ChunkedSegmentSealedImpl> segment_;
};

TEST_P(PkUpdateExpression, NativePredicatesAtHistoricalSnapshots) {
    for (int kind = 0; kind != 7; ++kind) {
        SCOPED_TRACE(kind);
        // Non-monotonic queries expose accidental cross-query cache reuse.
        for (uint64_t ts : {200, 99, 100, 140, 160, 99}) {
            SCOPED_TRACE(ts);
            ASSERT_NO_FATAL_FAILURE(Verify(kind, ts));
        }
    }
}

TEST_P(PkUpdateExpression, CountsVisibleIndexOrRawPatches) {
    const auto& store = segment_->GetUpdatesRecord();
    auto before = store.Diagnostics();
    Verify(0, 140);
    auto after = store.Diagnostics();
    if (GetParam()) {
        EXPECT_EQ(after.reverse_selected - before.reverse_selected, 4);
        EXPECT_EQ(after.reverse_scans - before.reverse_scans, 1);
        EXPECT_EQ(after.dictionary_probes, before.dictionary_probes);
    } else {
        EXPECT_EQ(after.reverse_scans, before.reverse_scans);
        EXPECT_EQ(after.dictionary_probes - before.dictionary_probes, 4);
        EXPECT_GT(after.overlay_batches, before.overlay_batches);
    }
}

TEST_P(PkUpdateExpression, ComparedColumnsRespectOrderedTakeMask) {
    // Both timestamp-bound ColumnExpr operands must map the parent mask by
    // candidate position. A duplicate row occurs once excluded and once live.
    for (auto field : {age_, name_}) {
        proto::plan::Expr expression;
        auto* compare = expression.mutable_compare_expr();
        Column(compare->mutable_left_column_info(), field);
        Column(compare->mutable_right_column_info(), field);
        compare->set_op(proto::plan::Equal);
        auto logical = query::ProtoParser(schema_).ParseExprs(expression);
        FixedVector<int32_t> rows{3, 1, 8, 1, 2};
        exec::QueryContext query_context(
            DEAFULT_QUERY_ID, segment_.get(), kRows, 140);
        exec::ExecContext exec_context(&query_context);
        exec::ExprSet exprs({logical}, &exec_context);
        exec::EvalCtx ctx(&exec_context, &rows);
        TargetBitmap mask(rows.size(), true);
        mask[1] = false;
        ctx.set_bitmap_input(std::move(mask));
        auto& records = segment_->GetUpdatesRecord();
        auto before = records.Diagnostics();
        std::vector<VectorPtr> results;
        exprs.Eval(0, 1, true, ctx, results);
        auto col = std::static_pointer_cast<ColumnVector>(results.at(0));
        ASSERT_EQ(col->size(), rows.size());
        TargetBitmapView hit(col->GetRawData(), rows.size());
        TargetBitmapView valid(col->GetValidRawData(), rows.size());
        EXPECT_FALSE(valid[0]);
        EXPECT_FALSE(hit[0]);
        for (int i : {2, 3, 4}) {
            EXPECT_TRUE(valid[i]);
            EXPECT_TRUE(hit[i]);
        }
        EXPECT_EQ(
            records.Diagnostics().dictionary_probes - before.dictionary_probes,
            6);
    }
}

void
LoadTypedBaseIndex(SegmentSealed& segment, FieldId field, DataType type) {
    const auto family = type == DataType::ARRAY ? index::families::kBitmap
                                                : index::families::kSort;
    const bool valid[] = {true, false, true, true};
    Config params = {{"field_type", static_cast<int>(type)},
                     {index::FIELD_ID, field.get()}};
    auto load = [&]<typename T>(const std::array<T, 4>& base) {
        auto built =
            BuildTestScalarIndex(family, 4, base.data(), valid, params);
        LoadIndexInfo info{};
        info.field_id = field.get();
        info.field_type = type;
        if (type == DataType::ARRAY)
            info.element_type = DataType::INT64;
        info.index_params = GenIndexParams(built.get());
        SetTestIndexMetadata(info, *built, family);
        info.index = CreateTestIndex("typed-update-base", std::move(built));
        segment.LoadIndex(info);
    };
    switch (type) {
        case DataType::BOOL:
            load(std::array<bool, 4>{});
            break;
        case DataType::INT8:
            load(std::array<int8_t, 4>{});
            break;
        case DataType::INT16:
            load(std::array<int16_t, 4>{});
            break;
        case DataType::INT32:
            load(std::array<int32_t, 4>{});
            break;
        case DataType::INT64:
            load(std::array<int64_t, 4>{});
            break;
        case DataType::TIMESTAMPTZ:
            params["value_type"] = static_cast<int>(DataType::TIMESTAMPTZ);
            load(std::array<int64_t, 4>{});
            break;
        case DataType::VARCHAR:
            load(std::array<std::string, 4>{"base", "base", "base", "base"});
            break;
        case DataType::ARRAY: {
            params["array_element_type"] = static_cast<int>(DataType::INT64);
            params["value_type"] = static_cast<int>(DataType::INT64);
            int64_t zero = 0;
            ArrayView view(reinterpret_cast<char*>(&zero),
                           1,
                           sizeof(zero),
                           DataType::INT64,
                           nullptr);
            load(std::array<ArrayView, 4>{view, view, view, view});
            break;
        }
        default:
            FAIL() << "unexpected test type";
    }
}

TEST(PkUpdateTypedEvaluation, GrowingAndSealedScanTakeAndCandidateMask) {
    enum class Source { Growing, Sealed, Indexed };
    for (const auto source :
         {Source::Growing, Source::Sealed, Source::Indexed}) {
        const bool sealed = source != Source::Growing;
        const bool indexed = source == Source::Indexed;
        auto schema = std::make_shared<Schema>();
        auto pk = schema->AddDebugField("pk", DataType::INT64);
        schema->set_primary_field_id(pk);
        std::vector<FieldId> fields;
        std::vector<UpdateAssignment> assignments;
        for (auto type : {DataType::BOOL,
                          DataType::INT8,
                          DataType::INT16,
                          DataType::INT32,
                          DataType::INT64,
                          DataType::TIMESTAMPTZ,
                          DataType::VARCHAR,
                          DataType::ARRAY}) {
            auto name = std::to_string(static_cast<int>(type));
            auto field =
                type == DataType::ARRAY
                    ? schema->AddDebugField(name, type, DataType::INT64, true)
                    : schema->AddDebugField(name, type, true);
            fields.push_back(field);
            UpdateAssignment a{field, {}};
            auto* scalar = a.literal.mutable_scalar_value();
            switch (type) {
                case DataType::BOOL:
                    scalar->set_bool_data(true);
                    break;
                case DataType::VARCHAR:
                    scalar->set_string_data("needle");
                    break;
                case DataType::TIMESTAMPTZ:
                    scalar->set_timestamptz_data(77);
                    break;
                case DataType::INT64:
                    scalar->set_long_data(77);
                    break;
                case DataType::ARRAY:
                    a.literal.mutable_array_value()
                        ->mutable_long_data()
                        ->add_data(77);
                    break;
                default:
                    scalar->set_int_data(77);
            }
            assignments.push_back(std::move(a));
        }
        auto data = DataGen(schema, 4);
        std::fill(data.timestamps_.begin(), data.timestamps_.end(), 1);
        for (auto& f : *data.raw_->mutable_fields_data()) {
            if (f.field_id() == pk.get()) {
                for (int i = 0; i < 4; ++i)
                    f.mutable_scalars()->mutable_long_data()->set_data(i, i);
                continue;
            }
            f.clear_valid_data();
            for (size_t i = 0; i < 4; ++i) f.add_valid_data(i != 1);
            auto* v = f.mutable_scalars();
            for (size_t i = 0; i < 4; ++i) {
                if (v->has_bool_data())
                    v->mutable_bool_data()->set_data(i, false);
                else if (v->has_int_data())
                    v->mutable_int_data()->set_data(i, 0);
                else if (v->has_long_data())
                    v->mutable_long_data()->set_data(i, 0);
                else if (v->has_timestamptz_data())
                    v->mutable_timestamptz_data()->set_data(i, 0);
                else if (v->has_string_data())
                    v->mutable_string_data()->set_data(i, "base");
                else if (v->has_array_data()) {
                    auto* row = v->mutable_array_data()
                                    ->mutable_data(i)
                                    ->mutable_long_data();
                    row->clear_data();
                    row->add_data(0);
                }
            }
        }
        std::unique_ptr<SegmentInternalInterface> segment;
        if (sealed) {
            segment = CreateSealedSegment(schema);
            LoadGeneratedDataIntoSegment(data, segment.get());
        } else {
            auto growing = CreateGrowingSegment(schema, empty_index_meta);
            auto start = growing->PreInsert(4);
            growing->Insert(start,
                            4,
                            data.row_ids_.data(),
                            data.timestamps_.data(),
                            data.SharedRaw());
            segment = std::move(growing);
        }
        if (indexed) {
            for (auto field : fields)
                LoadTypedBaseIndex(dynamic_cast<SegmentSealed&>(*segment),
                                   field,
                                   (*schema)[field].get_data_type());
        }
        IdArray keys;
        keys.mutable_int_id()->add_data(1);
        segment->ApplyUpdates(keys, 20, 0, assignments);
        for (auto& a : assignments)
            a.literal.set_null_value(google::protobuf::NULL_VALUE);
        keys.mutable_int_id()->set_data(0, 2);
        segment->ApplyUpdates(keys, 20, 1, assignments);
        for (auto field : fields) {
            auto type = (*schema)[field].get_data_type();
            SCOPED_TRACE(fmt::format("sealed={} field={}", sealed, type));
            proto::plan::Expr expr;
            proto::plan::ColumnInfo* col;
            if (type == DataType::ARRAY) {
                auto* contains = expr.mutable_json_contains_expr();
                col = contains->mutable_column_info();
                col->set_element_type(proto::schema::DataType::Int64);
                contains->set_op(proto::plan::JSONContainsExpr_JSONOp_Contains);
                contains->set_elements_same_type(true);
                contains->add_elements()->set_int64_val(77);
            } else {
                auto* eq = expr.mutable_unary_range_expr();
                col = eq->mutable_column_info();
                eq->set_op(proto::plan::Equal);
                if (type == DataType::BOOL)
                    eq->mutable_value()->set_bool_val(true);
                else if (type == DataType::VARCHAR)
                    eq->mutable_value()->set_string_val("needle");
                else
                    eq->mutable_value()->set_int64_val(77);
            }
            col->set_field_id(field.get());
            col->set_data_type(static_cast<proto::schema::DataType>(type));
            col->set_nullable(true);
            std::vector<std::pair<proto::plan::Expr, bool>> cases{
                {expr, indexed}};
            if (type == DataType::ARRAY) {
                for (auto op :
                     {proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
                      proto::plan::JSONContainsExpr_JSONOp_ContainsAll}) {
                    auto multi = expr;
                    auto* contains = multi.mutable_json_contains_expr();
                    contains->set_op(op);
                    contains->add_elements()->set_int64_val(
                        op == proto::plan::JSONContainsExpr_JSONOp_ContainsAny
                            ? 88
                            : 77);
                    cases.emplace_back(std::move(multi), indexed);
                }
                proto::plan::Expr subscript;
                auto* equals = subscript.mutable_unary_range_expr();
                *equals->mutable_column_info() = *col;
                equals->mutable_column_info()->add_nested_path("0");
                equals->set_op(proto::plan::Equal);
                equals->mutable_value()->set_int64_val(77);
                cases.emplace_back(std::move(subscript), false);
            }
            for (auto& [predicate, uses_index] : cases) {
                SCOPED_TRACE(predicate.ShortDebugString());
                auto logical = query::ProtoParser(schema).ParseExprs(predicate);
                for (bool take : {false, true}) {
                    FixedVector<int32_t> rows{2, 3, 1, 1, 0};
                    exec::QueryContext query_ctx(
                        DEAFULT_QUERY_ID, segment.get(), 4, 20);
                    exec::ExecContext exec_ctx(&query_ctx);
                    exec::ExprSet exprs({logical}, &exec_ctx);
                    exec::EvalCtx ctx(&exec_ctx, take ? &rows : nullptr);
                    TargetBitmap mask(take ? rows.size() : 4, true);
                    if (take)
                        mask[2] = false;
                    ctx.set_bitmap_input(std::move(mask));
                    std::vector<VectorPtr> results;
                    auto before = segment->GetUpdatesRecord().Diagnostics();
                    exprs.Eval(0, 1, true, ctx, results);
                    auto after = segment->GetUpdatesRecord().Diagnostics();
                    ASSERT_EQ(results.size(), 1);
                    auto v =
                        std::static_pointer_cast<ColumnVector>(results.front());
                    auto bits = TargetBitmapView(v->GetRawData(), v->size());
                    auto valid =
                        TargetBitmapView(v->GetValidRawData(), v->size());
                    if (uses_index && !take) {
                        EXPECT_EQ(
                            after.reverse_selected - before.reverse_selected,
                            2);
                    } else {
                        EXPECT_EQ(
                            after.dictionary_probes - before.dictionary_probes,
                            2);
                    }
                    for (size_t i = 0; i < v->size(); ++i) {
                        auto row = take ? rows[i] : static_cast<int>(i);
                        if (take && i == 2)
                            continue;
                        EXPECT_EQ(bits[i], row == 1);
                        EXPECT_EQ(valid[i], row != 2);
                    }
                }
            }
        }
    }
}

TEST(PkUpdateTypedEvaluation, SealedArrayLengthUsesSnapshotValuesAndValidity) {
    milvus::test::ExprBatchSizeGuard batch_size(2);
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto array =
        schema->AddDebugField("items", DataType::ARRAY, DataType::INT64, true);
    auto data = DataGen(schema, 4);
    std::fill(data.timestamps_.begin(), data.timestamps_.end(), 1);
    for (auto& field : *data.raw_->mutable_fields_data()) {
        if (field.field_id() == pk.get()) {
            for (int i = 0; i < 4; ++i)
                field.mutable_scalars()->mutable_long_data()->set_data(i, i);
        } else {
            field.clear_valid_data();
            for (int i = 0; i < 4; ++i) {
                field.add_valid_data(i != 1);
                auto* row = field.mutable_scalars()
                                ->mutable_array_data()
                                ->mutable_data(i)
                                ->mutable_long_data();
                row->clear_data();
                row->add_data(7);
                if (i == 2)
                    row->add_data(8);
            }
        }
    }
    auto segment = CreateSealedSegment(schema);
    LoadGeneratedDataIntoSegment(data, segment.get());
    UpdateAssignment replace{array, {}};
    auto* pair = replace.literal.mutable_array_value()->mutable_long_data();
    pair->add_data(7);
    pair->add_data(8);
    IdArray keys;
    keys.mutable_int_id()->add_data(0);
    keys.mutable_int_id()->add_data(1);
    segment->ApplyUpdates(keys, 20, 0, {replace});
    keys.mutable_int_id()->clear_data();
    keys.mutable_int_id()->add_data(2);
    replace.literal.set_null_value(google::protobuf::NULL_VALUE);
    segment->ApplyUpdates(keys, 20, 1, {replace});

    proto::plan::GenericValue target, unused;
    target.set_int64_val(2);
    unused.set_int64_val(0);
    auto logical = std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
        expr::ColumnInfo(array, DataType::ARRAY, DataType::INT64, {}, true),
        proto::plan::Equal,
        proto::plan::ArrayLength,
        target,
        unused);
    for (auto ts : {19, 20, 19}) {
        for (bool take : {false, true}) {
            SCOPED_TRACE(fmt::format("timestamp={} take={}", ts, take));
            FixedVector<int32_t> rows{2, 0, 1, 0, 3};
            exec::QueryContext query_ctx(
                DEAFULT_QUERY_ID, segment.get(), 4, ts);
            exec::ExecContext exec_ctx(&query_ctx);
            exec::ExprSet expressions({logical}, &exec_ctx);
            exec::EvalCtx ctx(&exec_ctx, take ? &rows : nullptr);
            for (size_t start = 0; start < (take ? rows.size() : 4);) {
                std::vector<VectorPtr> output;
                expressions.Eval(0, 1, true, ctx, output);
                auto column =
                    std::static_pointer_cast<ColumnVector>(output.at(0));
                ASSERT_EQ(column->size(), take ? rows.size() : 2);
                TargetBitmapView hits(column->GetRawData(), column->size());
                TargetBitmapView valid(column->GetValidRawData(),
                                       column->size());
                for (size_t i = 0; i < column->size(); ++i) {
                    int row = take ? rows[i] : start + i;
                    EXPECT_EQ(hits[i],
                              ts < 20 ? row == 2 : row == 0 || row == 1);
                    EXPECT_EQ(valid[i], ts < 20 ? row != 1 : row != 2);
                }
                start += column->size();
            }
        }
    }
}

INSTANTIATE_TEST_SUITE_P(NativeSources, PkUpdateExpression, ::testing::Bool());
}  // namespace
}  // namespace milvus::segcore
