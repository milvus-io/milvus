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
#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/c/abi.h>
#include "segcore/retrieve_result_export_c.h"
#include "common/Common.h"
#include "exec/expression/ExprCache.h"
#include "query/PlanProto.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SealedUpdatesRecord.h"
#include "test_utils/DataGen.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

namespace milvus::segcore {
namespace {
using experimental::UpdateValue;
class TsVectorSegment : public ::testing::TestWithParam<bool> {
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
        updates_ = experimental::SealedUpdatesRecord::Build(
            segment_, {age_.get(), name_.get()});
        ASSERT_EQ(updates_->Records().GetStats().update_versions, 0);
        mutations_ = {{1, 100, 18, "beta"},
                      {2, 100, 21, "alpha"},
                      {8192, 100, 180, "alpha"},
                      {1, 120, 35, "other"},
                      {3, 120, std::monostate{}, std::monostate{}},
                      {1, 160, 18, "alpha"}};
        for (const auto& m : mutations_)
            ASSERT_EQ(
                updates_->Apply({m.offset},
                                m.ts,
                                {{age_.get(), m.age}, {name_.get(), m.name}}),
                1);
        updates_->PublishUpdates();
        EXPECT_EQ(updates_->Records().GetStats().updated_cells, 8);
        EXPECT_EQ(updates_->Records().GetStats().directory_refs, 12);
    }
    void
    TearDown() override {
        updates_.reset();
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
    std::pair<UpdateValue, UpdateValue>
    ValueAt(int64_t row, uint64_t ts) const {
        UpdateValue age = row % 100;
        UpdateValue name = std::string(row % 2 ? "alpha" : "other");
        for (const auto& m : mutations_)
            if (m.offset == row && m.ts <= ts) {
                age = m.age;
                name = m.name;
            }
        return {age, name};
    }
    bool
    Matches(int kind, const UpdateValue& age, const UpdateValue& name) const {
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
    Verify(int kind, uint64_t ts, bool output) {
        proto::plan::PlanNode proto;
        *proto.mutable_query()->mutable_predicates() = Predicate(kind);
        proto.mutable_query()->set_limit(1000);
        if (output) {
            proto.add_output_field_ids(age_.get());
            proto.add_output_field_ids(name_.get());
        }
        auto plan = query::ProtoParser(schema_).CreateRetrievePlan(proto);
        auto result = segment_->Retrieve(nullptr,
                                         plan.get(),
                                         ts,
                                         std::numeric_limits<int64_t>::max(),
                                         false);
        std::vector<int64_t> expected;
        for (int64_t row = 0; row < kRows && expected.size() < 1000; ++row) {
            auto [age, name] = ValueAt(row, ts);
            if (Matches(kind, age, name))
                expected.push_back(row);
        }
        ASSERT_EQ(std::vector<int64_t>(result->offset().begin(),
                                       result->offset().end()),
                  expected);
        if (!output) {
            EXPECT_EQ(result->fields_data_size(), 0);
            return;
        }
        ASSERT_EQ(result->fields_data_size(), 2);
        for (const auto& field : result->fields_data()) {
            for (size_t i = 0; i < expected.size(); ++i) {
                auto [age, name] = ValueAt(expected[i], ts);
                const auto& value = field.field_id() == age_.get() ? age : name;
                const bool valid =
                    !std::holds_alternative<std::monostate>(value);
                ASSERT_EQ(field.valid_data(i), valid);
                if (!valid)
                    continue;
                if (field.field_id() == age_.get())
                    EXPECT_EQ(field.scalars().long_data().data(i),
                              std::get<int64_t>(value));
                else
                    EXPECT_EQ(field.scalars().string_data().data(i),
                              std::get<std::string>(value));
            }
        }
    }
    struct Mutation {
        int64_t offset;
        uint64_t ts;
        UpdateValue age;
        UpdateValue name;
    };
    bool cache_, prefetch_;
    int64_t batch_;
    SchemaPtr schema_;
    FieldId pk_{0}, age_{0}, name_{0};
    std::vector<Mutation> mutations_;
    std::shared_ptr<ChunkedSegmentSealedImpl> segment_;
    std::unique_ptr<experimental::SealedUpdatesRecord> updates_;
};

TEST_P(TsVectorSegment, FilterAndOutputUseSameTimestamp) {
    for (int kind = 0; kind != 6; ++kind) {
        SCOPED_TRACE(kind);
        // Non-monotonic queries expose accidental cross-query cache reuse.
        for (uint64_t ts : {200, 99, 100, 140, 160, 99}) {
            SCOPED_TRACE(ts);
            ASSERT_NO_FATAL_FAILURE(Verify(kind, ts, true));
        }
    }
}

TEST_P(TsVectorSegment, NativePathsAndSparseStorage) {
    const auto before = updates_->Records().GetStats();
    ASSERT_NO_FATAL_FAILURE(Verify(1, 200, false));
    const auto after = updates_->Records().GetStats();
    EXPECT_EQ(after.update_versions, 12);
    EXPECT_EQ(after.directory_refs, 12);
    if (GetParam()) {
        EXPECT_GT(after.old_index_reads, before.old_index_reads);
        EXPECT_GT(after.ts_entries_visited, before.ts_entries_visited);
        EXPECT_EQ(after.overlay_batches, before.overlay_batches);
        EXPECT_EQ(after.base_rows, before.base_rows);
    } else {
        EXPECT_GT(after.overlay_batches, before.overlay_batches);
        EXPECT_EQ(after.base_rows, before.base_rows);
        EXPECT_LE(after.overlay_history_rows - before.overlay_history_rows, 8);
    }
}
TEST_P(TsVectorSegment, DeferredOutputUsesQueryTimestamp) {
    proto::plan::PlanNode proto;
    *proto.mutable_query()->mutable_predicates() = Predicate(0);
    proto.add_output_field_ids(age_.get());
    proto.add_output_field_ids(name_.get());
    auto plan = query::ProtoParser(schema_).CreateRetrievePlan(proto);
    std::vector<int64_t> rows{1, 3, 8192, 1, 4};
    std::vector<int32_t> segments(rows.size(), 0);
    CSegmentInterface segment = segment_.get();
    for (uint64_t ts : {99, 100, 140, 200, 99}) {
        SCOPED_TRACE(ts);
        auto output = segment_->Retrieve(
            nullptr, plan.get(), rows.data(), rows.size(), {}, ts);
        ASSERT_EQ(output->fields_data_size(), 2);
        ArrowSchema schema{};
        ArrowArray array{};
        auto status = FillRetrieveFieldsOrdered(&segment,
                                                1,
                                                plan.get(),
                                                ts,
                                                segments.data(),
                                                rows.data(),
                                                rows.size(),
                                                1048576,
                                                &schema,
                                                &array,
                                                nullptr);
        ASSERT_EQ(status.error_code, 0) << status.error_msg;
        auto imported = arrow::ImportRecordBatch(&array, &schema);
        ASSERT_TRUE(imported.ok()) << imported.status().ToString();
        auto batch = *imported;
        ASSERT_EQ(batch->num_columns(), 2);
        auto ages =
            std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
        auto names =
            std::static_pointer_cast<arrow::StringArray>(batch->column(1));
        for (size_t i = 0; i < rows.size(); ++i) {
            auto [age, name] = ValueAt(rows[i], ts);
            bool valid_age = std::holds_alternative<int64_t>(age);
            bool valid_name = std::holds_alternative<std::string>(name);
            EXPECT_EQ(ages->IsValid(i), valid_age);
            EXPECT_EQ(names->IsValid(i), valid_name);
            EXPECT_EQ(output->fields_data(0).valid_data(i), valid_age);
            EXPECT_EQ(output->fields_data(1).valid_data(i), valid_name);
            if (valid_age) {
                EXPECT_EQ(ages->Value(i), std::get<int64_t>(age));
                EXPECT_EQ(output->fields_data(0).scalars().long_data().data(i),
                          std::get<int64_t>(age));
            }
            if (valid_name) {
                EXPECT_EQ(names->GetString(i), std::get<std::string>(name));
                EXPECT_EQ(
                    output->fields_data(1).scalars().string_data().data(i),
                    std::get<std::string>(name));
            }
        }
    }
}
INSTANTIATE_TEST_SUITE_P(NativeSources, TsVectorSegment, ::testing::Bool());
}  // namespace
}  // namespace milvus::segcore
