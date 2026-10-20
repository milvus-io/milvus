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

#include "ConsumerExprTestBase.h"
#include "ExprBatchTestUtils.h"
#include "ExprIndexIntegrationTestUtils.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"

namespace milvus::test {
namespace {

struct ValidityCalls {
    size_t owned = 0;
    std::vector<const TargetBitmap*> owners;
};

// Keep the production inverted reader and its loader; count only the consumer
// boundary calls so this test detects a full IsNotNull copy per leaf.
class CountingValidityReader final
    : public index::IIndexReaderBase,
      public index::IScalarPredicateReader<int64_t>,
      public index::INullReader {
 public:
    CountingValidityReader(index::IIndexReaderBasePtr inner,
                           std::shared_ptr<ValidityCalls> calls)
        : inner_(std::move(inner)), calls_(std::move(calls)) {
    }

    index::ReaderCaps
    Caps() const override {
        return inner_->Caps();
    }
    index::Domain
    CoordDomain() const override {
        return inner_->CoordDomain();
    }
    int64_t
    Count() const override {
        return inner_->Count();
    }
    DataType
    ValueType() const override {
        return inner_->ValueType();
    }
    int64_t
    MemoryUsage() const override {
        return inner_->MemoryUsage();
    }
    cachinglayer::ResourceUsage
    CellByteSize() const override {
        return inner_->CellByteSize();
    }

    TargetBitmap
    In(size_t n, const int64_t* values) const override {
        return Predicate().In(n, values);
    }
    TargetBitmap
    NotIn(size_t n, const int64_t* values) const override {
        return Predicate().NotIn(n, values);
    }
    TargetBitmap
    Range(const int64_t& value, index::CompareOp op) const override {
        return Predicate().Range(value, op);
    }
    TargetBitmap
    Range(const int64_t& lo,
          bool lo_inc,
          const int64_t& hi,
          bool hi_inc) const override {
        return Predicate().Range(lo, lo_inc, hi, hi_inc);
    }
    TargetBitmap
    IsNull() const override {
        return Nulls().IsNull();
    }
    TargetBitmap
    IsNotNull() const override {
        ++calls_->owned;
        return Nulls().IsNotNull();
    }
    std::optional<index::ScalarIndexValidity>
    GetImmutableValidity() const override {
        auto snapshot = Nulls().GetImmutableValidity();
        if (snapshot) {
            calls_->owners.push_back(snapshot->bitmap().get());
        }
        return snapshot;
    }

 private:
    const index::IScalarPredicateReader<int64_t>&
    Predicate() const {
        return dynamic_cast<const index::IScalarPredicateReader<int64_t>&>(
            *inner_);
    }
    const index::INullReader&
    Nulls() const {
        return dynamic_cast<const index::INullReader&>(*inner_);
    }
    index::IIndexReaderBasePtr inner_;
    std::shared_ptr<ValidityCalls> calls_;
};

class IndexValidityExpressionTest : public testing::TestWithParam<bool> {
 protected:
    void
    SetUp() override {
        previous_enabled_ = exec::ExprResCacheManager::IsEnabled();
        auto& cache = exec::ExprResCacheManager::Instance();
        exec::CacheConfig config;
        config.mode = exec::CacheMode::Memory;
        config.admission_threshold = 1;
        config.mem_min_eval_duration_us = 0;
        ASSERT_TRUE(cache.SetConfig(config));
        cache.Clear();
        exec::ExprResCacheManager::SetEnabled(GetParam());
    }
    void
    TearDown() override {
        exec::ExprResCacheManager::Instance().Clear();
        exec::ExprResCacheManager::SetEnabled(previous_enabled_);
    }
    bool previous_enabled_ = false;
};

TEST_P(IndexValidityExpressionTest, NullableContainsLeavesBatchOffsetAndMove) {
    ExprBatchSizeGuard batch_size_guard(7);
    constexpr int64_t rows = 33;
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    auto field =
        schema->AddDebugField("array", DataType::ARRAY, DataType::INT64, true);
    schema->set_primary_field_id(pk);
    auto raw = segcore::DataGen(schema, rows, 42, 0, 1, 3);
    proto::schema::FieldData* data = nullptr;
    for (auto& f : *raw.raw_->mutable_fields_data()) {
        if (f.field_id() == field.get()) {
            data = &f;
        }
    }
    ASSERT_NE(data, nullptr);
    FixedVector<Array> arrays;
    std::vector<uint8_t> validity((rows + 7) / 8, 0);
    for (int64_t row = 0; row < rows; ++row) {
        const bool valid = row % 5 != 0;
        data->mutable_valid_data()->Set(row, valid);
        auto* array =
            data->mutable_scalars()->mutable_array_data()->mutable_data(row);
        array->mutable_long_data()->clear_data();
        if (valid) {
            validity[row >> 3] |= 1 << (row & 7);
            if (row % 11 != 0) {
                array->mutable_long_data()->add_data(row % 7 == 0 ? 100
                                                                  : row % 3);
            }
        }
        arrays.emplace_back(*array);
    }
    auto raw_segment = CreateSealedWithFieldDataLoaded(schema, raw);
    auto segment = CreateSealedWithFieldDataLoaded(schema, raw);
    auto field_data =
        storage::CreateFieldData(DataType::ARRAY, DataType::INT64, true);
    field_data->FillFieldData(arrays.data(), validity.data(), rows, 0);
    auto opened = expr_index::BuildIndex(field,
                                         DataType::ARRAY,
                                         index::INVERTED_INDEX_TYPE,
                                         {field_data},
                                         Config::object(),
                                         DataType::INT64);
    auto calls = std::make_shared<ValidityCalls>();
    opened.reader = std::make_unique<CountingValidityReader>(
        std::move(opened.reader), calls);
    expr_index::InstallIndex(
        *segment, field, DataType::ARRAY, std::move(opened), DataType::INT64);

    expr::TypedExprPtr root;
    for (int i = 0; i < 16; ++i) {
        proto::plan::GenericValue value;
        value.set_int64_val(100 + i);
        auto leaf = std::make_shared<expr::JsonContainsExpr>(
            expr::ColumnInfo(field, DataType::ARRAY, DataType::INT64, {}, true),
            proto::plan::JSONContainsExpr_JSONOp_Contains,
            true,
            std::vector<proto::plan::GenericValue>{value});
        root = root ? std::make_shared<expr::LogicalBinaryExpr>(
                          expr::LogicalBinaryExpr::OpType::Or, root, leaf)
                    : std::static_pointer_cast<const expr::ITypeExpr>(leaf);
    }
    auto expected = EvalExprInBatches(root, raw_segment.get(), rows).result;
    // The helper uses default segment IDs; discard the raw reference's cache
    // before testing the installed reader's admission and repeated cache hits.
    exec::ExprResCacheManager::Instance().Clear();
    auto compare = [&](const ColumnVectorPtr& actual) {
        ASSERT_EQ(actual->size(), rows);
        TargetBitmapView bits(actual->GetRawData(), rows);
        TargetBitmapView valid(actual->GetValidRawData(), rows);
        TargetBitmapView expected_bits(expected->GetRawData(), rows);
        TargetBitmapView expected_valid(expected->GetValidRawData(), rows);
        for (int64_t row = 0; row < rows; ++row) {
            EXPECT_EQ(valid[row], expected_valid[row]) << row;
            if (valid[row]) {
                EXPECT_EQ(bits[row], expected_bits[row]) << row;
            }
        }
    };
    auto evaluation = EvalExprInBatches(root, segment.get(), rows);
    ASSERT_GT(evaluation.batch_sizes.size(), 1);
    compare(evaluation.result);
    if (!GetParam()) {
        ASSERT_EQ(calls->owners.size(), 16);
        EXPECT_EQ(calls->owned, 0);
        for (auto* owner : calls->owners) {
            EXPECT_EQ(owner, calls->owners.front());
        }
    } else {
        EXPECT_TRUE(calls->owners.empty());
        EXPECT_EQ(calls->owned, 16);
    }
    const auto owned_before_repeat = calls->owned;
    compare(EvalExprInBatches(root, segment.get(), rows).result);
    EXPECT_EQ(calls->owned, owned_before_repeat);

    // Whole-range output may move its locally owned hits, but must materialize
    // shared validity. A subsequent evaluation must still see an intact reader
    // and expression-cache entry.
    auto query = std::make_shared<exec::QueryContext>(
        DEAFULT_QUERY_ID, segment.get(), rows, MAX_TIMESTAMP);
    exec::ExecContext execution(query.get());
    auto compiled = exec::CompileExpressions({root}, &execution, {}, false);
    ASSERT_TRUE(compiled[0]->CanExecuteAllAtOnce());
    compiled[0]->SetExecuteAllAtOnce();
    exec::EvalCtx context(&execution);
    VectorPtr output;
    compiled[0]->Eval(context, output);
    auto whole = std::dynamic_pointer_cast<ColumnVector>(output);
    ASSERT_NE(whole, nullptr);
    compare(whole);
    TargetBitmapView(whole->GetValidRawData(), rows).flip();
    compare(EvalExprInBatches(root, segment.get(), rows).result);

    auto non_null = std::make_shared<expr::NullExpr>(
        expr::ColumnInfo(field, DataType::ARRAY, DataType::INT64, {}, true),
        proto::plan::NullExpr_NullOp_IsNotNull);
    auto sequential = EvalExprInBatches(non_null, segment.get(), rows);
    ASSERT_GT(sequential.batch_sizes.size(), 1);
    TargetBitmapView sequential_bits(sequential.result->GetRawData(), rows);
    for (int64_t row = 0; row < rows; ++row) {
        EXPECT_EQ(sequential_bits[row], row % 5 != 0);
    }
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, non_null);
    exec::OffsetVector offsets = {32, 0, 5, 7, 7, 1};
    auto selected = gen_filter_res(
        plan.get(), segment.get(), rows, MAX_TIMESTAMP, &offsets);
    TargetBitmapView selected_bits(selected->GetRawData(), offsets.size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        EXPECT_EQ(selected_bits[i], offsets[i] % 5 != 0);
    }
}

INSTANTIATE_TEST_SUITE_P(CacheModes,
                         IndexValidityExpressionTest,
                         testing::Values(false, true));

// Mutable/unsupported readers keep virtual IsNotNull semantics and snapshots
// must not retain a reference to a mutable source bitmap.
TEST(IndexValidityExpressionFallback, UsesOwnedSnapshotForMutableReader) {
    class MutableNullReader final : public index::INullReader {
     public:
        TargetBitmap
        IsNull() const override {
            return TargetBitmap(3, false);
        }
        TargetBitmap
        IsNotNull() const override {
            ++calls;
            TargetBitmap result(3, true);
            result.reset(null_offset);
            return result;
        }
        mutable int calls = 0;
        int null_offset = 1;
    } reader;
    auto first = exec::SegmentExpr::GetIndexValiditySnapshot(reader);
    reader.null_offset = 2;
    auto second = exec::SegmentExpr::GetIndexValiditySnapshot(reader);
    EXPECT_EQ(reader.calls, 2);
    EXPECT_NE(first.bitmap().get(), second.bitmap().get());
    EXPECT_FALSE(first[1]);
    EXPECT_TRUE(first[2]);
    EXPECT_TRUE(second[1]);
    EXPECT_FALSE(second[2]);
}

}  // namespace
}  // namespace milvus::test
