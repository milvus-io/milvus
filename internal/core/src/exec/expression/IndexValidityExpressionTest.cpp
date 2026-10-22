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
#include "index/contracts/query/IJsonIndexReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"

namespace milvus::test {
namespace {

struct ValidityCalls {
    size_t owned = 0;
    std::vector<const uint8_t*> owners;
};

// Keep the production inverted reader and its loader; count only the consumer
// boundary calls so this test detects a full IsNotNull copy per leaf.
template <typename T>
class CountingValidityReader final : public index::IIndexReaderBase,
                                     public index::IScalarPredicateReader<T>,
                                     public index::INullReader {
 public:
    CountingValidityReader(index::IIndexReaderBasePtr inner,
                           std::shared_ptr<ValidityCalls> calls)
        : inner_(index::JsonResolvedReader::Owned(std::move(inner))),
          calls_(std::move(calls)) {
    }
    CountingValidityReader(index::JsonResolvedReader inner,
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
    In(size_t n, const T* values) const override {
        return Predicate().In(n, values);
    }
    TargetBitmap
    NotIn(size_t n, const T* values) const override {
        return Predicate().NotIn(n, values);
    }
    TargetBitmap
    Range(const T& value, index::CompareOp op) const override {
        return Predicate().Range(value, op);
    }
    TargetBitmap
    Range(const T& lo, bool lo_inc, const T& hi, bool hi_inc) const override {
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
    std::optional<BitsetView>
    GetValidityView() const override {
        auto snapshot = Nulls().GetValidityView();
        if (snapshot) {
            calls_->owners.push_back(snapshot->data());
        }
        return snapshot;
    }

 private:
    const index::IScalarPredicateReader<T>&
    Predicate() const {
        return dynamic_cast<const index::IScalarPredicateReader<T>&>(
            *inner_.get());
    }
    const index::INullReader&
    Nulls() const {
        return dynamic_cast<const index::INullReader&>(*inner_.get());
    }
    index::JsonResolvedReader inner_;
    std::shared_ptr<ValidityCalls> calls_;
};

// Preserve the production JSON router/loader and wrap only its resolved
// DOUBLE reader to observe the validity boundary. The parent pin owns storage.
class CountingJsonValidityReader final : public index::IIndexReaderBase,
                                         public index::IJsonIndexReader {
 public:
    CountingJsonValidityReader(index::IIndexReaderBasePtr inner,
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
    index::JsonResolvedReader
    Resolve(std::string_view path, JsonCastType cast_type) const override {
        auto resolved = Json().Resolve(path, cast_type);
        if (!resolved) {
            return {};
        }
        return index::JsonResolvedReader::Owned(
            std::make_unique<CountingValidityReader<double>>(
                std::move(resolved), calls_));
    }
    TargetBitmap
    Exists(std::string_view path) const override {
        return Json().Exists(path);
    }
    std::vector<JsonCastType>
    CastTypesOf(std::string_view path) const override {
        return Json().CastTypesOf(path);
    }

 private:
    const index::IJsonIndexReader&
    Json() const {
        return dynamic_cast<const index::IJsonIndexReader&>(*inner_);
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
    ASSERT_TRUE((*schema)[field].is_nullable());
    ASSERT_EQ(data->scalars().array_data().data_size(), rows);
    // Master stores row validity in ScalarField; the legacy FieldData field
    // may be empty. Use the same accessor as the raw-segment loader.
    auto* row_validity = MutableFieldDataRowValidData(data);
    row_validity->Resize(rows, true);
    FixedVector<Array> arrays;
    std::vector<uint8_t> validity((rows + 7) / 8, 0);
    for (int64_t row = 0; row < rows; ++row) {
        const bool valid = row % 5 != 0;
        row_validity->Set(row, valid);
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
    opened.reader = std::make_unique<CountingValidityReader<int64_t>>(
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
    TargetBitmap private_data(whole->GetBitmap());
    TargetBitmap private_valid(whole->GetValidBitmap());
    private_data.flip();
    private_valid.flip();
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

TEST_P(IndexValidityExpressionTest,
       BinaryRangeOffsetsGatherValidityWithoutFullCopy) {
    constexpr int64_t rows = 33;
    for (bool has_nulls : {false, true}) {
        SCOPED_TRACE(has_nulls ? "has-nulls" : "all-valid");
        // Scalar offsets always scan raw data. A typed JSON-path index routes
        // offset ranges to ExecRangeVisitorImplForIndex, the borrowed-validity branch
        // under regression. Keep raw JSON unloaded so raw fallback cannot pass.
        auto schema = std::make_shared<Schema>();
        auto pk = schema->AddDebugField("pk", DataType::INT64);
        auto field = schema->AddDebugField("json", DataType::JSON, true);
        schema->set_primary_field_id(pk);
        auto raw = segcore::DataGen(schema, rows, 42);
        auto segment =
            CreateSealedWithFieldDataLoaded(schema, raw, false, {field.get()});
        ASSERT_FALSE(segment->HasFieldData(field));
        std::vector<std::string> documents;
        std::vector<uint8_t> validity((rows + 7) / 8, 0xff);
        for (int64_t row = 0; row < rows; ++row) {
            documents.push_back("{\"value\":" + std::to_string(row) + "}");
            if (has_nulls && row % 5 == 0) {
                validity[row >> 3] &= ~(1 << (row & 7));
            }
        }
        auto field_data =
            expr_index::JsonField(documents, true, validity.data());
        auto opened = expr_index::BuildIndex(
            field,
            DataType::JSON,
            index::INVERTED_INDEX_TYPE,
            {field_data},
            {{"json_path", "/value"}, {"json_cast_type", "DOUBLE"}});
        auto calls = std::make_shared<ValidityCalls>();
        opened.reader = std::make_unique<CountingJsonValidityReader>(
            std::move(opened.reader), calls);
        expr_index::InstallIndex(
            *segment, field, DataType::JSON, std::move(opened));
        ASSERT_FALSE(segment->HasFieldData(field));
        proto::plan::GenericValue lower, upper;
        lower.set_float_val(3);
        upper.set_float_val(20);
        auto range = std::make_shared<expr::BinaryRangeFilterExpr>(
            expr::ColumnInfo(field, DataType::JSON, {"value"}, true),
            lower,
            upper,
            true,
            true);
        auto query = std::make_shared<exec::QueryContext>(
            DEAFULT_QUERY_ID, segment.get(), rows, MAX_TIMESTAMP);
        exec::ExecContext execution(query.get());
        auto compiled =
            exec::CompileExpressions({range}, &execution, {}, false);
        exec::EvalCtx context(&execution);
        std::vector<exec::OffsetVector> batches{{32, 0, 5, 7, 7, 1},
                                                {20, 3, 21, 10, 10, 32}};
        for (auto& offsets : batches) {
            context.set_offset_input(&offsets);
            VectorPtr output;
            compiled[0]->Eval(context, output);
            auto column = std::dynamic_pointer_cast<ColumnVector>(output);
            ASSERT_NE(column, nullptr);
            ASSERT_EQ(column->size(), offsets.size());
            TargetBitmapView actual(column->GetRawData(), offsets.size());
            TargetBitmapView valid(column->GetValidRawData(), offsets.size());
            for (size_t i = 0; i < offsets.size(); ++i) {
                const auto row = offsets[i];
                const bool expected_valid = !has_nulls || row % 5 != 0;
                EXPECT_EQ(valid[i], expected_valid) << "row=" << row;
                if (expected_valid) {
                    EXPECT_EQ(actual[i], row >= 3 && row <= 20)
                        << "row=" << row;
                }
            }
            // Repeated Eval reads the retained hits and pinned validity directly;
            // callers can mutate their own output without changing either.
            TargetBitmap private_valid(valid);
            private_valid.flip();
            ASSERT_EQ(calls->owners.size(), 1);
            EXPECT_EQ(calls->owned, 0);
            EXPECT_EQ(calls->owners.front() == nullptr, !has_nulls);
        }
        context.set_offset_input(nullptr);
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
    // Fallback snapshots determine their own length even if a mutable reader's
    // separately sampled count disagrees. Copies retain that snapshot's owner.
    auto first = exec::SegmentExpr::CaptureIndexValidity(reader, 100);
    reader.null_offset = 2;
    auto second = exec::SegmentExpr::CaptureIndexValidity(reader, 1);
    EXPECT_EQ(reader.calls, 2);
    EXPECT_EQ(first.Size(), 3);
    EXPECT_EQ(second.Size(), 3);
    EXPECT_NE(first.View().data(), second.View().data());
    EXPECT_FALSE(first.View().test(1));
    EXPECT_TRUE(first.View().test(2));
    EXPECT_TRUE(second.View().test(1));
    EXPECT_FALSE(second.View().test(2));
    auto retained = first;
    first.Reset();
    EXPECT_FALSE(retained.View().test(1));
    retained.Reset();
}

TEST(IndexValidityState, ReplacementKeepsRepresentationAndLengthTogether) {
    exec::SegmentExpr::IndexValidity state;
    EXPECT_FALSE(state.Initialized());
    auto owner = std::make_shared<TargetBitmap>(17, false);
    std::weak_ptr<TargetBitmap> weak = owner;
    state.SetOwned(std::move(owner));
    EXPECT_EQ(state.Size(), 17);
    EXPECT_FALSE(state.Gather({0}).all());
    state.SetBorrowed(BitsetView{}, 130);
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(state.Size(), 130);
    EXPECT_TRUE(state.Gather({0}).all());
    auto all_valid = state.Gather({129, 0, 63, 63});
    EXPECT_EQ(all_valid.size(), 4);
    EXPECT_TRUE(all_valid.all());
    EXPECT_ANY_THROW(state.Gather({130}));
    EXPECT_ANY_THROW(state.Gather({-1}));

    TargetBitmap all_null(17, false);
    state.SetBorrowed(BitsetView(all_null), 17);
    EXPECT_FALSE(state.Gather({0}).all());
    EXPECT_EQ(state.Gather({16, 0, 0}).count(), 0);
    EXPECT_ANY_THROW(state.SetBorrowed(BitsetView(all_null), 16));
    EXPECT_EQ(state.Size(), 17);  // A rejected replacement preserves the state.
    state.SetOwned(std::make_shared<TargetBitmap>(5, true));
    EXPECT_EQ(state.Size(), 5);
    EXPECT_TRUE(state.Gather({0}).all());
    auto copy = state;
    auto moved = std::move(state);
    EXPECT_EQ(copy.View().data(), moved.View().data());
    EXPECT_FALSE(state.Initialized());
    moved.SetOwned(std::make_shared<TargetBitmap>(3, false));
    EXPECT_EQ(moved.Size(), 3);
    EXPECT_FALSE(moved.Gather({0}).all());
    EXPECT_EQ(copy.Size(), 5);
    EXPECT_TRUE(copy.Gather({0}).all());
    moved.Reset();
    EXPECT_FALSE(moved.Initialized());
}

TEST(IndexValidityView, AppendsUnalignedWindowsAndAllValidSentinel) {
    TargetBitmap source(130, true);
    source.reset(65);
    source.reset(129);
    BitsetView view(reinterpret_cast<const uint8_t*>(source.data()),
                    source.size());
    TargetBitmap output(5, false);
    output.set(2);
    exec::SegmentExpr::AppendValidityView(output, view, 130, 63, 7);
    ASSERT_EQ(output.size(), 12);
    for (size_t i = 0; i < 12; ++i) {
        EXPECT_EQ(output[i], i < 5 ? i == 2 : i != 7);
    }
    output.flip();
    EXPECT_FALSE(view.test(65));
    EXPECT_TRUE(view.test(64));
    EXPECT_ANY_THROW(
        exec::SegmentExpr::AppendValidityView(output, view, 130, 129, 2));
    EXPECT_ANY_THROW(
        exec::SegmentExpr::AppendValidityView(output, view, 129, 0, 1));
    TargetBitmap all_valid(5, false);
    all_valid.set(2);
    exec::SegmentExpr::AppendValidityView(all_valid, BitsetView{}, 130, 123, 7);
    ASSERT_EQ(all_valid.size(), 12);
    for (size_t i = 0; i < 12; ++i) {
        EXPECT_EQ(all_valid[i], i == 2 || i >= 5);
    }
    exec::SegmentExpr::AppendValidityView(all_valid, BitsetView{}, 130, 130, 0);
    EXPECT_EQ(all_valid.size(), 12);
    EXPECT_ANY_THROW(exec::SegmentExpr::AppendValidityView(
        all_valid, BitsetView{}, 130, -1, 1));
    // A nonempty all-zero mask is all-null, never the all-valid sentinel.
    TargetBitmap all_null(17, false), null_batch;
    BitsetView null_view(reinterpret_cast<const uint8_t*>(all_null.data()), 17);
    exec::SegmentExpr::AppendValidityView(null_batch, null_view, 17, 1, 9);
    ASSERT_EQ(null_batch.size(), 9);
    EXPECT_EQ(null_batch.count(), 0);
}

}  // namespace
}  // namespace milvus::test
