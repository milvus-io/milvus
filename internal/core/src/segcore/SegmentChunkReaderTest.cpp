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

#include <sys/mman.h>

#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <tuple>

#include "gtest/gtest.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/IndexMeta.h"
#include "common/Schema.h"
#include "expr/ITypeExpr.h"
#include "exec/expression/ColumnExpr.h"
#include "exec/expression/CompareExpr.h"
#include "exec/operator/search-groupby/GroupMembership.h"
#include "knowhere/comp/index_param.h"
#include "pb/plan.pb.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/PlanNode.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentChunkReader.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "mmap/ChunkedColumn.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/DataGen.h"
#include "test_utils/SegcoreConfigUtils.h"

namespace milvus::segcore {

TEST(SegmentChunkReader, NumericVariantMismatchIsSystemError) {
    const data_access_type value = int64_t{7};

    try {
        (void)get_from_variant<int32_t>(value);
        FAIL() << "expected a variant type mismatch";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::UnexpectedError);
    }
}

TEST(SegmentChunkReader, StringVariantMismatchIsSystemError) {
    const data_access_type value = true;

    try {
        (void)get_from_variant<std::string>(value);
        FAIL() << "expected a variant type mismatch";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::UnexpectedError);
    }
}

// MoveCursorForSingleChunk used to lack a break: once processed_rows reached
// batch_size the condition stayed true for every remaining row, so the loops ran
// to the end of the segment and left the cursor on the last row instead of on
// batch_size. Compare MoveCursorForMultipleChunk (same class) and
// MoveCursorForData (exec/expression/Expr.h), which both stop.
TEST(SegmentChunkReader, MoveCursorForSingleChunkStopsAtBatchBoundary) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto segment = CreateGrowingSegment(schema, empty_index_meta);

    const int64_t size_per_chunk = segment->size_per_chunk();
    ASSERT_GT(size_per_chunk, 0);
    const int64_t active_count = 2 * size_per_chunk + 1;
    const SegmentChunkReader reader(nullptr, segment.get(), active_count);

    const int64_t num_chunk =
        (active_count + size_per_chunk - 1) / size_per_chunk;
    const int64_t batch_size = size_per_chunk;

    int64_t chunk_id = 0;
    int64_t chunk_pos = 0;
    reader.MoveCursorForSingleChunk(chunk_id, chunk_pos, num_chunk, batch_size);

    const int64_t absolute_offset = chunk_id * size_per_chunk + chunk_pos;
    EXPECT_EQ(absolute_offset, batch_size)
        << "cursor must advance by exactly batch_size";
    EXPECT_LT(absolute_offset, active_count)
        << "cursor was pushed to the segment end";

    // A second move continues from where the first one stopped.
    reader.MoveCursorForSingleChunk(chunk_id, chunk_pos, num_chunk, batch_size);
    EXPECT_EQ(chunk_id * size_per_chunk + chunk_pos, 2 * batch_size);
}

// The same bug seen through the expression stack, which is what makes it a
// correctness bug rather than a wasted scan: PhyConjunctFilterExpr skips the
// remaining inputs of an all-false batch by calling MoveCursor() on them, so a
// cursor left at the segment end makes the *next* batch of the skipped
// expression read nothing. CompareExpr on a growing segment is the shortest
// route to MoveCursorForSingleChunk -- CompareExpr.h takes it whenever the
// segment is not chunked.
TEST(SegmentChunkReader, GrowingConjunctSkipKeepsNextBatchCorrect) {
    // Both knobs are process-global. Small values keep the fixture cheap while
    // still giving several batches, and several chunks per batch so the
    // chunk-by-chunk walk inside the helper actually runs.
    constexpr int64_t kBatch = 64;
    constexpr int64_t kChunkRows = 32;
    constexpr int64_t N = 4 * kBatch;

    struct BatchSizeGuard {
        int64_t saved;
        ~BatchSizeGuard() {
            EXEC_EVAL_EXPR_BATCH_SIZE.store(saved);
        }
    } batch_guard{EXEC_EVAL_EXPR_BATCH_SIZE.load()};
    EXEC_EVAL_EXPR_BATCH_SIZE.store(kBatch);

    ScopedSegcoreConfigRestore config_restore;
    auto& config = SegcoreConfig::default_config();
    config.set_chunk_rows(kChunkRows);

    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto gate = schema->AddDebugField("gate", DataType::INT64);
    auto left = schema->AddDebugField("left", DataType::INT64);
    auto right = schema->AddDebugField("right", DataType::INT64);

    auto raw_data = DataGen(schema, N);
    auto set_col = [&](FieldId field_id,
                       const std::function<int64_t(int64_t)>& gen) {
        for (auto& field_data : *raw_data.raw_->mutable_fields_data()) {
            if (field_data.field_id() != field_id.get()) {
                continue;
            }
            auto* col = field_data.mutable_scalars()
                            ->mutable_long_data()
                            ->mutable_data();
            for (int64_t i = 0; i < N; ++i) {
                col->at(i) = gen(i);
            }
            return;
        }
        FAIL() << "field " << field_id.get() << " missing from generated data";
    };
    // gate is false for the whole first batch and true for every later one, so
    // exactly the first batch short-circuits and skips the compare.
    set_col(gate, [](int64_t i) { return i; });
    // left < right alternates row by row: any odd cursor shift shows up as an
    // inverted result, and a cursor parked at the segment end as no hits at all.
    set_col(left, [](int64_t i) { return i; });
    set_col(right, [](int64_t i) { return i % 2 == 0 ? i + 1 : i - 1; });

    auto segment = CreateGrowingSegment(schema, empty_index_meta, 1, config);
    ASSERT_FALSE(segment->is_chunked());
    ASSERT_EQ(segment->size_per_chunk(), kChunkRows);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    raw_data.row_ids_.data(),
                    raw_data.timestamps_.data(),
                    std::make_shared<InsertRecordProto>(*raw_data.raw_));

    proto::plan::GenericValue gate_bound;
    gate_bound.set_int64_val(kBatch);
    auto gate_expr = std::make_shared<milvus::expr::UnaryRangeFilterExpr>(
        milvus::expr::ColumnInfo(gate, DataType::INT64),
        proto::plan::OpType::GreaterEqual,
        gate_bound);
    auto compare_expr = std::make_shared<milvus::expr::CompareExpr>(
        left,
        right,
        DataType::INT64,
        DataType::INT64,
        proto::plan::OpType::LessThan);
    auto filter = std::make_shared<milvus::expr::LogicalBinaryExpr>(
        milvus::expr::LogicalBinaryExpr::OpType::And, gate_expr, compare_expr);
    auto plan = std::make_shared<milvus::plan::FilterBitsNode>(
        DEFAULT_PLANNODE_ID, filter);

    auto final =
        milvus::query::ExecuteQueryExpr(plan, segment.get(), N, MAX_TIMESTAMP);
    ASSERT_EQ(final.size(), N);
    for (int64_t i = 0; i < N; ++i) {
        const bool expected = i >= kBatch && i % 2 == 0;
        ASSERT_EQ(final[i], expected) << "cursor desync at row " << i;
    }
}

namespace {
struct ReaderStats {
    int64_t legacy_view_rows = 0;
    int64_t legacy_spans = 0;
    int64_t scanned_rows = 0;
    int64_t largest_scan = 0;
    int64_t takes = 0;
    int64_t scans = 0;
    int64_t seeks = 0;
    int64_t pins = 0;
    bool fail_pin = false;
};

class CountingReaderScan final : public ScanCursor {
 public:
    CountingReaderScan(ScanResult cursor, std::shared_ptr<ReaderStats> stats)
        : cursor_(std::move(cursor)), stats_(std::move(stats)) {
    }
    int64_t
    Position() const override {
        return cursor_->Position();
    }
    void
    Seek(int64_t offset) override {
        ++stats_->seeks;
        cursor_->Seek(offset);
    }
    bool
    Next(int64_t length, ScanReadMode mode, ScanBatch* batch) override {
        const auto found = cursor_->Next(length, mode, batch);
        if (found) {
            stats_->scanned_rows += batch->size;
            stats_->largest_scan = std::max(stats_->largest_scan, batch->size);
        }
        return found;
    }

 private:
    ScanResult cursor_;
    std::shared_ptr<ReaderStats> stats_;
};

template <typename Base>
class ReaderColumn : public Base {
 public:
    ReaderColumn(std::shared_ptr<CacheSlot<Chunk>> slot,
                 const FieldMeta& meta,
                 std::shared_ptr<ReaderStats> stats)
        : Base(std::move(slot), meta), stats_(std::move(stats)) {
    }
    ScanResult
    Scan(milvus::OpContext* ctx, const ScanOptions& options) const override {
        ++stats_->scans;
        return std::make_unique<CountingReaderScan>(Base::Scan(ctx, options),
                                                    stats_);
    }
    TakeResultPtr
    Take(milvus::OpContext* ctx, TakeOptions options) const override {
        ++stats_->takes;
        return Base::Take(ctx, std::move(options));
    }
    PinWrapper<SpanBase>
    Span(milvus::OpContext* ctx, int64_t chunk) const override {
        ++stats_->legacy_spans;
        return Base::Span(ctx, chunk);
    }
    PinWrapper<std::pair<std::vector<std::string_view>, ValidityView>>
    StringViews(
        milvus::OpContext* ctx,
        int64_t chunk,
        std::optional<std::pair<int64_t, int64_t>> range) const override {
        stats_->legacy_view_rows +=
            range ? range->second : this->chunk_row_nums(chunk);
        return Base::StringViews(ctx, chunk, range);
    }
    PinWrapper<Chunk*>
    GetChunk(milvus::OpContext* ctx, int64_t chunk) const override {
        if (stats_->fail_pin) {
            ThrowInfo(ErrorCode::FileReadFailed,
                      "injected reader scan failure");
        }
        ++stats_->pins;
        return Base::GetChunk(ctx, chunk);
    }

 protected:
    using TakeCellAccess = typename Base::TakeCellAccess;
    using TakeCellPin = typename Base::TakeCellPin;
    TakeCellAccess
    MakeTakeCellAccess(
        milvus::OpContext* ctx,
        const std::vector<int64_t>* batch_cell_ids = nullptr) const override {
        if (batch_cell_ids != nullptr) {
            if (!batch_cell_ids->empty()) {
                if (stats_->fail_pin) {
                    ThrowInfo(ErrorCode::FileReadFailed,
                              "injected reader Take failure");
                }
                ++stats_->pins;
            }
            return Base::MakeTakeCellAccess(ctx, batch_cell_ids);
        }
        auto access = Base::MakeTakeCellAccess(ctx, batch_cell_ids);
        return TakeCellPin([pin = std::move(std::get<TakeCellPin>(access)),
                            stats = stats_](int64_t chunk) {
            if (stats->fail_pin) {
                ThrowInfo(ErrorCode::FileReadFailed,
                          "injected reader Take failure");
            }
            ++stats->pins;
            return pin(chunk);
        });
    }

 private:
    std::shared_ptr<ReaderStats> stats_;
};

using ReaderStringColumn = ReaderColumn<ChunkedVariableColumn<std::string>>;
using ReaderFixedColumn = ReaderColumn<ChunkedColumn>;

class ReaderSegment : public ChunkedSegmentSealedImpl {
 public:
    ReaderSegment(SchemaPtr schema,
                  std::shared_ptr<ChunkedColumnInterface> column)
        : ChunkedSegmentSealedImpl(
              schema, empty_index_meta, SegcoreConfig::default_config(), 101),
          column_(std::move(column)) {
    }
    std::shared_ptr<ChunkedColumnInterface>
    GetChunkedColumn(FieldId field) const override {
        EXPECT_FALSE(forbid_live_column_read);
        return Column(field);
    }
    bool
    HasFieldData(FieldId) const override {
        return true;
    }
    int64_t
    num_chunk_data(FieldId field, OpContext* = nullptr) const override {
        return Column(field)->num_chunks();
    }
    int64_t
    chunk_size(FieldId field,
               int64_t chunk,
               OpContext* = nullptr) const override {
        return Column(field)->chunk_row_nums(chunk);
    }
    int64_t
    num_rows_until_chunk(FieldId field,
                         int64_t chunk,
                         OpContext* = nullptr) const override {
        return Column(field)->GetNumRowsUntilChunk(chunk);
    }
    std::pair<int64_t, int64_t>
    get_chunk_by_offset(FieldId field,
                        int64_t offset,
                        OpContext* = nullptr) const override {
        return Column(field)->GetChunkIDByOffset(offset);
    }
    int64_t
    get_row_count() const override {
        return column_->NumRows();
    }

    void
    SetOtherColumn(FieldId field,
                   std::shared_ptr<ChunkedColumnInterface> column) {
        other_field_ = field;
        other_column_ = std::move(column);
    }
    bool forbid_live_column_read = false;

 protected:
    PinWrapper<std::pair<std::vector<std::string_view>, ValidityView>>
    chunk_string_view_impl(
        milvus::OpContext* ctx,
        FieldId,
        int64_t chunk,
        std::optional<std::pair<int64_t, int64_t>> range) const override {
        return column_->StringViews(ctx, chunk, range);
    }

 private:
    std::shared_ptr<ChunkedColumnInterface>
    Column(FieldId field) const {
        return field == other_field_ ? other_column_ : column_;
    }
    FieldId other_field_{-1};
    std::shared_ptr<ChunkedColumnInterface> other_column_;
    std::shared_ptr<ChunkedColumnInterface> column_;
};

class ReaderSnapshot : public SegmentReadSnapshot {
 public:
    explicit ReaderSnapshot(std::shared_ptr<ChunkedColumnInterface> column)
        : column_(std::move(column)) {
    }
    int64_t
    chunk_size(FieldId, int64_t chunk, OpContext* = nullptr) const override {
        return column_->chunk_row_nums(chunk);
    }
    int64_t
    num_rows_until_chunk(FieldId,
                         int64_t chunk,
                         OpContext* = nullptr) const override {
        return column_->GetNumRowsUntilChunk(chunk);
    }
    std::pair<int64_t, int64_t>
    get_chunk_by_offset(FieldId,
                        int64_t offset,
                        OpContext* = nullptr) const override {
        return column_->GetChunkIDByOffset(offset);
    }
    int64_t
    num_chunk_data(FieldId, OpContext* = nullptr) const override {
        return column_->num_chunks();
    }
    int64_t
    get_row_count() const override {
        return column_->NumRows();
    }
    std::pair<std::shared_ptr<ChunkedColumnInterface>, FieldSkipMetricsView>
    GetDataScanResources(FieldId) const override {
        ++column_reads;
        return {column_, FieldSkipMetricsView{}};
    }
    std::shared_ptr<const IStructElementOffsets>
    GetStructElementOffsets(FieldId) const override {
        ThrowInfo(ErrorCode::Unsupported,
                  "string test snapshot does not support struct offsets");
    }
    mutable int64_t column_reads = 0;

 private:
    std::shared_ptr<ChunkedColumnInterface> column_;
};

class SegmentChunkReaderStringTest
    : public ::testing::TestWithParam<std::tuple<bool, bool>> {
 protected:
    bool
    CursorOwnsPin() const {
        return std::get<1>(GetParam());
    }
    void
    SetUp() override {
        SegcoreConfig::default_config().set_scan_cursor_owns_pin(
            CursorOwnsPin());
        const auto nullable = std::get<0>(GetParam());
        auto schema = std::make_shared<Schema>();
        auto pk = schema->AddDebugField("pk", DataType::INT64);
        schema->set_primary_field_id(pk);
        field_ = schema->AddDebugField("value", DataType::VARCHAR, nullable);
        other_field_ =
            schema->AddDebugField("other", DataType::VARCHAR, nullable);
        schema_ = schema;
        stats_ = std::make_shared<ReaderStats>();
        column_ = MakeColumn({8192, 7, 20}, stats_, expected_);
        ASSERT_NE(column_, nullptr);
        segment_ = std::make_unique<ReaderSegment>(schema, column_);
    }
    void
    TearDown() override {
        SegcoreConfig::default_config().set_scan_cursor_owns_pin(
            saved_cursor_owns_pin_);
    }
    std::shared_ptr<ReaderStringColumn>
    MakeColumn(const std::vector<int64_t>& rows_per_chunk,
               const std::shared_ptr<ReaderStats>& stats,
               std::vector<std::optional<std::string>>& expected,
               bool other = false) {
        const auto nullable = std::get<0>(GetParam());
        std::vector<std::unique_ptr<Chunk>> chunks;
        int64_t row = 0;
        for (auto rows : rows_per_chunk) {
            std::vector<std::string> values;
            size_t payload = 0;
            for (int64_t i = 0; i < rows; ++i, ++row) {
                auto value =
                    row == 0 ? std::string{} : "value-" + std::to_string(row);
                if (row == 8192) {
                    value = std::string("embedded\0nul", 12);
                }
                if (other && row % 3 == 0) {
                    value += "-different";
                }
                payload += value.size();
                values.push_back(value);
                expected.push_back(nullable && (row == (other ? 17 : 16) ||
                                                row == (other ? 8192 : 8198))
                                       ? std::nullopt
                                       : std::optional<std::string>(value));
            }
            const auto bitmap = nullable ? (rows + 7) / 8 : 0;
            const auto header = bitmap + (rows + 1) * sizeof(uint32_t);
            const auto size = header + payload + MMAP_STRING_PADDING;
            auto* data = static_cast<char*>(mmap(nullptr,
                                                 size,
                                                 PROT_READ | PROT_WRITE,
                                                 MAP_PRIVATE | MAP_ANONYMOUS,
                                                 -1,
                                                 0));
            if (data == MAP_FAILED) {
                ADD_FAILURE() << "cannot allocate string fixture";
                return nullptr;
            }
            auto guard = std::make_shared<ChunkMmapGuard>(data, size, "");
            lifetimes_.push_back(guard);
            uint32_t offset = header;
            for (int64_t i = 0; i <= rows; ++i) {
                std::memcpy(data + bitmap + i * sizeof(offset),
                            &offset,
                            sizeof(offset));
                if (i == rows) {
                    break;
                }
                if (nullable && expected[row - rows + i].has_value()) {
                    data[i / 8] |= 1 << (i % 8);
                }
                std::memcpy(data + offset, values[i].data(), values[i].size());
                offset += values[i].size();
            }
            chunks.push_back(std::make_unique<StringChunk>(
                rows, data, size, nullable, guard));
        }
        auto translator = std::make_unique<TestChunkTranslator>(
            rows_per_chunk,
            other ? "segment_reader_other_strings" : "segment_reader_strings",
            std::move(chunks));
        auto slot = cachinglayer::Manager::GetInstance().CreateCacheSlot<Chunk>(
            std::move(translator), nullptr);
        return std::make_shared<ReaderStringColumn>(
            slot, (*schema_)[other ? other_field_ : field_], stats);
    }
    void
    Check(const data_access_type& value, int64_t row) {
        ASSERT_EQ(value.has_value(), expected_[row].has_value()) << row;
        if (value) {
            EXPECT_EQ(get_from_variant<std::string>(value), *expected_[row])
                << row;
        }
    }
    std::unique_ptr<exec::PhyColumnExpr>
    MakeExpression(int64_t batch = 17) {
        auto logical = std::make_shared<expr::ColumnExpr>(
            expr::ColumnInfo(field_, DataType::VARCHAR));
        return std::make_unique<exec::PhyColumnExpr>(
            std::vector<std::shared_ptr<exec::Expr>>{},
            logical,
            "direct column access",
            nullptr,
            segment_.get(),
            expected_.size(),
            batch);
    }
    std::shared_ptr<ColumnVector>
    Evaluate(exec::PhyColumnExpr& expression,
             int64_t start,
             exec::OffsetVector* offsets = nullptr) {
        exec::EvalCtx context(nullptr);
        context.set_offset_input(offsets);
        VectorPtr result;
        expression.Eval(context, result);
        auto values = std::dynamic_pointer_cast<ColumnVector>(result);
        EXPECT_NE(values, nullptr);
        if (!values) {
            return nullptr;
        }
        for (int64_t i = 0; i < values->size(); ++i) {
            const auto row = offsets ? (*offsets)[i] : start + i;
            EXPECT_EQ(values->ValidAt(i), expected_[row].has_value()) << row;
            if (expected_[row]) {
                EXPECT_EQ(values->RawAsValues<std::string>()[i],
                          *expected_[row])
                    << row;
            }
        }
        return values;
    }
    FieldId field_{0};
    FieldId other_field_{0};
    SchemaPtr schema_;
    std::shared_ptr<ReaderStats> stats_;
    std::shared_ptr<ReaderStringColumn> column_;
    std::unique_ptr<ReaderSegment> segment_;
    std::vector<std::optional<std::string>> expected_;
    std::vector<std::weak_ptr<ChunkMmapGuard>> lifetimes_;
    const bool saved_cursor_owns_pin_ =
        SegcoreConfig::default_config().get_scan_cursor_owns_pin();
};

template <typename Traits>
class SegmentChunkReaderScalarTest : public ::testing::Test {
 protected:
    using T = typename Traits::NativeType;

    void
    TearDown() override {
        SegcoreConfig::default_config().set_scan_cursor_owns_pin(
            saved_cursor_owns_pin_);
    }

    static T
    Value(int64_t row) {
        if constexpr (Traits::TypeKind == DataType::TIMESTAMPTZ) {
            return 1700000000000000LL + row;
        }
        return static_cast<T>(row % 7);
    }

    std::shared_ptr<ReaderFixedColumn>
    MakeColumn(const FieldMeta& meta,
               const std::vector<int64_t>& rows,
               const std::shared_ptr<ReaderStats>& stats) {
        std::vector<std::unique_ptr<Chunk>> chunks;
        chunks.reserve(rows.size());
        lifetimes_.clear();
        int64_t start = 0;
        for (const auto count : rows) {
            const auto bitmap = meta.is_nullable() ? (count + 7) / 8 : 0;
            const auto bytes = bitmap + count * sizeof(T);
            const auto mapped = std::max<size_t>(bytes, 1);
            auto* data = static_cast<char*>(mmap(nullptr,
                                                 mapped,
                                                 PROT_READ | PROT_WRITE,
                                                 MAP_PRIVATE | MAP_ANONYMOUS,
                                                 -1,
                                                 0));
            if (data == MAP_FAILED) {
                ADD_FAILURE() << "cannot allocate scalar fixture";
                return nullptr;
            }
            auto guard = std::make_shared<ChunkMmapGuard>(data, mapped, "");
            lifetimes_.push_back(guard);
            for (int64_t i = 0; i < count; ++i) {
                const auto row = start + i;
                const auto value = Value(row);
                std::memcpy(data + bitmap + i * sizeof(T), &value, sizeof(T));
                if (meta.is_nullable() && row != 6 && row != 14) {
                    data[i / 8] |= 1 << (i % 8);
                }
            }
            chunks.push_back(std::make_unique<FixedWidthChunk>(
                count, 1, data, bytes, sizeof(T), meta.is_nullable(), guard));
            start += count;
        }
        auto translator = std::make_unique<TestChunkTranslator>(
            rows, "segment_reader_scalars", std::move(chunks));
        auto slot = cachinglayer::Manager::GetInstance().CreateCacheSlot<Chunk>(
            std::move(translator), nullptr);
        return std::make_shared<ReaderFixedColumn>(slot, meta, stats);
    }

    std::vector<std::weak_ptr<ChunkMmapGuard>> lifetimes_;
    const bool saved_cursor_owns_pin_ =
        SegcoreConfig::default_config().get_scan_cursor_owns_pin();
};

using ReaderScalarTypes = ::testing::Types<TypeTraits<DataType::BOOL>,
                                           TypeTraits<DataType::INT8>,
                                           TypeTraits<DataType::INT16>,
                                           TypeTraits<DataType::INT32>,
                                           TypeTraits<DataType::INT64>,
                                           TypeTraits<DataType::FLOAT>,
                                           TypeTraits<DataType::DOUBLE>,
                                           TypeTraits<DataType::TIMESTAMPTZ>>;
TYPED_TEST_SUITE(SegmentChunkReaderScalarTest, ReaderScalarTypes);
}  // namespace

TYPED_TEST(SegmentChunkReaderScalarTest,
           ScanAndTakeKeepSnapshotWindowsOffsetsAndNulls) {
    for (const bool nullable : {false, true}) {
        for (const bool cursor_owns_pin : {false, true}) {
            SegcoreConfig::default_config().set_scan_cursor_owns_pin(
                cursor_owns_pin);
            auto schema = std::make_shared<Schema>();
            auto pk = schema->AddDebugField("pk", DataType::INT64);
            schema->set_primary_field_id(pk);
            auto field =
                schema->AddDebugField("value", TypeParam::TypeKind, nullable);
            auto stats = std::make_shared<ReaderStats>();
            auto column =
                this->MakeColumn((*schema)[field], {5, 0, 3, 9}, stats);
            ASSERT_NE(column, nullptr);
            ReaderSegment segment(schema, column);
            ReaderSnapshot snapshot(column);
            segment.forbid_live_column_read = true;
            auto logical = std::make_shared<expr::ColumnExpr>(
                expr::ColumnInfo(field, TypeParam::TypeKind));
            exec::PhyColumnExpr expression(
                {}, logical, "direct scalar", nullptr, &segment, 17, 4);
            expression.SetSnapshot(&snapshot);
            exec::EvalCtx context(nullptr);
            const auto check = [&](int64_t start,
                                   exec::OffsetVector* offsets = nullptr) {
                context.set_offset_input(offsets);
                VectorPtr result;
                expression.Eval(context, result);
                const auto values =
                    std::dynamic_pointer_cast<ColumnVector>(result);
                ASSERT_NE(values, nullptr);
                for (int64_t i = 0; i < values->size(); ++i) {
                    const auto row = offsets ? (*offsets)[i] : start + i;
                    const auto valid = !nullable || (row != 6 && row != 14);
                    EXPECT_EQ(values->ValidAt(i), valid);
                    if (valid) {
                        EXPECT_EQ(values->template RawAsValues<
                                      typename TestFixture::T>()[i],
                                  TestFixture::Value(row));
                    }
                }
            };
            for (int64_t start = 0; start < 17; start += 4) {
                if (start == 4) {
                    expression.MoveCursor();
                    continue;
                }
                check(start);
                if (start == 8) {
                    exec::OffsetVector offsets{16, 6, 0, 16, 5, 14};
                    const auto pins_before = stats->pins;
                    check(0, &offsets);
                    EXPECT_EQ(stats->pins - pins_before, 6);
                }
            }
            EXPECT_EQ(stats->scans, 1);
            EXPECT_EQ(stats->takes, 1);
            EXPECT_EQ(stats->seeks, 1);
            EXPECT_EQ(stats->scanned_rows, 13);
            EXPECT_LE(stats->largest_scan, 4);
            EXPECT_EQ(stats->legacy_spans, 0);
            EXPECT_EQ(stats->pins, cursor_owns_pin ? 8 : 10);
            EXPECT_EQ(snapshot.column_reads, 1);
        }
    }
}

TYPED_TEST(SegmentChunkReaderScalarTest,
           ContinuousScanRetainsOnlyCurrentCellWithExplicitPolicy) {
    SegcoreConfig::default_config().set_scan_cursor_owns_pin(false);
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto field = schema->AddDebugField("value", TypeParam::TypeKind, true);
    auto stats = std::make_shared<ReaderStats>();
    auto column = this->MakeColumn((*schema)[field], {5, 0, 3, 9}, stats);
    ASSERT_NE(column, nullptr);
    ReaderSegment segment(schema, column);
    const auto groups = exec::BuildGroupOffsets<typename TestFixture::T>(
        nullptr,
        segment,
        field,
        17,
        {TestFixture::Value(0), std::nullopt},
        nullptr);
    ASSERT_TRUE(groups.has_value());
    EXPECT_EQ((*groups)[1], (std::vector<int64_t>{6, 14}));
    EXPECT_EQ(stats->scans, 1);
    EXPECT_EQ(stats->pins, 3);
    EXPECT_EQ(stats->legacy_spans, 0);
    column->ManualEvictCache();
    EXPECT_TRUE(this->lifetimes_[0].expired());
    EXPECT_TRUE(this->lifetimes_[2].expired());
    EXPECT_TRUE(this->lifetimes_[3].expired());
}

TEST_P(SegmentChunkReaderStringTest, SmallWindowsDoNotRebuildWholeChunkViews) {
    auto expression = MakeExpression();
    for (int64_t start = 0; start < expected_.size(); start += 17) {
        Evaluate(*expression, start);
    }
    EXPECT_EQ(stats_->legacy_view_rows, 0);
    EXPECT_EQ(stats_->scanned_rows, expected_.size());
    EXPECT_LE(stats_->largest_scan, 17);
    EXPECT_EQ(stats_->scans, 1);
    EXPECT_EQ(stats_->pins, CursorOwnsPin() ? 3 : 486);
}

TEST_P(SegmentChunkReaderStringTest, StringAccessorsUseBoundSnapshot) {
    ReaderSnapshot snapshot(column_);
    auto expression = MakeExpression(2);
    expression->SetSnapshot(&snapshot);
    segment_->forbid_live_column_read = true;
    for (int64_t row = 0; row < 8192; row += 2) {
        expression->MoveCursor();
    }
    Evaluate(*expression, 8192);
    Evaluate(*expression, 8194);
    SegmentChunkReader reader(nullptr, segment_.get(), expected_.size());
    reader.SetSnapshot(&snapshot);
    auto chunk_accessor =
        reader.GetChunkDataAccessor(DataType::VARCHAR, field_, 1);
    Check(chunk_accessor(0), 8192);
    exec::OffsetVector offsets{8198, 0, 8198};
    Evaluate(*expression, 0, &offsets);
    EXPECT_EQ(snapshot.column_reads, 2);
    EXPECT_EQ(stats_->scans, 1);
}

// The pinned ApplyFieldValidData path must read the validity bitmap from the
// frozen snapshot column, not from the live segment. A NULL row (row 16 in
// chunk 0) must be cleared while valid rows stay set; if the write went to the
// live segment instead, forbid_live_column_read would trip on the fallback.
TEST_P(SegmentChunkReaderStringTest, ApplyFieldValidDataUsesBoundSnapshot) {
    if (!std::get<0>(GetParam())) {
        // Non-nullable columns short-circuit inside ApplyValidDataInChunk.
        return;
    }
    ReaderSnapshot snapshot(column_);
    segment_->forbid_live_column_read = true;
    SegmentChunkReader reader(nullptr, segment_.get(), expected_.size());
    reader.SetSnapshot(&snapshot);

    // chunk 0 spans rows [0, 8192); row 16 is NULL, rows 0 and 17 are valid.
    const int64_t size = 8192;
    TargetBitmap valid(size, true);
    reader.ApplyFieldValidData(
        nullptr, field_, 0, 0, size, TargetBitmapView(valid));

    EXPECT_FALSE(valid[16]);
    EXPECT_TRUE(valid[0]);
    EXPECT_TRUE(valid[17]);
    EXPECT_EQ(snapshot.column_reads, 1);
}

TEST_P(SegmentChunkReaderStringTest,
       StringExpressionsKeepIndependentScansAcrossWindowsAndOffsets) {
    auto other_stats = std::make_shared<ReaderStats>();
    std::vector<std::optional<std::string>> other_expected;
    auto other_column =
        MakeColumn({8187, 11, 21}, other_stats, other_expected, true);
    ASSERT_NE(other_column, nullptr);
    segment_->SetOtherColumn(other_field_, other_column);
    ASSERT_EQ(column_->NumRows(), other_column->NumRows());
    ASSERT_EQ(column_->num_chunks(), other_column->num_chunks());
    ASSERT_NE(column_->GetNumRowsUntilChunk(1),
              other_column->GetNumRowsUntilChunk(1));
    ASSERT_NE(column_->GetNumRowsUntilChunk(2),
              other_column->GetNumRowsUntilChunk(2));

    // Both real raw column boundaries lie inside small logical windows, but
    // each side returns a different sequence of short Scan batches.
    for (const bool same_field : {false, true}) {
        const auto scans_before = stats_->scans;
        const auto other_scans_before = other_stats->scans;
        auto logical = std::make_shared<expr::CompareExpr>(
            field_,
            same_field ? field_ : other_field_,
            DataType::VARCHAR,
            DataType::VARCHAR,
            proto::plan::OpType::Equal);
        exec::PhyCompareFilterExpr compare({},
                                           logical,
                                           "independent string scans",
                                           nullptr,
                                           segment_.get(),
                                           expected_.size(),
                                           17);
        auto column_logical = std::make_shared<expr::ColumnExpr>(
            expr::ColumnInfo(field_, DataType::VARCHAR));
        exec::PhyColumnExpr column_expr({},
                                        column_logical,
                                        "persistent string scan",
                                        nullptr,
                                        segment_.get(),
                                        expected_.size(),
                                        17);
        exec::EvalCtx context(nullptr);
        auto check = [&](int64_t start, exec::OffsetVector* offsets = nullptr) {
            context.set_offset_input(offsets);
            VectorPtr compared, selected;
            compare.Eval(context, compared);
            column_expr.Eval(context, selected);
            auto matches = std::dynamic_pointer_cast<ColumnVector>(compared);
            auto values = std::dynamic_pointer_cast<ColumnVector>(selected);
            ASSERT_NE(matches, nullptr);
            ASSERT_NE(values, nullptr);
            const auto count =
                offsets ? offsets->size()
                        : std::min<int64_t>(17, expected_.size() - start);
            ASSERT_EQ(matches->size(), count);
            ASSERT_EQ(values->size(), count);
            TargetBitmapView bits(matches->GetRawData(), count);
            for (int64_t i = 0; i < count; ++i) {
                const auto row = offsets ? (*offsets)[i] : start + i;
                const auto& left = expected_[row];
                const auto& right =
                    same_field ? expected_[row] : other_expected[row];
                const auto valid = left.has_value() && right.has_value();
                EXPECT_EQ(matches->ValidAt(i), valid) << row;
                EXPECT_EQ(bits[i], valid && *left == *right) << row;
                EXPECT_EQ(values->ValidAt(i), left.has_value()) << row;
                if (left) {
                    EXPECT_EQ(values->RawAsValues<std::string>()[i], *left)
                        << row;
                }
            }
        };
        for (int64_t start = 0; start < expected_.size(); start += 17) {
            if (start == 0 || start == 51 || (!same_field && start == 8194)) {
                // Also skip across each column's short middle Cell after
                // reading the window spanning their unequal first boundaries.
                compare.MoveCursor();
                column_expr.MoveCursor();
                continue;
            }
            if (start == 34) {
                exec::OffsetVector offsets;
                for (int32_t row :
                     {8198, 8187, 0, 8192, 16, 8198, 8218, 8187}) {
                    offsets.push_back(row);
                }
                check(0, &offsets);
            }
            check(start);
        }
        EXPECT_EQ(stats_->scans - scans_before, same_field ? 3 : 2);
        EXPECT_EQ(other_stats->scans - other_scans_before, same_field ? 0 : 1);
    }
    EXPECT_LE(stats_->largest_scan, 17);
    EXPECT_LE(other_stats->largest_scan, 17);
    EXPECT_EQ(stats_->seeks, 7);
    EXPECT_EQ(other_stats->seeks, 2);
}

TEST_P(SegmentChunkReaderStringTest, OneAccessorCanScanAcrossManyWindows) {
    auto expression = MakeExpression();
    for (int64_t start = 0; start < expected_.size(); start += 17) {
        Evaluate(*expression, start);
    }
    EXPECT_EQ(stats_->scanned_rows, expected_.size());
    EXPECT_EQ(stats_->pins, CursorOwnsPin() ? 3 : 486);
}

TEST_P(SegmentChunkReaderStringTest,
       SparseTakePreservesOrderDuplicatesAndNulls) {
    auto expression = MakeExpression();
    exec::OffsetVector offsets{8192, 8198, 8192, 16, 0, 16, 8218};
    Evaluate(*expression, 0, &offsets);
    EXPECT_EQ(stats_->takes, 1);
    EXPECT_EQ(stats_->legacy_view_rows, 0);
    EXPECT_EQ(stats_->scanned_rows, 0);
    EXPECT_EQ(stats_->pins, 3);
}

TEST_P(SegmentChunkReaderStringTest,
       RandomChunkAccessorKeepsBorrowedDataPinned) {
    SegmentChunkReader reader(nullptr, segment_.get(), expected_.size());
    auto accessor = reader.GetChunkDataAccessor(DataType::VARCHAR, field_, 1);
    auto value = accessor(0);
    column_->ManualEvictCache();
    EXPECT_FALSE(lifetimes_[1].expired());
    Check(value, 8192);
    Check(accessor(6), 8198);
    EXPECT_EQ(stats_->legacy_view_rows, 0);
    accessor = {};
    column_->ManualEvictCache();
    EXPECT_TRUE(lifetimes_[1].expired());
}

TEST_P(SegmentChunkReaderStringTest,
       ScanPinSurvivesEvictionUntilAccessorRelease) {
    auto expression = MakeExpression(2);
    for (int64_t row = 0; row < 8192; row += 2) {
        expression->MoveCursor();
    }
    Evaluate(*expression, 8192);
    auto result = Evaluate(*expression, 8194);
    column_->ManualEvictCache();
    EXPECT_EQ(lifetimes_[1].expired(), !CursorOwnsPin());
    EXPECT_EQ(result->RawAsValues<std::string>()[1], *expected_[8195]);
    EXPECT_EQ(stats_->pins, CursorOwnsPin() ? 1 : 2);
    expression.reset();
    column_->ManualEvictCache();
    EXPECT_TRUE(lifetimes_[1].expired());
}

TEST_P(SegmentChunkReaderStringTest,
       PersistentScanUsesConfiguredPinLifetimeAcrossWindows) {
    auto expression = MakeExpression(2);
    for (int64_t row = 0; row < 8192; row += 2) {
        expression->MoveCursor();
    }
    Evaluate(*expression, 8192);
    // Scan captures its policy once, not once per expression window.
    SegcoreConfig::default_config().set_scan_cursor_owns_pin(!CursorOwnsPin());
    Evaluate(*expression, 8194);
    EXPECT_EQ(stats_->pins, CursorOwnsPin() ? 1 : 2);
    EXPECT_EQ(stats_->scans, 1);
    column_->ManualEvictCache();
    EXPECT_EQ(lifetimes_[1].expired(), !CursorOwnsPin());
    expression.reset();
    column_->ManualEvictCache();
    EXPECT_TRUE(lifetimes_[1].expired());
}

TEST_P(SegmentChunkReaderStringTest, TakeOwnsItsPinAfterColumnRelease) {
    const std::vector<int32_t> offsets{8192, 8193};
    auto take = column_->Take(
        nullptr,
        TakeOptions{OffsetView::From(offsets.data(), offsets.size()),
                    TargetType::StringView});
    const auto items = take->Access<std::string_view>();
    const auto value = items[0];
    column_->ManualEvictCache();
    segment_.reset();
    column_.reset();
    EXPECT_FALSE(lifetimes_[1].expired());
    Check(value.is_valid ? data_access_type(*value.value) : std::nullopt, 8192);
    Check(data_access_type(*items[1].value), 8193);
    take.reset();
    EXPECT_TRUE(lifetimes_[1].expired());
}

TEST_P(SegmentChunkReaderStringTest, ReadFailureKeepsItsCode) {
    auto expression = MakeExpression();
    stats_->fail_pin = true;
    exec::EvalCtx context(nullptr);
    VectorPtr result;
    try {
        expression->Eval(context, result);
        FAIL() << "expected injected scan failure";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FileReadFailed);
    }
    exec::OffsetVector offsets{1};
    context.set_offset_input(&offsets);
    try {
        expression->Eval(context, result);
        FAIL() << "expected injected Take failure";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FileReadFailed);
    }
}

INSTANTIATE_TEST_SUITE_P(NullabilityAndPinPolicy,
                         SegmentChunkReaderStringTest,
                         ::testing::Combine(::testing::Bool(),
                                            ::testing::Bool()));

}  // namespace milvus::segcore
