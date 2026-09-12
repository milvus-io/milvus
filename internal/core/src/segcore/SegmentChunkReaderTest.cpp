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

#include "gtest/gtest.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/IndexMeta.h"
#include "common/Schema.h"
#include "expr/ITypeExpr.h"
#include "exec/expression/ColumnExpr.h"
#include "exec/expression/CompareExpr.h"
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
                    raw_data.raw_);

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
struct StringReadStats {
    int64_t legacy_view_rows = 0;
    int64_t scanned_rows = 0;
    int64_t largest_scan = 0;
    int64_t takes = 0;
    int64_t scans = 0;
    int64_t seeks = 0;
    int64_t pins = 0;
    bool fail_pin = false;
};

class CountingStringScan final : public ScanCursor {
 public:
    CountingStringScan(ScanResult cursor,
                       std::shared_ptr<StringReadStats> stats)
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
    std::shared_ptr<StringReadStats> stats_;
};

class ReaderStringColumn : public ChunkedVariableColumn<std::string> {
 public:
    ReaderStringColumn(std::shared_ptr<CacheSlot<Chunk>> slot,
                       const FieldMeta& meta,
                       std::shared_ptr<StringReadStats> stats)
        : ChunkedVariableColumn(std::move(slot), meta),
          stats_(std::move(stats)) {
    }
    ScanResult
    Scan(milvus::OpContext* ctx, const ScanOptions& options) const override {
        ++stats_->scans;
        return std::make_unique<CountingStringScan>(
            ChunkedVariableColumn::Scan(ctx, options), stats_);
    }
    TakeResultPtr
    Take(milvus::OpContext* ctx, TakeOptions options) const override {
        ++stats_->takes;
        return ChunkedVariableColumn::Take(ctx, std::move(options));
    }
    PinWrapper<std::pair<std::vector<std::string_view>, ValidityView>>
    StringViews(
        milvus::OpContext* ctx,
        int64_t chunk,
        std::optional<std::pair<int64_t, int64_t>> range) const override {
        stats_->legacy_view_rows +=
            range ? range->second : chunk_row_nums(chunk);
        return ChunkedVariableColumn::StringViews(ctx, chunk, range);
    }
    PinWrapper<Chunk*>
    GetChunk(milvus::OpContext* ctx, int64_t chunk) const override {
        if (stats_->fail_pin) {
            ThrowInfo(ErrorCode::FileReadFailed,
                      "injected string scan failure");
        }
        ++stats_->pins;
        return ChunkedVariableColumn::GetChunk(ctx, chunk);
    }

 protected:
    TakeCellPin
    MakeTakeCellPin(milvus::OpContext* ctx) const override {
        auto pin = ChunkedVariableColumn::MakeTakeCellPin(ctx);
        return [pin = std::move(pin), stats = stats_](int64_t chunk) {
            if (stats->fail_pin) {
                ThrowInfo(ErrorCode::FileReadFailed,
                          "injected string Take failure");
            }
            ++stats->pins;
            return pin(chunk);
        };
    }

 private:
    std::shared_ptr<StringReadStats> stats_;
};

class ReaderStringSegment : public ChunkedSegmentSealedImpl {
 public:
    ReaderStringSegment(SchemaPtr schema,
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
    int64_t
    num_chunk_data(FieldId field) const override {
        return Column(field)->num_chunks();
    }
    int64_t
    chunk_size(FieldId field, int64_t chunk) const override {
        return Column(field)->chunk_row_nums(chunk);
    }
    int64_t
    num_rows_until_chunk(FieldId field, int64_t chunk) const override {
        return Column(field)->GetNumRowsUntilChunk(chunk);
    }
    std::pair<int64_t, int64_t>
    get_chunk_by_offset(FieldId field, int64_t offset) const override {
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

class ReaderStringSnapshot : public SegmentReadSnapshot {
 public:
    explicit ReaderStringSnapshot(
        std::shared_ptr<ChunkedColumnInterface> column)
        : column_(std::move(column)) {
    }
    int64_t
    chunk_size(FieldId, int64_t chunk) const override {
        return column_->chunk_row_nums(chunk);
    }
    int64_t
    num_rows_until_chunk(FieldId, int64_t chunk) const override {
        return column_->GetNumRowsUntilChunk(chunk);
    }
    std::pair<int64_t, int64_t>
    get_chunk_by_offset(FieldId, int64_t offset) const override {
        return column_->GetChunkIDByOffset(offset);
    }
    int64_t
    num_chunk_data(FieldId) const override {
        return column_->num_chunks();
    }
    int64_t
    get_row_count() const override {
        return column_->NumRows();
    }
    std::pair<std::shared_ptr<ChunkedColumnInterface>,
              std::shared_ptr<const SkipIndex>>
    GetDataScanResources(FieldId) const override {
        ++column_reads;
        return {column_, nullptr};
    }
    mutable int64_t column_reads = 0;

 private:
    std::shared_ptr<ChunkedColumnInterface> column_;
};

class SegmentChunkReaderStringTest : public ::testing::TestWithParam<bool> {
 protected:
    void
    SetUp() override {
        const auto nullable = GetParam();
        auto schema = std::make_shared<Schema>();
        auto pk = schema->AddDebugField("pk", DataType::INT64);
        schema->set_primary_field_id(pk);
        field_ = schema->AddDebugField("value", DataType::VARCHAR, nullable);
        other_field_ =
            schema->AddDebugField("other", DataType::VARCHAR, nullable);
        schema_ = schema;
        stats_ = std::make_shared<StringReadStats>();
        column_ = MakeColumn({8192, 7, 20}, stats_, expected_);
        ASSERT_NE(column_, nullptr);
        segment_ = std::make_unique<ReaderStringSegment>(schema, column_);
    }
    std::shared_ptr<ReaderStringColumn>
    MakeColumn(const std::vector<int64_t>& rows_per_chunk,
               const std::shared_ptr<StringReadStats>& stats,
               std::vector<std::optional<std::string>>& expected,
               bool other = false) {
        const auto nullable = GetParam();
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
    FieldId field_{0};
    FieldId other_field_{0};
    SchemaPtr schema_;
    std::shared_ptr<StringReadStats> stats_;
    std::shared_ptr<ReaderStringColumn> column_;
    std::unique_ptr<ReaderStringSegment> segment_;
    std::vector<std::optional<std::string>> expected_;
    std::vector<std::weak_ptr<ChunkMmapGuard>> lifetimes_;
};
}  // namespace

TEST_P(SegmentChunkReaderStringTest, SmallWindowsDoNotRebuildWholeChunkViews) {
    SegmentChunkReader reader(nullptr, segment_.get(), expected_.size());
    int64_t chunk = 0, pos = 0;
    constexpr int64_t batch = 17;
    StringScanState scan_state;
    for (int64_t start = 0; start < expected_.size(); start += batch) {
        const auto count = std::min<int64_t>(batch, expected_.size() - start);
        auto accessor = reader.GetMultipleChunkDataAccessor(
            DataType::VARCHAR, field_, chunk, pos, {}, count, &scan_state);
        for (int64_t i = 0; i < count; ++i) {
            Check(accessor(), start + i);
        }
        EXPECT_EQ(segment_->num_rows_until_chunk(field_, chunk) + pos,
                  start + count);
    }
    // The old accessor constructed 8192 views for each 17-row expression
    // window. Count materialized rows, not timing, to catch that amplification.
    EXPECT_EQ(stats_->legacy_view_rows, 0);
    EXPECT_EQ(stats_->scanned_rows, expected_.size());
    EXPECT_LE(stats_->largest_scan, batch);
    EXPECT_EQ(stats_->scans, 1);
    EXPECT_EQ(stats_->pins, 3);
}

TEST_P(SegmentChunkReaderStringTest, StringAccessorsUseBoundSnapshot) {
    ReaderStringSnapshot snapshot(column_);
    segment_->forbid_live_column_read = true;
    SegmentChunkReader reader(nullptr, segment_.get(), expected_.size());
    reader.SetSnapshot(&snapshot);
    StringScanState scan_state;
    int64_t chunk = 1, pos = 0;
    for (int64_t row = 8192; row < 8196; row += 2) {
        auto scan = reader.GetMultipleChunkDataAccessor(
            DataType::VARCHAR, field_, chunk, pos, {}, 2, &scan_state);
        Check(scan(), row);
        Check(scan(), row + 1);
    }
    auto chunk_accessor =
        reader.GetChunkDataAccessor(DataType::VARCHAR, field_, 1, {});
    Check(chunk_accessor(0), 8192);
    const std::vector<int32_t> offsets{8198, 0, 8198};
    auto take = reader.GetStringDataAccessorByOffsets(
        field_, OffsetView::From(offsets.data(), offsets.size()), {});
    for (int64_t i = 0; i < offsets.size(); ++i) {
        Check(take(i), offsets[i]);
    }
    EXPECT_EQ(snapshot.column_reads, 3);
    EXPECT_EQ(stats_->scans, 1);
}

TEST_P(SegmentChunkReaderStringTest,
       StringExpressionsKeepIndependentScansAcrossWindowsAndOffsets) {
    auto other_stats = std::make_shared<StringReadStats>();
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
    SegmentChunkReader reader(nullptr, segment_.get(), expected_.size());
    int64_t chunk = 0, pos = 0;
    auto accessor = reader.GetMultipleChunkDataAccessor(
        DataType::VARCHAR, field_, chunk, pos, {}, 17);
    for (int64_t i = 0; i < expected_.size(); ++i) {
        Check(accessor(), i);
    }
    EXPECT_EQ(stats_->scanned_rows, expected_.size());
    EXPECT_EQ(stats_->pins, 3);
}

TEST_P(SegmentChunkReaderStringTest,
       SparseTakePreservesOrderDuplicatesAndNulls) {
    SegmentChunkReader reader(nullptr, segment_.get(), expected_.size());
    const std::vector<int32_t> offsets{8192, 8198, 8192, 16, 0, 16, 8218};
    auto accessor = reader.GetStringDataAccessorByOffsets(
        field_, OffsetView::From(offsets.data(), offsets.size()), {});
    for (int i = 0; i < offsets.size(); ++i) {
        Check(accessor(i), offsets[i]);
    }
    EXPECT_EQ(stats_->takes, 1);
    EXPECT_EQ(stats_->legacy_view_rows, 0);
    EXPECT_EQ(stats_->scanned_rows, 0);
    EXPECT_EQ(stats_->pins, 3);
}

TEST_P(SegmentChunkReaderStringTest,
       RandomChunkAccessorKeepsBorrowedDataPinned) {
    SegmentChunkReader reader(nullptr, segment_.get(), expected_.size());
    auto accessor =
        reader.GetChunkDataAccessor(DataType::VARCHAR, field_, 1, {});
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
    SegmentChunkReader reader(nullptr, segment_.get(), expected_.size());
    int64_t chunk = 1, pos = 0;
    auto accessor = reader.GetMultipleChunkDataAccessor(
        DataType::VARCHAR, field_, chunk, pos, {}, 2);
    auto value = accessor();
    column_->ManualEvictCache();
    EXPECT_FALSE(lifetimes_[1].expired());
    Check(value, 8192);
    Check(accessor(), 8193);
    Check(accessor(), 8194);  // Next window reuses the cursor's chunk pin.
    EXPECT_EQ(stats_->pins, 1);
    accessor = {};
    column_->ManualEvictCache();
    EXPECT_TRUE(lifetimes_[1].expired());
}

TEST_P(SegmentChunkReaderStringTest, PersistentScanPinOutlivesWindowAccessor) {
    SegmentChunkReader reader(nullptr, segment_.get(), expected_.size());
    StringScanState scan_state;
    int64_t chunk = 1, pos = 0;
    {
        auto accessor = reader.GetMultipleChunkDataAccessor(
            DataType::VARCHAR, field_, chunk, pos, {}, 2, &scan_state);
        Check(accessor(), 8192);
        Check(accessor(), 8193);
    }
    column_->ManualEvictCache();
    EXPECT_FALSE(lifetimes_[1].expired());
    {
        auto accessor = reader.GetMultipleChunkDataAccessor(
            DataType::VARCHAR, field_, chunk, pos, {}, 2, &scan_state);
        Check(accessor(), 8194);
        Check(accessor(), 8195);
    }
    EXPECT_EQ(stats_->pins, 1);
    EXPECT_EQ(stats_->scans, 1);
    scan_state = {};
    column_->ManualEvictCache();
    EXPECT_TRUE(lifetimes_[1].expired());
}

TEST_P(SegmentChunkReaderStringTest, TakeOwnsItsPinAfterColumnRelease) {
    const std::vector<int32_t> offsets{8192, 8193};
    ChunkDataAccessor accessor;
    {
        SegmentChunkReader reader(nullptr, segment_.get(), expected_.size());
        accessor = reader.GetStringDataAccessorByOffsets(
            field_, OffsetView::From(offsets.data(), offsets.size()), {});
    }
    auto value = accessor(0);
    column_->ManualEvictCache();
    segment_.reset();
    column_.reset();
    EXPECT_FALSE(lifetimes_[1].expired());
    Check(value, 8192);
    Check(accessor(1), 8193);
    accessor = {};
    EXPECT_TRUE(lifetimes_[1].expired());
}

TEST_P(SegmentChunkReaderStringTest, ReadFailureKeepsItsCode) {
    SegmentChunkReader reader(nullptr, segment_.get(), expected_.size());
    stats_->fail_pin = true;
    int64_t chunk = 0, pos = 0;
    auto accessor = reader.GetMultipleChunkDataAccessor(
        DataType::VARCHAR, field_, chunk, pos, {}, 17);
    try {
        accessor();
        FAIL() << "expected injected scan failure";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FileReadFailed);
    }
    EXPECT_EQ(pos, 0);
    const int32_t offset = 1;
    auto take = reader.GetStringDataAccessorByOffsets(
        field_, OffsetView::From(&offset, 1), {});
    try {
        take(0);
        FAIL() << "expected injected Take failure";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FileReadFailed);
    }
}

INSTANTIATE_TEST_SUITE_P(NullableAndRequired,
                         SegmentChunkReaderStringTest,
                         ::testing::Bool());

}  // namespace milvus::segcore
