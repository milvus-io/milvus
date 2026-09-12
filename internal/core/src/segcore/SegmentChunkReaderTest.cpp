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
    GetChunkedColumn(FieldId) const override {
        return column_;
    }
    int64_t
    num_chunk_data(FieldId) const override {
        return column_->num_chunks();
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
    get_row_count() const override {
        return column_->NumRows();
    }

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
        const std::vector<int64_t> rows_per_chunk{8192, 7, 20};
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
                payload += value.size();
                values.push_back(value);
                expected_.push_back(nullable && (row == 16 || row == 8198)
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
            ASSERT_NE(data, MAP_FAILED);
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
                if (nullable && expected_[row - rows + i].has_value()) {
                    data[i / 8] |= 1 << (i % 8);
                }
                std::memcpy(data + offset, values[i].data(), values[i].size());
                offset += values[i].size();
            }
            chunks.push_back(std::make_unique<StringChunk>(
                rows, data, size, nullable, guard));
        }
        auto translator = std::make_unique<TestChunkTranslator>(
            rows_per_chunk, "segment_reader_strings", std::move(chunks));
        auto slot = cachinglayer::Manager::GetInstance().CreateCacheSlot<Chunk>(
            std::move(translator), nullptr);
        stats_ = std::make_shared<StringReadStats>();
        column_ = std::make_shared<ReaderStringColumn>(
            slot, (*schema)[field_], stats_);
        segment_ = std::make_unique<ReaderStringSegment>(schema, column_);
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
    for (int64_t start = 0; start < expected_.size(); start += batch) {
        const auto count = std::min<int64_t>(batch, expected_.size() - start);
        auto accessor = reader.GetMultipleChunkDataAccessor(
            DataType::VARCHAR, field_, chunk, pos, {}, count);
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
