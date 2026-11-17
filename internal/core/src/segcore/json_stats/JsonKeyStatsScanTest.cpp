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

#include <atomic>
#include <cstring>
#include <functional>
#include <future>
#include <numeric>
#include <optional>
#include <stdexcept>

#include "common/Chunk.h"
#include "exec/expression/UnaryExpr.h"
#include "segcore/json_stats/JsonKeyStats.h"
#include "mmap/ChunkedColumn.h"
#include "segcore/SegcoreConfig.h"
#include "test_utils/cachinglayer_test_utils.h"

class JsonStatsScanTestAccessor {
 public:
    static void
    SetColumn(milvus::index::JsonKeyStats& stats,
              std::shared_ptr<milvus::ChunkedColumnInterface> column) {
        stats.num_rows_ = column->NumRows();
        stats.shredding_columns_["test_path"] = std::move(column);
    }

    static void
    AddProjection(
        milvus::index::JsonKeyStats& stats,
        const std::string& pointer,
        milvus::index::JSONType type,
        const std::string& field,
        const std::shared_ptr<milvus::ChunkedColumnInterface>& column) {
        stats.key_field_map_[pointer].insert(field);
        stats.shred_field_data_type_map_[field] = type;
        stats.shredding_columns_[field] = column;
    }
};

namespace milvus::index {
namespace {

class JsonKeyStatsScanTest : public ::testing::TestWithParam<bool> {
 protected:
    void
    SetUp() override {
        auto& config = segcore::SegcoreConfig::default_config();
        previous_cursor_owns_pin_ = config.get_scan_cursor_owns_pin();
        config.set_scan_cursor_owns_pin(GetParam());
    }

    void
    TearDown() override {
        segcore::SegcoreConfig::default_config().set_scan_cursor_owns_pin(
            previous_cursor_owns_pin_);
    }

 private:
    bool previous_cursor_owns_pin_ = false;
};

struct ScanTrace {
    std::atomic<size_t> prefetched_cells{0};
    std::atomic<size_t> loaded_cells{0};
    std::vector<int64_t> pinned_chunks;
    std::optional<ScanPinPolicy> pin_policy;
    int64_t scans = 0;
    int64_t legacy_string_views = 0;
    int64_t returned_rows = 0;
    int64_t largest_batch = 0;
    const void* last_values = nullptr;
    bool fail_pin = false;
    std::function<void()> after_prefetch;
};

class CountingChunkTranslator : public TestChunkTranslator {
 public:
    CountingChunkTranslator(const std::vector<int64_t>& rows,
                            std::vector<std::unique_ptr<Chunk>> chunks,
                            std::shared_ptr<ScanTrace> trace)
        : TestChunkTranslator(rows, "json_stats_scan", std::move(chunks)),
          trace_(std::move(trace)) {
    }

    std::vector<std::pair<cid_t, std::unique_ptr<Chunk>>>
    get_cells(OpContext* ctx, const std::vector<cid_t>& cells) override {
        trace_->loaded_cells.fetch_add(cells.size());
        return TestChunkTranslator::get_cells(ctx, cells);
    }

 private:
    std::shared_ptr<ScanTrace> trace_;
};

// Delegate to the production RawScanCursor. Record its output pointer so the
// predicate can prove that no intermediate view array was copied.
class RecordingCursor : public ScanCursor {
 public:
    RecordingCursor(ScanResult cursor, std::shared_ptr<ScanTrace> trace)
        : cursor_(std::move(cursor)), trace_(std::move(trace)) {
    }

    int64_t
    Position() const override {
        return cursor_->Position();
    }

    void
    Seek(int64_t position) override {
        cursor_->Seek(position);
    }

    bool
    Next(int64_t length, ScanReadMode mode, ScanBatch* batch) override {
        if (!cursor_->Next(length, mode, batch)) {
            return false;
        }
        trace_->returned_rows += batch->size;
        trace_->largest_batch = std::max(trace_->largest_batch, batch->size);
        trace_->last_values = batch->values.data;
        return true;
    }

 private:
    ScanResult cursor_;
    std::shared_ptr<ScanTrace> trace_;
};

template <typename Base>
class RecordingColumn : public Base {
 public:
    RecordingColumn(std::shared_ptr<cachinglayer::CacheSlot<Chunk>> slot,
                    const FieldMeta& meta,
                    std::shared_ptr<ScanTrace> trace)
        : Base(std::move(slot), meta), trace_(std::move(trace)) {
    }

    void
    PrefetchChunks(OpContext* ctx,
                   const std::vector<int64_t>& cells) const override {
        trace_->prefetched_cells.fetch_add(cells.size());
        Base::PrefetchChunks(ctx, cells);
        if (trace_->after_prefetch) {
            trace_->after_prefetch();
        }
    }

    PinWrapper<Chunk*>
    GetChunk(OpContext* ctx, int64_t chunk_id) const override {
        if (trace_->fail_pin) {
            throw std::runtime_error("injected scan pin failure");
        }
        trace_->pinned_chunks.push_back(chunk_id);
        return Base::GetChunk(ctx, chunk_id);
    }

    ScanResult
    Scan(OpContext* ctx, const ScanOptions& options) const override {
        ++trace_->scans;
        trace_->pin_policy = options.pin_policy;
        return std::make_unique<RecordingCursor>(Base::Scan(ctx, options),
                                                 trace_);
    }

    PinWrapper<std::pair<std::vector<std::string_view>, ValidityView>>
    StringViews(
        OpContext* ctx,
        int64_t chunk_id,
        std::optional<std::pair<int64_t, int64_t>> range) const override {
        ++trace_->legacy_string_views;
        return Base::StringViews(ctx, chunk_id, range);
    }

 private:
    std::shared_ptr<ScanTrace> trace_;
};

struct ScanFixture {
    // The column borrows these backing bytes; destroy it before the buffers.
    std::vector<std::vector<char>> buffers;
    std::shared_ptr<ScanTrace> trace = std::make_shared<ScanTrace>();
    std::shared_ptr<ChunkedColumnInterface> column;
};

bool
ValidRow(int64_t row) {
    return row % 7 != 1;
}

template <typename T>
ScanFixture
MakeColumn(const std::vector<int64_t>& chunk_rows,
           const std::vector<T>& values,
           DataType type,
           bool nullable) {
    ScanFixture fixture;
    std::vector<std::unique_ptr<Chunk>> chunks;
    int64_t row = 0;
    for (auto rows : chunk_rows) {
        const auto validity_bytes = nullable ? (rows + 7) / 8 : 0;
        size_t payload_bytes = rows * sizeof(T);
        if constexpr (std::is_same_v<T, std::string>) {
            payload_bytes = (rows + 1) * sizeof(uint32_t);
            for (int64_t i = 0; i < rows; ++i) {
                payload_bytes += values[row + i].size();
            }
        }
        fixture.buffers.emplace_back(validity_bytes + payload_bytes, 0);
        auto& buffer = fixture.buffers.back();
        for (int64_t i = 0; nullable && i < rows; ++i) {
            if (ValidRow(row + i)) {
                buffer[i >> 3] |= 1 << (i & 7);
            }
        }
        auto guard = std::make_shared<ChunkMmapGuard>(nullptr, 0, "");
        if constexpr (std::is_same_v<T, std::string>) {
            uint32_t offset = validity_bytes + (rows + 1) * sizeof(uint32_t);
            for (int64_t i = 0; i <= rows; ++i) {
                std::memcpy(
                    buffer.data() + validity_bytes + i * sizeof(uint32_t),
                    &offset,
                    sizeof(offset));
                if (i < rows) {
                    const auto& value = values[row + i];
                    std::memcpy(
                        buffer.data() + offset, value.data(), value.size());
                    offset += value.size();
                }
            }
            chunks.push_back(std::make_unique<StringChunk>(
                rows, buffer.data(), buffer.size(), nullable, guard));
        } else {
            for (int64_t i = 0; i < rows; ++i) {
                T value = values[row + i];
                std::memcpy(buffer.data() + validity_bytes + i * sizeof(T),
                            &value,
                            sizeof(value));
            }
            chunks.push_back(std::make_unique<FixedWidthChunk>(rows,
                                                               1,
                                                               buffer.data(),
                                                               buffer.size(),
                                                               sizeof(T),
                                                               nullable,
                                                               guard));
        }
        row += rows;
    }
    auto translator = std::make_unique<CountingChunkTranslator>(
        chunk_rows, std::move(chunks), fixture.trace);
    auto slot = cachinglayer::Manager::GetInstance().CreateCacheSlot<Chunk>(
        std::move(translator), nullptr);
    FieldMeta meta(
        FieldName("test_path"), FieldId(101), type, nullable, std::nullopt);
    using Base = std::conditional_t<std::is_same_v<T, std::string>,
                                    ChunkedVariableColumn<std::string>,
                                    ChunkedColumn>;
    fixture.column = std::make_shared<RecordingColumn<Base>>(
        std::move(slot), meta, fixture.trace);
    return fixture;
}

std::unique_ptr<JsonKeyStats>
MakeStats(const std::shared_ptr<ChunkedColumnInterface>& column) {
    storage::FileManagerContext ctx;
    ctx.fieldDataMeta = {1, 2, 3, 101, {}};
    ctx.fieldDataMeta.field_schema.set_data_type(proto::schema::DataType::JSON);
    ctx.indexMeta = {3, 101, 533805, 1};
    auto stats = std::make_unique<JsonKeyStats>(ctx, /*is_load=*/true);
    JsonStatsScanTestAccessor::SetColumn(*stats, column);
    return stats;
}

TEST(JsonStatsPrefetchTest, RepeatedStringPathSharesPhysicalReads) {
    auto kind = MakeColumn<std::string>(
        {1, 2}, {"commit", "issue", "commit"}, DataType::STRING, false);
    auto operation = MakeColumn<std::string>(
        {3}, {"create", "close", "push"}, DataType::STRING, false);
    auto stats = MakeStats(kind.column);
    JsonStatsScanTestAccessor::AddProjection(
        *stats, "/kind", JSONType::STRING, "kind_s", kind.column);
    JsonStatsScanTestAccessor::AddProjection(*stats,
                                             "/commit/operation",
                                             JSONType::STRING,
                                             "op_s",
                                             operation.column);

    JsonStatsPrefetchBatch batch;
    auto fetch_kind = [&] {
        stats->PrefetchTypedColumns(
            nullptr, "/kind", {JSONType::STRING}, &batch);
    };
    auto first = std::async(std::launch::async, fetch_kind);
    fetch_kind();
    first.get();
    EXPECT_EQ(kind.trace->prefetched_cells.load(), 2);
    EXPECT_EQ(kind.trace->loaded_cells.load(), 2);
    EXPECT_EQ(operation.trace->loaded_cells.load(), 0);

    auto reader = stats->CreateShreddingReader("/kind", JSONType::STRING, 3);
    ASSERT_NE(reader, nullptr);
    // A demand read of the same projected cells reuses the warmed slot.
    const std::vector<std::string> expected{"commit", "issue", "commit"};
    for (int64_t i = 0; i < expected.size(); ++i) {
        auto value = reader->Get(nullptr, i);
        ASSERT_TRUE(value.has_value());
        EXPECT_EQ(std::get<std::string>(*value), expected[i]);
    }
    EXPECT_EQ(kind.trace->loaded_cells.load(), 2);
    stats->PrefetchTypedColumns(
        nullptr, "/commit/operation", {JSONType::STRING});
    EXPECT_EQ(operation.trace->loaded_cells.load(), 1);
}

TEST(JsonStatsPrefetchTest, StoppedWorkerLeavesRemainingCellsForDemand) {
    auto kind = MakeColumn<std::string>(
        {1, 1, 1}, {"a", "b", "c"}, DataType::STRING, false);
    auto stats = MakeStats(kind.column);
    JsonStatsScanTestAccessor::AddProjection(
        *stats, "/kind", JSONType::STRING, "kind_s", kind.column);
    folly::CancellationSource stop;
    folly::CancellationSource request;
    OpContext ctx(request.getToken());
    // Stop after the first shared fill has published its cell.
    kind.trace->after_prefetch = [&] { stop.requestCancellation(); };
    auto pool = std::make_shared<folly::CPUThreadPoolExecutor>(1);
    PrefetchExecutor executor(pool, stop.getToken());
    executor
        .Submit([&] {
            stats->PrefetchTypedColumns(&ctx, "/kind", {JSONType::STRING});
        })
        .get();
    EXPECT_EQ(kind.trace->loaded_cells.load(), 1);
    EXPECT_FALSE(ctx.cancellation_token.isCancellationRequested());
    auto reader = stats->CreateShreddingReader("/kind", JSONType::STRING, 3);
    ASSERT_NE(reader, nullptr);
    auto last = reader->Get(&ctx, 2);
    ASSERT_TRUE(last.has_value());
    EXPECT_EQ(std::get<std::string>(*last), "c");
    EXPECT_EQ(kind.trace->loaded_cells.load(), 2);
}

TEST(JsonStatsPrefetchTest, MixedNumberTypesAndMissingProjection) {
    auto ints = MakeColumn<int64_t>({2}, {1, 2}, DataType::INT64, false);
    auto doubles =
        MakeColumn<double>({1, 1}, {1.5, 2.5}, DataType::DOUBLE, false);
    auto strings =
        MakeColumn<std::string>({2}, {"one", "two"}, DataType::STRING, false);
    auto stats = MakeStats(ints.column);
    JsonStatsScanTestAccessor::AddProjection(
        *stats, "/n", JSONType::INT64, "n_i", ints.column);
    JsonStatsScanTestAccessor::AddProjection(
        *stats, "/n", JSONType::DOUBLE, "n_d", doubles.column);
    JsonStatsScanTestAccessor::AddProjection(
        *stats, "/n", JSONType::STRING, "n_s", strings.column);
    stats->PrefetchTypedColumns(nullptr, "/n", {JSONType::BOOL});
    EXPECT_EQ(ints.trace->loaded_cells.load(), 0);
    EXPECT_EQ(doubles.trace->loaded_cells.load(), 0);
    stats->PrefetchTypedColumns(
        nullptr, "/n", {JSONType::INT64, JSONType::DOUBLE});
    EXPECT_EQ(ints.trace->loaded_cells.load(), 1);
    EXPECT_EQ(doubles.trace->loaded_cells.load(), 2);
    EXPECT_EQ(strings.trace->loaded_cells.load(), 0);
}

TEST(JsonStatsPrefetchTest, ExistsMatchesPathComponentsAndHonorsCancellation) {
    auto child = MakeColumn<int64_t>({1}, {1}, DataType::INT64, false);
    auto other = MakeColumn<int64_t>({1}, {2}, DataType::INT64, false);
    auto stats = MakeStats(child.column);
    JsonStatsScanTestAccessor::AddProjection(
        *stats, "/a/b", JSONType::INT64, "ab_i", child.column);
    JsonStatsScanTestAccessor::AddProjection(
        *stats, "/aa/b", JSONType::INT64, "aab_i", other.column);
    folly::CancellationSource source;
    OpContext canceled(source.getToken());
    source.requestCancellation();
    stats->PrefetchExistsColumns(&canceled, "/a");
    EXPECT_EQ(child.trace->loaded_cells.load(), 0);
    stats->PrefetchExistsColumns(nullptr, "/a");
    EXPECT_EQ(child.trace->loaded_cells.load(), 1);
    EXPECT_EQ(other.trace->loaded_cells.load(), 0);
}

TEST_P(JsonKeyStatsScanTest, StringWindowsBorrowViewsAndHonorPinPolicy) {
    const int64_t rows = DEFAULT_EXEC_EVAL_EXPR_BATCH_SIZE * 3 + 17;
    std::vector<std::string> values(rows);
    for (int64_t i = 0; i < rows; ++i) {
        values[i] = i % 3 == 0 ? "match" : "other";
    }
    // Odd boundaries exercise packed validity subviews and short batches.
    auto fixture =
        MakeColumn<std::string>({rows - 5, 5}, values, DataType::STRING, true);
    auto stats = MakeStats(fixture.column);
    TargetBitmap result(rows), valid(rows, true);
    auto predicate = [&](const std::string_view* data,
                         ValidityView validity,
                         int64_t size,
                         TargetBitmapView res,
                         TargetBitmapView valid_res,
                         std::string_view target) {
        EXPECT_EQ(data, fixture.trace->last_values);
        for (int64_t i = 0; i < size; ++i) {
            valid_res[i] = validity[i];
            res[i] = validity[i] && data[i] == target;
        }
    };
    EXPECT_EQ(stats->ExecutorForShreddingData<std::string_view>(
                  nullptr,
                  "test_path",
                  predicate,
                  nullptr,
                  TargetBitmapView(result),
                  TargetBitmapView(valid),
                  std::string_view("match")),
              rows);
    EXPECT_EQ(fixture.trace->scans, 1);
    EXPECT_EQ(fixture.trace->returned_rows, rows);
    EXPECT_LE(fixture.trace->largest_batch, DEFAULT_EXEC_EVAL_EXPR_BATCH_SIZE);
    EXPECT_EQ(fixture.trace->legacy_string_views, 0);
    EXPECT_EQ(
        fixture.trace->pin_policy,
        GetParam() ? ScanPinPolicy::CursorOwned : ScanPinPolicy::ResultOwned);
    EXPECT_EQ(fixture.trace->pinned_chunks,
              GetParam() ? (std::vector<int64_t>{0, 1})
                         : (std::vector<int64_t>{0, 0, 0, 0, 1}));
    for (int64_t i = 0; i < rows; ++i) {
        EXPECT_EQ(valid[i], ValidRow(i));
        EXPECT_EQ(result[i], ValidRow(i) && i % 3 == 0);
    }
}

TEST_P(JsonKeyStatsScanTest, BinaryStringValuesKeepEmptyAndEmbeddedNulBytes) {
    // Shredded ARRAY columns use STRING storage for BSON bytes. Reading must
    // not apply string terminators or reinterpret them as schema ARRAY views.
    const std::vector<std::string> values{
        "", std::string("\x05\0\0\0\0", 5), std::string("a\0b", 3), "tail"};
    auto fixture = MakeColumn({1, 3}, values, DataType::STRING, false);
    auto stats = MakeStats(fixture.column);
    TargetBitmap result(4), valid(4, true);
    int64_t row = 0;
    auto predicate = [&](const std::string_view* data,
                         ValidityView validity,
                         int64_t size,
                         TargetBitmapView res,
                         TargetBitmapView) {
        EXPECT_EQ(data, fixture.trace->last_values);
        EXPECT_FALSE(validity);
        for (int64_t i = 0; i < size; ++i) {
            EXPECT_EQ(data[i], values[row++]);
            res[i] = true;
        }
    };
    EXPECT_EQ(stats->ExecutorForShreddingData<std::string_view>(
                  nullptr,
                  "test_path",
                  predicate,
                  nullptr,
                  TargetBitmapView(result),
                  TargetBitmapView(valid)),
              4);
    EXPECT_EQ(row, 4);
    EXPECT_EQ(result.count(), 4);
}

TEST_P(JsonKeyStatsScanTest, SkipUsesPhysicalCellOnceAndPreservesIncomingBits) {
    for (bool nullable : {false, true}) {
        SCOPED_TRACE(nullable);
        const int64_t first_rows = DEFAULT_EXEC_EVAL_EXPR_BATCH_SIZE + 3;
        const int64_t rows = first_rows + 5;
        std::vector<std::string> values(rows, "value");
        auto fixture =
            MakeColumn({first_rows, 5}, values, DataType::STRING, nullable);
        auto stats = MakeStats(fixture.column);
        TargetBitmap result(rows), valid(rows, true);
        for (int64_t i = 0; i < rows; ++i) {
            result[i] = i % 2 == 0;
        }
        std::vector<int> skip_calls;
        auto skip = [&](const SkipIndex&, std::string path, int chunk) {
            EXPECT_EQ(path, "test_path");
            skip_calls.push_back(chunk);
            return chunk == 0;
        };
        int64_t evaluated_rows = 0;
        auto predicate = [&](const std::string_view* data,
                             ValidityView validity,
                             int64_t size,
                             TargetBitmapView res,
                             TargetBitmapView valid_res) {
            EXPECT_EQ(data, fixture.trace->last_values);
            evaluated_rows += size;
            for (int64_t i = 0; i < size; ++i) {
                valid_res[i] = !validity || validity[i];
                res[i] = !validity || validity[i];
            }
        };
        EXPECT_EQ(stats->ExecutorForShreddingData<std::string_view>(
                      nullptr,
                      "test_path",
                      predicate,
                      skip,
                      TargetBitmapView(result),
                      TargetBitmapView(valid)),
                  rows);
        EXPECT_EQ(skip_calls, (std::vector<int>{0, 1}));
        EXPECT_EQ(evaluated_rows, 5);
        EXPECT_EQ(fixture.trace->pinned_chunks,
                  nullable ? (GetParam() ? (std::vector<int64_t>{0, 1})
                                         : (std::vector<int64_t>{0, 0, 1}))
                           : (std::vector<int64_t>{1}));
        for (int64_t i = 0; i < rows; ++i) {
            const bool is_valid = !nullable || ValidRow(i);
            EXPECT_EQ(valid[i], is_valid);
            EXPECT_EQ(result[i], is_valid && (i >= first_rows || i % 2 == 0));
        }
    }
}

template <typename T>
void
CheckFixedWidth(DataType type,
                const std::vector<T>& values,
                const std::vector<int64_t>& chunk_rows = {2, 3}) {
    const int64_t rows = values.size();
    auto fixture = MakeColumn(chunk_rows, values, type, true);
    auto stats = MakeStats(fixture.column);
    TargetBitmap result(rows), valid(rows, true);
    int64_t row = 0;
    auto predicate = [&](const T* data,
                         ValidityView validity,
                         int64_t size,
                         TargetBitmapView res,
                         TargetBitmapView valid_res) {
        for (int64_t i = 0; i < size; ++i, ++row) {
            valid_res[i] = validity[i];
            res[i] = validity[i];
            if (validity[i]) {
                EXPECT_EQ(data[i], values[row]);
            }
        }
    };
    EXPECT_EQ(
        stats->template ExecutorForShreddingData<T>(nullptr,
                                                    "test_path",
                                                    predicate,
                                                    nullptr,
                                                    TargetBitmapView(result),
                                                    TargetBitmapView(valid)),
        rows);
    EXPECT_EQ(row, rows);
    for (int64_t i = 0; i < rows; ++i) {
        EXPECT_EQ(result[i], ValidRow(i));
        EXPECT_EQ(valid[i], ValidRow(i));
    }
    EXPECT_EQ(fixture.trace->pinned_chunks, (std::vector<int64_t>{0, 1}));
    EXPECT_EQ(fixture.trace->largest_batch,
              *std::max_element(chunk_rows.begin(), chunk_rows.end()));
}

TEST_P(JsonKeyStatsScanTest, FixedWidthKeepsTypesAndDenseRowPositions) {
    CheckFixedWidth<int64_t>(
        DataType::INT64, {9007199254740993LL, 0, -9007199254740993LL, 7, -3});
    CheckFixedWidth<double>(DataType::DOUBLE, {0.5, 0, -1.25, 1e100, -1e100});
    CheckFixedWidth<bool>(DataType::BOOL, {true, false, false, true, false});
}

TEST_P(JsonKeyStatsScanTest, LargeFixedWidthChunksPinOncePerChunk) {
    const int64_t first_rows = DEFAULT_EXEC_EVAL_EXPR_BATCH_SIZE * 16 + 3;
    const int64_t rows = first_rows + 5;
    std::vector<int64_t> integers(rows);
    std::vector<double> doubles(rows);
    std::vector<bool> booleans(rows);
    for (int64_t i = 0; i < rows; ++i) {
        integers[i] = 9007199254740993LL + i;
        doubles[i] = i * 0.5 - rows;
        booleans[i] = i % 2 == 0;
    }
    CheckFixedWidth(DataType::INT64, integers, {first_rows, 5});
    CheckFixedWidth(DataType::DOUBLE, doubles, {first_rows, 5});
    CheckFixedWidth(DataType::BOOL, booleans, {first_rows, 5});
}

TEST_P(JsonKeyStatsScanTest,
       PatternPredicatesKeepResultsAcrossWindowsAndChunks) {
    const int64_t first_rows = DEFAULT_EXEC_EVAL_EXPR_BATCH_SIZE * 3 + 3;
    const int64_t rows = first_rows + 5;
    const std::vector<std::string> samples{"ab猫-tail",
                                           "ab-tail",
                                           "ab",
                                           "other",
                                           "ab\nx",
                                           std::string("ab\0x", 4)};
    std::vector<std::string> values(rows);
    for (int64_t i = 0; i < rows; ++i) {
        values[i] = samples[i % samples.size()];
    }
    auto fixture = MakeColumn({first_rows, 5}, values, DataType::STRING, true);
    auto stats = MakeStats(fixture.column);
    const std::vector<std::pair<proto::plan::OpType, std::string>> patterns{
        {proto::plan::Match, "ab_%"},
        {proto::plan::RegexMatch, "^ab.(?:-tail|tail|x)$"},
        {proto::plan::Match, "other"},
        {proto::plan::RegexMatch, "^other$"}};
    for (size_t pattern_id = 0; pattern_id < patterns.size(); ++pattern_id) {
        SCOPED_TRACE(pattern_id);
        const auto& [op, pattern] = patterns[pattern_id];
        exec::ShreddingExecutor<std::string_view, std::string> executor(
            op, "/test", pattern);
        TargetBitmap result(rows), valid(rows, true);
        EXPECT_EQ(stats->ExecutorForShreddingData<std::string_view>(
                      nullptr,
                      "test_path",
                      std::move(executor),
                      nullptr,
                      TargetBitmapView(result),
                      TargetBitmapView(valid)),
                  rows);
        for (int64_t i = 0; i < rows; ++i) {
            const auto sample = i % samples.size();
            const bool matched =
                pattern_id < 2 ? sample != 2 && sample != 3 : sample == 3;
            EXPECT_EQ(valid[i], ValidRow(i));
            EXPECT_EQ(result[i], ValidRow(i) && matched);
        }
    }
    EXPECT_LE(fixture.trace->largest_batch, DEFAULT_EXEC_EVAL_EXPR_BATCH_SIZE);
}

TEST_P(JsonKeyStatsScanTest, SkippedPatternDoesNotCompileInvalidRegex) {
    auto fixture = MakeColumn<std::string>(
        {DEFAULT_EXEC_EVAL_EXPR_BATCH_SIZE + 1},
        std::vector<std::string>(DEFAULT_EXEC_EVAL_EXPR_BATCH_SIZE + 1,
                                 "value"),
        DataType::STRING,
        true);
    auto stats = MakeStats(fixture.column);
    const auto rows = fixture.column->NumRows();
    TargetBitmap result(rows), valid(rows, true);
    auto skip = [](const SkipIndex&, std::string, int) { return true; };
    exec::ShreddingExecutor<std::string_view, std::string> executor(
        proto::plan::RegexMatch, "/test", "[");
    EXPECT_EQ(stats->ExecutorForShreddingData<std::string_view>(
                  nullptr,
                  "test_path",
                  std::move(executor),
                  skip,
                  TargetBitmapView(result),
                  TargetBitmapView(valid)),
              rows);
    exec::ShreddingExecutor<std::string_view, std::string> evaluated(
        proto::plan::RegexMatch, "/test", "[");
    try {
        stats->ExecutorForShreddingData<std::string_view>(
            nullptr,
            "test_path",
            std::move(evaluated),
            nullptr,
            TargetBitmapView(result),
            TargetBitmapView(valid));
        FAIL() << "Invalid regex must fail when a batch is evaluated";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::InvalidParameter);
    }
}

TEST_P(JsonKeyStatsScanTest, MissingPathDoesNotScanAndPinFailurePropagates) {
    auto fixture =
        MakeColumn<std::string>({2}, {"a", "b"}, DataType::STRING, false);
    auto stats = MakeStats(fixture.column);
    TargetBitmap result(2, true), valid(2, true);
    auto predicate = [](const std::string_view*,
                        ValidityView,
                        int64_t,
                        TargetBitmapView,
                        TargetBitmapView) { FAIL(); };
    EXPECT_EQ(stats->ExecutorForShreddingData<std::string_view>(
                  nullptr,
                  "absent",
                  predicate,
                  nullptr,
                  TargetBitmapView(result),
                  TargetBitmapView(valid)),
              0);
    EXPECT_EQ(fixture.trace->scans, 0);
    EXPECT_EQ(result.count(), 2);
    fixture.trace->fail_pin = true;
    EXPECT_THROW(stats->ExecutorForShreddingData<std::string_view>(
                     nullptr,
                     "test_path",
                     predicate,
                     nullptr,
                     TargetBitmapView(result),
                     TargetBitmapView(valid)),
                 std::runtime_error);
}

INSTANTIATE_TEST_SUITE_P(ScanPinPolicy,
                         JsonKeyStatsScanTest,
                         ::testing::Values(false, true),
                         [](const ::testing::TestParamInfo<bool>& info) {
                             return info.param ? "CursorOwned" : "ResultOwned";
                         });

}  // namespace
}  // namespace milvus::index
