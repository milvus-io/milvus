// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>
#include <fstream>
#include <random>
#include <set>

#include "exec/expression/BinaryArithOpEvalRangeExpr.h"
#include "exec/expression/OffsetExpressionEvaluator.h"
#include "exec/expression/OffsetExpressionCallback.h"
#include "mmap/ChunkedColumnGroup.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "test_utils/cachinglayer_test_utils.h"

namespace milvus::exec {
namespace {

// Tests use unchanged original Cohere IDs supplied by the provenance-checking
// runner. Chunk boundaries and candidate schedules vary, not source values.
class OriginalOffsetReaderTest : public testing::Test {
 protected:
    class TracedColumn : public ProxyChunkColumn {
     public:
        using ProxyChunkColumn::ProxyChunkColumn;
        std::pair<PinWrapper<SpanBase>, size_t>
        PinOffsetSpan(milvus::OpContext* ctx, int64_t chunk) const override {
            pins.push_back(chunk);
            return ProxyChunkColumn::PinOffsetSpan(ctx, chunk);
        }
        mutable std::vector<int64_t> pins;
    };
    void
    SetUp() override {
        const auto* path = std::getenv("MILVUS_LOOKUP_VIEWS_FIXTURE");
        if (!path)
            GTEST_SKIP() << "verified original-ID fixture not configured";
        std::ifstream stream(path);
        ASSERT_TRUE(stream.good());
        for (std::string line; std::getline(stream, line);) {
            ids.push_back(nlohmann::json::parse(line).at(0).get<int64_t>());
        }
        ASSERT_EQ(ids.size(), 65536);
        char directory[] = "/tmp/milvus-offset-reader-XXXXXX";
        ASSERT_NE(mkdtemp(directory), nullptr);
        storage::LocalChunkManagerSingleton::GetInstance().Init(directory);
        storage::MmapConfig config{};
        config.cache_read_ahead_policy = "willneed";
        config.mmap_path = directory;
        config.disk_limit = 512 * 1024 * 1024;
        config.fix_file_size = 4 * 1024 * 1024;
        storage::MmapManager::GetInstance().Init(config);
        constexpr int64_t mb = 1024 * 1024;
        cachinglayer::Manager::ConfigureTieredStorage(
            {CacheWarmupPolicy::CacheWarmupPolicy_Disable,
             CacheWarmupPolicy::CacheWarmupPolicy_Disable,
             CacheWarmupPolicy::CacheWarmupPolicy_Disable,
             CacheWarmupPolicy::CacheWarmupPolicy_Disable},
            {512 * mb, 512 * mb, 512 * mb, 512 * mb, 512 * mb, 512 * mb},
            true,
            false,
            {10, true, 30},
            std::chrono::milliseconds(0),
            std::chrono::milliseconds(-1));
    }

    std::shared_ptr<TracedColumn>
    Column(bool irregular, bool shared_fields = false) {
        std::vector<std::unique_ptr<GroupChunk>> chunks;
        std::vector<int64_t> lengths;
        for (size_t first = 0; first < ids.size();) {
            auto count = std::min(
                ids.size() - first,
                irregular ? size_t(997 + lengths.size() % 7) : size_t(1024));
            std::unordered_map<FieldId, std::shared_ptr<Chunk>> fields;
            fields.emplace(FieldId(100),
                           std::make_shared<FixedWidthChunk>(
                               count,
                               1,
                               reinterpret_cast<char*>(ids.data() + first),
                               count * sizeof(int64_t),
                               sizeof(int64_t),
                               false,
                               nullptr));
            if (shared_fields) {
                // The existing anchor exposes its original ID as both id and
                // bucket. Mirror that mapping, not new/generated scalar data.
                fields.emplace(FieldId(101), fields.at(FieldId(100)));
            }
            chunks.push_back(std::make_unique<GroupChunk>(fields));
            lengths.push_back(count);
            first += count;
        }
        auto group = std::make_shared<ChunkedColumnGroup>(
            std::make_unique<TestGroupChunkTranslator>(
                shared_fields ? 2 : 1,
                lengths,
                "original-offset-" + std::to_string(irregular) + "-" +
                    std::to_string(shared_fields),
                std::move(chunks)));
        last_group = group;
        return std::make_shared<TracedColumn>(
            group,
            FieldId(100),
            FieldMeta(FieldName("original_id"),
                      FieldId(100),
                      DataType::INT64,
                      false,
                      std::nullopt));
    }

    class Segment : public segcore::ChunkedSegmentSealedImpl {
     public:
        Segment(SchemaPtr schema, std::shared_ptr<ProxyChunkColumn> column)
            : ChunkedSegmentSealedImpl(
                  schema, nullptr, segcore::SegcoreConfig::default_config(), 0),
              column(std::move(column)) {
        }
        std::shared_ptr<const ChunkedColumnInterface>
        CaptureOffsetColumn(FieldId) const override {
            ++captures;
            return column;
        }
        std::shared_ptr<ProxyChunkColumn> column;
        mutable size_t captures{0};
    };

    class Reader : public SegmentExpr {
     public:
        Reader(const Segment* segment)
            : SegmentExpr({},
                          "original-offset-test",
                          nullptr,
                          segment,
                          FieldId(100),
                          {},
                          DataType::INT64,
                          65536,
                          1024,
                          0) {
        }
    };

    // Only storage metadata is adapted. Sampling and predicate evaluation use
    // the production sampler/Expr/reader and unchanged original Cohere IDs.
    class SamplingSegment : public Segment {
     public:
        using Segment::Segment;
        std::shared_ptr<TracedColumn> other;
        std::shared_ptr<const ChunkedColumnInterface>
        CaptureOffsetColumn(FieldId field) const override {
            if (field == FieldId(101) && other) {
                ++captures;
                return other;
            }
            return Segment::CaptureOffsetColumn(field);
        }
        bool raw{true}, indexed{false};
        bool
        HasFieldData(FieldId) const override {
            return raw;
        }
        bool
        HasIndex(FieldId) const override {
            return indexed;
        }
        int64_t
        num_chunk_data(FieldId) const override {
            return raw ? column->num_chunks() : 0;
        }
        int64_t
        chunk_size(FieldId, int64_t chunk) const override {
            return column->chunk_row_nums(chunk);
        }
        int64_t
        num_rows_until_chunk(FieldId, int64_t chunk) const override {
            return column->GetNumRowsUntilChunk(chunk);
        }
    };

    static expr::TypedExprPtr
    Mod() {
        proto::plan::GenericValue value, divisor;
        value.set_int64_val(3);
        divisor.set_int64_val(5);
        return std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
            expr::ColumnInfo(FieldId(100), DataType::INT64),
            proto::plan::LessThan,
            proto::plan::Mod,
            value,
            divisor);
    }

    static expr::TypedExprPtr
    Range(int64_t threshold, FieldId field = FieldId(100)) {
        proto::plan::GenericValue value;
        value.set_int64_val(threshold);
        return std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(field, DataType::INT64),
            proto::plan::GreaterEqual,
            value);
    }
    std::vector<int64_t> ids;
    std::shared_ptr<ChunkedColumnGroup> last_group;
};

TEST_F(OriginalOffsetReaderTest, ActualSamplerSharedAndSeparateFieldGroups) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("original_id", DataType::INT64);
    schema->AddDebugField("original_id_alias", DataType::INT64);
    auto first = Column(false, true);
    auto sibling =
        std::make_shared<TracedColumn>(last_group,
                                       FieldId(101),
                                       FieldMeta(FieldName("original_id_alias"),
                                                 FieldId(101),
                                                 DataType::INT64,
                                                 false,
                                                 std::nullopt));
    SamplingSegment segment(schema, first);
    segment.other = sibling;
    QueryContext query("actual-shared-group", &segment, ids.size(), 17);
    SearchInfo info;
    info.search_params_ = nlohmann::json::object();
    query.set_search_info(info);
    ExecContext exec(&query);
    for (auto op : {expr::LogicalBinaryExpr::OpType::And,
                    expr::LogicalBinaryExpr::OpType::Or}) {
        auto same = std::make_shared<expr::LogicalBinaryExpr>(
            op, Mod(), Range(ids[32768]));
        auto shared = std::make_shared<expr::LogicalBinaryExpr>(
            op, Mod(), Range(ids[32768], FieldId(101)));
        auto physical = CompileExpression(shared, &query, {}, true);
        ASSERT_NE(physical->OffsetSamplingColumn(), nullptr);
        const auto a = SampleOffsetFilterRatio(same, &exec);
        const auto b = SampleOffsetFilterRatio(shared, &exec);
        ASSERT_TRUE(a.has_value());
        EXPECT_EQ(a, b);
    }
    ASSERT_FALSE(first->pins.empty());
    ASSERT_FALSE(sibling->pins.empty());
    EXPECT_EQ(std::set<int64_t>(first->pins.begin(), first->pins.end()),
              std::set<int64_t>(sibling->pins.begin(), sibling->pins.end()));
    auto separate = Column(true, true);
    segment.other = separate;
    first->pins.clear();
    for (auto op : {expr::LogicalBinaryExpr::OpType::And,
                    expr::LogicalBinaryExpr::OpType::Or}) {
        auto logical = std::make_shared<expr::LogicalBinaryExpr>(
            op, Mod(), Range(ids[32768], FieldId(101)));
        EXPECT_EQ(CompileExpression(logical, &query, {}, true)
                      ->OffsetSamplingColumn(),
                  nullptr);
        EXPECT_EQ(SampleOffsetFilterRatio(logical, &exec), std::nullopt);
    }
    EXPECT_TRUE(first->pins.empty());
    EXPECT_TRUE(separate->pins.empty());
}

TEST_F(OriginalOffsetReaderTest, ActualSamplerWholeExpressionOneCell) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("original_id", DataType::INT64);
    using Op = expr::LogicalBinaryExpr::OpType;
    for (bool irregular : {false, true}) {
        auto column = Column(irregular);
        SamplingSegment segment(schema, column);
        for (const int active : {7, 65536}) {
            for (const int requested : {10, 20}) {
                for (auto op : {Op::And, Op::Or}) {
                    for (uint64_t timestamp : {1, 17, 1001}) {
                        QueryContext query("single-cell-original",
                                           &segment,
                                           active,
                                           timestamp);
                        SearchInfo info;
                        info.search_params_["ann_fusing_sample_rows"] =
                            requested;
                        query.set_search_info(info);
                        ExecContext exec(&query);
                        const auto threshold = ids[ids.size() / 2];
                        auto logical =
                            std::make_shared<expr::LogicalBinaryExpr>(
                                op, Mod(), Range(threshold));
                        auto root =
                            CompileExpression(logical, &query, {}, true);
                        ASSERT_NE(root->OffsetSamplingColumn(), nullptr);
                        column->pins.clear();
                        const auto ratio =
                            SampleOffsetFilterRatio(logical, &exec);
                        ASSERT_TRUE(ratio.has_value());
                        // Reproduce only the row schedule for an independent
                        // scalar oracle, never synthesize any source values.
                        std::mt19937_64 random(timestamp);
                        const auto last =
                            column->GetChunkIDByOffset(active - 1).first;
                        const auto chunk =
                            std::uniform_int_distribution<int64_t>(
                                0, last)(random);
                        const auto first = column->GetNumRowsUntilChunk(chunk);
                        const auto rows = std::min<int64_t>(
                            column->chunk_row_nums(chunk), active - first);
                        std::uniform_int_distribution<int64_t> choose(0,
                                                                      rows - 1);
                        std::set<int64_t> offsets;
                        while (offsets.size() <
                               std::min<int64_t>(requested, rows)) {
                            offsets.insert(first + choose(random));
                        }
                        size_t accepted = 0;
                        for (const auto offset : offsets) {
                            const auto a = ids[offset] % 5 < 3;
                            const auto b = ids[offset] >= threshold;
                            accepted += op == Op::And ? a && b : a || b;
                        }
                        EXPECT_DOUBLE_EQ(
                            *ratio, 1.0 - double(accepted) / offsets.size());
                        ASSERT_FALSE(column->pins.empty());
                        EXPECT_EQ(std::set<int64_t>(column->pins.begin(),
                                                    column->pins.end()),
                                  std::set<int64_t>{chunk});
                    }
                }
            }
        }
    }
}

TEST_F(OriginalOffsetReaderTest, SamplingRejectsIndexWithoutPinning) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("original_id", DataType::INT64);
    auto column = Column(false);
    SamplingSegment segment(schema, column);
    segment.indexed = true;
    for (bool raw : {false, true}) {
        segment.raw = raw;
        QueryContext query("unknown-index-locality", &segment, ids.size(), 1);
        SearchInfo info;
        info.search_params_ = nlohmann::json::object();
        query.set_search_info(info);
        ExecContext exec(&query);
        auto root = CompileExpression(Mod(), &query, {}, true);
        EXPECT_EQ(root->OffsetSamplingColumn(), nullptr);
        EXPECT_EQ(SampleOffsetFilterRatio(Mod(), &exec), std::nullopt);
        EXPECT_EQ(segment.captures, 0);
        EXPECT_TRUE(column->pins.empty());
    }
}

TEST_F(OriginalOffsetReaderTest, CallbackWithoutAnnSearchParameters) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("original_id", DataType::INT64);
    auto column = Column(false);
    SamplingSegment segment(schema, column);
    QueryContext query("offset-only", &segment, ids.size(), 1);
    ExecContext exec(&query);
    EXPECT_NO_THROW(OffsetExpressionCallback(Mod(), &exec, ids.size()));
}

TEST_F(OriginalOffsetReaderTest, PhysicalIdentityNotMatchingChunkNumbers) {
    auto first = Column(false);
    auto second = Column(false);
    ASSERT_EQ(first->num_chunks(), second->num_chunks());
    ASSERT_NE(first->OffsetSamplingStorageIdentity(),
              second->OffsetSamplingStorageIdentity());
    class Source : public Expr {
     public:
        explicit Source(std::shared_ptr<const ChunkedColumnInterface> column)
            : Expr(DataType::BOOL, {}, "layout-only", nullptr),
              column_(std::move(column)) {
        }
        std::shared_ptr<const ChunkedColumnInterface>
        OffsetSamplingColumn() const override {
            return column_;
        }

     private:
        std::shared_ptr<const ChunkedColumnInterface> column_;
    };
    auto a = std::make_shared<Source>(first);
    auto b = std::make_shared<Source>(second);
    Expr same(DataType::BOOL, {a, a}, "shared-cell", nullptr);
    Expr different(DataType::BOOL, {a, b}, "distinct-cells", nullptr);
    Expr unknown(DataType::BOOL, {}, "unknown-leaf", nullptr);
    EXPECT_EQ(same.OffsetSamplingColumn(), first);
    EXPECT_EQ(different.OffsetSamplingColumn(), nullptr);
    EXPECT_EQ(unknown.OffsetSamplingColumn(), nullptr);
    EXPECT_TRUE(first->pins.empty());
    EXPECT_TRUE(second->pins.empty());
}

TEST_F(OriginalOffsetReaderTest, AllArithmeticOpsUseSharedReader) {
    auto schema = std::make_shared<Schema>();
    ASSERT_EQ(schema->AddDebugField("original_id", DataType::INT64),
              FieldId(100));
    for (bool irregular : {false, true}) {
        auto column = Column(irregular);
        Segment segment(schema, column);
        auto check = [&]<proto::plan::ArithOpType op>(int64_t threshold) {
            Reader reader(&segment);
            const auto captures = segment.captures;
            using Kernel = ArithOpElementFunc<int64_t,
                                              proto::plan::LessThan,
                                              op,
                                              FilterType::random>;
            for (size_t count :
                 {0, 1, 2, 3, 31, 32, 33, 63, 64, 65, 257, 1025}) {
                OffsetVector offsets(count);
                for (size_t i = 0; i < count; ++i)
                    offsets[i] = (i * 6143 + 17) % ids.size();
                if (count > 1)
                    offsets.back() = offsets.front();
                TargetBitmap actual(count, false), expected(count, false),
                    valid(count, true);
                size_t cursor = 0, largest = 0;
                auto consume = [&]<FilterType mode>(const int64_t* values,
                                                    ValidityView validity,
                                                    const int32_t* lanes,
                                                    int size,
                                                    TargetBitmapView out,
                                                    TargetBitmapView) {
                    EXPECT_EQ(mode, FilterType::random);
                    EXPECT_EQ(lanes, nullptr);
                    EXPECT_FALSE(bool(validity));
                    largest = std::max(largest, size_t(size));
                    Kernel{}(values, size, threshold, 5, out);
                    cursor += size;
                };
                EXPECT_EQ(
                    reader.ProcessDataByOffsets<int64_t>(
                        consume, {}, &offsets, actual.view(), valid.view()),
                    count);
                Kernel{}(ids.data(),
                         count,
                         threshold,
                         5,
                         expected.view(),
                         offsets.data());
                EXPECT_EQ(cursor, count);
                if (count)
                    EXPECT_EQ(largest, std::min(count, size_t(64)));
                if (count >= 257) {
                    EXPECT_GT(expected.count(), 0);
                    EXPECT_LT(expected.count(), count);
                }
                for (size_t i = 0; i < count; ++i) {
                    EXPECT_EQ(bool(actual[i]), bool(expected[i]));
                    EXPECT_TRUE(valid[i]);
                }
            }
            EXPECT_EQ(segment.captures - captures, 1);
        };
        // Exercise both outcomes using predicate constants, never altered
        // scalar values. Values in the original source remain untouched.
        check.template operator()<proto::plan::Add>(32768);
        check.template operator()<proto::plan::Sub>(32768);
        check.template operator()<proto::plan::Mul>(163840);
        check.template operator()<proto::plan::Div>(6553);
        check.template operator()<proto::plan::Mod>(3);
    }
}

TEST_F(OriginalOffsetReaderTest, SkipChunksPreserveMaskedOutputPositions) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("original_id", DataType::INT64);
    auto column = Column(true);
    Segment segment(schema, column);
    for (int skip_mode : {0, 1, 2}) {
        Reader reader(&segment);
        auto skip = [skip_mode](const SkipIndex&, FieldId, int chunk) {
            return skip_mode == 1 || (skip_mode == 2 && chunk % 2 == 0);
        };
        for (size_t count : {1, 31, 32, 33, 64, 65, 257, 1025}) {
            OffsetVector offsets(count);
            for (size_t i = 0; i < count; ++i)
                offsets[i] = (i * 6143 + 17) % ids.size();
            TargetBitmap actual(count, false), valid(count, true),
                mask(count, true);
            for (size_t i = 0; i < count; ++i) mask[i] = i % 3 != 1;
            size_t cursor = 0;
            auto consume = [&]<FilterType mode>(const int64_t* values,
                                                ValidityView,
                                                const int32_t* lanes,
                                                int size,
                                                TargetBitmapView out,
                                                TargetBitmapView) {
                EXPECT_EQ(mode, FilterType::random);
                EXPECT_EQ(lanes, nullptr);
                for (int i = 0; i < size; ++i) {
                    auto [chunk, _] =
                        column->GetChunkIDByOffset(offsets[cursor + i]);
                    EXPECT_EQ(values == nullptr,
                              skip(SkipIndex{}, FieldId(100), chunk));
                    if (values) {
                        EXPECT_EQ(values[i], ids[offsets[cursor + i]]);
                        out[i] = mask[cursor + i] && values[i] >= 0;
                    }
                }
                cursor += size;
            };
            EXPECT_EQ(
                reader.ProcessDataByOffsetsWithMask<int64_t>(
                    consume, skip, &offsets, actual.view(), valid.view(), mask),
                count);
            EXPECT_EQ(cursor, count);
            for (size_t i = 0; i < count; ++i) {
                auto [chunk, _] = column->GetChunkIDByOffset(offsets[i]);
                EXPECT_EQ(bool(actual[i]),
                          !skip(SkipIndex{}, FieldId(100), chunk) && mask[i] &&
                              ids[offsets[i]] >= 0);
                EXPECT_TRUE(valid[i]);
            }
        }
    }
}
}  // namespace
}  // namespace milvus::exec
