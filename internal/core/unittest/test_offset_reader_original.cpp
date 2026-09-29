// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>
#include <fstream>

#include "exec/expression/BinaryArithOpEvalRangeExpr.h"
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

    std::shared_ptr<ProxyChunkColumn>
    Column(bool irregular) {
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
            chunks.push_back(std::make_unique<GroupChunk>(fields));
            lengths.push_back(count);
            first += count;
        }
        auto group = std::make_shared<ChunkedColumnGroup>(
            std::make_unique<TestGroupChunkTranslator>(
                1,
                lengths,
                "original-offset-" + std::to_string(irregular),
                std::move(chunks)));
        return std::make_shared<ProxyChunkColumn>(
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
    std::vector<int64_t> ids;
};

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
