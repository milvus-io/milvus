#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <algorithm>
#include <cstddef>
#include <cmath>
#include <cstring>
#include <type_traits>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <random>
#include <unordered_set>
#include <utility>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "bitset/bitset.h"
#include "common/Array.h"
#include "common/FieldData.h"
#include "common/Tracer.h"
#include "common/TracerBase.h"
#include "common/Types.h"
#include "common/Slice.h"
#include "folly/coro/BlockingWait.h"
#include "gtest/gtest.h"
#include "index/Meta.h"
#include "index/IndexFactory.h"
#include "index/HybridScalarIndex.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "index/ScalarIndexSort.h"
#include "index/SortedMembership.h"
#include "milvus-storage/filesystem/fs.h"
#include "pb/common.pb.h"
#include "storage/ChunkManager.h"
#include "storage/FileManager.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/AsyncLoadTestUtils.h"
#include "test_utils/Constants.h"
#include "test_utils/TmpPath.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::index;

namespace {

class ExposedScalarIndexSort : public ScalarIndexSort<int64_t> {
 public:
    using ScalarIndexSort<int64_t>::ScalarIndexSort;

    void
    LoadDirectForTest(storage::AsyncIndexEntryReader& reader,
                      const Config& config,
                      proto::common::LoadPriority priority) {
        auto plan = PlanLoad(reader.Directory(), reader.IndexMeta(), config);
        folly::coro::blockingWait(
            reader.ReadEntriesAsync(plan.entries, priority));
        folly::coro::blockingWait(FinishLoadAsync(plan, config));
        plan.Commit();
    }
};

struct ScalarSortAsyncLoadFixture {
    explicit ScalarSortAsyncLoadFixture(
        std::string test_name,
        proto::schema::DataType type = proto::schema::DataType::Int64)
        : root_path(TestLocalPath + "/" + std::move(test_name)) {
        boost::filesystem::remove_all(root_path);
        storage::StorageConfig storage_config;
        storage_config.storage_type = "local";
        storage_config.root_path = root_path;
        chunk_manager = storage::CreateChunkManager(storage_config);
        fs = storage::InitArrowFileSystem(storage_config);

        field_schema.set_data_type(type);
        field_meta = storage::FieldDataMeta{1, 2, 3, 101, field_schema};
        index_meta = storage::IndexMeta{3, 101, 1000, 10000};
        ctx = storage::FileManagerContext(
            field_meta, index_meta, chunk_manager, fs);
    }

    ~ScalarSortAsyncLoadFixture() {
        boost::filesystem::remove_all(root_path);
    }

    std::string root_path;
    proto::schema::FieldSchema field_schema;
    storage::FieldDataMeta field_meta;
    storage::IndexMeta index_meta;
    storage::ChunkManagerPtr chunk_manager;
    milvus_storage::ArrowFileSystemPtr fs;
    storage::FileManagerContext ctx;
};

}  // namespace

namespace {

template <typename T>
class ScalarIndexSortNaNTest : public testing::Test {};
using NaNTypes = testing::Types<float, double>;
TYPED_TEST_SUITE(ScalarIndexSortNaNTest, NaNTypes);

template <typename T>
FieldDataPtr
NaNScalarFieldData(const std::vector<T>& rows, uint8_t validity) {
    const auto type =
        std::is_same_v<T, float> ? DataType::FLOAT : DataType::DOUBLE;
    auto field = std::make_shared<FieldData<T>>(type, true);
    field->FillFieldData(rows.data(), &validity, rows.size(), 0);
    return field;
}

template <typename T>
FieldDataPtr
NaNArrayFieldData(const std::vector<std::vector<T>>& rows, uint8_t validity) {
    std::vector<Array> arrays;
    for (const auto& row : rows) {
        ScalarFieldProto values;
        for (T value : row) {
            if constexpr (std::is_same_v<T, float>) {
                values.mutable_float_data()->add_data(value);
            } else {
                values.mutable_double_data()->add_data(value);
            }
        }
        arrays.emplace_back(values);
    }
    auto field = std::make_shared<FieldData<Array>>(DataType::ARRAY, true);
    field->FillFieldData(arrays.data(), &validity, arrays.size(), 0);
    return field;
}

template <typename T>
void
CheckIndexedNaN(ScalarIndexSort<T>& index,
                const std::vector<T>& rows,
                const bool* valid = nullptr) {
    ASSERT_EQ(index.Count(), rows.size());
    const auto is_null = index.IsNull();
    const auto is_not_null = index.IsNotNull();
    size_t indexed = 0;
    for (size_t i = 0; i < rows.size(); ++i) {
        const bool source_valid = valid == nullptr || valid[i];
        EXPECT_EQ(is_null[i], !source_valid);
        EXPECT_EQ(is_not_null[i], source_valid);
        const auto value = index.Reverse_Lookup(i);
        ASSERT_EQ(value.has_value(), source_valid);
        if (source_valid) {
            ++indexed;
            if (std::isnan(rows[i])) {
                EXPECT_TRUE(std::isnan(*value));
            } else {
                EXPECT_EQ(*value, rows[i]);
                EXPECT_EQ(std::signbit(*value), std::signbit(rows[i]));
            }
        }
    }
    ASSERT_EQ(index.Size(), indexed);
    for (auto it = index.begin(); it != index.end(); ++it) {
        ASSERT_LT(static_cast<size_t>(it->idx_), rows.size());
        EXPECT_TRUE(ScalarEqual(it->a_, rows[it->idx_]));
    }
    for (const auto& queries : std::vector<std::vector<T>>{
             {},
             {T(2)},
             {T(9)},
             {T(0)},
             {std::numeric_limits<T>::quiet_NaN()},
             {T(2), std::numeric_limits<T>::quiet_NaN()},
             std::vector<T>(129, T(2))}) {
        const auto in = index.In(queries.size(), queries.data());
        const auto not_in = index.NotIn(queries.size(), queries.data());
        for (size_t row = 0; row < rows.size(); ++row) {
            const bool source_valid = valid == nullptr || valid[row];
            const bool hit =
                source_valid &&
                std::any_of(queries.begin(), queries.end(), [&](T query) {
                    return ScalarEqual(rows[row], query);
                });
            EXPECT_EQ(in[row], hit);
            EXPECT_EQ(not_in[row], source_valid && !hit);
        }
    }
    for (T value : {T(0),
                    T(2),
                    T(9),
                    std::numeric_limits<T>::infinity(),
                    std::numeric_limits<T>::quiet_NaN()}) {
        for (auto op : {OpType::LessThan,
                        OpType::LessEqual,
                        OpType::GreaterThan,
                        OpType::GreaterEqual}) {
            const auto result = index.Range(value, op);
            for (size_t row = 0; row < rows.size(); ++row) {
                bool hit = false;
                switch (op) {
                    case OpType::LessThan:
                        hit = ScalarLess(rows[row], value);
                        break;
                    case OpType::LessEqual:
                        hit = ScalarLessEqual(rows[row], value);
                        break;
                    case OpType::GreaterThan:
                        hit = ScalarGreater(rows[row], value);
                        break;
                    case OpType::GreaterEqual:
                        hit = ScalarGreaterEqual(rows[row], value);
                        break;
                    default:
                        FAIL() << "unexpected operator";
                }
                EXPECT_EQ(result[row], (valid == nullptr || valid[row]) && hit);
            }
        }
    }
    const auto range = index.Range(-std::numeric_limits<T>::infinity(),
                                   true,
                                   std::numeric_limits<T>::infinity(),
                                   true);
    for (size_t row = 0; row < rows.size(); ++row) {
        EXPECT_EQ(range[row],
                  (valid == nullptr || valid[row]) && !std::isnan(rows[row]));
    }
}

TYPED_TEST(ScalarIndexSortNaNTest, IndexesNaNInEveryBuildRoute) {
    using T = TypeParam;
    const T nan = std::numeric_limits<T>::quiet_NaN();
    const std::vector<T> rows{nan, T(1), T(2), nan, T(3), T(4), T(5)};
    ScalarIndexSort<T> raw;
    ASSERT_NO_THROW(raw.Build(rows.size(), rows.data()));
    CheckIndexedNaN(raw, rows);

    auto scalar_data = NaNScalarFieldData(rows, 0x7f);
    ScalarIndexSort<T> scalar;
    ASSERT_NO_THROW(scalar.BuildWithFieldData({scalar_data}));
    CheckIndexedNaN(scalar, rows);

    auto array_data = NaNArrayFieldData<T>(
        {{nan, T(1)}, {T(2), nan}, {T(3), T(4), T(5)}}, 0x07);
    ScalarIndexSort<T> nested({}, true);
    ASSERT_NO_THROW(nested.BuildWithFieldData({array_data}));
    CheckIndexedNaN(nested, rows);
}

TYPED_TEST(ScalarIndexSortNaNTest, AllNaNBuildsRemainSourceValid) {
    using T = TypeParam;
    const T nan = std::numeric_limits<T>::quiet_NaN();
    const std::vector<T> rows{nan, nan, nan};
    ScalarIndexSort<T> raw;
    ASSERT_NO_THROW(raw.Build(rows.size(), rows.data()));
    CheckIndexedNaN(raw, rows);
    ScalarIndexSort<T> scalar;
    ASSERT_NO_THROW(
        scalar.BuildWithFieldData({NaNScalarFieldData(rows, 0x07)}));
    CheckIndexedNaN(scalar, rows);
    ScalarIndexSort<T> nested({}, true);
    ASSERT_NO_THROW(nested.BuildWithFieldData(
        {NaNArrayFieldData<T>({{nan}, {nan, nan}}, 0x03)}));
    CheckIndexedNaN(nested, rows);
}

TYPED_TEST(ScalarIndexSortNaNTest, LegacyAndPackedReloadsPreserveNaNRows) {
    using T = TypeParam;
    milvus::test::ScopedLoadTransientBudget budget_guard(0);
    const auto type =
        std::is_same_v<T, float> ? proto::schema::Float : proto::schema::Double;
    const T nan = std::numeric_limits<T>::quiet_NaN();
    for (bool nested : {false, true}) {
        for (bool all_nan : {false, true}) {
            SCOPED_TRACE(testing::Message()
                         << "nested=" << nested << " all_nan=" << all_nan);
            ScalarSortAsyncLoadFixture fixture("scalar_sort_nan_reload", type);
            auto ctx = fixture.ctx;
            if (nested) {
                ctx.fieldDataMeta.field_schema.set_data_type(
                    proto::schema::Array);
                ctx.fieldDataMeta.field_schema.set_element_type(type);
            }
            const std::vector<T> rows =
                all_nan
                    ? std::vector<T>{nan, nan, nan}
                    : std::vector<T>{nan, nan, T(1), T(2), T(3), T(4), T(5)};
            auto valid = std::make_unique<bool[]>(rows.size());
            for (size_t i = 0; i < rows.size(); ++i) {
                valid[i] = i != 0;
            }
            ScalarIndexSort<T> built(ctx, nested);
            std::vector<T> expected_rows = rows;
            const bool* expected_valid = valid.get();
            if (nested) {
                expected_rows.erase(expected_rows.begin());
                expected_valid = nullptr;
                std::vector<T> remaining(rows.begin() + 1, rows.end());
                built.BuildWithFieldData(
                    {NaNArrayFieldData<T>({{nan}, remaining}, 0x02)});
            } else {
                const uint8_t mask = all_nan ? 0x06 : 0x7e;
                built.BuildWithFieldData({NaNScalarFieldData(rows, mask)});
            }
            CheckIndexedNaN(built, expected_rows, expected_valid);
            const auto stats = built.UploadUnified({});
            const auto binaries = built.Serialize({});
            ASSERT_FALSE(binaries.Contains("nan_rows"));
            EXPECT_EQ(binaries.Contains("valid_bitset"), nested);
            for (bool mmap : {false, true}) {
                Config config;
                config[ENABLE_MMAP] = mmap;
                config[milvus::LOAD_PRIORITY] =
                    proto::common::LoadPriority::HIGH;
                config[INDEX_FILES] = stats->GetIndexFiles();
                for (bool async : {false, true}) {
                    SCOPED_TRACE(testing::Message()
                                 << "mmap=" << mmap << " async=" << async);
                    auto load_ctx = ctx;
                    load_ctx.use_async_load = async;
                    ScalarIndexSort<T> loaded(load_ctx, nested);
                    ASSERT_NO_THROW(loaded.LoadUnified(config));
                    CheckIndexedNaN(loaded, expected_rows, expected_valid);
                }
                ScalarIndexSort<T> legacy(ctx, nested);
                ASSERT_NO_THROW(legacy.Load(binaries, config));
                CheckIndexedNaN(legacy, expected_rows, expected_valid);
            }
        }
    }
}

TYPED_TEST(ScalarIndexSortNaNTest, FactoryGatesNaNTotalOrderByEngineVersion) {
    using T = TypeParam;
    const auto type =
        std::is_same_v<T, float> ? proto::schema::Float : proto::schema::Double;
    ScalarSortAsyncLoadFixture fixture("scalar_sort_nan_factory", type);
    const std::vector<T> rows{std::numeric_limits<T>::quiet_NaN(), T(1), T(2)};
    for (int32_t version : {5, kMinScalarIndexVersionForNaNTotalOrder}) {
        for (const auto& index_type : {ASCENDING_SORT, HYBRID_INDEX_TYPE}) {
            SCOPED_TRACE(testing::Message()
                         << "version=" << version << " type=" << index_type);
            CreateIndexInfo info;
            info.field_type = static_cast<DataType>(type);
            info.index_type = index_type;
            info.scalar_index_engine_version = version;
            auto base =
                IndexFactory::GetInstance().CreateIndex(info, fixture.ctx);
            auto* scalar = dynamic_cast<ScalarIndex<T>*>(base.get());
            ASSERT_NE(scalar, nullptr);
            auto* hybrid = dynamic_cast<HybridScalarIndex<T>*>(base.get());
            if (hybrid != nullptr) {
                // Exercise the legacy physical SORT backend without asserting
                // that the old NaN cardinality calculation selects it by default.
                hybrid->bitmap_index_cardinality_limit_ = 0;
                hybrid->high_cardinality_index_type_ = ScalarIndexType::STLSORT;
            }
            if (version < kMinScalarIndexVersionForNaNTotalOrder) {
                EXPECT_THROW(scalar->Build(rows.size(), rows.data()),
                             SegcoreError);
                continue;
            }
            ASSERT_NO_THROW(scalar->Build(rows.size(), rows.data()));
            ScalarIndexSort<T>* sorted = nullptr;
            if (hybrid != nullptr) {
                EXPECT_EQ(hybrid->internal_index_type_,
                          ScalarIndexType::STLSORT);
                sorted = dynamic_cast<ScalarIndexSort<T>*>(
                    hybrid->internal_index_.get());
            } else {
                sorted = dynamic_cast<ScalarIndexSort<T>*>(base.get());
            }
            ASSERT_NE(sorted, nullptr);
            EXPECT_EQ(sorted->Count(), rows.size());
            EXPECT_EQ(sorted->Size(), 3);
            EXPECT_FALSE(sorted->Serialize({}).Contains("nan_rows"));
            EXPECT_FALSE(sorted->Serialize({}).Contains("valid_bitset"));
            if (version >= 6) {
                CheckIndexedNaN(*sorted, rows);
            }
        }

        auto ctx = fixture.ctx;
        ctx.fieldDataMeta.field_schema.set_data_type(proto::schema::Array);
        ctx.fieldDataMeta.field_schema.set_element_type(type);
        CreateIndexInfo info;
        info.field_type = DataType::ARRAY;
        info.field_name = "parent[value]";
        info.index_type = ASCENDING_SORT;
        info.scalar_index_engine_version = version;
        auto base = IndexFactory::GetInstance().CreateIndex(info, ctx);
        auto* sorted = dynamic_cast<ScalarIndexSort<T>*>(base.get());
        ASSERT_NE(sorted, nullptr);
        if (version < kMinScalarIndexVersionForNaNTotalOrder) {
            EXPECT_THROW(sorted->BuildWithFieldData({NaNArrayFieldData<T>(
                             {{rows[0], rows[1]}, {rows[2]}}, 0x03)}),
                         SegcoreError);
            continue;
        }
        ASSERT_NO_THROW(sorted->BuildWithFieldData(
            {NaNArrayFieldData<T>({{rows[0], rows[1]}, {rows[2]}}, 0x03)}));
        EXPECT_EQ(sorted->Count(), rows.size());
        EXPECT_EQ(sorted->Size(), 3);
        EXPECT_FALSE(sorted->Serialize({}).Contains("nan_rows"));
        EXPECT_TRUE(sorted->Serialize({}).Contains("valid_bitset"));
        if (version >= 6) {
            CheckIndexedNaN(*sorted, rows);
        }
    }
}

TYPED_TEST(ScalarIndexSortNaNTest, InvalidOffsetsRemainDataFormatErrors) {
    using T = TypeParam;
    const auto dtype =
        std::is_same_v<T, float> ? proto::schema::Float : proto::schema::Double;
    ScalarSortAsyncLoadFixture fixture("scalar_sort_nan_offsets", dtype);
    const std::vector<T> rows{std::numeric_limits<T>::quiet_NaN(), T(1), T(2)};
    ScalarIndexSort<T> built(fixture.ctx);
    built.Build(rows.size(), rows.data());
    const auto binaries = built.Serialize({});
    ASSERT_FALSE(binaries.Contains("nan_rows"));
    const auto stats = built.UploadUnified({});
    for (int32_t offset : {-1, -2, 3}) {
        SCOPED_TRACE(offset);
        milvus::test::ControlledDirectReadFile* remote_file = nullptr;
        auto reader = milvus::test::OpenDirectIndexEntryReader(
            milvus::test::ReadPackedIndexBytes(fixture.ctx,
                                               stats->GetIndexFiles()),
            &remote_file);
        ASSERT_FALSE(reader->Directory().HasEntry("nan_rows"));
        Config config;
        config[ENABLE_MMAP] = false;
        ScalarIndexSort<T> loaded(fixture.ctx);
        auto plan =
            loaded.PlanLoad(reader->Directory(), reader->IndexMeta(), config);
        folly::coro::blockingWait(reader->ReadEntriesAsync(
            plan.entries, proto::common::LoadPriority::HIGH));
        int32_t* offsets = nullptr;
        for (auto& entry : plan.entries) {
            if (entry.name == "idx_to_offsets") {
                auto* target =
                    std::get_if<storage::MemoryEntryTarget>(&entry.target);
                ASSERT_NE(target, nullptr);
                offsets = reinterpret_cast<int32_t*>(target->data);
            }
        }
        ASSERT_NE(offsets, nullptr);
        EXPECT_EQ(offsets[0], 2);
        EXPECT_EQ(offsets[1], 0);
        EXPECT_EQ(offsets[2], 1);
        offsets[0] = offset;
        folly::coro::blockingWait(loaded.FinishLoadAsync(plan, config));
        plan.Commit();
        try {
            loaded.Reverse_Lookup(0);
            FAIL() << "A source-valid row must have a valid sorted offset";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), ErrorCode::DataFormatBroken);
        }
    }
}

TYPED_TEST(ScalarIndexSortNaNTest,
           IgnoresNullPayloadAndPreservesOrderedValues) {
    using T = TypeParam;
    const std::vector<T> rows{std::numeric_limits<T>::quiet_NaN(),
                              -std::numeric_limits<T>::infinity(),
                              T(-0.0),
                              T(0.0),
                              std::numeric_limits<T>::infinity()};
    const bool valid[] = {false, true, true, true, true};
    ScalarIndexSort<T> raw;
    ASSERT_NO_THROW(raw.Build(rows.size(), rows.data(), valid));
    auto scalar_data = NaNScalarFieldData(rows, 0x1e);
    ScalarIndexSort<T> scalar;
    ASSERT_NO_THROW(scalar.BuildWithFieldData({scalar_data}));
    for (auto* index : {&raw, &scalar}) {
        CheckIndexedNaN(*index, rows, valid);
        EXPECT_EQ(index->Count(), rows.size());
        EXPECT_EQ(index->Size(), 4);
        EXPECT_FALSE(index->Reverse_Lookup(0).has_value());
        for (size_t row = 1; row < rows.size(); ++row) {
            const auto value = index->Reverse_Lookup(row);
            ASSERT_TRUE(value.has_value());
            EXPECT_EQ(*value, rows[row]);
            EXPECT_EQ(std::signbit(*value), std::signbit(rows[row]));
        }
        const T zero = T(0);
        auto in = index->In(1, &zero);
        auto not_in = index->NotIn(1, &zero);
        EXPECT_FALSE(in[0]);
        EXPECT_FALSE(not_in[0]);
        EXPECT_TRUE(in[2]);
        EXPECT_TRUE(in[3]);
        EXPECT_EQ(in.count(), 2);
        EXPECT_TRUE(not_in[1]);
        EXPECT_TRUE(not_in[4]);
        EXPECT_EQ(not_in.count(), 2);
    }

    auto array_data = NaNArrayFieldData<T>(
        {{rows[0]}, {rows[1], rows[2], rows[3], rows[4]}}, 0x02);
    ScalarIndexSort<T> nested({}, true);
    ASSERT_NO_THROW(nested.BuildWithFieldData({array_data}));
    EXPECT_EQ(nested.Count(), 4);
    EXPECT_EQ(nested.Size(), 4);
    for (size_t offset = 0; offset < 4; ++offset) {
        const auto value = nested.Reverse_Lookup(offset);
        ASSERT_TRUE(value.has_value());
        EXPECT_EQ(*value, rows[offset + 1]);
        EXPECT_EQ(std::signbit(*value), std::signbit(rows[offset + 1]));
    }
    const T zero = T(0);
    auto in = nested.In(1, &zero);
    auto not_in = nested.NotIn(1, &zero);
    EXPECT_TRUE(in[1]);
    EXPECT_TRUE(in[2]);
    EXPECT_EQ(in.count(), 2);
    EXPECT_TRUE(not_in[0]);
    EXPECT_TRUE(not_in[3]);
    EXPECT_EQ(not_in.count(), 2);
}

}  // namespace

static storage::FileManagerContext
CreateScalarSortTestFileManagerContext() {
    storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = TestLocalPath;
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);
    storage::FieldDataMeta field_meta{1, 2, 3, 101};
    field_meta.field_schema.set_data_type(proto::schema::DataType::Int64);
    storage::IndexMeta index_meta{3, 101, 1000, 10000};
    storage::FileManagerContext ctx(field_meta, index_meta, chunk_manager, fs);
    return ctx;
}

TEST(ScalarIndexSortV3AsyncLoadTest, MemoryPathUsesNativeDirectEntryReads) {
    milvus::test::ScopedLoadTransientBudget budget_guard(0);
    ScalarSortAsyncLoadFixture fixture("scalar_sort_async_memory");
    std::vector<int64_t> data{50, 10, 30, 20, 40};

    ExposedScalarIndexSort build_index(fixture.ctx);
    build_index.Build(data.size(), data.data());
    auto stats = build_index.UploadUnified({});

    milvus::test::ControlledDirectReadFile* remote_file = nullptr;
    auto reader = milvus::test::OpenDirectIndexEntryReader(
        milvus::test::ReadPackedIndexBytes(fixture.ctx, stats->GetIndexFiles()),
        &remote_file);
    auto read_at_calls_after_open = remote_file->ReadAtCalls();

    ExposedScalarIndexSort load_index(fixture.ctx);
    Config config;
    config[milvus::index::ENABLE_MMAP] = false;
    config[milvus::LOAD_PRIORITY] = milvus::proto::common::LoadPriority::HIGH;
    load_index.LoadDirectForTest(
        *reader, config, milvus::proto::common::LoadPriority::HIGH);

    EXPECT_GE(remote_file->DirectReadCalls().size(), 3);
    EXPECT_EQ(remote_file->AsyncReadCalls(), 0);
    EXPECT_EQ(remote_file->ReadAtCalls(), read_at_calls_after_open);
    ASSERT_EQ(load_index.Count(), data.size());
    auto bitset = load_index.Range(
        static_cast<int64_t>(20), true, static_cast<int64_t>(40), true);
    EXPECT_FALSE(bitset[0]);
    EXPECT_FALSE(bitset[1]);
    EXPECT_TRUE(bitset[2]);
    EXPECT_TRUE(bitset[3]);
    EXPECT_TRUE(bitset[4]);
    EXPECT_EQ(load_index.Reverse_Lookup(0), data[0]);
}

TEST(ScalarIndexSortV3AsyncLoadTest, MmapPathUsesNativeDirectEntryReads) {
    milvus::test::ScopedLoadTransientBudget budget_guard(0);
    ScalarSortAsyncLoadFixture fixture("scalar_sort_async_mmap");
    std::vector<int64_t> data{5, 4, 3, 2, 1, 0};

    ExposedScalarIndexSort build_index(fixture.ctx);
    build_index.Build(data.size(), data.data());
    auto stats = build_index.UploadUnified({});

    milvus::test::ControlledDirectReadFile* remote_file = nullptr;
    auto reader = milvus::test::OpenDirectIndexEntryReader(
        milvus::test::ReadPackedIndexBytes(fixture.ctx, stats->GetIndexFiles()),
        &remote_file);
    auto read_at_calls_after_open = remote_file->ReadAtCalls();

    ExposedScalarIndexSort load_index(fixture.ctx);
    Config config;
    config[milvus::index::ENABLE_MMAP] = true;
    config[milvus::LOAD_PRIORITY] = milvus::proto::common::LoadPriority::HIGH;
    load_index.LoadDirectForTest(
        *reader, config, milvus::proto::common::LoadPriority::HIGH);

    EXPECT_GE(remote_file->DirectReadCalls().size(), 3);
    EXPECT_EQ(remote_file->AsyncReadCalls(), 0);
    EXPECT_EQ(remote_file->ReadAtCalls(), read_at_calls_after_open);
    ASSERT_EQ(load_index.Count(), data.size());
    std::vector<int64_t> values{0, 5};
    auto bitset = load_index.In(values.size(), values.data());
    EXPECT_TRUE(bitset[0]);
    EXPECT_FALSE(bitset[1]);
    EXPECT_FALSE(bitset[2]);
    EXPECT_FALSE(bitset[3]);
    EXPECT_FALSE(bitset[4]);
    EXPECT_TRUE(bitset[5]);
    EXPECT_EQ(load_index.Reverse_Lookup(5), data[5]);
}

namespace {

std::vector<std::vector<int64_t>>
MembershipQueries() {
    constexpr auto min = std::numeric_limits<int64_t>::min();
    constexpr auto max = std::numeric_limits<int64_t>::max();
    constexpr size_t threshold = 128;
    std::vector<std::vector<int64_t>> queries{
        {}, {min}, {max}, {0}, {17, -17}, {min, max, 0, 0}};
    std::mt19937_64 rng(53853);
    for (size_t n : {threshold - 1, threshold, threshold + 1, size_t{4096}}) {
        // Duplicate hits, all misses on either side, and mixed sparse probes.
        queries.emplace_back(n, 0);
        queries.emplace_back(n, -1000000);
        queries.emplace_back(n, 1000000);
        std::vector<int64_t> mixed(n);
        for (size_t i = 0; i < n; ++i) {
            mixed[i] = static_cast<int64_t>(rng() % 8193) - 4096;
        }
        mixed[0] = min;
        mixed[1] = max;
        mixed[2] = 0;
        mixed[3] = 0;
        queries.push_back(mixed);
        std::sort(mixed.begin(), mixed.end());
        queries.push_back(mixed);
        std::reverse(mixed.begin(), mixed.end());
        queries.push_back(std::move(mixed));
    }
    return queries;
}

void
CheckMembership(ScalarIndexSort<int64_t>& index,
                const std::vector<int64_t>& rows,
                const bool* valid) {
    ASSERT_EQ(index.Count(), rows.size());
    auto queries = MembershipQueries();
    queries.push_back(rows);  // Full coverage, including repeated index values.
    for (const auto& query : queries) {
        SCOPED_TRACE("query size=" + std::to_string(query.size()));
        const auto original = query;
        const auto* values = query.empty() ? nullptr : query.data();
        const auto in = index.In(query.size(), values);
        const auto not_in = index.NotIn(query.size(), values);
        ASSERT_EQ(in.size(), rows.size());
        ASSERT_EQ(not_in.size(), rows.size());
        const std::unordered_set<int64_t> terms(query.begin(), query.end());
        for (size_t row = 0; row < rows.size(); ++row) {
            const bool hit = terms.count(rows[row]) != 0;
            const bool is_valid = valid == nullptr || valid[row];
            ASSERT_EQ(in[row], is_valid && hit) << "row=" << row;
            ASSERT_EQ(not_in[row], is_valid && !hit) << "row=" << row;
        }
        EXPECT_EQ(query, original);
    }
}

std::vector<int64_t>
MembershipRows(size_t n, size_t cardinality) {
    std::vector<int64_t> rows(n);
    for (size_t i = 0; i < n; ++i) {
        rows[i] = 2 * static_cast<int64_t>(i % cardinality) -
                  static_cast<int64_t>(cardinality);
    }
    rows.front() = std::numeric_limits<int64_t>::min();
    rows.back() = std::numeric_limits<int64_t>::max();
    std::mt19937_64 rng(53853);
    std::shuffle(rows.begin(), rows.end(), rng);
    return rows;
}

}  // namespace

TEST(ScalarIndexSortMembershipTest, MatchesScanAcrossCardinalityAndNulls) {
    for (size_t n : {1, 2, 63, 64, 65, 127, 128, 129, 2049}) {
        for (size_t cardinality : {size_t{2}, size_t{32}, n}) {
            const auto rows = MembershipRows(n, cardinality);
            for (int null_mode : {0, 1, 2}) {
                SCOPED_TRACE("rows=" + std::to_string(n) +
                             " cardinality=" + std::to_string(cardinality) +
                             " null_mode=" + std::to_string(null_mode));
                auto valid = std::make_unique<bool[]>(n);
                for (size_t i = 0; i < n; ++i) {
                    valid[i] = null_mode == 0 || (null_mode == 1 && i % 3 != 0);
                }
                const auto* validity = null_mode == 0 ? nullptr : valid.get();
                ScalarIndexSort<int64_t> index;
                index.Build(n, rows.data(), validity);
                CheckMembership(index, rows, validity);
            }
        }
    }
}

TEST(ScalarIndexSortMembershipTest, LargeListWithFewNonNullEntries) {
    const auto rows = MembershipRows(4096, 32);
    auto valid = std::make_unique<bool[]>(rows.size());
    valid[0] = valid[rows.size() / 2] = valid[rows.size() - 1] = true;
    ScalarIndexSort<int64_t> index;
    index.Build(rows.size(), rows.data(), valid.get());
    ASSERT_EQ(index.Size(), 3);
    CheckMembership(index, rows, valid.get());
}

TEST(ScalarIndexSortMembershipTest, MatchesScanAfterLegacyAndPackedReloads) {
    milvus::test::ScopedLoadTransientBudget budget_guard(0);
    ScalarSortAsyncLoadFixture fixture("scalar_sort_membership_reload");
    auto rows = MembershipRows(257, 32);
    auto valid = std::make_unique<bool[]>(rows.size());
    for (bool all_null : {false, true}) {
        SCOPED_TRACE("all_null=" + std::to_string(all_null));
        for (size_t i = 0; i < rows.size(); ++i) {
            valid[i] = !all_null && i % 3 != 0;
        }
        ScalarIndexSort<int64_t> built(fixture.ctx);
        built.Build(rows.size(), rows.data(), valid.get());
        CheckMembership(built, rows, valid.get());
        const auto stats = built.UploadUnified({});
        for (bool mmap : {false, true}) {
            SCOPED_TRACE("mmap=" + std::to_string(mmap));
            Config config;
            config[ENABLE_MMAP] = mmap;
            config[milvus::LOAD_PRIORITY] = proto::common::LoadPriority::HIGH;
            config[INDEX_FILES] = stats->GetIndexFiles();
            for (bool async : {false, true}) {
                SCOPED_TRACE("async=" + std::to_string(async));
                auto ctx = fixture.ctx;
                ctx.use_async_load = async;
                ScalarIndexSort<int64_t> loaded(ctx);
                loaded.LoadUnified(config);
                CheckMembership(loaded, rows, valid.get());
            }
            ScalarIndexSort<int64_t> legacy(fixture.ctx);
            legacy.Load(built.Serialize({}), config);
            CheckMembership(legacy, rows, valid.get());
        }
    }
}

namespace {

template <typename T>
proto::schema::DataType
MembershipDataType() {
    if constexpr (std::is_same_v<T, bool>)
        return proto::schema::Bool;
    if constexpr (std::is_same_v<T, int8_t>)
        return proto::schema::Int8;
    if constexpr (std::is_same_v<T, int16_t>)
        return proto::schema::Int16;
    if constexpr (std::is_same_v<T, int32_t>)
        return proto::schema::Int32;
    if constexpr (std::is_same_v<T, int64_t>)
        return proto::schema::Int64;
    if constexpr (std::is_same_v<T, float>)
        return proto::schema::Float;
    return proto::schema::Double;
}

template <typename T>
std::vector<T>
TypedMembershipRows(size_t n) {
    std::vector<T> rows(n);
    for (size_t i = 0; i < n; ++i) {
        if constexpr (std::is_same_v<T, bool>) {
            rows[i] = i % 2 != 0;
        } else {
            rows[i] = static_cast<T>(2 * static_cast<int>(i % 31) - 30);
        }
    }
    if (n >= 8) {
        rows[0] = std::numeric_limits<T>::lowest();
        rows[1] = std::numeric_limits<T>::max();
        if constexpr (std::is_floating_point_v<T>) {
            rows[2] = -std::numeric_limits<T>::infinity();
            rows[3] = std::numeric_limits<T>::infinity();
            rows[4] = T(-0.0);
            rows[5] = T(0.0);
            rows[6] = std::numeric_limits<T>::denorm_min();
            rows[7] = -std::numeric_limits<T>::denorm_min();
            if (n >= 10) {
                rows[8] = T(1.25);
                rows[9] = std::nextafter(T(1.25), T(2));
            }
        }
    }
    return rows;
}

// A real bool[] also exercises ScalarIndexSort<bool>'s pointer API, without
// relying on the packed vector<bool> specialization. Used for every type.
template <typename T>
std::unique_ptr<T[]>
MembershipBuffer(const std::vector<T>& values) {
    auto buffer = std::make_unique<T[]>(values.size());
    std::copy(values.begin(), values.end(), buffer.get());
    return buffer;
}

template <typename T>
void
CheckTypedMembership(ScalarIndexSort<T>& index,
                     const std::vector<T>& rows,
                     const bool* valid) {
    std::vector<std::vector<T>> queries{{},
                                        {T(0)},
                                        {T(1)},
                                        {T(1), T(0), T(1)},
                                        rows,
                                        std::vector<T>(129, T(0)),
                                        std::vector<T>(4096, T(1))};
    auto reverse = rows;
    std::reverse(reverse.begin(), reverse.end());
    queries.push_back(reverse);
    if constexpr (!std::is_same_v<T, bool>) {
        queries.push_back({std::numeric_limits<T>::lowest(),
                           std::numeric_limits<T>::max(),
                           T(-31),
                           T(31)});
    }
    if constexpr (std::is_floating_point_v<T>) {
        queries.push_back({T(-0.0), T(0.0), T(-0.0)});
        queries.push_back({-std::numeric_limits<T>::infinity(),
                           std::numeric_limits<T>::infinity()});
        queries.push_back({std::numeric_limits<T>::denorm_min(),
                           -std::numeric_limits<T>::denorm_min()});
        queries.push_back({T(1.25),
                           std::nextafter(T(1.25), T(2)),
                           std::nextafter(T(1.25), T(0))});
    }
    ASSERT_EQ(index.Count(), rows.size());
    for (const auto& query : queries) {
        SCOPED_TRACE("query size=" + std::to_string(query.size()));
        auto input = MembershipBuffer(query);
        auto original = MembershipBuffer(query);
        auto* values = query.empty() ? nullptr : input.get();
        const auto in = index.In(query.size(), values);
        const auto not_in = index.NotIn(query.size(), values);
        ASSERT_EQ(in.size(), rows.size());
        ASSERT_EQ(not_in.size(), rows.size());
        for (size_t row = 0; row < rows.size(); ++row) {
            const bool hit =
                std::find(query.begin(), query.end(), rows[row]) != query.end();
            const bool is_valid = !valid || valid[row];
            ASSERT_EQ(in[row], is_valid && hit) << "row=" << row;
            ASSERT_EQ(not_in[row], is_valid && !hit) << "row=" << row;
        }
        if (!query.empty()) {
            EXPECT_EQ(
                std::memcmp(
                    input.get(), original.get(), query.size() * sizeof(T)),
                0);
        }
    }
}

template <typename T>
class ScalarIndexSortTypedMembershipTest : public testing::Test {};
using MembershipTypes =
    testing::Types<int8_t, int16_t, int32_t, int64_t, bool, float, double>;
TYPED_TEST_SUITE(ScalarIndexSortTypedMembershipTest, MembershipTypes);

TYPED_TEST(ScalarIndexSortTypedMembershipTest, ScanOracleAndInputImmutability) {
    for (size_t n : {1, 2, 63, 64, 65, 127, 128, 129, 257}) {
        const auto rows = TypedMembershipRows<TypeParam>(n);
        const auto data = MembershipBuffer(rows);
        for (int null_mode : {0, 1, 2}) {
            SCOPED_TRACE("rows=" + std::to_string(n) +
                         " null_mode=" + std::to_string(null_mode));
            auto valid = std::make_unique<bool[]>(n);
            for (size_t i = 0; i < n; ++i) {
                valid[i] = null_mode == 0 || (null_mode == 1 && i % 3 != 0);
            }
            const bool* validity = null_mode == 0 ? nullptr : valid.get();
            ScalarIndexSort<TypeParam> index;
            index.Build(n, data.get(), validity);
            CheckTypedMembership(index, rows, validity);
        }
    }
}

TYPED_TEST(ScalarIndexSortTypedMembershipTest, LegacyAndPackedReloads) {
    milvus::test::ScopedLoadTransientBudget budget_guard(0);
    ScalarSortAsyncLoadFixture fixture("scalar_sort_typed_membership_reload",
                                       MembershipDataType<TypeParam>());
    const auto rows = TypedMembershipRows<TypeParam>(65);
    const auto data = MembershipBuffer(rows);
    auto valid = std::make_unique<bool[]>(rows.size());
    for (bool all_null : {false, true}) {
        SCOPED_TRACE("all_null=" + std::to_string(all_null));
        for (size_t i = 0; i < rows.size(); ++i) {
            valid[i] = !all_null && i % 3 != 1;
        }
        ScalarIndexSort<TypeParam> built(fixture.ctx);
        built.Build(rows.size(), data.get(), valid.get());
        const auto stats = built.UploadUnified({});
        for (bool mmap : {false, true}) {
            SCOPED_TRACE("mmap=" + std::to_string(mmap));
            Config config;
            config[ENABLE_MMAP] = mmap;
            config[milvus::LOAD_PRIORITY] = proto::common::LoadPriority::HIGH;
            config[INDEX_FILES] = stats->GetIndexFiles();
            for (bool async : {false, true}) {
                SCOPED_TRACE("async=" + std::to_string(async));
                auto ctx = fixture.ctx;
                ctx.use_async_load = async;
                ScalarIndexSort<TypeParam> loaded(ctx);
                loaded.LoadUnified(config);
                CheckTypedMembership(loaded, rows, valid.get());
            }
            ScalarIndexSort<TypeParam> legacy(fixture.ctx);
            legacy.Load(built.Serialize({}), config);
            CheckTypedMembership(legacy, rows, valid.get());
        }
    }
}

}  // namespace

void
test_stlsort_for_range(
    const std::vector<int64_t>& data,
    DataType data_type,
    bool enable_mmap,
    std::function<TargetBitmap(
        const std::shared_ptr<ScalarIndexSort<int64_t>>&)> exec_expr,
    const std::vector<bool>& expected_result) {
    size_t nb = data.size();
    std::vector<std::string> index_files;
    {
        Config config;

        auto index = std::make_shared<index::ScalarIndexSort<int64_t>>(
            CreateScalarSortTestFileManagerContext());
        index->Build(nb, data.data());

        auto create_index_result = index->UploadUnified({});
        index_files = create_index_result->GetIndexFiles();
    }
    {
        Config config;
        config[milvus::index::ENABLE_MMAP] = enable_mmap;
        config[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;
        config["index_files"] = index_files;

        auto index = std::make_shared<index::ScalarIndexSort<int64_t>>(
            CreateScalarSortTestFileManagerContext());
        index->LoadUnified(config);

        auto cnt = index->Count();
        ASSERT_EQ(cnt, nb);
        auto bitset = exec_expr(index);
        for (size_t i = 0; i < nb; i++) {
            ASSERT_EQ(bitset[i], expected_result[i]);
        }
    }
}
TEST(StlSortIndexTest, TestRange) {
    std::vector<int64_t> data = {10, 2, 6, 5, 9, 3, 7, 8, 4, 1};
    {
        std::vector<bool> expected_result = {
            false, false, true, true, false, true, true, false, true, false};
        auto exec_expr =
            [](const std::shared_ptr<ScalarIndexSort<int64_t>>& index) {
                return index->Range(3, true, 7, true);
            };

        test_stlsort_for_range(
            data, DataType::INT64, false, exec_expr, expected_result);

        test_stlsort_for_range(
            data, DataType::INT64, true, exec_expr, expected_result);
    }

    {
        std::vector<bool> expected_result(data.size(), false);
        auto exec_expr =
            [](const std::shared_ptr<ScalarIndexSort<int64_t>>& index) {
                return index->Range(10, false, 70, true);
            };

        test_stlsort_for_range(
            data, DataType::INT64, false, exec_expr, expected_result);

        test_stlsort_for_range(
            data, DataType::INT64, true, exec_expr, expected_result);
    }
}

TEST(StlSortIndexTest, TestIn) {
    std::vector<int64_t> data = {10, 2, 6, 5, 9, 3, 7, 8, 4, 1};
    std::vector<bool> expected_result = {
        false, false, false, true, false, true, true, false, false, false};

    std::vector<int64_t> values = {3, 5, 7};

    auto exec_expr =
        [&values](const std::shared_ptr<ScalarIndexSort<int64_t>>& index) {
            return index->In(values.size(), values.data());
        };
    test_stlsort_for_range(
        data, DataType::INT64, false, exec_expr, expected_result);

    test_stlsort_for_range(
        data, DataType::INT64, true, exec_expr, expected_result);
}

TEST(StlSortIndexTest, MmapByteSizeCountsValidBitsetOnce) {
    constexpr size_t kAlignment = 32;
    constexpr uint64_t kMmapIndexPadding = 1;
    const std::vector<int64_t> data = {
        10, 2, 6, 5, 9, 3, 7, 8, 4, 1, 11, 12, 13};

    std::vector<std::string> index_files;
    {
        auto index = std::make_shared<index::ScalarIndexSort<int64_t>>(
            CreateScalarSortTestFileManagerContext());
        index->Build(data.size(), data.data());

        auto create_index_result = index->UploadUnified({});
        index_files = create_index_result->GetIndexFiles();
    }

    auto index = std::make_shared<index::ScalarIndexSort<int64_t>>(
        CreateScalarSortTestFileManagerContext());
    Config config;
    config[milvus::index::ENABLE_MMAP] = true;
    config[milvus::LOAD_PRIORITY] = milvus::proto::common::LoadPriority::HIGH;
    config["index_files"] = index_files;
    index->LoadUnified(config);

    auto index_data_bytes = data.size() * sizeof(IndexStructure<int64_t>);
    auto aligned_data_bytes =
        ((index_data_bytes + kAlignment - 1) / kAlignment) * kAlignment;
    TargetBitmap valid_bitset(data.size(), true);
    auto expected_byte_size = aligned_data_bytes + kMmapIndexPadding +
                              data.size() * sizeof(int32_t) +
                              valid_bitset.size_in_bytes();

    ASSERT_EQ(index->ByteSize(), static_cast<int64_t>(expected_byte_size));
}

// V2 compat test removed: kScalarIndexUseV3 flag deleted,
// Upload()/Load() now always route to V3 paths.

namespace {

FieldDataPtr
OrdinaryNumericArrayData() {
    std::vector<Array> arrays;
    for (const auto& values : std::vector<std::vector<int64_t>>{
             {1, 1, 100}, {}, {}, {2, 3}, {1, 4}}) {
        ScalarFieldProto proto;
        for (auto value : values) {
            proto.mutable_long_data()->add_data(value);
        }
        // Preserve the element type for an empty array.
        proto.mutable_long_data();
        arrays.emplace_back(proto);
    }
    auto data =
        storage::CreateFieldData(DataType::ARRAY, DataType::INT64, true);
    const uint8_t valid =
        0x1b;  // row 2 is null; row 1 is an empty valid array.
    data->FillFieldData(arrays.data(), &valid, arrays.size(), 0);
    return data;
}

void
CheckOrdinaryNumericArray(ScalarIndexSort<int64_t>& index) {
    ASSERT_EQ(index.Count(), 5);
    EXPECT_FALSE(index.HasRawData());
    EXPECT_FALSE(index.IsNestedIndex());
    EXPECT_EQ(index.Reverse_Lookup(0), std::nullopt);
    auto valid = index.IsNotNull();
    auto nulls = index.IsNull();
    for (size_t row = 0; row < 5; ++row) {
        EXPECT_EQ(valid[row], row != 2);
        EXPECT_EQ(nulls[row], row == 2);
    }
    const int64_t one = 1, four = 4;
    auto any = index.In(1, &one);
    auto all = any.clone();
    all &= index.In(1, &four);
    EXPECT_TRUE(any[0]);
    EXPECT_TRUE(any[4]);
    EXPECT_EQ(any.count(), 2);
    EXPECT_EQ(all.count(), 1);
    EXPECT_TRUE(all[4]);
    // A matching element must survive another element outside the range.
    auto range = index.Range(int64_t(1), true, int64_t(4), true);
    EXPECT_TRUE(range[0]);
    EXPECT_TRUE(range[3]);
    EXPECT_TRUE(range[4]);
    EXPECT_EQ(range.count(), 3);
    auto upper = index.Range(int64_t(4), OpType::LessEqual);
    EXPECT_EQ(upper.count(), 3);
    auto not_in = index.NotIn(1, &one);
    EXPECT_TRUE(not_in[1]);
    EXPECT_FALSE(not_in[2]);
    EXPECT_TRUE(not_in[3]);
}

}  // namespace

TEST(ScalarIndexSortArrayTest, RowPostingsLegacyAndV3Reload) {
    milvus::test::ScopedLoadTransientBudget budget_guard(0);
    ScalarSortAsyncLoadFixture fixture("scalar_sort_ordinary_array",
                                       proto::schema::DataType::Array);
    fixture.field_meta.field_schema.set_element_type(
        proto::schema::DataType::Int64);
    fixture.ctx.fieldDataMeta = fixture.field_meta;
    ExposedScalarIndexSort index(fixture.ctx);
    index.BuildWithFieldData({OrdinaryNumericArrayData()});
    CheckOrdinaryNumericArray(index);
    auto binary = index.Serialize({});
    auto stats = index.UploadUnified({});
    for (bool mmap : {false, true}) {
        Config config;
        config[milvus::index::ENABLE_MMAP] = mmap;
        ExposedScalarIndexSort legacy(fixture.ctx);
        legacy.Load(binary, config);
        CheckOrdinaryNumericArray(legacy);
        milvus::test::ControlledDirectReadFile* remote_file = nullptr;
        auto reader = milvus::test::OpenDirectIndexEntryReader(
            milvus::test::ReadPackedIndexBytes(fixture.ctx,
                                               stats->GetIndexFiles()),
            &remote_file);
        ExposedScalarIndexSort packed(fixture.ctx);
        packed.LoadDirectForTest(
            *reader, config, proto::common::LoadPriority::HIGH);
        CheckOrdinaryNumericArray(packed);
    }
}

TEST(ScalarIndexSortArrayTest, NaNMatchesAndEmptyRowsRemainValid) {
    ScalarSortAsyncLoadFixture fixture("scalar_sort_float_array",
                                       proto::schema::DataType::Array);
    ScalarFieldProto values;
    values.mutable_double_data()->add_data(
        std::numeric_limits<double>::quiet_NaN());
    values.mutable_double_data()->add_data(3);
    ScalarFieldProto empty;
    empty.mutable_double_data();
    std::vector<Array> arrays{Array(values), Array(empty)};
    auto data = storage::CreateFieldData(DataType::ARRAY, DataType::DOUBLE);
    data->FillFieldData(arrays.data(), arrays.size());
    ScalarIndexSort<double> index(fixture.ctx);
    index.BuildWithFieldData({data});
    double three = 3;
    EXPECT_EQ(index.In(1, &three).count(), 1);
    EXPECT_EQ(index.Range(three, true, three, true).count(), 1);
    const double nan = std::numeric_limits<double>::quiet_NaN();
    EXPECT_EQ(index.In(1, &nan).count(), 1);
    EXPECT_EQ(index.Range(three, OpType::GreaterThan).count(), 1);
    EXPECT_EQ(index.IsNotNull().count(), 2);
}

TEST(ScalarIndexSortArrayTest, EmptyArraysHaveNoPostingsAcrossReloads) {
    milvus::test::ScopedLoadTransientBudget budget_guard(0);
    ScalarSortAsyncLoadFixture fixture("scalar_sort_empty_array",
                                       proto::schema::DataType::Array);
    ScalarFieldProto empty;
    empty.mutable_long_data();
    std::vector<Array> arrays{Array(empty), Array(empty)};
    auto data =
        storage::CreateFieldData(DataType::ARRAY, DataType::INT64, true);
    const uint8_t validity = 1;
    data->FillFieldData(arrays.data(), &validity, arrays.size(), 0);
    ExposedScalarIndexSort built(fixture.ctx);
    built.BuildWithFieldData({data});
    auto check = [](ScalarIndexSort<int64_t>& index) {
        ASSERT_EQ(index.Count(), 2);
        EXPECT_EQ(index.Size(), 0);
        auto valid = index.IsNotNull();
        EXPECT_TRUE(valid[0]);
        EXPECT_FALSE(valid[1]);
        const int64_t query = 1;
        EXPECT_EQ(index.In(1, &query).count(), 0);
        EXPECT_EQ(index.NotIn(1, &query).count(), 1);
        EXPECT_EQ(index.Range(query, OpType::LessEqual).count(), 0);
    };
    check(built);
    auto binary = built.Serialize({});
    auto stats = built.UploadUnified({});
    for (bool mmap : {false, true}) {
        Config config;
        config[milvus::index::ENABLE_MMAP] = mmap;
        ExposedScalarIndexSort legacy(fixture.ctx);
        legacy.Load(binary, config);
        check(legacy);
        milvus::test::ControlledDirectReadFile* remote_file = nullptr;
        auto reader = milvus::test::OpenDirectIndexEntryReader(
            milvus::test::ReadPackedIndexBytes(fixture.ctx,
                                               stats->GetIndexFiles()),
            &remote_file);
        ExposedScalarIndexSort packed(fixture.ctx);
        packed.LoadDirectForTest(
            *reader, config, proto::common::LoadPriority::HIGH);
        check(packed);
    }
}

TYPED_TEST(ScalarIndexSortNaNTest, OrdinaryArrayNaNRowsRetainParentValidity) {
    using T = TypeParam;
    ScalarSortAsyncLoadFixture fixture("ordinary_array_nan_parent_rows",
                                       proto::schema::DataType::Array);
    const T nan = std::numeric_limits<T>::quiet_NaN();
    ScalarIndexSort<T> built(fixture.ctx);
    built.BuildWithFieldData({NaNArrayFieldData<T>(
        {{nan, T(1), T(100)}, {nan}, {}, {}, {T(2), T(3)}}, 0x17)});
    auto check = [&](ScalarIndexSort<T>& index) {
        EXPECT_EQ(index.Count(), 5);
        EXPECT_FALSE(index.HasRawData());
        auto valid = index.IsNotNull();
        EXPECT_TRUE(valid[0]);
        EXPECT_TRUE(valid[1]);
        EXPECT_TRUE(valid[2]);
        EXPECT_FALSE(valid[3]);
        EXPECT_TRUE(valid[4]);
        EXPECT_EQ(index.In(1, &nan).count(), 2);
        EXPECT_EQ(index.Range(T(100), OpType::GreaterThan).count(), 2);
        const T one = T(1);
        auto matches = index.In(1, &one);
        EXPECT_TRUE(matches[0]);
        EXPECT_EQ(matches.count(), 1);
        EXPECT_EQ(index.NotIn(1, &one).count(), 3);
        auto range = index.Range(T(1), true, T(3), true);
        EXPECT_TRUE(range[0]);
        EXPECT_TRUE(range[4]);
        EXPECT_EQ(range.count(), 2);
        EXPECT_EQ(index.Reverse_Lookup(0), std::nullopt);
    };
    check(built);
    auto binary = built.Serialize({});
    auto stats = built.UploadUnified({});
    // Ordinary arrays retain raw data and need no scalar NaN reverse lookup.
    EXPECT_FALSE(binary.Contains("nan_rows"));
    for (bool mmap : {false, true}) {
        Config config;
        config[milvus::index::ENABLE_MMAP] = mmap;
        ScalarIndexSort<T> reloaded(fixture.ctx);
        reloaded.Load(binary, config);
        check(reloaded);
        milvus::test::ControlledDirectReadFile* remote_file = nullptr;
        auto reader = milvus::test::OpenDirectIndexEntryReader(
            milvus::test::ReadPackedIndexBytes(fixture.ctx,
                                               stats->GetIndexFiles()),
            &remote_file);
        ScalarIndexSort<T> packed(fixture.ctx);
        auto plan =
            packed.PlanLoad(reader->Directory(), reader->IndexMeta(), config);
        folly::coro::blockingWait(reader->ReadEntriesAsync(
            plan.entries, proto::common::LoadPriority::HIGH));
        folly::coro::blockingWait(packed.FinishLoadAsync(plan, config));
        plan.Commit();
        check(packed);
    }
}

TYPED_TEST(ScalarIndexSortNaNTest, NestedIgnoresInvalidNaNPayload) {
    using T = TypeParam;
    ScalarSortAsyncLoadFixture fixture("nested_invalid_nan_payload");
    const T nan = std::numeric_limits<T>::quiet_NaN();
    ScalarFieldProto members;
    for (T value : {nan, nan, T(3), T(9)}) {
        if constexpr (std::is_same_v<T, float>) {
            members.mutable_float_data()->add_data(value);
        } else {
            members.mutable_double_data()->add_data(value);
        }
    }
    for (bool valid : {false, true, true, true}) {
        members.add_valid_data(valid);
    }
    ScalarFieldProto empty;
    std::vector<Array> arrays{Array(members, true), Array(empty), Array(empty)};
    auto field = std::make_shared<FieldData<Array>>(DataType::ARRAY, true);
    const uint8_t parent_validity = 0x03;  // empty parent then null parent
    field->FillFieldData(arrays.data(), &parent_validity, arrays.size(), 0);
    ScalarIndexSort<T> built(fixture.ctx, true);
    built.BuildWithFieldData({field});
    auto check = [&](ScalarIndexSort<T>& index) {
        ASSERT_EQ(index.Count(), 4);
        ASSERT_EQ(index.Size(), 3);
        auto valid = index.IsNotNull();
        EXPECT_FALSE(valid[0]);
        EXPECT_TRUE(valid[1]);
        EXPECT_TRUE(valid[2]);
        EXPECT_TRUE(valid[3]);
        EXPECT_EQ(index.Reverse_Lookup(0), std::nullopt);
        auto lookup = index.Reverse_Lookup(1);
        ASSERT_TRUE(lookup.has_value());
        EXPECT_TRUE(std::isnan(*lookup));
        EXPECT_EQ(index.Reverse_Lookup(2), T(3));
        EXPECT_EQ(index.Reverse_Lookup(3), T(9));
        const T three = T(3), nine = T(9);
        auto matches = index.In(1, &three);
        EXPECT_EQ(matches.count(), 1);
        EXPECT_TRUE(matches[2]);
        EXPECT_EQ(index.In(1, &nine).count(), 1);
        auto misses = index.NotIn(1, &three);
        EXPECT_EQ(misses.count(), 2);
        EXPECT_TRUE(misses[1]);
        EXPECT_TRUE(misses[3]);
        auto range = index.Range(three, OpType::LessEqual);
        EXPECT_EQ(range.count(), 1);
        EXPECT_TRUE(range[2]);
    };
    check(built);
    auto binary = built.Serialize({});
    auto stats = built.UploadUnified({});
    for (bool mmap : {false, true}) {
        Config config;
        config[milvus::index::ENABLE_MMAP] = mmap;
        ScalarIndexSort<T> legacy(fixture.ctx, true);
        legacy.Load(binary, config);
        check(legacy);
        milvus::test::ControlledDirectReadFile* remote_file = nullptr;
        auto reader = milvus::test::OpenDirectIndexEntryReader(
            milvus::test::ReadPackedIndexBytes(fixture.ctx,
                                               stats->GetIndexFiles()),
            &remote_file);
        ScalarIndexSort<T> packed(fixture.ctx, true);
        auto plan =
            packed.PlanLoad(reader->Directory(), reader->IndexMeta(), config);
        folly::coro::blockingWait(reader->ReadEntriesAsync(
            plan.entries, proto::common::LoadPriority::HIGH));
        folly::coro::blockingWait(packed.FinishLoadAsync(plan, config));
        plan.Commit();
        check(packed);
    }
}

TYPED_TEST(ScalarIndexSortNaNTest,
           UnsupportedVersionRejectsValidNaNInEveryBuildRoute) {
    using T = TypeParam;
    const T nan = std::numeric_limits<T>::quiet_NaN();
    const std::vector<T> rows{nan, T(3)};
    auto expect_unsupported = [](auto build) {
        try {
            build();
            FAIL() << "An older version must never store NaN in sorted entries";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), ErrorCode::Unsupported);
        }
    };
    ScalarIndexSort<T> raw;
    raw.SetSupportsNaNTotalOrder(false);
    expect_unsupported([&] { raw.Build(rows.size(), rows.data()); });
    ScalarIndexSort<T> scalar;
    scalar.SetSupportsNaNTotalOrder(false);
    expect_unsupported(
        [&] { scalar.BuildWithFieldData({NaNScalarFieldData(rows, 0x03)}); });
    ScalarIndexSort<T> nested({}, true);
    nested.SetSupportsNaNTotalOrder(false);
    expect_unsupported([&] {
        nested.BuildWithFieldData({NaNArrayFieldData<T>({rows}, 0x01)});
    });
    ScalarSortAsyncLoadFixture fixture("ordinary_array_reject_unsupported_nan",
                                       proto::schema::DataType::Array);
    ScalarIndexSort<T> ordinary(fixture.ctx);
    ordinary.SetSupportsNaNTotalOrder(false);
    expect_unsupported([&] {
        ordinary.BuildWithFieldData({NaNArrayFieldData<T>({rows}, 0x01)});
    });
}

TYPED_TEST(ScalarIndexSortNaNTest, UnsupportedVersionIgnoresNullPayloadNaN) {
    using T = TypeParam;
    const T nan = std::numeric_limits<T>::quiet_NaN();
    const std::vector<T> rows{nan, T(3)};
    const bool validity[] = {false, true};
    auto check = [](ScalarIndexSort<T>& index) {
        EXPECT_EQ(index.Size(), 1);
        EXPECT_FALSE(std::isnan(index[0].a_));
        EXPECT_FALSE(index.Serialize({}).Contains("nan_rows"));
    };
    ScalarIndexSort<T> raw;
    raw.SetSupportsNaNTotalOrder(false);
    ASSERT_NO_THROW(raw.Build(rows.size(), rows.data(), validity));
    check(raw);
    ScalarIndexSort<T> scalar;
    scalar.SetSupportsNaNTotalOrder(false);
    ASSERT_NO_THROW(
        scalar.BuildWithFieldData({NaNScalarFieldData(rows, 0x02)}));
    check(scalar);
    ScalarFieldProto values;
    if constexpr (std::is_same_v<T, float>) {
        values.mutable_float_data()->add_data(nan);
        values.mutable_float_data()->add_data(T(3));
    } else {
        values.mutable_double_data()->add_data(nan);
        values.mutable_double_data()->add_data(T(3));
    }
    values.add_valid_data(false);
    values.add_valid_data(true);
    std::vector<Array> arrays{Array(values, true)};
    auto field = std::make_shared<FieldData<Array>>(DataType::ARRAY, false);
    field->FillFieldData(arrays.data(), arrays.size());
    ScalarIndexSort<T> nested({}, true);
    nested.SetSupportsNaNTotalOrder(false);
    ASSERT_NO_THROW(nested.BuildWithFieldData({field}));
    check(nested);
    EXPECT_EQ(nested.Count(), 2);
    EXPECT_FALSE(nested.IsNotNull()[0]);
    EXPECT_TRUE(nested.IsNotNull()[1]);
    ScalarSortAsyncLoadFixture fixture("ordinary_array_null_payload_nan",
                                       proto::schema::DataType::Array);
    ScalarIndexSort<T> ordinary(fixture.ctx);
    ordinary.SetSupportsNaNTotalOrder(false);
    ASSERT_NO_THROW(ordinary.BuildWithFieldData({field}));
    check(ordinary);
    EXPECT_EQ(ordinary.Count(), 1);
    EXPECT_TRUE(ordinary.IsNotNull()[0]);
}
