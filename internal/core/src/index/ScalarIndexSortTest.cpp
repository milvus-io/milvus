#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "bitset/bitset.h"
#include "common/Tracer.h"
#include "common/TracerBase.h"
#include "common/Types.h"
#include "gtest/gtest.h"
#include "index/Meta.h"
#include "index/ScalarIndexSort.h"
#include "milvus-storage/filesystem/fs.h"
#include "pb/common.pb.h"
#include "storage/ChunkManager.h"
#include "storage/FileManager.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "segcore/async_load/AsyncLoadScheduler.h"
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
    LoadEntriesWithAsyncReadForTest(storage::IndexEntryReader& reader,
                                    const Config& config,
                                    ScalarIndexV3AsyncLoadContext& async_ctx) {
        LoadEntriesWithAsyncRead(reader, config, async_ctx);
    }
};

struct ScalarSortAsyncLoadFixture {
    explicit ScalarSortAsyncLoadFixture(std::string test_name)
        : root_path(TestLocalPath + "/" + std::move(test_name)) {
        boost::filesystem::remove_all(root_path);
        storage::StorageConfig storage_config;
        storage_config.storage_type = "local";
        storage_config.root_path = root_path;
        chunk_manager = storage::CreateChunkManager(storage_config);
        fs = storage::InitArrowFileSystem(storage_config);

        field_schema.set_data_type(proto::schema::DataType::Int64);
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

TEST(ScalarIndexSortV3AsyncLoadTest, MemoryPathUsesAsyncEntryReads) {
    ScalarSortAsyncLoadFixture fixture("scalar_sort_async_memory");
    std::vector<int64_t> data{50, 10, 30, 20, 40};

    ExposedScalarIndexSort build_index(fixture.ctx);
    build_index.Build(data.size(), data.data());
    auto stats = build_index.UploadUnified({});

    milvus::test::AsyncTrackingRandomAccessFile* remote_file = nullptr;
    auto reader = milvus::test::OpenAsyncIndexEntryReader(
        milvus::test::ReadPackedIndexBytes(fixture.ctx, stats->GetIndexFiles()),
        &remote_file);
    auto read_at_calls_after_open = remote_file->ReadAtCalls();

    ExposedScalarIndexSort load_index(fixture.ctx);
    Config config;
    config[milvus::index::ENABLE_MMAP] = false;
    config[milvus::LOAD_PRIORITY] = milvus::proto::common::LoadPriority::HIGH;
    milvus::segcore::async_load::LoadAdmissionScheduler scheduler(
        {/*total_bytes=*/0, /*high_reserved_bytes=*/0});
    milvus::index::ScalarIndexV3AsyncLoadContext async_ctx{
        nullptr,
        milvus::proto::common::LoadPriority::HIGH,
        scheduler,
        "scalar_sort_async_memory"};

    load_index.LoadEntriesWithAsyncReadForTest(*reader, config, async_ctx);

    EXPECT_GE(remote_file->AsyncReadCalls(), 3);
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

TEST(ScalarIndexSortV3AsyncLoadTest, MmapPathUsesAsyncEntryReads) {
    ScalarSortAsyncLoadFixture fixture("scalar_sort_async_mmap");
    std::vector<int64_t> data{5, 4, 3, 2, 1, 0};

    ExposedScalarIndexSort build_index(fixture.ctx);
    build_index.Build(data.size(), data.data());
    auto stats = build_index.UploadUnified({});

    milvus::test::AsyncTrackingRandomAccessFile* remote_file = nullptr;
    auto reader = milvus::test::OpenAsyncIndexEntryReader(
        milvus::test::ReadPackedIndexBytes(fixture.ctx, stats->GetIndexFiles()),
        &remote_file);
    auto read_at_calls_after_open = remote_file->ReadAtCalls();

    ExposedScalarIndexSort load_index(fixture.ctx);
    Config config;
    config[milvus::index::ENABLE_MMAP] = true;
    config[milvus::LOAD_PRIORITY] = milvus::proto::common::LoadPriority::HIGH;
    milvus::segcore::async_load::LoadAdmissionScheduler scheduler(
        {/*total_bytes=*/0, /*high_reserved_bytes=*/0});
    milvus::index::ScalarIndexV3AsyncLoadContext async_ctx{
        nullptr,
        milvus::proto::common::LoadPriority::HIGH,
        scheduler,
        "scalar_sort_async_mmap"};

    load_index.LoadEntriesWithAsyncReadForTest(*reader, config, async_ctx);

    EXPECT_GE(remote_file->AsyncReadCalls(), 3);
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
