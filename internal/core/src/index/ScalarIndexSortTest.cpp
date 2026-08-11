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
#include "common/Array.h"
#include "common/Slice.h"
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
#include "test_utils/Constants.h"
#include "test_utils/TmpPath.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::index;

namespace {

ScalarFieldProto
BuildNullableIntArrayValue(const std::vector<int32_t>& values,
                           const std::vector<bool>& valid_data) {
    ScalarFieldProto proto;
    proto.mutable_int_data()->mutable_data()->Add(values.begin(), values.end());
    for (auto valid : valid_data) {
        proto.add_valid_data(valid);
    }
    return proto;
}

FieldDataPtr
BuildElementNullableIntArrayFieldData() {
    std::vector<Array> rows = {
        Array(BuildNullableIntArrayValue({0, 2}, {false, true}), true),
        Array(BuildNullableIntArrayValue({1, 2}, {true, true}), true),
        Array(BuildNullableIntArrayValue({0, 3}, {true, true}), true),
        Array(BuildNullableIntArrayValue({}, {}), true),
        Array(BuildNullableIntArrayValue({4, 0}, {true, false}), true),
    };
    auto field_data = storage::CreateFieldData(
        DataType::ARRAY, DataType::INT32, true, true, 1, rows.size());
    uint8_t row_valid_data = 0x1D;  // rows 0,2,3,4 valid; row 1 null.
    field_data->FillFieldData(
        rows.data(), &row_valid_data, rows.size(), 0);
    return field_data;
}

storage::FileManagerContext
CreateElementNullableScalarSortContext() {
    storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = TestLocalPath;
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);

    storage::FieldDataMeta field_meta{1, 2, 3, 102};
    field_meta.field_schema.set_data_type(proto::schema::DataType::Array);
    field_meta.field_schema.set_element_type(proto::schema::DataType::Int32);
    field_meta.field_schema.set_nullable(true);
    field_meta.field_schema.set_element_nullable(true);
    storage::IndexMeta index_meta{3, 102, 1001, 10001};
    return storage::FileManagerContext(
        field_meta, index_meta, chunk_manager, fs);
}

void
AssertElementNullableScalarSort(ScalarIndex<int32_t>& index) {
    auto assert_bitmap = [](const TargetBitmap& actual,
                            const std::vector<bool>& expected) {
        ASSERT_EQ(actual.size(), expected.size());
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(actual[i], expected[i]) << "offset " << i;
        }
    };

    ASSERT_TRUE(index.HasRowLevelValidity());
    int32_t zero = 0;
    assert_bitmap(index.In(1, &zero),
                  {false, false, true, false, false, false});
    assert_bitmap(index.NotIn(1, &zero),
                  {false, true, false, true, true, false});
    assert_bitmap(index.IsNull(), {false, true, false, false, false});
    assert_bitmap(index.IsNotNull(), {true, false, true, true, true});
    assert_bitmap(index.IsElementNull(),
                  {true, false, false, false, false, true});
    assert_bitmap(index.IsElementNotNull(),
                  {false, true, true, true, true, false});
    assert_bitmap(index.Range(1, OpType::GreaterThan),
                  {false, true, false, true, true, false});
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

TEST(StlSortIndexTest, NestedElementNullPersistsAcrossIndexFormats) {
    auto field_data = BuildElementNullableIntArrayFieldData();
    auto ctx = CreateElementNullableScalarSortContext();

    ScalarIndexSort<int32_t> index(ctx, true);
    index.BuildWithFieldData({field_data});
    ASSERT_EQ(index.Count(), 6);
    AssertElementNullableScalarSort(index);

    auto binary_set = index.Serialize({});
    ScalarIndexSort<int32_t> binary_loaded(ctx, true);
    binary_loaded.Load(binary_set, {});
    ASSERT_EQ(binary_loaded.Count(), 6);
    AssertElementNullableScalarSort(binary_loaded);

    auto legacy_binary_set = index.Serialize({});
    milvus::Assemble(legacy_binary_set);
    ASSERT_NE(legacy_binary_set.Erase("row_valid_bitset"), nullptr);
    ASSERT_NE(legacy_binary_set.Erase("index_row_count"), nullptr);
    Config legacy_config;
    legacy_config[INDEX_NUM_ROWS_KEY] = 5;
    ScalarIndexSort<int32_t> legacy_loaded(ctx, true);
    legacy_loaded.Load(legacy_binary_set, legacy_config);
    ASSERT_FALSE(legacy_loaded.HasRowLevelValidity());
    EXPECT_EQ(legacy_loaded.IsNull().size(), 5);
    EXPECT_EQ(legacy_loaded.IsNotNull().size(), 5);

    auto upload_result = index.UploadUnified({});
    Config config;
    config["index_files"] = upload_result->GetIndexFiles();
    config[milvus::LOAD_PRIORITY] =
        proto::common::LoadPriority::HIGH;
    ScalarIndexSort<int32_t> v3_loaded(ctx, true);
    v3_loaded.LoadUnified(config);
    ASSERT_EQ(v3_loaded.Count(), 6);
    AssertElementNullableScalarSort(v3_loaded);
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
