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
#include "test_utils/Constants.h"
#include "test_utils/TmpPath.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::index;

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

        auto create_index_result = index->UploadV3({});
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
        index->LoadV3(config);

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

// V2 compat test removed: kScalarIndexUseV3 flag deleted,
// Upload()/Load() now always route to V3 paths.
