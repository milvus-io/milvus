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
#include "storage/Util.h"
#include "test_utils/TmpPath.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::index;

void
test_stlsort_for_range(
    const std::vector<int64_t>& data,
    DataType data_type,
    bool enable_mmap,
    std::function<TargetBitmap(
        const std::shared_ptr<ScalarIndexSort<int64_t>>&)> exec_expr,
    const std::vector<bool>& expected_result) {
    size_t nb = data.size();
    BinarySet binary_set;
    {
        Config config;

        auto index = std::make_shared<index::ScalarIndexSort<int64_t>>();
        index->Build(nb, data.data());

        binary_set = index->Serialize(config);
    }
    {
        Config config;
        config[milvus::index::ENABLE_MMAP] = enable_mmap;
        config[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;

        auto index = std::make_shared<index::ScalarIndexSort<int64_t>>();
        index->Load(binary_set, config);

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

// Verify that forcing kScalarIndexUseV3=false still produces valid V2
// index files (multiple files) and that the data round-trips correctly.
TEST(ScalarIndexV2Compat, ForceV2Upload) {
    // RAII guard to save and restore kScalarIndexUseV3
    const bool original_value = milvus::index::kScalarIndexUseV3;
    struct VersionGuard {
        bool saved;
        ~VersionGuard() {
            milvus::index::kScalarIndexUseV3 = saved;
        }
    } guard{original_value};

    // Force V2 mode
    milvus::index::kScalarIndexUseV3 = false;

    // Setup local storage and ArrowFileSystem via TmpPath
    milvus::test::TmpPath temp_path;
    auto storage_config = gen_local_storage_config(temp_path.get().string());
    auto chunk_manager = milvus::storage::CreateChunkManager(storage_config);
    auto fs = milvus::storage::InitArrowFileSystem(storage_config);

    // Prepare field and index metadata
    auto field_meta = milvus::segcore::gen_field_meta(
        /*collection_id=*/1,
        /*partition_id=*/1,
        /*segment_id=*/1,
        /*field_id=*/100,
        DataType::INT64);
    auto index_meta = gen_index_meta(
        /*segment_id=*/1,
        /*field_id=*/100,
        /*index_build_id=*/1,
        /*index_version=*/1);

    // Create FileManagerContext for building
    milvus::storage::FileManagerContext ctx_build(
        field_meta, index_meta, chunk_manager, fs);

    // Build index with test data
    const size_t nb = 100;
    std::vector<int64_t> data(nb);
    for (size_t i = 0; i < nb; ++i) {
        data[i] = static_cast<int64_t>(nb - i);  // reverse order: 100..1
    }

    auto index_build =
        std::make_shared<milvus::index::ScalarIndexSort<int64_t>>(ctx_build);
    index_build->Build(nb, data.data());
    ASSERT_EQ(index_build->Count(), static_cast<int64_t>(nb));

    // Upload (should use V2 path since kScalarIndexUseV3 == false)
    auto stats = index_build->Upload({});
    ASSERT_NE(stats, nullptr);

    // V2 format produces multiple files (index_data, index_length, etc.)
    auto index_files = stats->GetIndexFiles();
    ASSERT_GT(index_files.size(), 1)
        << "V2 upload should produce multiple files, got "
        << index_files.size();

    // Verify the files exist in chunk manager
    for (const auto& file : index_files) {
        ASSERT_TRUE(chunk_manager->Exist(file))
            << "Index file should exist: " << file;
        ASSERT_GT(chunk_manager->Size(file), 0)
            << "Index file should be non-empty: " << file;
    }

    // Load back the index using the V2 Load path
    milvus::storage::FileManagerContext ctx_load(
        field_meta, index_meta, chunk_manager, fs);
    auto index_load =
        std::make_shared<milvus::index::ScalarIndexSort<int64_t>>(ctx_load);

    nlohmann::json load_config;
    load_config[milvus::index::INDEX_FILES] = index_files;
    load_config[milvus::index::ENABLE_MMAP] = false;
    load_config[milvus::LOAD_PRIORITY] =
        milvus::proto::common::LoadPriority::HIGH;

    milvus::tracer::TraceContext trace_ctx;
    index_load->Load(trace_ctx, load_config);

    ASSERT_EQ(index_load->Count(), static_cast<int64_t>(nb));

    // Verify data correctness via Reverse_Lookup
    for (size_t i = 0; i < nb; ++i) {
        auto val = index_load->Reverse_Lookup(i);
        ASSERT_TRUE(val.has_value())
            << "Reverse_Lookup should succeed at " << i;
        ASSERT_EQ(val.value(), data[i]) << "Value mismatch at offset " << i;
    }

    // Verify query correctness: Range [50, 60] inclusive
    auto bitset = index_load->Range(
        static_cast<int64_t>(50), true, static_cast<int64_t>(60), true);
    int64_t count = 0;
    for (size_t i = 0; i < nb; ++i) {
        if (data[i] >= 50 && data[i] <= 60) {
            ASSERT_TRUE(bitset[i])
                << "Expected bit set for value " << data[i] << " at " << i;
            count++;
        } else {
            ASSERT_FALSE(bitset[i])
                << "Expected bit unset for value " << data[i] << " at " << i;
        }
    }
    ASSERT_EQ(count, 11);  // values 50..60 inclusive = 11 values
}