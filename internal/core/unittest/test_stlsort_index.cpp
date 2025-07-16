#include <gtest/gtest.h>
#include <cstdint>

#include "index/ScalarIndexSort.h"
#include "common/Types.h"

using namespace milvus;
using namespace milvus::index;

template <typename T>
void
test_stlsort_for_range(
    const std::vector<T>& data,
    DataType data_type,
    std::optional<std::string> mmap_path,
    std::function<TargetBitmap(const std::shared_ptr<ScalarIndexSort<T>>&)>
        exec_expr,
    const std::vector<bool>& expected_result) {
    size_t nb = data.size();
    BinarySet binary_set;
    {
        Config config;

        auto index = std::make_shared<index::ScalarIndexSort<T>>();
        index->Build(nb, data.data());

        binary_set = index->Serialize(config);
    }

    {
        Config config;
        if (mmap_path.has_value()) {
            config[milvus::index::MMAP_FILE_PATH] = mmap_path.value();
            config[milvus::index::ENABLE_MMAP] = true;
        } else {
            config[milvus::index::ENABLE_MMAP] = false;
        }

        auto index = std::make_shared<index::ScalarIndexSort<T>>();
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
    {
        std::vector<int64_t> data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        std::vector<bool> expected_result = {
            false, false, true, true, true, true, true, false, false, false};

        auto exec_expr =
            [](const std::shared_ptr<ScalarIndexSort<int64_t>>& index) {
                return index->Range(3, true, 7, true);
            };

        test_stlsort_for_range<int64_t>(
            data, DataType::INT64, std::nullopt, exec_expr, expected_result);

        std::string mmap_dir = "/tmp/test-stlsort-index/mmap-dir";
        test_stlsort_for_range<int64_t>(
            data, DataType::INT64, mmap_dir, exec_expr, expected_result);
    }

    {
        std::vector<std::string> data = {
            "a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
        std::vector<bool> expected_result = {
            false, false, true, true, true, true, true, false, false, false};

        auto exec_expr =
            [](const std::shared_ptr<ScalarIndexSort<std::string>>& index) {
                return index->Range("c", true, "g", true);
            };

        test_stlsort_for_range<std::string>(
            data, DataType::STRING, std::nullopt, exec_expr, expected_result);

        std::string mmap_dir = "/tmp/test-stlsort-index/mmap-dir";
        test_stlsort_for_range<std::string>(
            data, DataType::STRING, mmap_dir, exec_expr, expected_result);
    }
}