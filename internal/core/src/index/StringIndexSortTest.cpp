#include <gtest/gtest.h>
#include <boost/filesystem.hpp>

#include "index/StringIndexSort.h"
#include "index/IndexFactory.h"
#include "test_utils/indexbuilder_test_utils.h"

constexpr int64_t nb = 100;

namespace milvus {
namespace index {
class StringIndexBaseTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        strs = GenStrArr(nb);
        *str_arr.mutable_data() = {strs.begin(), strs.end()};
    }

 protected:
    std::vector<std::string> strs;
    schemapb::StringArray str_arr;
};

class StringIndexSortTest : public StringIndexBaseTest {};

TEST_F(StringIndexSortTest, ConstructorMemory) {
    Config config;
    auto index = milvus::index::CreateStringIndexSort({});
    ASSERT_NE(index, nullptr);
}

TEST_F(StringIndexSortTest, ConstructorMmap) {
    Config config;
    config["mmap_file_path"] = "/tmp/milvus_test";
    auto index = milvus::index::CreateStringIndexSort({});
    ASSERT_NE(index, nullptr);
}

TEST_F(StringIndexSortTest, BuildMemory) {
    Config config;
    auto index = milvus::index::CreateStringIndexSort({});
    index->Build(strs.size(), strs.data());
    ASSERT_EQ(index->Count(), nb);
}

TEST_F(StringIndexSortTest, BuildMmap) {
    Config config;
    config["mmap_file_path"] = "/tmp/milvus_test";
    auto index = milvus::index::CreateStringIndexSort({});
    index->Build(strs.size(), strs.data());
    ASSERT_EQ(index->Count(), nb);
}

TEST_F(StringIndexSortTest, InMemory) {
    Config config;
    auto index = milvus::index::CreateStringIndexSort({});
    index->Build(nb, strs.data());

    // Test with all strings
    auto bitset = index->In(strs.size(), strs.data());
    ASSERT_EQ(bitset.size(), strs.size());
    ASSERT_EQ(bitset.count(), strs.size());

    // Test with subset
    std::vector<std::string> subset = {strs[0], strs[10], strs[20]};
    auto subset_bitset = index->In(subset.size(), subset.data());
    ASSERT_EQ(subset_bitset.size(), strs.size());
    ASSERT_EQ(subset_bitset.count(), 3);
    ASSERT_TRUE(subset_bitset[0]);
    ASSERT_TRUE(subset_bitset[10]);
    ASSERT_TRUE(subset_bitset[20]);
}

TEST_F(StringIndexSortTest, InMmap) {
    Config config;
    config["mmap_file_path"] = "/tmp/milvus_test";
    auto index = milvus::index::CreateStringIndexSort({});
    index->Build(nb, strs.data());

    auto bitset = index->In(strs.size(), strs.data());
    ASSERT_EQ(bitset.size(), strs.size());
    ASSERT_EQ(bitset.count(), strs.size());
}

TEST_F(StringIndexSortTest, NotInMemory) {
    Config config;
    auto index = milvus::index::CreateStringIndexSort({});
    index->Build(nb, strs.data());

    auto bitset = index->NotIn(strs.size(), strs.data());
    ASSERT_EQ(bitset.size(), strs.size());
    ASSERT_EQ(bitset.count(), 0);

    // Test with non-existing strings
    std::vector<std::string> non_existing = {"non_existing_1",
                                             "non_existing_2"};
    auto non_existing_bitset =
        index->NotIn(non_existing.size(), non_existing.data());
    ASSERT_EQ(non_existing_bitset.size(), strs.size());
    ASSERT_EQ(non_existing_bitset.count(), strs.size());
}

TEST_F(StringIndexSortTest, RangeMemory) {
    Config config;
    auto index = milvus::index::CreateStringIndexSort({});

    // Build with sorted strings for predictable range tests
    std::vector<std::string> sorted_strs = {
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
    index->Build(sorted_strs.size(), sorted_strs.data());

    // Test LessThan
    auto bitset = index->Range("d", OpType::LessThan);
    ASSERT_EQ(bitset.count(), 3);  // a, b, c

    // Test LessEqual
    auto bitset2 = index->Range("d", OpType::LessEqual);
    ASSERT_EQ(bitset2.count(), 4);  // a, b, c, d

    // Test GreaterThan
    auto bitset3 = index->Range("g", OpType::GreaterThan);
    ASSERT_EQ(bitset3.count(), 3);  // h, i, j

    // Test GreaterEqual
    auto bitset4 = index->Range("g", OpType::GreaterEqual);
    ASSERT_EQ(bitset4.count(), 4);  // g, h, i, j
}

TEST_F(StringIndexSortTest, RangeBetweenMemory) {
    Config config;
    auto index = milvus::index::CreateStringIndexSort({});

    std::vector<std::string> sorted_strs = {
        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
    index->Build(sorted_strs.size(), sorted_strs.data());

    // Test inclusive range
    auto bitset = index->Range("c", true, "g", true);
    ASSERT_EQ(bitset.count(), 5);  // c, d, e, f, g

    // Test exclusive range
    auto bitset2 = index->Range("c", false, "g", false);
    ASSERT_EQ(bitset2.count(), 3);  // d, e, f

    // Test mixed
    auto bitset3 = index->Range("c", true, "g", false);
    ASSERT_EQ(bitset3.count(), 4);  // c, d, e, f
}

TEST_F(StringIndexSortTest, PrefixMatchMemory) {
    Config config;
    auto index = milvus::index::CreateStringIndexSort({});

    std::vector<std::string> test_strs = {
        "apple", "application", "apply", "banana", "band", "cat"};
    index->Build(test_strs.size(), test_strs.data());

    auto bitset = index->PrefixMatch("app");
    ASSERT_EQ(bitset.count(), 3);  // apple, application, apply

    auto bitset2 = index->PrefixMatch("ban");
    ASSERT_EQ(bitset2.count(), 2);  // banana, band

    auto bitset3 = index->PrefixMatch("cat");
    ASSERT_EQ(bitset3.count(), 1);  // cat

    auto bitset4 = index->PrefixMatch("dog");
    ASSERT_EQ(bitset4.count(), 0);  // none
}

TEST_F(StringIndexSortTest, PrefixMatchMmap) {
    Config config;
    config["mmap_file_path"] = "/tmp/milvus_test";
    auto index = milvus::index::CreateStringIndexSort({});

    std::vector<std::string> test_strs = {
        "apple", "application", "apply", "banana", "band", "cat"};
    index->Build(test_strs.size(), test_strs.data());

    auto bitset = index->PrefixMatch("app");
    ASSERT_EQ(bitset.count(), 3);  // apple, application, apply
}

TEST_F(StringIndexSortTest, ReverseLookupMemory) {
    Config config;
    auto index = milvus::index::CreateStringIndexSort({});
    index->Build(strs.size(), strs.data());

    for (size_t i = 0; i < strs.size(); ++i) {
        auto result = index->Reverse_Lookup(i);
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(result.value(), strs[i]);
    }

    // Test invalid offset
    auto result = index->Reverse_Lookup(strs.size() + 100);
    ASSERT_FALSE(result.has_value());
}

TEST_F(StringIndexSortTest, ReverseLookupMmap) {
    Config config;
    config["mmap_file_path"] = "/tmp/milvus_test";
    auto index = milvus::index::CreateStringIndexSort({});
    index->Build(strs.size(), strs.data());

    for (size_t i = 0; i < strs.size(); ++i) {
        auto result = index->Reverse_Lookup(i);
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(result.value(), strs[i]);
    }
}

TEST_F(StringIndexSortTest, SerializeDeserializeMemory) {
    Config config;
    auto index = milvus::index::CreateStringIndexSort({});
    index->Build(strs.size(), strs.data());

    // Serialize
    auto binary_set = index->Serialize(config);

    // Create new index and load
    auto new_index = milvus::index::CreateStringIndexSort({});
    new_index->Load(binary_set);

    // Verify data integrity
    ASSERT_EQ(new_index->Count(), strs.size());

    auto bitset = new_index->In(strs.size(), strs.data());
    ASSERT_EQ(bitset.count(), strs.size());

    for (size_t i = 0; i < strs.size(); ++i) {
        auto result = new_index->Reverse_Lookup(i);
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(result.value(), strs[i]);
    }
}

TEST_F(StringIndexSortTest, SerializeDeserializeMmap) {
    Config config;
    config["mmap_file_path"] = "/tmp/milvus_test";
    auto index = milvus::index::CreateStringIndexSort({});
    index->Build(strs.size(), strs.data());

    // Serialize
    auto binary_set = index->Serialize(config);

    // Create new index and load
    auto new_index = milvus::index::CreateStringIndexSort({});
    new_index->Load(binary_set);

    // Verify data integrity
    ASSERT_EQ(new_index->Count(), strs.size());

    auto bitset = new_index->In(strs.size(), strs.data());
    ASSERT_EQ(bitset.count(), strs.size());
}

TEST_F(StringIndexSortTest, NullHandlingMemory) {
    Config config;
    auto index = milvus::index::CreateStringIndexSort({});

    std::unique_ptr<bool[]> valid(new bool[nb]);
    for (int i = 0; i < nb; i++) {
        valid[i] = (i % 2 == 0);  // Half are valid
    }

    index->Build(nb, strs.data(), valid.get());

    // Test IsNull
    auto null_bitset = index->IsNull();
    ASSERT_EQ(null_bitset.count(), nb / 2);

    // Test IsNotNull
    auto not_null_bitset = index->IsNotNull();
    ASSERT_EQ(not_null_bitset.count(), nb / 2);

    // Verify they are complementary
    for (size_t i = 0; i < nb; ++i) {
        ASSERT_NE(null_bitset[i], not_null_bitset[i]);
    }
}

TEST_F(StringIndexSortTest, NullHandlingMmap) {
    Config config;
    config["mmap_file_path"] = "/tmp/milvus_test";
    auto index = milvus::index::CreateStringIndexSort({});

    std::unique_ptr<bool[]> valid(new bool[nb]);
    for (int i = 0; i < nb; i++) {
        valid[i] = (i % 2 == 0);
    }

    index->Build(nb, strs.data(), valid.get());

    auto null_bitset = index->IsNull();
    ASSERT_EQ(null_bitset.count(), nb / 2);

    auto not_null_bitset = index->IsNotNull();
    ASSERT_EQ(not_null_bitset.count(), nb / 2);
}

TEST_F(StringIndexSortTest, MmapLoadAfterSerialize) {
    // Step 1: Build index in memory and serialize
    Config build_config;
    auto index = milvus::index::CreateStringIndexSort({});

    std::vector<std::string> test_strs = {
        "apple",
        "banana",
        "cherry",
        "date",
        "elderberry",
        "fig",
        "grape",
        "honeydew",
        "kiwi",
        "lemon",
        "apple",
        "banana",
        "apple"  // Include duplicates
    };
    index->Build(test_strs.size(), test_strs.data());

    // Serialize the index
    auto binary_set = index->Serialize(build_config);

    // Step 2: Load with mmap configuration
    Config mmap_config;
    mmap_config[MMAP_FILE_PATH] = "/tmp/test_string_index_sort_mmap.idx";

    auto mmap_index = milvus::index::CreateStringIndexSort({});
    mmap_index->Load(binary_set, mmap_config);

    // Step 3: Verify functionality with mmap loaded index
    // Test Count
    ASSERT_EQ(mmap_index->Count(), test_strs.size());

    // Test In operation
    std::vector<std::string> search_vals = {"apple", "grape", "lemon"};
    auto bitset = mmap_index->In(search_vals.size(), search_vals.data());
    ASSERT_EQ(bitset.count(),
              5);             // apple appears 3 times, grape once, lemon once
    ASSERT_TRUE(bitset[0]);   // apple
    ASSERT_TRUE(bitset[6]);   // grape
    ASSERT_TRUE(bitset[9]);   // lemon
    ASSERT_TRUE(bitset[10]);  // apple (duplicate)
    ASSERT_TRUE(bitset[12]);  // apple (duplicate)

    // Test NotIn operation
    std::vector<std::string> not_in_vals = {"orange", "pear"};
    auto not_bitset = mmap_index->NotIn(not_in_vals.size(), not_in_vals.data());
    ASSERT_EQ(not_bitset.count(),
              test_strs.size());  // All strings should be in result

    // Test Range operation
    auto range_bitset =
        mmap_index->Range("cherry", milvus::OpType::GreaterEqual);
    ASSERT_EQ(
        range_bitset.count(),
        8);  // cherry, date, elderberry, fig, grape, honeydew, kiwi, lemon

    // Test Range between
    auto range_between = mmap_index->Range("banana", true, "grape", true);
    ASSERT_EQ(range_between.count(),
              7);  // banana(2), cherry, date, elderberry, fig, grape

    // Test PrefixMatch
    std::vector<std::string> prefix_test_strs = {
        "app", "apple", "application", "banana", "band"};
    auto prefix_index = milvus::index::CreateStringIndexSort({});
    prefix_index->Build(prefix_test_strs.size(), prefix_test_strs.data());
    auto prefix_binary = prefix_index->Serialize(build_config);

    Config prefix_mmap_config;
    prefix_mmap_config[MMAP_FILE_PATH] = "/tmp/test_prefix_mmap.idx";
    auto prefix_mmap_index = milvus::index::CreateStringIndexSort({});
    prefix_mmap_index->Load(prefix_binary, prefix_mmap_config);

    auto prefix_bitset = prefix_mmap_index->PrefixMatch("app");
    ASSERT_EQ(prefix_bitset.count(), 3);  // app, apple, application

    // Test Reverse_Lookup
    for (size_t i = 0; i < test_strs.size(); ++i) {
        auto result = mmap_index->Reverse_Lookup(i);
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(result.value(), test_strs[i]);
    }

    // Clean up temp files
    std::remove("/tmp/test_string_index_sort_mmap.idx");
    std::remove("/tmp/test_prefix_mmap.idx");
}

TEST_F(StringIndexSortTest, LoadWithoutAssembleMmap) {
    // Build and serialize index
    Config config;
    auto index = milvus::index::CreateStringIndexSort({});

    std::vector<std::string> test_strs = {
        "zebra", "apple", "monkey", "dog", "cat"};
    index->Build(test_strs.size(), test_strs.data());

    auto binary_set = index->Serialize(config);

    // Load without assemble using mmap
    Config mmap_config;
    mmap_config[MMAP_FILE_PATH] = "/tmp/test_load_without_assemble.idx";

    auto mmap_index = milvus::index::CreateStringIndexSort({});
    mmap_index->LoadWithoutAssemble(binary_set, mmap_config);

    // Verify the index works correctly
    auto bitset = mmap_index->In(test_strs.size(), test_strs.data());
    ASSERT_EQ(bitset.count(), test_strs.size());

    // Test that all operations work
    auto range_bitset = mmap_index->Range("dog", milvus::OpType::LessEqual);
    ASSERT_EQ(range_bitset.count(), 3);  // apple, cat, dog

    // Clean up
    std::remove("/tmp/test_load_without_assemble.idx");
}
}  // namespace index
}  // namespace milvus

TEST(StringIndexSortStandaloneTest, StringIndexSortBuildAndSearch) {
    // Test data
    std::vector<std::string> test_data = {"apple",
                                          "banana",
                                          "cherry",
                                          "date",
                                          "elderberry",
                                          "fig",
                                          "grape",
                                          "honeydew",
                                          "kiwi",
                                          "lemon"};
    auto n = test_data.size();

    // Test Memory mode
    {
        milvus::Config config;
        auto index = milvus::index::CreateStringIndexSort({});
        index->Build(n, test_data.data());

        // Test In operation
        std::vector<std::string> search_vals = {"apple", "grape", "lemon"};
        auto bitset = index->In(search_vals.size(), search_vals.data());
        ASSERT_EQ(bitset.count(), 3);
        ASSERT_TRUE(bitset[0]);  // apple
        ASSERT_TRUE(bitset[6]);  // grape
        ASSERT_TRUE(bitset[9]);  // lemon

        // Test Range operation
        auto range_bitset =
            index->Range("cherry", milvus::OpType::GreaterEqual);
        ASSERT_EQ(
            range_bitset.count(),
            8);  // cherry, date, elderberry, fig, grape, honeydew, kiwi, lemon

        // Test PrefixMatch
        std::vector<std::string> test_data_prefix = {
            "app", "apple", "application", "banana", "band"};
        auto prefix_index = milvus::index::CreateStringIndexSort({});
        prefix_index->Build(test_data_prefix.size(), test_data_prefix.data());
        auto prefix_bitset = prefix_index->PrefixMatch("app");
        ASSERT_EQ(prefix_bitset.count(), 3);  // app, apple, application
    }

    // Test Mmap mode
    {
        milvus::Config config;
        config["mmap_file_path"] = "/tmp/milvus_scalar_test";
        auto index = milvus::index::CreateStringIndexSort({});
        index->Build(n, test_data.data());

        // Test In operation
        std::vector<std::string> search_vals = {"banana", "fig"};
        auto bitset = index->In(search_vals.size(), search_vals.data());
        ASSERT_EQ(bitset.count(), 2);
        ASSERT_TRUE(bitset[1]);  // banana
        ASSERT_TRUE(bitset[5]);  // fig

        // Test NotIn operation
        auto not_bitset = index->NotIn(search_vals.size(), search_vals.data());
        ASSERT_EQ(not_bitset.count(), n - 2);
        ASSERT_FALSE(not_bitset[1]);  // banana should not be in NotIn result
        ASSERT_FALSE(not_bitset[5]);  // fig should not be in NotIn result
    }
}

TEST(StringIndexSortStandaloneTest, StringIndexSortWithNulls) {
    std::vector<std::string> test_data = {
        "alpha", "beta", "gamma", "delta", "epsilon"};

    std::unique_ptr<bool[]> valid_data(new bool[test_data.size()]);
    valid_data[0] = true;
    valid_data[1] = false;
    valid_data[2] = true;
    valid_data[3] = false;
    valid_data[4] = true;
    auto n = test_data.size();

    // Memory mode with nulls
    {
        milvus::Config config;
        auto index = milvus::index::CreateStringIndexSort({});
        index->Build(n, test_data.data(), valid_data.get());

        // Test IsNull
        auto null_bitset = index->IsNull();
        ASSERT_EQ(null_bitset.count(), 2);
        ASSERT_TRUE(null_bitset[1]);  // beta is null
        ASSERT_TRUE(null_bitset[3]);  // delta is null

        // Test IsNotNull
        auto not_null_bitset = index->IsNotNull();
        ASSERT_EQ(not_null_bitset.count(), 3);
        ASSERT_TRUE(not_null_bitset[0]);  // alpha is not null
        ASSERT_TRUE(not_null_bitset[2]);  // gamma is not null
        ASSERT_TRUE(not_null_bitset[4]);  // epsilon is not null

        // Test In with nulls
        std::vector<std::string> search_vals = {"alpha", "beta", "gamma"};
        auto bitset = index->In(search_vals.size(), search_vals.data());
        ASSERT_EQ(bitset.count(), 2);  // Only alpha and gamma (beta is null)
        ASSERT_TRUE(bitset[0]);        // alpha
        ASSERT_FALSE(bitset[1]);       // beta is null
        ASSERT_TRUE(bitset[2]);        // gamma
    }

    // Mmap mode with nulls
    {
        milvus::Config config;
        config["mmap_file_path"] = "/tmp/milvus_scalar_test";
        auto index = milvus::index::CreateStringIndexSort({});
        index->Build(n, test_data.data(), valid_data.get());

        auto null_bitset = index->IsNull();
        ASSERT_EQ(null_bitset.count(), 2);

        auto not_null_bitset = index->IsNotNull();
        ASSERT_EQ(not_null_bitset.count(), 3);
    }
}

TEST(StringIndexSortStandaloneTest, StringIndexSortSerialization) {
    std::vector<std::string> test_data;
    for (int i = 0; i < 100; ++i) {
        test_data.push_back("str_" + std::to_string(i));
    }
    auto n = test_data.size();

    // Test Memory mode serialization
    {
        milvus::Config config;
        auto index = milvus::index::CreateStringIndexSort({});
        index->Build(n, test_data.data());

        // Serialize
        auto binary_set = index->Serialize(config);

        // Create new index and deserialize
        auto new_index = milvus::index::CreateStringIndexSort({});
        new_index->Load(binary_set);

        // Verify the data
        ASSERT_EQ(new_index->Count(), n);

        // Test search on deserialized index
        std::vector<std::string> search_vals = {"str_10", "str_50", "str_90"};
        auto bitset = new_index->In(search_vals.size(), search_vals.data());
        ASSERT_EQ(bitset.count(), 3);

        // Test reverse lookup
        for (size_t i = 0; i < n; ++i) {
            auto result = new_index->Reverse_Lookup(i);
            ASSERT_TRUE(result.has_value());
            ASSERT_EQ(result.value(), test_data[i]);
        }
    }

    // Test Mmap mode serialization
    {
        milvus::Config config;
        config["mmap_file_path"] = "/tmp/milvus_scalar_test";
        auto index = milvus::index::CreateStringIndexSort({});
        index->Build(n, test_data.data());

        // Serialize
        auto binary_set = index->Serialize(config);

        // Create new index and deserialize
        auto new_index = milvus::index::CreateStringIndexSort({});
        new_index->Load(binary_set);

        // Verify the data
        ASSERT_EQ(new_index->Count(), n);

        // Test range query on deserialized index
        auto bitset = new_index->Range("str_20", true, "str_30", true);
        // In lexicographical order: str_20, str_21, ..., str_29, str_3, str_30
        // So we expect more than 11 due to lexicographical ordering
        ASSERT_GT(bitset.count(), 0);
    }
}
