#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <stdint.h>
#include <cstdio>
#include <iosfwd>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "bitset/bitset.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "gtest/gtest.h"
#include "index/Meta.h"
#include "index/StringIndexSort.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "test_utils/Constants.h"
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
    config["mmap_file_path"] = TestLocalPath + "milvus_test";
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
    config["mmap_file_path"] = TestLocalPath + "milvus_test";
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
    config["mmap_file_path"] = TestLocalPath + "milvus_test";
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
    config["mmap_file_path"] = TestLocalPath + "milvus_test";
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
    config["mmap_file_path"] = TestLocalPath + "milvus_test";
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
    config["mmap_file_path"] = TestLocalPath + "milvus_test";
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
    config["mmap_file_path"] = TestLocalPath + "milvus_test";
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
    mmap_config[MMAP_FILE_PATH] =
        TestLocalPath + "test_string_index_sort_mmap.idx";

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
    prefix_mmap_config[MMAP_FILE_PATH] = TestLocalPath + "test_prefix_mmap.idx";
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
    std::remove((TestLocalPath + "test_string_index_sort_mmap.idx").c_str());
    std::remove((TestLocalPath + "test_prefix_mmap.idx").c_str());
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
    mmap_config[MMAP_FILE_PATH] =
        TestLocalPath + "test_load_without_assemble.idx";

    auto mmap_index = milvus::index::CreateStringIndexSort({});
    mmap_index->LoadWithoutAssemble(binary_set, mmap_config);

    // Verify the index works correctly
    auto bitset = mmap_index->In(test_strs.size(), test_strs.data());
    ASSERT_EQ(bitset.count(), test_strs.size());

    // Test that all operations work
    auto range_bitset = mmap_index->Range("dog", milvus::OpType::LessEqual);
    ASSERT_EQ(range_bitset.count(), 3);  // apple, cat, dog

    // Clean up
    std::remove((TestLocalPath + "test_load_without_assemble.idx").c_str());
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
        config["mmap_file_path"] = TestLocalPath + "milvus_scalar_test";
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
        config["mmap_file_path"] = TestLocalPath + "milvus_scalar_test";
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
        config["mmap_file_path"] = TestLocalPath + "milvus_scalar_test";
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

// ============== PatternMatch Tests ==============

using milvus::proto::plan::OpType;

TEST(StringIndexSortPatternMatchTest, PatternMatchBasicMemory) {
    std::vector<std::string> test_data = {
        "apple",        // 0
        "application",  // 1
        "apply",        // 2
        "banana",       // 3
        "band",         // 4
        "cat",          // 5
        "category",     // 6
        "dog",          // 7
        "application",  // 8 (duplicate)
        "apple"         // 9 (duplicate)
    };

    milvus::Config config;
    auto index = milvus::index::CreateStringIndexSort({});
    index->Build(test_data.size(), test_data.data());

    // Test pattern "app%" - should match apple, application, apply
    {
        auto bitset = index->PatternMatch("app%", OpType::Match);
        ASSERT_EQ(bitset.count(), 5);  // apple(2), application(2), apply(1)
        ASSERT_TRUE(bitset[0]);        // apple
        ASSERT_TRUE(bitset[1]);        // application
        ASSERT_TRUE(bitset[2]);        // apply
        ASSERT_TRUE(bitset[8]);        // application (dup)
        ASSERT_TRUE(bitset[9]);        // apple (dup)
    }

    // Test pattern "app%ion" - should match application only
    {
        auto bitset = index->PatternMatch("app%ion", OpType::Match);
        ASSERT_EQ(bitset.count(), 2);  // application appears twice
        ASSERT_TRUE(bitset[1]);        // application
        ASSERT_TRUE(bitset[8]);        // application (dup)
    }

    // Test pattern "%ana%" - should match banana
    {
        auto bitset = index->PatternMatch("%ana%", OpType::Match);
        ASSERT_EQ(bitset.count(), 1);
        ASSERT_TRUE(bitset[3]);  // banana
    }

    // Test pattern "cat%" - should match cat, category
    {
        auto bitset = index->PatternMatch("cat%", OpType::Match);
        ASSERT_EQ(bitset.count(), 2);
        ASSERT_TRUE(bitset[5]);  // cat
        ASSERT_TRUE(bitset[6]);  // category
    }
}

TEST(StringIndexSortPatternMatchTest, PatternMatchWithUnderscoreMemory) {
    std::vector<std::string> test_data = {
        "abc",   // 0
        "aXc",   // 1
        "a1c",   // 2
        "abcd",  // 3
        "ac",    // 4
        "abbc",  // 5
    };

    milvus::Config config;
    auto index = milvus::index::CreateStringIndexSort({});
    index->Build(test_data.size(), test_data.data());

    // Test pattern "a_c" - matches any single character between a and c
    {
        auto bitset = index->PatternMatch("a_c", OpType::Match);
        ASSERT_EQ(bitset.count(), 3);  // abc, aXc, a1c
        ASSERT_TRUE(bitset[0]);        // abc
        ASSERT_TRUE(bitset[1]);        // aXc
        ASSERT_TRUE(bitset[2]);        // a1c
        ASSERT_FALSE(bitset[3]);       // abcd - too long
        ASSERT_FALSE(bitset[4]);       // ac - too short
        ASSERT_FALSE(bitset[5]);       // abbc - two chars between
    }

    // Test pattern "a_c%" - prefix with underscore
    {
        auto bitset = index->PatternMatch("a_c%", OpType::Match);
        ASSERT_EQ(bitset.count(), 4);  // abc, aXc, a1c, abcd
        ASSERT_TRUE(bitset[0]);        // abc
        ASSERT_TRUE(bitset[1]);        // aXc
        ASSERT_TRUE(bitset[2]);        // a1c
        ASSERT_TRUE(bitset[3]);        // abcd
    }
}

TEST(StringIndexSortPatternMatchTest, PatternMatchEscapeMemory) {
    std::vector<std::string> test_data = {
        "100%",        // 0 - contains literal %
        "100percent",  // 1
        "50%off",      // 2 - contains literal %
        "a_b",         // 3 - contains literal _
        "axb",         // 4
        "a%b",         // 5 - contains literal %
        "ab",          // 6
    };

    milvus::Config config;
    auto index = milvus::index::CreateStringIndexSort({});
    index->Build(test_data.size(), test_data.data());

    // Test pattern "100\%" - matches literal "100%"
    {
        auto bitset = index->PatternMatch("100\\%", OpType::Match);
        ASSERT_EQ(bitset.count(), 1);
        ASSERT_TRUE(bitset[0]);  // 100%
    }

    // Test pattern "%\%%" - matches strings containing literal %
    {
        auto bitset = index->PatternMatch("%\\%%", OpType::Match);
        ASSERT_EQ(bitset.count(), 3);
        ASSERT_TRUE(bitset[0]);  // 100%
        ASSERT_TRUE(bitset[2]);  // 50%off
        ASSERT_TRUE(bitset[5]);  // a%b
    }

    // Test pattern "a\_b" - matches literal "a_b"
    {
        auto bitset = index->PatternMatch("a\\_b", OpType::Match);
        ASSERT_EQ(bitset.count(), 1);
        ASSERT_TRUE(bitset[3]);  // a_b
    }

    // Test pattern "a\%b" - matches literal "a%b"
    {
        auto bitset = index->PatternMatch("a\\%b", OpType::Match);
        ASSERT_EQ(bitset.count(), 1);
        ASSERT_TRUE(bitset[5]);  // a%b
    }
}

TEST(StringIndexSortPatternMatchTest, PatternMatchNoPrefix) {
    std::vector<std::string> test_data = {
        "hello world",  // 0
        "world hello",  // 1
        "hello",        // 2
        "world",        // 3
        "say hello",    // 4
    };

    milvus::Config config;
    auto index = milvus::index::CreateStringIndexSort({});
    index->Build(test_data.size(), test_data.data());

    // Test pattern "%hello" - postfix match (no fixed prefix)
    {
        auto bitset = index->PatternMatch("%hello", OpType::Match);
        ASSERT_EQ(bitset.count(), 3);
        ASSERT_TRUE(bitset[1]);  // world hello
        ASSERT_TRUE(bitset[2]);  // hello
        ASSERT_TRUE(bitset[4]);  // say hello
    }

    // Test pattern "%world%" - inner match (no fixed prefix)
    {
        auto bitset = index->PatternMatch("%world%", OpType::Match);
        ASSERT_EQ(bitset.count(), 3);
        ASSERT_TRUE(bitset[0]);  // hello world
        ASSERT_TRUE(bitset[1]);  // world hello
        ASSERT_TRUE(bitset[3]);  // world
    }
}

TEST(StringIndexSortPatternMatchTest, PatternMatchMmap) {
    std::vector<std::string> test_data = {
        "apple",
        "application",
        "apply",
        "banana",
        "band",
    };

    milvus::Config config;
    auto index = milvus::index::CreateStringIndexSort({});
    index->Build(test_data.size(), test_data.data());

    // Serialize and reload with mmap
    auto binary_set = index->Serialize(config);

    milvus::Config mmap_config;
    mmap_config[milvus::index::MMAP_FILE_PATH] =
        TestLocalPath + "test_pattern_match_mmap.idx";

    auto mmap_index = milvus::index::CreateStringIndexSort({});
    mmap_index->Load(binary_set, mmap_config);

    // Test pattern "app%ion"
    {
        auto bitset = mmap_index->PatternMatch("app%ion", OpType::Match);
        ASSERT_EQ(bitset.count(), 1);
        ASSERT_TRUE(bitset[1]);  // application
    }

    // Test pattern "ban%" with underscore
    {
        auto bitset = mmap_index->PatternMatch("ban%", OpType::Match);
        ASSERT_EQ(bitset.count(), 2);  // banana, band
        ASSERT_TRUE(bitset[3]);
        ASSERT_TRUE(bitset[4]);
    }

    std::remove((TestLocalPath + "test_pattern_match_mmap.idx").c_str());
}

TEST(StringIndexSortPatternMatchTest, PatternMatchComplexEscape) {
    std::vector<std::string> test_data = {
        "10%_off",   // 0 - contains both % and _
        "10%aoff",   // 1
        "10%boff",   // 2
        "10a_off",   // 3
        "discount",  // 4
    };

    milvus::Config config;
    auto index = milvus::index::CreateStringIndexSort({});
    index->Build(test_data.size(), test_data.data());

    // Test pattern "10\%\_off" - matches literal "10%_off"
    {
        auto bitset = index->PatternMatch("10\\%\\_off", OpType::Match);
        ASSERT_EQ(bitset.count(), 1);
        ASSERT_TRUE(bitset[0]);
    }

    // Test pattern "10\%_off" - matches "10%" followed by any single char and "off"
    {
        auto bitset = index->PatternMatch("10\\%_off", OpType::Match);
        ASSERT_EQ(bitset.count(), 3);  // 10%_off, 10%aoff, 10%boff
        ASSERT_TRUE(bitset[0]);
        ASSERT_TRUE(bitset[1]);
        ASSERT_TRUE(bitset[2]);
    }
}

TEST(StringIndexSortPatternMatchTest, PatternMatchPrefixOp) {
    std::vector<std::string> test_data = {
        "apple",
        "application",
        "banana",
    };

    milvus::Config config;
    auto index = milvus::index::CreateStringIndexSort({});
    index->Build(test_data.size(), test_data.data());

    // Test PrefixMatch op - should delegate to PrefixMatch
    auto bitset = index->PatternMatch("app", OpType::PrefixMatch);
    ASSERT_EQ(bitset.count(), 2);  // apple, application
    ASSERT_TRUE(bitset[0]);
    ASSERT_TRUE(bitset[1]);
}

TEST(StringIndexSortPatternMatchTest, PatternMatchDuplicateValues) {
    // Test that duplicate values are handled correctly
    // (each unique value should only be regex-matched once)
    std::vector<std::string> test_data;
    for (int i = 0; i < 1000; ++i) {
        test_data.push_back("repeated_value");
    }
    test_data.push_back("other_value");

    milvus::Config config;
    auto index = milvus::index::CreateStringIndexSort({});
    index->Build(test_data.size(), test_data.data());

    // Pattern that matches repeated_value
    auto bitset = index->PatternMatch("repeated%", OpType::Match);
    ASSERT_EQ(bitset.count(), 1000);

    // Pattern that matches other_value
    auto bitset2 = index->PatternMatch("other%", OpType::Match);
    ASSERT_EQ(bitset2.count(), 1);
    ASSERT_TRUE(bitset2[1000]);
}

TEST(StringIndexSortPatternMatchTest, PostfixMatch) {
    std::vector<std::string> test_data = {
        "hello_world",  // 0 - ends with "world"
        "new_world",    // 1 - ends with "world"
        "world_peace",  // 2 - ends with "peace"
        "hello",        // 3
        "world",        // 4 - ends with "world"
    };

    auto index = milvus::index::CreateStringIndexSort({});
    index->Build(test_data.size(), test_data.data());

    // PostfixMatch: find strings ending with "world"
    auto bitset = index->PatternMatch("world", OpType::PostfixMatch);
    ASSERT_EQ(bitset.count(), 3);
    ASSERT_TRUE(bitset[0]);   // hello_world
    ASSERT_TRUE(bitset[1]);   // new_world
    ASSERT_FALSE(bitset[2]);  // world_peace
    ASSERT_FALSE(bitset[3]);  // hello
    ASSERT_TRUE(bitset[4]);   // world

    // PostfixMatch: find strings ending with "peace"
    auto bitset2 = index->PatternMatch("peace", OpType::PostfixMatch);
    ASSERT_EQ(bitset2.count(), 1);
    ASSERT_TRUE(bitset2[2]);  // world_peace
}

TEST(StringIndexSortPatternMatchTest, InnerMatch) {
    std::vector<std::string> test_data = {
        "hello_world",  // 0 - contains "world"
        "new_world",    // 1 - contains "world"
        "world_peace",  // 2 - contains "world"
        "hello",        // 3 - no "world"
        "worldwide",    // 4 - contains "world"
    };

    auto index = milvus::index::CreateStringIndexSort({});
    index->Build(test_data.size(), test_data.data());

    // InnerMatch: find strings containing "world"
    auto bitset = index->PatternMatch("world", OpType::InnerMatch);
    ASSERT_EQ(bitset.count(), 4);
    ASSERT_TRUE(bitset[0]);   // hello_world
    ASSERT_TRUE(bitset[1]);   // new_world
    ASSERT_TRUE(bitset[2]);   // world_peace
    ASSERT_FALSE(bitset[3]);  // hello
    ASSERT_TRUE(bitset[4]);   // worldwide

    // InnerMatch: find strings containing "ello"
    auto bitset2 = index->PatternMatch("ello", OpType::InnerMatch);
    ASSERT_EQ(bitset2.count(), 2);
    ASSERT_TRUE(bitset2[0]);  // hello_world
    ASSERT_FALSE(bitset2[1]);
    ASSERT_FALSE(bitset2[2]);
    ASSERT_TRUE(bitset2[3]);  // hello
    ASSERT_FALSE(bitset2[4]);
}

TEST(StringIndexSortPatternMatchTest, PostfixMatchMmap) {
    std::vector<std::string> test_data = {
        "application",  // 0 - ends with "ion"
        "revolution",   // 1 - ends with "ion"
        "apple",        // 2
    };

    auto index = milvus::index::CreateStringIndexSort({});
    index->Build(test_data.size(), test_data.data());

    // Serialize and reload as mmap
    auto binaryset = index->Serialize({});
    auto mmap_index = milvus::index::CreateStringIndexSort({});
    mmap_index->Load(binaryset, {});

    // PostfixMatch on mmap
    auto bitset = mmap_index->PatternMatch("ion", OpType::PostfixMatch);
    ASSERT_EQ(bitset.count(), 2);
    ASSERT_TRUE(bitset[0]);   // application
    ASSERT_TRUE(bitset[1]);   // revolution
    ASSERT_FALSE(bitset[2]);  // apple
}

TEST(StringIndexSortPatternMatchTest, InnerMatchMmap) {
    std::vector<std::string> test_data = {
        "application",  // 0 - contains "cat"
        "category",     // 1 - contains "cat"
        "dog",          // 2 - no "cat"
    };

    auto index = milvus::index::CreateStringIndexSort({});
    index->Build(test_data.size(), test_data.data());

    // Serialize and reload as mmap
    auto binaryset = index->Serialize({});
    auto mmap_index = milvus::index::CreateStringIndexSort({});
    mmap_index->Load(binaryset, {});

    // InnerMatch on mmap
    auto bitset = mmap_index->PatternMatch("cat", OpType::InnerMatch);
    ASSERT_EQ(bitset.count(), 2);
    ASSERT_TRUE(bitset[0]);   // application
    ASSERT_TRUE(bitset[1]);   // category
    ASSERT_FALSE(bitset[2]);  // dog
}
