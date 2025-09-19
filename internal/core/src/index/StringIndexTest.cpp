// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <arrow/array/builder_binary.h>
#include <arrow/type.h>
#include <gtest/gtest.h>
#include <cstdio>

#include "index/Index.h"
#include "index/ScalarIndex.h"
#include "index/StringIndexMarisa.h"
#include "index/StringIndexSort.h"

#include "index/IndexFactory.h"
#include "test_utils/indexbuilder_test_utils.h"
#include "test_utils/AssertUtils.h"
#include <boost/filesystem.hpp>
#include <numeric>
#include "test_utils/storage_test_utils.h"

constexpr int64_t nb = 100;
namespace schemapb = milvus::proto::schema;

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

class StringIndexMarisaTest : public StringIndexBaseTest {};

TEST_F(StringIndexMarisaTest, Constructor) {
    auto index = milvus::index::CreateStringIndexMarisa();
}

TEST_F(StringIndexMarisaTest, Build) {
    auto index = milvus::index::CreateStringIndexMarisa();
    index->Build(strs.size(), strs.data());
}

TEST_F(StringIndexMarisaTest, HasRawData) {
    auto index = milvus::index::CreateStringIndexMarisa();
    index->Build(nb, strs.data());
    ASSERT_TRUE(index->HasRawData());
}

TEST_F(StringIndexMarisaTest, Count) {
    auto index = milvus::index::CreateStringIndexMarisa();
    index->Build(nb, strs.data());
    ASSERT_EQ(strs.size(), index->Count());
}

TEST_F(StringIndexMarisaTest, In) {
    auto index = milvus::index::CreateStringIndexMarisa();
    index->Build(nb, strs.data());
    auto bitset = index->In(strs.size(), strs.data());
    ASSERT_EQ(bitset.size(), strs.size());
    ASSERT_TRUE(Any(bitset));
}

TEST_F(StringIndexMarisaTest, InHasNull) {
    auto index = milvus::index::CreateStringIndexMarisa();
    FixedVector<bool> is_null(nb);
    std::vector<std::string> in_strs(nb / 2);
    int j = 0;
    for (int i = 0; i < nb; i++) {
        is_null[i] = i % 2 == 0;
        if (i % 2 == 0) {
            in_strs[j] = strs[i];
            j++;
        }
    }

    index->Build(nb, strs.data(), is_null.data());
    auto bitset = index->In(in_strs.size(), in_strs.data());
    ASSERT_EQ(bitset.size(), strs.size());
    ASSERT_TRUE(bitset.count() == (nb / 2))
        << "count: " << bitset.count() << " nb: " << nb;
}

TEST_F(StringIndexMarisaTest, NotIn) {
    auto index = milvus::index::CreateStringIndexMarisa();
    index->Build(nb, strs.data());
    auto bitset = index->NotIn(strs.size(), strs.data());
    ASSERT_EQ(bitset.size(), strs.size());
    ASSERT_TRUE(BitSetNone(bitset));
}

TEST_F(StringIndexMarisaTest, NotInHasNull) {
    auto index = milvus::index::CreateStringIndexMarisa();
    FixedVector<bool> is_null(nb);
    std::vector<std::string> in_strs(nb / 2);
    int j = 0;
    for (int i = 0; i < nb; i++) {
        is_null[i] = i % 2 == 0;
        if (i % 2 == 0) {
            in_strs[j] = strs[i];
            j++;
        }
    }
    index->Build(nb, strs.data(), is_null.data());
    auto bitset = index->NotIn(in_strs.size(), in_strs.data());
    ASSERT_EQ(bitset.size(), strs.size());
    std::cout << "bitset: " << bitset.count() << std::endl;
    ASSERT_TRUE(bitset.count() == 0);
}

TEST_F(StringIndexMarisaTest, Range) {
    auto index = milvus::index::CreateStringIndexMarisa();
    std::vector<std::string> strings(nb);
    std::vector<int> counts(10);
    for (int i = 0; i < nb; ++i) {
        int val = std::rand() % 10;
        counts[val]++;
        strings[i] = std::to_string(val);
    }
    index->Build(nb, strings.data());

    {
        // [0...9]
        auto bitset = index->Range("0", milvus::OpType::GreaterEqual);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        // [5...9]
        int expect = std::accumulate(counts.begin() + 5, counts.end(), 0);
        auto bitset = index->Range("5", milvus::OpType::GreaterEqual);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), expect);
    }

    {
        // [6...9]
        int expect = std::accumulate(counts.begin() + 6, counts.end(), 0);
        auto bitset = index->Range("5", milvus::OpType::GreaterThan);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), expect);
    }

    {
        // [0...3]
        int expect = std::accumulate(counts.begin(), counts.begin() + 4, 0);
        auto bitset = index->Range("4", milvus::OpType::LessThan);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), expect);
    }

    {
        // [0...4]
        int expect = std::accumulate(counts.begin(), counts.begin() + 5, 0);
        auto bitset = index->Range("4", milvus::OpType::LessEqual);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), expect);
    }

    {
        // [2...8]
        int expect = std::accumulate(counts.begin() + 2, counts.begin() + 9, 0);
        auto bitset = index->Range("2", true, "8", true);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), expect);
    }

    {
        // [0...8]
        int expect = std::accumulate(counts.begin(), counts.begin() + 9, 0);
        auto bitset = index->Range("0", true, "9", false);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), expect);
    }
}

TEST_F(StringIndexMarisaTest, Reverse) {
    auto index_types = GetIndexTypes<std::string>();
    for (const auto& index_type : index_types) {
        CreateIndexInfo create_index_info{
            .index_type = index_type,
        };
        auto index =
            milvus::index::IndexFactory::GetInstance()
                .CreatePrimitiveScalarIndex<std::string>(create_index_info);
        index->Build(nb, strs.data());
        assert_reverse<std::string>(index.get(), strs);
    }
}

TEST_F(StringIndexMarisaTest, PrefixMatch) {
    auto index = milvus::index::CreateStringIndexMarisa();
    index->Build(nb, strs.data());

    for (size_t i = 0; i < strs.size(); i++) {
        auto str = strs[i];
        auto bitset = index->PrefixMatch(str);
        ASSERT_EQ(bitset.size(), strs.size());
        ASSERT_TRUE(bitset[i]);
    }
}

TEST_F(StringIndexMarisaTest, IsNull) {
    auto index = milvus::index::CreateStringIndexMarisa();
    index->Build(nb, strs.data());
    auto bitset = index->IsNull();
    ASSERT_EQ(bitset.size(), strs.size());
    ASSERT_TRUE(bitset.count() == 0);
}

TEST_F(StringIndexMarisaTest, IsNullHasNull) {
    auto index = milvus::index::CreateStringIndexMarisa();
    FixedVector<bool> is_null(nb);
    for (int i = 0; i < nb; i++) {
        is_null[i] = i % 2 == 0;
    }
    index->Build(nb, strs.data(), is_null.data());
    auto bitset = index->IsNull();
    ASSERT_EQ(bitset.size(), strs.size());
    ASSERT_TRUE(bitset.count() == (nb / 2))
        << "count: " << bitset.count() << " nb: " << nb;
}

TEST_F(StringIndexMarisaTest, IsNotNull) {
    auto index = milvus::index::CreateStringIndexMarisa();
    index->Build(nb, strs.data());
    auto bitset = index->IsNotNull();
    ASSERT_EQ(bitset.size(), strs.size());
    ASSERT_TRUE(bitset.count() == strs.size());
}

TEST_F(StringIndexMarisaTest, IsNotNullHasNull) {
    auto index = milvus::index::CreateStringIndexMarisa();
    FixedVector<bool> is_null(nb);
    for (int i = 0; i < nb; i++) {
        is_null[i] = i % 2 == 0;
    }
    index->Build(nb, strs.data(), is_null.data());
    auto bitset = index->IsNotNull();
    ASSERT_EQ(bitset.size(), strs.size());
    ASSERT_TRUE(bitset.count() == (nb / 2))
        << "count: " << bitset.count() << " nb: " << nb;
}

TEST_F(StringIndexMarisaTest, Query) {
    auto index = milvus::index::CreateStringIndexMarisa();
    index->Build(nb, strs.data());

    {
        auto ds = knowhere::GenDataSet(strs.size(), 8, strs.data());
        ds->Set<milvus::OpType>(milvus::index::OPERATOR_TYPE,
                                milvus::OpType::In);
        auto bitset = index->Query(ds);
        ASSERT_TRUE(Any(bitset));
    }

    {
        auto ds = knowhere::GenDataSet(strs.size(), 8, strs.data());
        ds->Set<milvus::OpType>(milvus::index::OPERATOR_TYPE,
                                milvus::OpType::NotIn);
        auto bitset = index->Query(ds);
        ASSERT_TRUE(BitSetNone(bitset));
    }

    {
        auto ds = std::make_shared<knowhere::DataSet>();
        ds->Set<milvus::OpType>(milvus::index::OPERATOR_TYPE,
                                milvus::OpType::GreaterEqual);
        ds->Set<std::string>(milvus::index::RANGE_VALUE, "0");
        auto bitset = index->Query(ds);
        ASSERT_EQ(bitset.size(), strs.size());
        ASSERT_EQ(Count(bitset), strs.size());
    }

    {
        auto ds = std::make_shared<knowhere::DataSet>();
        ds->Set<milvus::OpType>(milvus::index::OPERATOR_TYPE,
                                milvus::OpType::Range);
        ds->Set<std::string>(milvus::index::LOWER_BOUND_VALUE, "0");
        ds->Set<std::string>(milvus::index::UPPER_BOUND_VALUE, "range");
        ds->Set<bool>(milvus::index::LOWER_BOUND_INCLUSIVE, true);
        ds->Set<bool>(milvus::index::UPPER_BOUND_INCLUSIVE, true);
        auto bitset = index->Query(ds);
        ASSERT_TRUE(Any(bitset));
    }

    {
        for (size_t i = 0; i < strs.size(); i++) {
            auto ds = std::make_shared<knowhere::DataSet>();
            ds->Set<milvus::OpType>(milvus::index::OPERATOR_TYPE,
                                    milvus::OpType::PrefixMatch);
            ds->Set<std::string>(milvus::index::MATCH_VALUE,
                                 std::move(strs[i]));
            auto bitset = index->Query(ds);
            ASSERT_EQ(bitset.size(), strs.size());
            ASSERT_TRUE(bitset[i]);
        }
    }
}

TEST_F(StringIndexMarisaTest, Codec) {
    auto index = milvus::index::CreateStringIndexMarisa();
    std::vector<std::string> strings(nb);
    for (int i = 0; i < nb; ++i) {
        strings[i] = std::to_string(std::rand() % 10);
    }

    index->Build(nb, strings.data());

    std::vector<std::string> invalid_strings = {std::to_string(nb)};
    auto copy_index = milvus::index::CreateStringIndexMarisa();

    {
        auto binary_set = index->Serialize(nullptr);
        copy_index->Load(binary_set);
    }

    {
        auto bitset = copy_index->In(nb, strings.data());
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_TRUE(Any(bitset));
    }

    {
        auto bitset = copy_index->In(1, invalid_strings.data());
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_TRUE(BitSetNone(bitset));
    }

    {
        auto bitset = copy_index->NotIn(nb, strings.data());
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_TRUE(BitSetNone(bitset));
    }

    {
        auto bitset = copy_index->Range("0", milvus::OpType::GreaterEqual);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        auto bitset = copy_index->Range("90", milvus::OpType::LessThan);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        auto bitset = copy_index->Range("9", milvus::OpType::LessEqual);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        auto bitset = copy_index->Range("0", true, "9", true);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        auto bitset = copy_index->Range("0", true, "90", false);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        for (size_t i = 0; i < nb; i++) {
            auto str = strings[i];
            auto bitset = copy_index->PrefixMatch(str);
            ASSERT_EQ(bitset.size(), nb);
            ASSERT_TRUE(bitset[i]);
        }
    }
}

TEST_F(StringIndexMarisaTest, BaseIndexCodec) {
    milvus::index::IndexBasePtr index =
        milvus::index::CreateStringIndexMarisa();
    std::vector<std::string> strings(nb);
    for (int i = 0; i < nb; ++i) {
        strings[i] = std::to_string(std::rand() % 10);
    }
    *str_arr.mutable_data() = {strings.begin(), strings.end()};
    std::vector<uint8_t> data(str_arr.ByteSizeLong(), 0);
    str_arr.SerializeToArray(data.data(), str_arr.ByteSizeLong());
    index->BuildWithRawDataForUT(str_arr.ByteSizeLong(), data.data());

    std::vector<std::string> invalid_strings = {std::to_string(nb)};
    auto copy_index = milvus::index::CreateStringIndexMarisa();

    {
        auto binary_set = index->Serialize(nullptr);
        copy_index->Load(binary_set);
    }

    {
        auto bitset = copy_index->In(nb, strings.data());
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_TRUE(Any(bitset));
    }

    {
        auto bitset = copy_index->In(1, invalid_strings.data());
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_TRUE(BitSetNone(bitset));
    }

    {
        auto bitset = copy_index->NotIn(nb, strings.data());
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_TRUE(BitSetNone(bitset));
    }

    {
        auto bitset = copy_index->Range("0", milvus::OpType::GreaterEqual);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        auto bitset = copy_index->Range("90", milvus::OpType::LessThan);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        auto bitset = copy_index->Range("9", milvus::OpType::LessEqual);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        auto bitset = copy_index->Range("0", true, "9", true);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        auto bitset = copy_index->Range("0", true, "90", false);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        for (size_t i = 0; i < nb; i++) {
            auto str = strings[i];
            auto bitset = copy_index->PrefixMatch(str);
            ASSERT_EQ(bitset.size(), nb);
            ASSERT_TRUE(bitset[i]);
        }
    }
}

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
