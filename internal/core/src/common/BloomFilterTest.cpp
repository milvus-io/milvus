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

#include <gtest/gtest.h>
#include "common/BloomFilter.h"

using namespace milvus;

TEST(BloomFilterTest, BlockedBF_BasicOperations) {
    auto bf = NewBloomFilterWithType(1000, 0.01, BFType::Blocked);

    ASSERT_EQ(bf->Type(), BFType::Blocked);
    ASSERT_GT(bf->Cap(), 0);
    ASSERT_GT(bf->K(), 0);

    bf->Add("hello");
    bf->Add("world");
    bf->Add("milvus");

    EXPECT_TRUE(bf->Test("hello"));
    EXPECT_TRUE(bf->Test("world"));
    EXPECT_TRUE(bf->Test("milvus"));
}

TEST(BloomFilterTest, BlockedBF_Locations) {
    auto bf = NewBloomFilterWithType(1000, 0.01, BFType::Blocked);

    bf->Add("test_key");

    auto locs = Locations("test_key", bf->K(), bf->Type());

    ASSERT_EQ(locs.size(), 1);
    EXPECT_TRUE(bf->TestLocations(locs));

    auto locs2 = Locations("non_existent", bf->K(), bf->Type());
    EXPECT_FALSE(bf->TestLocations(locs2));
}

TEST(BloomFilterTest, BlockedBF_BatchTestLocations) {
    auto bf = NewBloomFilterWithType(1000, 0.01, BFType::Blocked);

    std::vector<std::string> keys = {"key1", "key2", "key3", "key4"};
    std::vector<std::vector<uint64_t>> locs;

    for (size_t i = 0; i < 2; ++i) {
        bf->Add(keys[i]);
    }

    for (const auto& key : keys) {
        locs.push_back(Locations(key, bf->K(), bf->Type()));
    }

    std::vector<bool> hits(keys.size(), false);
    auto results = bf->BatchTestLocations(locs, hits);

    EXPECT_TRUE(results[0]);
    EXPECT_TRUE(results[1]);
    EXPECT_FALSE(results[2]);
    EXPECT_FALSE(results[3]);
}

TEST(BloomFilterTest, BlockedBF_Serialization) {
    auto bf = NewBloomFilterWithType(1000, 0.01, BFType::Blocked);

    bf->Add("serialize_test");
    bf->Add("data");

    nlohmann::json json_data = bf->ToJson();

    auto bf2 = BloomFilterFromJson(json_data);

    ASSERT_EQ(bf2->Type(), BFType::Blocked);
    EXPECT_TRUE(bf2->Test("serialize_test"));
    EXPECT_TRUE(bf2->Test("data"));
    EXPECT_FALSE(bf2->Test("not_added"));
}

TEST(BloomFilterTest, AlwaysTrueBF_BasicOperations) {
    auto bf = NewBloomFilterWithType(0, 0.0, BFType::AlwaysTrue);

    ASSERT_EQ(bf->Type(), BFType::AlwaysTrue);
    ASSERT_EQ(bf->Cap(), 0);
    ASSERT_EQ(bf->K(), 0);

    bf->Add("anything");

    EXPECT_TRUE(bf->Test("anything"));
    EXPECT_TRUE(bf->Test("something"));
    EXPECT_TRUE(bf->Test(""));
}

TEST(BloomFilterTest, AlwaysTrueBF_Locations) {
    auto bf = NewBloomFilterWithType(0, 0.0, BFType::AlwaysTrue);

    auto locs = Locations("test", bf->K(), bf->Type());
    EXPECT_TRUE(locs.empty());

    EXPECT_TRUE(bf->TestLocations({}));
    EXPECT_TRUE(bf->TestLocations({1, 2, 3}));
}

TEST(BloomFilterTest, AlwaysTrueBF_Serialization) {
    auto bf = NewBloomFilterWithType(0, 0.0, BFType::AlwaysTrue);

    nlohmann::json json_data = bf->ToJson();
    auto bf2 = BloomFilterFromJson(json_data);

    ASSERT_EQ(bf2->Type(), BFType::AlwaysTrue);
    EXPECT_TRUE(bf2->Test("anything"));
}

TEST(BloomFilterTest, BlockedBF_EmptyString) {
    auto bf = NewBloomFilterWithType(100, 0.01, BFType::Blocked);

    bf->Add("");
    EXPECT_TRUE(bf->Test(""));
    EXPECT_FALSE(bf->Test("non_empty"));
}

TEST(BloomFilterTest, BlockedBF_SpecialCharacters) {
    auto bf = NewBloomFilterWithType(1000, 0.01, BFType::Blocked);

    std::vector<std::string> special_strings = {"\0\0\0",
                                                "\n\r\t",
                                                "无懈可击",
                                                "🚀🎉🔥",
                                                "\\x00\\xff\\xaa",
                                                "\"'`",
                                                "<>&",
                                                ""};

    for (const auto& s : special_strings) {
        bf->Add(s);
    }

    for (const auto& s : special_strings) {
        EXPECT_TRUE(bf->Test(s)) << "Failed for: " << s;
    }
}

TEST(BloomFilterTest, BlockedBF_BinaryData) {
    auto bf = NewBloomFilterWithType(1000, 0.01, BFType::Blocked);

    uint8_t binary_data1[] = {0x00, 0x01, 0x02, 0xff, 0xfe};
    uint8_t binary_data2[] = {0xaa, 0xbb, 0xcc, 0xdd, 0xee};
    uint8_t binary_data3[] = {0x00, 0x00, 0x00, 0x00};

    bf->Add(binary_data1, sizeof(binary_data1));
    bf->Add(binary_data2, sizeof(binary_data2));
    bf->Add(binary_data3, sizeof(binary_data3));

    EXPECT_TRUE(bf->Test(binary_data1, sizeof(binary_data1)));
    EXPECT_TRUE(bf->Test(binary_data2, sizeof(binary_data2)));
    EXPECT_TRUE(bf->Test(binary_data3, sizeof(binary_data3)));

    uint8_t not_added[] = {0x11, 0x22, 0x33};
    EXPECT_FALSE(bf->Test(not_added, sizeof(not_added)));
}

TEST(BloomFilterTest, BlockedBF_IntegerTypes) {
    auto bf = NewBloomFilterWithType(1000, 0.01, BFType::Blocked);

    int32_t int32_vals[] = {0, -1, 1, INT32_MAX, INT32_MIN};
    for (auto val : int32_vals) {
        bf->Add(reinterpret_cast<const uint8_t*>(&val), sizeof(val));
    }
    for (auto val : int32_vals) {
        EXPECT_TRUE(
            bf->Test(reinterpret_cast<const uint8_t*>(&val), sizeof(val)));
    }

    int64_t int64_vals[] = {0L, -1L, 1L, INT64_MAX, INT64_MIN};
    for (auto val : int64_vals) {
        bf->Add(reinterpret_cast<const uint8_t*>(&val), sizeof(val));
    }
    for (auto val : int64_vals) {
        EXPECT_TRUE(
            bf->Test(reinterpret_cast<const uint8_t*>(&val), sizeof(val)));
    }
}

TEST(BloomFilterTest, BlockedBF_VerySmallCapacity) {
    auto bf = NewBloomFilterWithType(1, 0.01, BFType::Blocked);

    bf->Add("test");
    EXPECT_TRUE(bf->Test("test"));
}

TEST(BloomFilterTest, BlockedBF_VeryLargeCapacity) {
    auto bf = NewBloomFilterWithType(1000000, 0.01, BFType::Blocked);

    ASSERT_GT(bf->Cap(), 0);
    ASSERT_GT(bf->K(), 0);

    bf->Add("test");
    EXPECT_TRUE(bf->Test("test"));
}

TEST(BloomFilterTest, BlockedBF_FalsePositiveRate) {
    const int capacity = 10000;
    const double expected_fp_rate = 0.01;
    auto bf =
        NewBloomFilterWithType(capacity, expected_fp_rate, BFType::Blocked);

    std::vector<std::string> added_keys;
    for (int i = 0; i < capacity; ++i) {
        std::string key = "key_" + std::to_string(i);
        added_keys.push_back(key);
        bf->Add(key);
    }

    for (const auto& key : added_keys) {
        EXPECT_TRUE(bf->Test(key));
    }

    int false_positives = 0;
    const int test_count = 10000;
    for (int i = 0; i < test_count; ++i) {
        std::string test_key = "test_key_" + std::to_string(i + capacity);
        if (bf->Test(test_key)) {
            false_positives++;
        }
    }

    double actual_fp_rate = static_cast<double>(false_positives) / test_count;
    EXPECT_LT(actual_fp_rate, expected_fp_rate * 3);
}

TEST(BloomFilterTest, BlockedBF_SerializationIntegrity) {
    auto bf1 = NewBloomFilterWithType(5000, 0.01, BFType::Blocked);

    std::vector<std::string> test_data;
    for (int i = 0; i < 1000; ++i) {
        test_data.push_back("key_" + std::to_string(i));
        bf1->Add(test_data.back());
    }

    nlohmann::json json_data = bf1->ToJson();

    ASSERT_TRUE(json_data.contains("type"));
    ASSERT_TRUE(json_data.contains("bits"));
    ASSERT_TRUE(json_data.contains("num_bits"));
    ASSERT_TRUE(json_data.contains("k"));

    auto bf2 = BloomFilterFromJson(json_data);

    EXPECT_EQ(bf1->Type(), bf2->Type());
    EXPECT_EQ(bf1->Cap(), bf2->Cap());
    EXPECT_EQ(bf1->K(), bf2->K());

    for (const auto& key : test_data) {
        EXPECT_TRUE(bf2->Test(key));
    }
}

TEST(BloomFilterTest, BlockedBF_InvalidJsonDeserialization) {
    nlohmann::json invalid_json1;
    invalid_json1["bits"] = std::vector<uint64_t>{1, 2, 3};
    invalid_json1["num_bits"] = 192;
    invalid_json1["k"] = 7;
    EXPECT_THROW(BloomFilterFromJson(invalid_json1), std::runtime_error);

    nlohmann::json invalid_json2;
    invalid_json2["type"] = "BlockedBloomFilter";
    invalid_json2["num_bits"] = 192;
    invalid_json2["k"] = 7;
    EXPECT_THROW(BloomFilterFromJson(invalid_json2), std::runtime_error);

    nlohmann::json invalid_json3;
    invalid_json3["type"] = "UnknownType";
    EXPECT_THROW(BloomFilterFromJson(invalid_json3), std::runtime_error);
}

TEST(BloomFilterTest, LocationsConsistency) {
    std::string data1 = "consistent_test";
    std::string data2 = "consistent_test";

    auto locs1 = Locations(data1, 7, BFType::Blocked);
    auto locs2 = Locations(data2, 7, BFType::Blocked);

    ASSERT_EQ(locs1.size(), locs2.size());
    for (size_t i = 0; i < locs1.size(); ++i) {
        EXPECT_EQ(locs1[i], locs2[i]);
    }

    std::string data3 = "different_test";
    auto locs3 = Locations(data3, 7, BFType::Blocked);
    EXPECT_NE(locs1, locs3);
}
