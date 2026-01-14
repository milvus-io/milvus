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
#include <vector>
#include <optional>
#include <variant>
#include <string>
#include <cmath>

#include "segcore/ReduceUtils.h"
#include "segcore/ReduceStructure.h"
#include "common/Types.h"
#include "plan/PlanNode.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::plan;

// ====================================================================================
// CompareOrderByValue Tests
// ====================================================================================

TEST(OrderBy, CompareOrderByValueInt64) {
    // Test int64 comparison
    OrderByValueType val1 = MakeOrderByValue(int64_t(100));
    OrderByValueType val2 = MakeOrderByValue(int64_t(200));
    OrderByValueType val3 = MakeOrderByValue(int64_t(100));

    EXPECT_EQ(-1, CompareOrderByValue(val1, val2));  // 100 < 200
    EXPECT_EQ(1, CompareOrderByValue(val2, val1));   // 200 > 100
    EXPECT_EQ(0, CompareOrderByValue(val1, val3));   // 100 == 100
}

TEST(OrderBy, CompareOrderByValueInt32) {
    OrderByValueType val1 = MakeOrderByValue(int32_t(50));
    OrderByValueType val2 = MakeOrderByValue(int32_t(100));

    EXPECT_EQ(-1, CompareOrderByValue(val1, val2));
    EXPECT_EQ(1, CompareOrderByValue(val2, val1));
}

TEST(OrderBy, CompareOrderByValueFloat) {
    OrderByValueType val1 = MakeOrderByValue(1.5f);
    OrderByValueType val2 = MakeOrderByValue(2.5f);
    OrderByValueType val3 = MakeOrderByValue(1.5f);

    EXPECT_EQ(-1, CompareOrderByValue(val1, val2));
    EXPECT_EQ(1, CompareOrderByValue(val2, val1));
    EXPECT_EQ(0, CompareOrderByValue(val1, val3));
}

TEST(OrderBy, CompareOrderByValueDouble) {
    OrderByValueType val1 =
        std::make_optional(std::variant<std::monostate,
                                        int8_t,
                                        int16_t,
                                        int32_t,
                                        int64_t,
                                        bool,
                                        float,
                                        double,
                                        std::string>(double(1.5)));
    OrderByValueType val2 =
        std::make_optional(std::variant<std::monostate,
                                        int8_t,
                                        int16_t,
                                        int32_t,
                                        int64_t,
                                        bool,
                                        float,
                                        double,
                                        std::string>(double(2.5)));

    EXPECT_EQ(-1, CompareOrderByValue(val1, val2));
    EXPECT_EQ(1, CompareOrderByValue(val2, val1));
}

TEST(OrderBy, CompareOrderByValueString) {
    OrderByValueType val1 = MakeOrderByValue(std::string("apple"));
    OrderByValueType val2 = MakeOrderByValue(std::string("banana"));
    OrderByValueType val3 = MakeOrderByValue(std::string("apple"));

    EXPECT_EQ(-1, CompareOrderByValue(val1, val2));  // "apple" < "banana"
    EXPECT_EQ(1, CompareOrderByValue(val2, val1));   // "banana" > "apple"
    EXPECT_EQ(0, CompareOrderByValue(val1, val3));   // "apple" == "apple"
}

TEST(OrderBy, CompareOrderByValueBool) {
    OrderByValueType val1 = MakeOrderByValue(false);
    OrderByValueType val2 = MakeOrderByValue(true);

    EXPECT_EQ(-1, CompareOrderByValue(val1, val2));  // false < true
    EXPECT_EQ(1, CompareOrderByValue(val2, val1));   // true > false
}

TEST(OrderBy, CompareOrderByValueNullHandling) {
    OrderByValueType nullVal = std::nullopt;
    OrderByValueType nonNullVal = MakeOrderByValue(int64_t(100));

    // null < non-null
    EXPECT_EQ(-1, CompareOrderByValue(nullVal, nonNullVal));
    // non-null > null
    EXPECT_EQ(1, CompareOrderByValue(nonNullVal, nullVal));
    // null == null
    EXPECT_EQ(0, CompareOrderByValue(nullVal, nullVal));
}

TEST(OrderBy, CompareOrderByValueNaN) {
    // Test NaN handling for float
    OrderByValueType nanVal = MakeOrderByValue(std::nanf(""));
    OrderByValueType normalVal = MakeOrderByValue(1.0f);

    // NaN is treated as less than non-NaN
    EXPECT_EQ(-1, CompareOrderByValue(nanVal, normalVal));
    EXPECT_EQ(1, CompareOrderByValue(normalVal, nanVal));
}

// ====================================================================================
// SearchResultPairComparator Tests
// ====================================================================================

TEST(OrderBy, SearchResultPairComparatorByDistance) {
    // Test comparator without order_by (falls back to distance comparison)
    SearchResultPairComparator comparator;

    SearchResultPair pair1(PkType(int64_t(1)), 0.5f, nullptr, 0, 0, 1);
    SearchResultPair pair2(PkType(int64_t(2)), 0.8f, nullptr, 0, 0, 1);

    // For min-heap: pair1 (0.5) should NOT be "greater than" pair2 (0.8)
    // The comparator returns true if lhs should come AFTER rhs in the heap
    EXPECT_TRUE(
        comparator(&pair1, &pair2));  // 0.5 < 0.8, so pair1 has higher priority
    EXPECT_FALSE(comparator(&pair2, &pair1));  // 0.8 > 0.5
}

TEST(OrderBy, SearchResultPairComparatorByOrderByField) {
    // Test comparator with order_by field
    std::vector<OrderByField> orderByFields;
    orderByFields.push_back(
        OrderByField{FieldId(100), true, std::nullopt});  // Ascending

    SearchResultPairComparator comparator(orderByFields);

    SearchResultPair pair1(PkType(int64_t(1)), 0.5f, nullptr, 0, 0, 1);
    SearchResultPair pair2(PkType(int64_t(2)), 0.8f, nullptr, 0, 0, 1);

    // Set order_by values
    pair1.order_by_values_ =
        std::vector<OrderByValueType>{MakeOrderByValue(int64_t(100))};
    pair2.order_by_values_ =
        std::vector<OrderByValueType>{MakeOrderByValue(int64_t(200))};

    // With ASC order, pair1 (100) is better than pair2 (200)
    // For priority_queue: comp(better, worse) = false (better stays at top)
    EXPECT_FALSE(comparator(&pair1, &pair2));
    EXPECT_TRUE(comparator(&pair2, &pair1));
}

TEST(OrderBy, SearchResultPairComparatorByOrderByFieldDescending) {
    // Test comparator with descending order_by field
    std::vector<OrderByField> orderByFields;
    orderByFields.push_back(
        OrderByField{FieldId(100), false, std::nullopt});  // Descending

    SearchResultPairComparator comparator(orderByFields);

    SearchResultPair pair1(PkType(int64_t(1)), 0.5f, nullptr, 0, 0, 1);
    SearchResultPair pair2(PkType(int64_t(2)), 0.8f, nullptr, 0, 0, 1);

    pair1.order_by_values_ =
        std::vector<OrderByValueType>{MakeOrderByValue(int64_t(100))};
    pair2.order_by_values_ =
        std::vector<OrderByValueType>{MakeOrderByValue(int64_t(200))};

    // With DESC order, pair2 (200) is better than pair1 (100)
    // For priority_queue: comp(worse, better) = true (better goes to top)
    EXPECT_TRUE(comparator(&pair1, &pair2));
    EXPECT_FALSE(comparator(&pair2, &pair1));
}

TEST(OrderBy, SearchResultPairComparatorMultipleFields) {
    // Test comparator with multiple order_by fields
    std::vector<OrderByField> orderByFields;
    orderByFields.push_back(
        OrderByField{FieldId(100), true, std::nullopt});  // First field ASC
    orderByFields.push_back(
        OrderByField{FieldId(101), false, std::nullopt});  // Second field DESC

    SearchResultPairComparator comparator(orderByFields);

    SearchResultPair pair1(PkType(int64_t(1)), 0.5f, nullptr, 0, 0, 1);
    SearchResultPair pair2(PkType(int64_t(2)), 0.8f, nullptr, 0, 0, 1);

    // Same first field value, different second field
    pair1.order_by_values_ = std::vector<OrderByValueType>{
        MakeOrderByValue(int64_t(100)), MakeOrderByValue(int64_t(50))};
    pair2.order_by_values_ = std::vector<OrderByValueType>{
        MakeOrderByValue(int64_t(100)), MakeOrderByValue(int64_t(80))};

    // First field is equal, second field: pair2 (80) > pair1 (50) with DESC
    // So pair2 is better (larger value with DESC order)
    // For priority_queue: comp(worse, better) = true
    EXPECT_TRUE(comparator(&pair1, &pair2));
    EXPECT_FALSE(comparator(&pair2, &pair1));
}

TEST(OrderBy, SearchResultPairComparatorTieBreaker) {
    // Test that distance is used as tie-breaker when order_by values are equal
    std::vector<OrderByField> orderByFields;
    orderByFields.push_back(OrderByField{FieldId(100), true, std::nullopt});

    // Test with L2 metric (smaller distance is better)
    SearchResultPairComparator comparator_l2(orderByFields,
                                             knowhere::metric::L2);

    SearchResultPair pair1(PkType(int64_t(1)), 0.5f, nullptr, 0, 0, 1);
    SearchResultPair pair2(PkType(int64_t(2)), 0.8f, nullptr, 0, 0, 1);

    // Same order_by value
    pair1.order_by_values_ =
        std::vector<OrderByValueType>{MakeOrderByValue(int64_t(100))};
    pair2.order_by_values_ =
        std::vector<OrderByValueType>{MakeOrderByValue(int64_t(100))};

    // Same order_by value, should fall back to distance comparison
    // For L2: pair1 (0.5) is better than pair2 (0.8) - smaller distance is better
    // For priority_queue: comp(better, worse) = false
    EXPECT_FALSE(comparator_l2(&pair1, &pair2));
    EXPECT_TRUE(comparator_l2(&pair2, &pair1));

    // Test with IP metric (larger distance is better)
    SearchResultPairComparator comparator_ip(orderByFields,
                                             knowhere::metric::IP);

    // For IP: pair2 (0.8) is better than pair1 (0.5) - larger distance is better
    // For priority_queue: comp(worse, better) = true
    EXPECT_TRUE(comparator_ip(&pair1, &pair2));
    EXPECT_FALSE(comparator_ip(&pair2, &pair1));
}

// ====================================================================================
// OrderByFieldReader Tests (basic structure tests)
// ====================================================================================

TEST(OrderBy, OrderByFieldReaderEmpty) {
    std::vector<OrderByField> emptyFields;
    OrderByFieldReader reader(emptyFields);

    EXPECT_TRUE(reader.Empty());
    EXPECT_EQ(0, reader.Size());
}

TEST(OrderBy, OrderByFieldReaderNonEmpty) {
    std::vector<OrderByField> orderByFields;
    orderByFields.push_back(OrderByField{FieldId(100), true, std::nullopt});
    orderByFields.push_back(OrderByField{FieldId(101), false, std::nullopt});

    OrderByFieldReader reader(orderByFields);

    EXPECT_FALSE(reader.Empty());
    EXPECT_EQ(2, reader.Size());
}

// ====================================================================================
// Integration-style tests for sorting behavior
// ====================================================================================

TEST(OrderBy, SortingWithOrderByValuesAscending) {
    // Simulate sorting SearchResultPairs by order_by values
    std::vector<OrderByField> orderByFields;
    orderByFields.push_back(
        OrderByField{FieldId(100), true, std::nullopt});  // Ascending

    SearchResultPairComparator comparator(orderByFields);

    // Create pairs with different order_by values
    std::vector<SearchResultPair> pairs;
    pairs.emplace_back(PkType(int64_t(1)), 0.9f, nullptr, 0, 0, 1);
    pairs.emplace_back(PkType(int64_t(2)), 0.8f, nullptr, 0, 0, 1);
    pairs.emplace_back(PkType(int64_t(3)), 0.7f, nullptr, 0, 0, 1);

    // Set order_by values: 300, 100, 200
    pairs[0].order_by_values_ =
        std::vector<OrderByValueType>{MakeOrderByValue(int64_t(300))};
    pairs[1].order_by_values_ =
        std::vector<OrderByValueType>{MakeOrderByValue(int64_t(100))};
    pairs[2].order_by_values_ =
        std::vector<OrderByValueType>{MakeOrderByValue(int64_t(200))};

    // Sort using pointers (as used in priority queue)
    std::vector<SearchResultPair*> ptrPairs = {&pairs[0], &pairs[1], &pairs[2]};
    std::sort(ptrPairs.begin(),
              ptrPairs.end(),
              [&comparator](SearchResultPair* a, SearchResultPair* b) {
                  return !comparator(a, b);  // Reverse for ascending sort
              });

    // Expected order after ASC sort: 100 (ID2), 200 (ID3), 300 (ID1)
    EXPECT_EQ(int64_t(2), std::get<int64_t>(ptrPairs[0]->primary_key_));
    EXPECT_EQ(int64_t(3), std::get<int64_t>(ptrPairs[1]->primary_key_));
    EXPECT_EQ(int64_t(1), std::get<int64_t>(ptrPairs[2]->primary_key_));
}

TEST(OrderBy, SortingWithOrderByValuesDescending) {
    std::vector<OrderByField> orderByFields;
    orderByFields.push_back(
        OrderByField{FieldId(100), false, std::nullopt});  // Descending

    SearchResultPairComparator comparator(orderByFields);

    std::vector<SearchResultPair> pairs;
    pairs.emplace_back(PkType(int64_t(1)), 0.9f, nullptr, 0, 0, 1);
    pairs.emplace_back(PkType(int64_t(2)), 0.8f, nullptr, 0, 0, 1);
    pairs.emplace_back(PkType(int64_t(3)), 0.7f, nullptr, 0, 0, 1);

    pairs[0].order_by_values_ =
        std::vector<OrderByValueType>{MakeOrderByValue(int64_t(300))};
    pairs[1].order_by_values_ =
        std::vector<OrderByValueType>{MakeOrderByValue(int64_t(100))};
    pairs[2].order_by_values_ =
        std::vector<OrderByValueType>{MakeOrderByValue(int64_t(200))};

    std::vector<SearchResultPair*> ptrPairs = {&pairs[0], &pairs[1], &pairs[2]};
    std::sort(ptrPairs.begin(),
              ptrPairs.end(),
              [&comparator](SearchResultPair* a, SearchResultPair* b) {
                  return !comparator(a, b);
              });

    // Expected order after DESC sort: 300 (ID1), 200 (ID3), 100 (ID2)
    EXPECT_EQ(int64_t(1), std::get<int64_t>(ptrPairs[0]->primary_key_));
    EXPECT_EQ(int64_t(3), std::get<int64_t>(ptrPairs[1]->primary_key_));
    EXPECT_EQ(int64_t(2), std::get<int64_t>(ptrPairs[2]->primary_key_));
}

TEST(OrderBy, SortingWithNullValues) {
    std::vector<OrderByField> orderByFields;
    orderByFields.push_back(
        OrderByField{FieldId(100), true, std::nullopt});  // Ascending

    SearchResultPairComparator comparator(orderByFields);

    std::vector<SearchResultPair> pairs;
    pairs.emplace_back(PkType(int64_t(1)), 0.9f, nullptr, 0, 0, 1);
    pairs.emplace_back(PkType(int64_t(2)), 0.8f, nullptr, 0, 0, 1);
    pairs.emplace_back(PkType(int64_t(3)), 0.7f, nullptr, 0, 0, 1);

    // ID1: null, ID2: 100, ID3: 200
    pairs[0].order_by_values_ = std::vector<OrderByValueType>{std::nullopt};
    pairs[1].order_by_values_ =
        std::vector<OrderByValueType>{MakeOrderByValue(int64_t(100))};
    pairs[2].order_by_values_ =
        std::vector<OrderByValueType>{MakeOrderByValue(int64_t(200))};

    std::vector<SearchResultPair*> ptrPairs = {&pairs[0], &pairs[1], &pairs[2]};
    std::sort(ptrPairs.begin(),
              ptrPairs.end(),
              [&comparator](SearchResultPair* a, SearchResultPair* b) {
                  return !comparator(a, b);
              });

    // Expected order with ASC (null first): null (ID1), 100 (ID2), 200 (ID3)
    EXPECT_EQ(int64_t(1), std::get<int64_t>(ptrPairs[0]->primary_key_));
    EXPECT_EQ(int64_t(2), std::get<int64_t>(ptrPairs[1]->primary_key_));
    EXPECT_EQ(int64_t(3), std::get<int64_t>(ptrPairs[2]->primary_key_));
}
