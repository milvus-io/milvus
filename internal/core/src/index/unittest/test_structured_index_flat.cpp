// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <gtest/gtest.h>
#include <algorithm>
#include <chrono>
#include <iostream>
#include <random>
#include <sstream>

#include "knowhere/index/structured_index/StructuredIndexFlat.h"

#include "unittest/utils.h"

static void
gen_rand_data(int range, int n, int*& p) {
    srand((unsigned int)time(nullptr));
    p = (int*)malloc(n * sizeof(int));
    int* q = p;
    for (auto i = 0; i < n; ++i) {
        *q++ = (int)random() % range;
    }
}

static void
gen_rand_int64_data(int64_t range, int64_t n, int64_t*& p) {
    srand((int64_t)time(nullptr));
    p = (int64_t*)malloc(n * sizeof(int64_t));
    int64_t* q = p;
    for (auto i = 0; i < n; ++i) {
        *q++ = (int64_t)random() % range;
    }
}

static void
gen_rand_double_data(double range, int64_t n, double*& p) {
    std::uniform_real_distribution<double> unif(0, range);
    std::default_random_engine re;
    p = (double*)malloc(n * sizeof(double));
    double* q = p;
    for (auto i = 0; i < n; ++i) {
        *q++ = unif(re);
    }
}

TEST(STRUCTUREDINDEXFLAT_TEST, test_build) {
    int range = 100, n = 1000, *p = nullptr;
    gen_rand_data(range, n, p);

    milvus::knowhere::StructuredIndexFlat<int> structuredIndexFlat((size_t)n, p);  // Build default
    const std::vector<milvus::knowhere::IndexStructure<int>> index_data = structuredIndexFlat.GetData();
    for (auto i = 0; i < n; ++i) {
        ASSERT_EQ(*(p + i), index_data[i].a_);
    }
    free(p);
}

TEST(STRUCTUREDINDEXFLAT_TEST, test_in) {
    int range = 1000, n = 1000, *p = nullptr;
    gen_rand_data(range, n, p);
    milvus::knowhere::StructuredIndexFlat<int> structuredIndexFlat((size_t)n, p);  // Build default

    int test_times = 10;
    std::vector<int> test_vals, test_off;
    test_vals.reserve(test_times);
    test_off.reserve(test_times);
    for (auto i = 0; i < test_times; ++i) {
        auto off = random() % n;
        test_vals.emplace_back(*(p + off));
        test_off.emplace_back(off);
    }
    auto res = structuredIndexFlat.In(test_times, test_vals.data());
    for (auto i = 0; i < test_times; ++i) {
        ASSERT_EQ(true, res->test(test_off[i]));
    }

    free(p);
}

TEST(STRUCTUREDINDEXFLAT_TEST, test_not_in) {
    int range = 10000, n = 1000, *p = nullptr;
    gen_rand_data(range, n, p);
    milvus::knowhere::StructuredIndexFlat<int> structuredIndexFlat((size_t)n, p);  // Build default

    int test_times = 10;
    std::vector<int> test_vals, test_off;
    test_vals.reserve(test_times);
    test_off.reserve(test_times);
    for (auto i = 0; i < test_times; ++i) {
        auto off = random() % n;
        test_vals.emplace_back(*(p + off));
        test_off.emplace_back(off);
    }
    auto res = structuredIndexFlat.NotIn(test_times, test_vals.data());
    for (auto i = 0; i < test_times; ++i) {
        ASSERT_EQ(false, res->test(test_off[i]));
    }

    free(p);
}

TEST(STRUCTUREDINDEXFLAT_TEST, test_single_border_range) {
    int range = 100, n = 1000, *p = nullptr;
    gen_rand_data(range, n, p);
    milvus::knowhere::StructuredIndexFlat<int> structuredIndexFlat((size_t)n, p);  // Build default

    srand((unsigned int)time(nullptr));
    int val;
    // test LT
    val = 10000;
    auto lt_res = structuredIndexFlat.Range(val, milvus::knowhere::OperatorType::LT);
    for (auto i = 0; i < n; ++i) {
        if (*(p + i) < val)
            ASSERT_EQ(true, lt_res->test(i));
        else {
            ASSERT_EQ(false, lt_res->test(i));
        }
    }
    // test LE
    val = (int)random() % 100;

    auto le_res = structuredIndexFlat.Range(val, milvus::knowhere::OperatorType::LE);
    for (auto i = 0; i < n; ++i) {
        if (*(p + i) <= val)
            ASSERT_EQ(true, le_res->test(i));
        else {
            ASSERT_EQ(false, le_res->test(i));
        }
    }
    // test GE
    val = (int)random() % 100;
    auto ge_res = structuredIndexFlat.Range(val, milvus::knowhere::OperatorType::GE);
    for (auto i = 0; i < n; ++i) {
        if (*(p + i) >= val)
            ASSERT_EQ(true, ge_res->test(i));
        else
            ASSERT_EQ(false, ge_res->test(i));
    }
    // test GT
    val = (int)random() % 100;
    auto gt_res = structuredIndexFlat.Range(val, milvus::knowhere::OperatorType::GT);
    for (auto i = 0; i < n; ++i) {
        if (*(p + i) > val)
            ASSERT_EQ(true, gt_res->test(i));
        else
            ASSERT_EQ(false, gt_res->test(i));
    }

    free(p);
}

TEST(STRUCTUREDINDEXFLAT_TEST, test_double_border_range) {
    int range = 100, n = 1000, *p = nullptr;
    gen_rand_data(range, n, p);
    milvus::knowhere::StructuredIndexFlat<int> structuredIndexFlat((size_t)n, p);  // Build default

    srand((unsigned int)time(nullptr));
    int lb, ub;
    // []
    lb = (int)random() % 100;
    ub = (int)random() % 100;
    if (lb > ub)
        std::swap(lb, ub);
    auto res1 = structuredIndexFlat.Range(lb, true, ub, true);
    for (auto i = 0; i < n; ++i) {
        if (*(p + i) >= lb && *(p + i) <= ub)
            ASSERT_EQ(true, res1->test(i));
        else
            ASSERT_EQ(false, res1->test(i));
    }
    // [)
    lb = (int)random() % 100;
    ub = (int)random() % 100;
    if (lb > ub)
        std::swap(lb, ub);
    auto res2 = structuredIndexFlat.Range(lb, true, ub, false);
    for (auto i = 0; i < n; ++i) {
        if (*(p + i) >= lb && *(p + i) < ub)
            ASSERT_EQ(true, res2->test(i));
        else
            ASSERT_EQ(false, res2->test(i));
    }
    // (]
    lb = (int)random() % 100;
    ub = (int)random() % 100;
    if (lb > ub)
        std::swap(lb, ub);
    auto res3 = structuredIndexFlat.Range(lb, false, ub, true);
    for (auto i = 0; i < n; ++i) {
        if (*(p + i) > lb && *(p + i) <= ub)
            ASSERT_EQ(true, res3->test(i));
        else
            ASSERT_EQ(false, res3->test(i));
    }
    // ()
    lb = (int)random() % 100;
    ub = (int)random() % 100;
    if (lb > ub)
        std::swap(lb, ub);
    auto res4 = structuredIndexFlat.Range(lb, false, ub, false);
    for (auto i = 0; i < n; ++i) {
        if (*(p + i) > lb && *(p + i) < ub)
            ASSERT_EQ(true, res4->test(i));
        else
            ASSERT_EQ(false, res4->test(i));
    }
    free(p);
}
