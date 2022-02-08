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

#include "knowhere/index/structured_index/StructuredIndexSort.h"

#include "unittest/utils.h"

void
gen_rand_data(int range, int n, int*& p) {
    srand((unsigned int)time(nullptr));
    p = (int*)malloc(n * sizeof(int));
    int* q = p;
    for (auto i = 0; i < n; ++i) {
        *q++ = (int)random() % range;
    }
}

void
gen_rand_int64_data(int64_t range, int64_t n, int64_t*& p) {
    srand((int64_t)time(nullptr));
    p = (int64_t*)malloc(n * sizeof(int64_t));
    int64_t* q = p;
    for (auto i = 0; i < n; ++i) {
        *q++ = (int64_t)random() % range;
    }
}

void
gen_rand_double_data(double range, int64_t n, double*& p) {
    std::uniform_real_distribution<double> unif(0, range);
    std::default_random_engine re;
    p = (double*)malloc(n * sizeof(double));
    double* q = p;
    for (auto i = 0; i < n; ++i) {
        *q++ = unif(re);
    }
}

TEST(STRUCTUREDINDEXSORT_TEST, test_build) {
    int range = 100, n = 1000, *p = nullptr;
    gen_rand_data(range, n, p);

    milvus::knowhere::StructuredIndexSort<int> structuredIndexSort((size_t)n, p);  // Build default
    std::sort(p, p + n);
    const std::vector<milvus::knowhere::IndexStructure<int>> index_data = structuredIndexSort.GetData();
    for (auto i = 0; i < n; ++i) {
        ASSERT_EQ(*(p + i), index_data[i].a_);
    }
    free(p);
}

TEST(STRUCTUREDINDEXSORT_TEST, test_serialize_and_load) {
    auto serialize = [](const std::string& filename, milvus::knowhere::BinaryPtr& bin, uint8_t* ret) {
        {
            // write and flush
            FileIOWriter writer(filename);
            writer(static_cast<void*>(bin->data.get()), bin->size);
        }

        FileIOReader reader(filename);
        reader(ret, bin->size);
    };

    int range = 100, n = 1000, *p = nullptr;
    gen_rand_data(range, n, p);

    milvus::knowhere::StructuredIndexSort<int> structuredIndexSort((size_t)n, p);  // Build default
    auto binaryset = structuredIndexSort.Serialize();

    auto bin_data = binaryset.GetByName("index_data");
    std::string data_file = "/tmp/sort_test_data_serialize.bin";
    auto load_data = new uint8_t[bin_data->size];
    serialize(data_file, bin_data, load_data);

    auto bin_length = binaryset.GetByName("index_length");
    std::string length_file = "/tmp/sort_test_length_serialize.bin";
    auto load_length = new uint8_t[bin_length->size];
    serialize(length_file, bin_length, load_length);

    binaryset.clear();
    std::shared_ptr<uint8_t[]> index_data(load_data);
    binaryset.Append("index_data", index_data, bin_data->size);

    std::shared_ptr<uint8_t[]> length_data(load_length);
    binaryset.Append("index_length", length_data, bin_length->size);

    structuredIndexSort.Load(binaryset);
    EXPECT_EQ(n * sizeof(milvus::knowhere::IndexStructure<int>), (int)structuredIndexSort.Size());
    EXPECT_EQ(true, structuredIndexSort.IsBuilt());
    std::sort(p, p + n);
    const std::vector<milvus::knowhere::IndexStructure<int>> const_index_data = structuredIndexSort.GetData();
    for (auto i = 0; i < n; ++i) {
        ASSERT_EQ(*(p + i), const_index_data[i].a_);
    }

    free(p);
}

TEST(STRUCTUREDINDEXSORT_TEST, test_in) {
    int range = 1000, n = 1000, *p = nullptr;
    gen_rand_data(range, n, p);
    milvus::knowhere::StructuredIndexSort<int> structuredIndexSort((size_t)n, p);  // Build default

    int test_times = 10;
    std::vector<int> test_vals, test_off;
    test_vals.reserve(test_times);
    test_off.reserve(test_times);
    //    std::cout << "STRUCTUREDINDEXSORT_TEST test_in" << std::endl;
    for (auto i = 0; i < test_times; ++i) {
        auto off = random() % n;
        test_vals.emplace_back(*(p + off));
        test_off.emplace_back(off);
        //        std::cout << "val: " << *(p + off) << ", off: " << off << std::endl;
    }
    auto res = structuredIndexSort.In(test_times, test_vals.data());
    for (auto i = 0; i < test_times; ++i) {
        //        std::cout << test_off[i] << " ";
        ASSERT_EQ(true, res->test(test_off[i]));
    }

    free(p);
}

TEST(STRUCTUREDINDEXSORT_TEST, test_not_in) {
    int range = 10000, n = 1000, *p = nullptr;
    gen_rand_data(range, n, p);
    milvus::knowhere::StructuredIndexSort<int> structuredIndexSort((size_t)n, p);  // Build default

    int test_times = 10;
    std::vector<int> test_vals, test_off;
    test_vals.reserve(test_times);
    test_off.reserve(test_times);
    // std::cout << "STRUCTUREDINDEXSORT_TEST test_notin" << std::endl;
    for (auto i = 0; i < test_times; ++i) {
        auto off = random() % n;
        test_vals.emplace_back(*(p + off));
        test_off.emplace_back(off);
        // std::cout << off << " ";
    }
    // std::cout << std::endl;
    auto res = structuredIndexSort.NotIn(test_times, test_vals.data());
    // std::cout << "assert values: " << std::endl;
    for (auto i = 0; i < test_times; ++i) {
        // std::cout << test_off[i] << " ";
        ASSERT_EQ(false, res->test(test_off[i]));
    }
    // std::cout << std::endl;

    free(p);
}

TEST(STRUCTUREDINDEXSORT_TEST, test_single_border_range) {
    int range = 100, n = 1000, *p = nullptr;
    gen_rand_data(range, n, p);
    milvus::knowhere::StructuredIndexSort<int> structuredIndexSort((size_t)n, p);  // Build default

    srand((unsigned int)time(nullptr));
    int val;
    // test LT
    val = (int)random() % 100;
    auto lt_res = structuredIndexSort.Range(val, milvus::knowhere::OperatorType::LT);
    for (auto i = 0; i < n; ++i) {
        if (*(p + i) < val)
            ASSERT_EQ(true, lt_res->test(i));
        else
            ASSERT_EQ(false, lt_res->test(i));
    }
    // test LE
    val = (int)random() % 100;
    auto le_res = structuredIndexSort.Range(val, milvus::knowhere::OperatorType::LE);
    for (auto i = 0; i < n; ++i) {
        if (*(p + i) <= val)
            ASSERT_EQ(true, le_res->test(i));
        else
            ASSERT_EQ(false, le_res->test(i));
    }
    // test GE
    val = (int)random() % 100;
    auto ge_res = structuredIndexSort.Range(val, milvus::knowhere::OperatorType::GE);
    for (auto i = 0; i < n; ++i) {
        if (*(p + i) >= val)
            ASSERT_EQ(true, ge_res->test(i));
        else
            ASSERT_EQ(false, ge_res->test(i));
    }
    // test GT
    val = (int)random() % 100;
    auto gt_res = structuredIndexSort.Range(val, milvus::knowhere::OperatorType::GT);
    for (auto i = 0; i < n; ++i) {
        if (*(p + i) > val)
            ASSERT_EQ(true, gt_res->test(i));
        else
            ASSERT_EQ(false, gt_res->test(i));
    }

    free(p);
}

TEST(STRUCTUREDINDEXSORT_TEST, test_double_border_range) {
    int range = 100, n = 1000, *p = nullptr;
    gen_rand_data(range, n, p);
    milvus::knowhere::StructuredIndexSort<int> structuredIndexSort((size_t)n, p);  // Build default

    srand((unsigned int)time(nullptr));
    int lb, ub;
    // []
    lb = (int)random() % 100;
    ub = (int)random() % 100;
    if (lb > ub)
        std::swap(lb, ub);
    auto res1 = structuredIndexSort.Range(lb, true, ub, true);
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
    auto res2 = structuredIndexSort.Range(lb, true, ub, false);
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
    auto res3 = structuredIndexSort.Range(lb, false, ub, true);
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
    auto res4 = structuredIndexSort.Range(lb, false, ub, false);
    for (auto i = 0; i < n; ++i) {
        if (*(p + i) > lb && *(p + i) < ub)
            ASSERT_EQ(true, res4->test(i));
        else
            ASSERT_EQ(false, res4->test(i));
    }
    free(p);
}
