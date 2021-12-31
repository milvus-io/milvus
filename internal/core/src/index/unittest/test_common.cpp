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
#include "knowhere/common/Dataset.h"
#include "knowhere/common/Timer.h"
#include "knowhere/knowhere/common/Exception.h"
#include "unittest/utils.h"
#include "faiss/utils/BitsetView.h"
#include "faiss/utils/ConcurrentBitset.h"
#include <boost/dynamic_bitset.hpp>

/*Some unittest for knowhere/common, mainly for improve code coverage.*/

TEST(COMMON_TEST, dataset_test) {
    milvus::knowhere::Dataset set;
    int64_t v1 = 111;

    set.Set("key1", v1);
    auto get_v1 = set.Get<int64_t>("key1");
    ASSERT_EQ(get_v1, v1);

    ASSERT_ANY_THROW(set.Get<int8_t>("key1"));
    ASSERT_ANY_THROW(set.Get<int64_t>("dummy"));
}

TEST(COMMON_TEST, knowhere_exception) {
    const std::string msg = "test";
    milvus::knowhere::KnowhereException ex(msg);
    ASSERT_EQ(ex.what(), msg);
}

TEST(COMMON_TEST, time_recoder) {
    InitLog();

    milvus::knowhere::TimeRecorder recoder("COMMTEST", 0);
    sleep(1);
    double span = recoder.ElapseFromBegin("get time");
    ASSERT_GE(span, 1.0);
}

TEST(COMMON_TEST, BitsetView) {
    using faiss::BitsetView;
    using faiss::ConcurrentBitset;
    int N = 1000 * 3;
    auto con_bitset = std::make_shared<ConcurrentBitset>(N);
    for (int i = 0; i < N; ++i) {
        if (i % 3 == 0) {
            con_bitset->set(i);
        } else {
            con_bitset->clear(i);
        }
    }

    auto view = [](const BitsetView view_para) { return view_para; }(con_bitset);

    boost::dynamic_bitset<> boo_bitset(N);
    for (int i = 0; i < N; ++i) {
        boo_bitset[i] = con_bitset->test(i);
    }
    ASSERT_EQ(boo_bitset.count(), N / 3);
}
