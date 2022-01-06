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
#include <string.h>
#include <knowhere/common/MetricType.h>

#include "segcore/Utils.h"

TEST(Util, FaissMetricTypeToString) {
    using namespace milvus::segcore;
    using namespace faiss;

    ASSERT_EQ(MetricTypeToString(MetricType::METRIC_INNER_PRODUCT), "METRIC_INNER_PRODUCT");
    ASSERT_EQ(MetricTypeToString(MetricType::METRIC_L2), "METRIC_L2");
    ASSERT_EQ(MetricTypeToString(MetricType::METRIC_L1), "METRIC_L1");
    ASSERT_EQ(MetricTypeToString(MetricType::METRIC_Linf), "METRIC_Linf");
    ASSERT_EQ(MetricTypeToString(MetricType::METRIC_Lp), "METRIC_Lp");
    ASSERT_EQ(MetricTypeToString(MetricType::METRIC_Jaccard), "METRIC_Jaccard");
    ASSERT_EQ(MetricTypeToString(MetricType::METRIC_Tanimoto), "METRIC_Tanimoto");
    ASSERT_EQ(MetricTypeToString(MetricType::METRIC_Hamming), "METRIC_Hamming");
    ASSERT_EQ(MetricTypeToString(MetricType::METRIC_Substructure), "METRIC_Substructure");
    ASSERT_EQ(MetricTypeToString(MetricType::METRIC_Superstructure), "METRIC_Superstructure");
    ASSERT_EQ(MetricTypeToString(MetricType::METRIC_Canberra), "METRIC_Canberra");
    ASSERT_EQ(MetricTypeToString(MetricType::METRIC_BrayCurtis), "METRIC_BrayCurtis");
    ASSERT_EQ(MetricTypeToString(MetricType::METRIC_JensenShannon), "METRIC_JensenShannon");
}
