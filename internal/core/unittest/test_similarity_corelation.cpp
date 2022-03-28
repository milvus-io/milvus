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

#include "segcore/SimilarityCorelation.h"

TEST(SimilarityCorelation, Naive) {
    ASSERT_TRUE(milvus::segcore::PositivelyRelated(faiss::METRIC_INNER_PRODUCT));

    ASSERT_FALSE(milvus::segcore::PositivelyRelated(faiss::METRIC_Jaccard));
    ASSERT_FALSE(milvus::segcore::PositivelyRelated(faiss::METRIC_Tanimoto));
    ASSERT_FALSE(milvus::segcore::PositivelyRelated(faiss::METRIC_L2));
    ASSERT_FALSE(milvus::segcore::PositivelyRelated(faiss::METRIC_Hamming));
    ASSERT_FALSE(milvus::segcore::PositivelyRelated(faiss::METRIC_Substructure));
    ASSERT_FALSE(milvus::segcore::PositivelyRelated(faiss::METRIC_Superstructure));
}
