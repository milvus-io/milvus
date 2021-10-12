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
#include "test_utils/DataGen.h"

using namespace milvus;
using namespace milvus::segcore;

TEST(Binary, Insert) {
    int64_t N = 100000;
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("vecbin", DataType::VECTOR_BINARY, 128, MetricType::METRIC_Jaccard);
    schema->AddDebugField("age", DataType::INT32);
    auto dataset = DataGen(schema, N, 10);
    auto segment = CreateGrowingSegment(schema);
    auto offset = segment->PreInsert(N);
    segment->Insert(offset, N, dataset.row_ids_.data(), dataset.timestamps_.data(), dataset.raw_);
}
