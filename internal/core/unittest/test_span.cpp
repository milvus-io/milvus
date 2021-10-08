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
#include "utils/tools.h"
#include "test_utils/DataGen.h"
#include "segcore/SegmentGrowing.h"

namespace {
const int64_t ROW_COUNT = 100 * 1000;
}

TEST(Span, Naive) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    int64_t N = ROW_COUNT;
    constexpr int64_t size_per_chunk = 32 * 1024;
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("binaryvec", DataType::VECTOR_BINARY, 512, MetricType::METRIC_Jaccard);
    schema->AddDebugField("age", DataType::FLOAT);
    schema->AddDebugField("floatvec", DataType::VECTOR_FLOAT, 32, MetricType::METRIC_L2);

    auto dataset = DataGen(schema, N);
    auto seg_conf = SegcoreConfig::default_config();
    auto segment = CreateGrowingSegment(schema, seg_conf);
    segment->PreInsert(N);
    segment->Insert(0, N, dataset.row_ids_.data(), dataset.timestamps_.data(), dataset.raw_);
    auto vec_ptr = dataset.get_col<uint8_t>(0);
    auto age_ptr = dataset.get_col<float>(1);
    auto float_ptr = dataset.get_col<float>(2);
    SegmentInternalInterface& interface = *segment;
    auto num_chunk = interface.num_chunk();
    ASSERT_EQ(num_chunk, upper_div(N, size_per_chunk));
    auto row_count = interface.get_row_count();
    ASSERT_EQ(N, row_count);
    for (auto chunk_id = 0; chunk_id < num_chunk; ++chunk_id) {
        auto vec_span = interface.chunk_data<BinaryVector>(FieldOffset(0), chunk_id);
        auto age_span = interface.chunk_data<float>(FieldOffset(1), chunk_id);
        auto float_span = interface.chunk_data<FloatVector>(FieldOffset(2), chunk_id);
        auto begin = chunk_id * size_per_chunk;
        auto end = std::min((chunk_id + 1) * size_per_chunk, N);
        auto size_of_chunk = end - begin;
        for (int i = 0; i < size_of_chunk * 512 / 8; ++i) {
            ASSERT_EQ(vec_span.data()[i], vec_ptr[i + begin * 512 / 8]);
        }
        for (int i = 0; i < size_of_chunk; ++i) {
            ASSERT_EQ(age_span.data()[i], age_ptr[i + begin]);
        }
        for (int i = 0; i < size_of_chunk; ++i) {
            ASSERT_EQ(float_span.data()[i], float_ptr[i + begin * 32]);
        }
    }
}
