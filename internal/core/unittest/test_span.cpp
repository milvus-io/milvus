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

TEST(Span, Naive) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    int64_t N = 1000 * 1000;
    constexpr int64_t chunk_size = 32 * 1024;
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("binaryvec", DataType::VECTOR_BINARY, 512, MetricType::METRIC_Jaccard);
    schema->AddDebugField("age", DataType::FLOAT);
    schema->AddDebugField("floatvec", DataType::VECTOR_FLOAT, 32, MetricType::METRIC_L2);

    auto dataset = DataGen(schema, N);
    auto segment = CreateGrowingSegment(schema, chunk_size);
    segment->PreInsert(N);
    segment->Insert(0, N, dataset.row_ids_.data(), dataset.timestamps_.data(), dataset.raw_);
    auto vec_ptr = dataset.get_col<uint8_t>(0);
    auto age_ptr = dataset.get_col<float>(1);
    auto float_ptr = dataset.get_col<float>(2);
    SegmentInternalInterface& interface = *segment;
    auto num_chunk = interface.get_safe_num_chunk();
    ASSERT_EQ(num_chunk, upper_div(N, chunk_size));
    auto row_count = interface.get_row_count();
    ASSERT_EQ(N, row_count);
    for (auto chunk_id = 0; chunk_id < num_chunk; ++chunk_id) {
        auto vec_span = interface.chunk_data<BinaryVector>(FieldOffset(0), chunk_id);
        auto age_span = interface.chunk_data<float>(FieldOffset(1), chunk_id);
        auto float_span = interface.chunk_data<FloatVector>(FieldOffset(2), chunk_id);
        auto begin = chunk_id * chunk_size;
        auto end = std::min((chunk_id + 1) * chunk_size, N);
        auto chunk_size = end - begin;
        for (int i = 0; i < chunk_size * 512 / 8; ++i) {
            ASSERT_EQ(vec_span.data()[i], vec_ptr[i + begin * 512 / 8]);
        }
        for (int i = 0; i < chunk_size; ++i) {
            ASSERT_EQ(age_span.data()[i], age_ptr[i + begin]);
        }
        for (int i = 0; i < chunk_size; ++i) {
            ASSERT_EQ(float_span.data()[i], float_ptr[i + begin * 32]);
        }
    }
}