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

#include "segcore/SegmentGrowing.h"
#include "test_utils/DataGen.h"

const int64_t ROW_COUNT = 100 * 1000;

TEST(Span, Naive) {
    using namespace milvus;
    using namespace milvus::query;
    using namespace milvus::segcore;
    int64_t N = ROW_COUNT;
    constexpr int64_t size_per_chunk = 32 * 1024;
    auto schema = std::make_shared<Schema>();
    auto bin_vec_fid = schema->AddDebugField(
        "binaryvec", DataType::VECTOR_BINARY, 512, knowhere::metric::JACCARD);
    auto float_fid = schema->AddDebugField("age", DataType::FLOAT);
    auto float_vec_fid = schema->AddDebugField(
        "floatvec", DataType::VECTOR_FLOAT, 32, knowhere::metric::L2);
    auto i64_fid = schema->AddDebugField("counter", DataType::INT64);
    auto nullable_fid =
        schema->AddDebugField("nullable", DataType::INT64, true);
    schema->set_primary_field_id(i64_fid);

    auto dataset = DataGen(schema, N, 42, 0, 1, 10, false, true, true);
    auto segment = CreateGrowingSegment(schema, empty_index_meta, -1);
    segment->PreInsert(N);
    segment->Insert(0,
                    N,
                    dataset.row_ids_.data(),
                    dataset.timestamps_.data(),
                    dataset.raw_);
    auto vec_ptr = dataset.get_col<uint8_t>(bin_vec_fid);
    auto age_ptr = dataset.get_col<float>(float_fid);
    auto float_ptr = dataset.get_col<float>(float_vec_fid);
    auto nullable_data_ptr = dataset.get_col<int64_t>(nullable_fid);
    auto nullable_valid_data_ptr = dataset.get_col_valid(nullable_fid);
    auto num_chunk = segment->num_chunk(FieldId(0));
    ASSERT_EQ(num_chunk, upper_div(N, size_per_chunk));
    auto row_count = segment->get_row_count();
    ASSERT_EQ(N, row_count);
    for (auto chunk_id = 0; chunk_id < num_chunk; ++chunk_id) {
        auto vec_span =
            segment->chunk_data<milvus::BinaryVector>(bin_vec_fid, chunk_id);
        auto age_span = segment->chunk_data<float>(float_fid, chunk_id);
        auto float_span =
            segment->chunk_data<milvus::FloatVector>(float_vec_fid, chunk_id);
        auto null_field_span =
            segment->chunk_data<int64_t>(nullable_fid, chunk_id);
        auto begin = chunk_id * size_per_chunk;
        auto end = std::min((chunk_id + 1) * size_per_chunk, N);
        auto size_of_chunk = end - begin;
        ASSERT_EQ(age_span.get().valid_data(), nullptr);
        for (int i = 0; i < size_of_chunk * 512 / 8; ++i) {
            ASSERT_EQ(vec_span.get().data()[i], vec_ptr[i + begin * 512 / 8]);
        }
        for (int i = 0; i < size_of_chunk; ++i) {
            ASSERT_EQ(age_span.get().data()[i], age_ptr[i + begin]);
        }
        for (int i = 0; i < size_of_chunk; ++i) {
            ASSERT_EQ(float_span.get().data()[i], float_ptr[i + begin * 32]);
        }
        for (int i = 0; i < size_of_chunk; ++i) {
            ASSERT_EQ(null_field_span.get().data()[i],
                      nullable_data_ptr[i + begin]);
        }
        for (int i = 0; i < size_of_chunk; ++i) {
            ASSERT_EQ(null_field_span.get().valid_data()[i],
                      nullable_valid_data_ptr[i + begin]);
        }
    }
}