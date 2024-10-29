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
#include <algorithm>
#include <cstdint>
#include "common/BitsetView.h"
#include "common/QueryInfo.h"
#include "common/Schema.h"
#include "knowhere/comp/index_param.h"
#include "mmap/ChunkedColumn.h"
#include "query/SearchOnSealed.h"
#include "test_utils/DataGen.h"
#include <vector>

struct DeferRelease {
    using functype = std::function<void()>;
    void
    AddDefer(const functype& closure) {
        closures.push_back(closure);
    }

    ~DeferRelease() {
        for (auto& closure : closures) {
            closure();
        }
    }

    std::vector<functype> closures;
};

using namespace milvus;
TEST(test_chunk_segment, TestSearchOnSealed) {
    DeferRelease defer;

    int dim = 16;
    int chunk_num = 3;
    int chunk_size = 100;
    int total_row_count = chunk_num * chunk_size;
    int bitset_size = (total_row_count + 7) / 8;
    int chunk_bitset_size = (chunk_size + 7) / 8;

    auto column = std::make_shared<ChunkedColumn>();
    auto schema = std::make_shared<Schema>();
    auto fakevec_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::COSINE);

    for (int i = 0; i < chunk_num; i++) {
        auto dataset = segcore::DataGen(schema, chunk_size);
        auto data = dataset.get_col<float>(fakevec_id);
        auto buf_size = chunk_bitset_size + 4 * data.size();

        char* buf = new char[buf_size];
        defer.AddDefer([buf]() { delete[] buf; });
        memcpy(buf + chunk_bitset_size, data.data(), 4 * data.size());

        auto chunk = std::make_shared<FixedWidthChunk>(
            chunk_size, dim, buf, buf_size, 4, false);
        column->AddChunk(chunk);
    }

    SearchInfo search_info;
    auto search_conf = knowhere::Json{
        {knowhere::meta::METRIC_TYPE, knowhere::metric::COSINE},
    };
    search_info.search_params_ = search_conf;
    search_info.field_id_ = fakevec_id;
    search_info.metric_type_ = knowhere::metric::COSINE;
    // expect to return all rows
    search_info.topk_ = total_row_count;

    uint8_t* bitset_data = new uint8_t[bitset_size];
    defer.AddDefer([bitset_data]() { delete[] bitset_data; });
    std::fill(bitset_data, bitset_data + bitset_size, 0);
    BitsetView bv(bitset_data, total_row_count);

    auto query_ds = segcore::DataGen(schema, 1);
    auto col_query_data = query_ds.get_col<float>(fakevec_id);
    auto query_data = col_query_data.data();
    SearchResult search_result;

    query::SearchOnSealed(*schema,
                          column,
                          search_info,
                          query_data,
                          1,
                          chunk_size * chunk_num,
                          bv,
                          search_result);

    std::set<int64_t> offsets;
    for (auto& offset : search_result.seg_offsets_) {
        if (offset != -1) {
            offsets.insert(offset);
        }
    }
    // check all rows are returned
    ASSERT_EQ(total_row_count, offsets.size());
    for (int i = 0; i < total_row_count; i++) {
        ASSERT_TRUE(offsets.find(i) != offsets.end());
    }
}
