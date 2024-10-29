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

using namespace milvus;
TEST(test_chunk_segment, TestSearchOnSealed) {
    int dim = 16;
    auto schema = std::make_shared<Schema>();
    auto fakevec_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::COSINE);
    auto counter_id = schema->AddDebugField("counter", DataType::INT64);
    schema->set_primary_field_id(counter_id);

    auto column = std::make_shared<ChunkedColumn>();
    int chunk_num = 3;
    int chunk_size = 100;
    for (int i = 0; i < chunk_num; i++) {
        auto dataset = segcore::DataGen(schema, chunk_size);
        auto data = dataset.get_col<float>(fakevec_id);
        auto null_size = (chunk_size + 7) / 8;
        auto buf_size = null_size + 4 * data.size();
        char* buf = new char[buf_size];
        memcpy(buf + null_size, data.data(), 4 * data.size());
        auto chunk = std::make_shared<FixedWidthChunk>(
            chunk_size, dim, buf, buf_size, 4, false);

        column->AddChunk(chunk);
    }

    auto query_ds = segcore::DataGen(schema, 1);
    auto query_data = query_ds.get_col<float>(fakevec_id).data();
    auto search_conf = knowhere::Json{
        {knowhere::meta::METRIC_TYPE, knowhere::metric::COSINE},
    };
    int total_row_count = chunk_num * chunk_size;
    SearchInfo search_info;
    search_info.search_params_ = search_conf;
    search_info.field_id_ = fakevec_id;
    search_info.metric_type_ = knowhere::metric::COSINE;
    // expect to return all rows
    search_info.topk_ = total_row_count;
    SearchResult search_result;

    int bitset_size = (total_row_count + 7) / 8;
    uint8_t* bitset_data = new uint8_t[bitset_size];
    std::fill(bitset_data, bitset_data + bitset_size, 0);
    BitsetView bv(bitset_data, total_row_count);
    query::SearchOnSealed(*schema,
                          column,
                          search_info,
                          query_data,
                          1,
                          chunk_size * chunk_num,
                          bv,
                          search_result);

    std::set<int64_t> offsets;
    int cnt = 0;
    for (auto& offset : search_result.seg_offsets_) {
        if (offset != -1) {
            cnt++;
            offsets.insert(offset);
        }
    }
    // check all rows are returned
    ASSERT_EQ(total_row_count, cnt);
    ASSERT_EQ(total_row_count, offsets.size());
    for (int i = 0; i < total_row_count; i++) {
        ASSERT_TRUE(offsets.find(i) != offsets.end());
    }
}
