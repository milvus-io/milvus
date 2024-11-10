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
#include "arrow/table_builder.h"
#include "arrow/type_fwd.h"
#include "common/BitsetView.h"
#include "common/Consts.h"
#include "common/FieldDataInterface.h"
#include "common/QueryInfo.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "expr/ITypeExpr.h"
#include "knowhere/comp/index_param.h"
#include "mmap/ChunkedColumn.h"
#include "mmap/Types.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/SearchOnSealed.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentSealedImpl.h"
#include "test_utils/DataGen.h"
#include <memory>
#include <numeric>
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
                          total_row_count,
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

    // test with group by
    search_info.group_by_field_id_ = fakevec_id;
    std::fill(bitset_data, bitset_data + bitset_size, 0);
    query::SearchOnSealed(*schema,
                          column,
                          search_info,
                          query_data,
                          1,
                          total_row_count,
                          bv,
                          search_result);

    ASSERT_EQ(1, search_result.vector_iterators_->size());

    auto iter = search_result.vector_iterators_->at(0);
    // collect all offsets
    offsets.clear();
    while (iter->HasNext()) {
        auto [offset, distance] = iter->Next().value();
        offsets.insert(offset);
    }

    ASSERT_EQ(total_row_count, offsets.size());
    for (int i = 0; i < total_row_count; i++) {
        ASSERT_TRUE(offsets.find(i) != offsets.end());
    }
}

TEST(test_chunk_segment, TestTermExpr) {
    auto schema = std::make_shared<Schema>();
    auto int64_fid = schema->AddDebugField("int64", DataType::INT64, true);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64, true);
    schema->AddField(FieldName("ts"), TimestampFieldID, DataType::INT64, true);
    schema->set_primary_field_id(pk_fid);
    auto segment =
        segcore::CreateSealedSegment(schema,
                                     nullptr,
                                     -1,
                                     segcore::SegcoreConfig::default_config(),
                                     false,
                                     false,
                                     true);
    size_t test_data_count = 1000;

    auto arrow_i64_field = arrow::field("int64", arrow::int64());
    auto arrow_pk_field = arrow::field("pk", arrow::int64());
    auto arrow_ts_field = arrow::field("ts", arrow::int64());
    std::vector<std::shared_ptr<arrow::Field>> arrow_fields = {
        arrow_i64_field, arrow_pk_field, arrow_ts_field};

    std::vector<FieldId> field_ids = {int64_fid, pk_fid, TimestampFieldID};

    int start_id = 1;
    int chunk_num = 2;

    std::vector<FieldDataInfo> field_infos;
    for (auto fid : field_ids) {
        FieldDataInfo field_info;
        field_info.field_id = fid.get();
        field_info.row_count = test_data_count * chunk_num;
        field_infos.push_back(field_info);
    }

    // generate data
    for (int chunk_id = 0; chunk_id < chunk_num;
         chunk_id++, start_id += test_data_count) {
        std::vector<int64_t> test_data(test_data_count);
        std::iota(test_data.begin(), test_data.end(), start_id);

        auto builder = std::make_shared<arrow::Int64Builder>();
        auto status = builder->AppendValues(test_data.begin(), test_data.end());
        ASSERT_TRUE(status.ok());
        auto res = builder->Finish();
        ASSERT_TRUE(res.ok());
        std::shared_ptr<arrow::Array> arrow_int64;
        arrow_int64 = res.ValueOrDie();

        for (int i = 0; i < arrow_fields.size(); i++) {
            auto f = arrow_fields[i];
            auto fid = field_ids[i];
            auto arrow_schema =
                std::make_shared<arrow::Schema>(arrow::FieldVector(1, f));
            auto record_batch = arrow::RecordBatch::Make(
                arrow_schema, arrow_int64->length(), {arrow_int64});

            auto res2 = arrow::RecordBatchReader::Make({record_batch});
            ASSERT_TRUE(res2.ok());
            auto arrow_reader = res2.ValueOrDie();

            field_infos[i].arrow_reader_channel->push(
                std::make_shared<ArrowDataWrapper>(
                    arrow_reader, nullptr, nullptr));
        }
    }

    // load
    for (int i = 0; i < field_infos.size(); i++) {
        field_infos[i].arrow_reader_channel->close();
        segment->LoadFieldData(field_ids[i], field_infos[i]);
    }

    // query int64 expr
    std::vector<proto::plan::GenericValue> filter_data;
    for (int i = 1; i <= 10; ++i) {
        proto::plan::GenericValue v;
        v.set_int64_val(i);
        filter_data.push_back(v);
    }
    auto term_filter_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(int64_fid, DataType::INT64), filter_data);
    BitsetType final;
    auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                       term_filter_expr);
    final = query::ExecuteQueryExpr(
        plan, segment.get(), chunk_num * test_data_count, MAX_TIMESTAMP);
    ASSERT_EQ(10, final.count());

    // query pk expr
    auto pk_term_filter_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(pk_fid, DataType::INT64), filter_data);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                  pk_term_filter_expr);
    final = query::ExecuteQueryExpr(
        plan, segment.get(), chunk_num * test_data_count, MAX_TIMESTAMP);
    ASSERT_EQ(10, final.count());

    // query pk in second chunk
    std::vector<proto::plan::GenericValue> filter_data2;
    proto::plan::GenericValue v;
    v.set_int64_val(test_data_count + 1);
    filter_data2.push_back(v);
    pk_term_filter_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(pk_fid, DataType::INT64), filter_data2);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                  pk_term_filter_expr);
    final = query::ExecuteQueryExpr(
        plan, segment.get(), chunk_num * test_data_count, MAX_TIMESTAMP);
    ASSERT_EQ(1, final.count());
}
