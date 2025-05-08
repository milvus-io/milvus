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

#include <arrow/util/key_value_metadata.h>
#include <gtest/gtest.h>

#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <cstdint>

#include <gtest/gtest.h>
#include "arrow/type_fwd.h"
#include "common/BitsetView.h"
#include "common/Consts.h"
#include "common/QueryInfo.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "expr/ITypeExpr.h"
#include "gtest/gtest.h"
#include "index/IndexFactory.h"
#include "index/IndexInfo.h"
#include "index/Meta.h"
#include "knowhere/comp/index_param.h"
#include "milvus-storage/common/constants.h"
#include "mmap/ChunkedColumn.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/SearchOnSealed.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentSealed.h"
#include "storage/RemoteChunkManagerSingleton.h"

#include "segcore/Types.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"
#include "test_cachinglayer/cachinglayer_test_utils.h"

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

    auto schema = std::make_shared<Schema>();
    auto fakevec_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::COSINE);

    auto field_meta = schema->operator[](fakevec_id);

    std::vector<std::unique_ptr<Chunk>> chunks;
    std::vector<int64_t> num_rows_per_chunk;
    for (int i = 0; i < chunk_num; i++) {
        num_rows_per_chunk.push_back(chunk_size);
        auto dataset = segcore::DataGen(schema, chunk_size);
        auto data = dataset.get_col<float>(fakevec_id);
        auto buf_size = 4 * data.size();

        char* buf = new char[buf_size];
        defer.AddDefer([buf]() { delete[] buf; });
        memcpy(buf, data.data(), 4 * data.size());

        chunks.emplace_back(std::make_unique<FixedWidthChunk>(
            chunk_size, dim, buf, buf_size, 4, false));
    }

    auto translator = std::make_unique<TestChunkTranslator>(
        num_rows_per_chunk, "", std::move(chunks));
    auto column =
        std::make_shared<ChunkedColumn>(std::move(translator), field_meta);

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
    auto index_info = std::map<std::string, std::string>{};
    SearchResult search_result;

    query::SearchOnSealedColumn(*schema,
                                column.get(),
                                search_info,
                                index_info,
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
    query::SearchOnSealedColumn(*schema,
                                column.get(),
                                search_info,
                                index_info,
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

class TestChunkSegment : public testing::TestWithParam<bool> {
 protected:
    void
    SetUp() override {
        bool pk_is_string = GetParam();
        auto schema = segcore::GenChunkedSegmentTestSchema(pk_is_string);
        segment = segcore::CreateSealedSegment(
            schema,
            nullptr,
            -1,
            segcore::SegcoreConfig::default_config(),
            false,
            true);
        test_data_count = 10000;

        std::vector<FieldId> field_ids = {
            schema->get_field_id(FieldName("int64")),
            schema->get_field_id(FieldName("pk")),
            TimestampFieldID,
            schema->get_field_id(FieldName("string1")),
            schema->get_field_id(FieldName("string2"))};
        fields = {{"int64", schema->get_field_id(FieldName("int64"))},
                  {"pk", schema->get_field_id(FieldName("pk"))},
                  {"ts", TimestampFieldID},
                  {"string1", schema->get_field_id(FieldName("string1"))},
                  {"string2", schema->get_field_id(FieldName("string2"))}};

        int start_id = 0;
        chunk_num = 2;

        std::vector<std::string> str_data;
        for (int i = 0; i < test_data_count * chunk_num; i++) {
            str_data.push_back("test" + std::to_string(i));
        }
        std::sort(str_data.begin(), str_data.end());

        auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                      .GetRemoteChunkManager();

        std::unordered_map<FieldId, std::vector<FieldDataPtr>> field_data_map;

        // generate data
        for (int chunk_id = 0; chunk_id < chunk_num;
             chunk_id++, start_id += test_data_count) {
            std::vector<int64_t> test_data(test_data_count);
            std::iota(test_data.begin(), test_data.end(), start_id);

            for (int i = 0; i < field_ids.size(); i++) {
                // if i < 3, this is system field, thus must be int64.
                // other fields include a pk field and 2 string fields. pk field is string if pk_is_string is true.
                auto datatype =
                    i < 3 && (field_ids[i] !=
                                  schema->get_field_id(FieldName("pk")) ||
                              !pk_is_string)
                        ? DataType::INT64
                        : DataType::VARCHAR;
                FieldDataPtr field_data{nullptr};

                if (datatype == DataType::INT64) {
                    field_data =
                        std::make_shared<FieldData<int64_t>>(datatype, false);
                    field_data->FillFieldData(test_data.data(),
                                              test_data_count);
                } else {
                    field_data = std::make_shared<FieldData<std::string>>(
                        datatype, false);
                    field_data->FillFieldData(str_data.data() + start_id,
                                              test_data_count);
                }
                auto fid = field_ids[i];
                field_data_map[fid].push_back(field_data);
            }
        }
        for (auto& [fid, field_datas] : field_data_map) {
            auto load_info = PrepareSingleFieldInsertBinlog(kCollectionID,
                                                            kPartitionID,
                                                            kSegmentID,
                                                            fid.get(),
                                                            field_datas,
                                                            cm);
            segment->LoadFieldData(load_info);
        }
    }

    segcore::SegmentSealedUPtr segment;
    int chunk_num;
    int test_data_count;
    std::unordered_map<std::string, FieldId> fields;
};

INSTANTIATE_TEST_SUITE_P(TestChunkSegment, TestChunkSegment, testing::Bool());

TEST_P(TestChunkSegment, TestTermExpr) {
    bool pk_is_string = GetParam();
    // query int64 expr
    std::vector<proto::plan::GenericValue> filter_data;
    for (int i = 1; i <= 10; ++i) {
        proto::plan::GenericValue v;
        v.set_int64_val(i);
        filter_data.push_back(v);
    }
    auto term_filter_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(fields.at("int64"), DataType::INT64), filter_data);
    BitsetType final;
    auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                       term_filter_expr);
    final = query::ExecuteQueryExpr(
        plan, segment.get(), chunk_num * test_data_count, MAX_TIMESTAMP);
    ASSERT_EQ(10, final.count());

    std::vector<proto::plan::GenericValue> filter_str_data;
    for (int i = 1; i <= 10; ++i) {
        proto::plan::GenericValue v;
        v.set_string_val("test" + std::to_string(i));
        filter_str_data.push_back(v);
    }
    // query pk expr
    auto pk_term_filter_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(fields.at("pk"),
                         pk_is_string ? DataType::VARCHAR : DataType::INT64),
        pk_is_string ? filter_str_data : filter_data);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                  pk_term_filter_expr);
    final = query::ExecuteQueryExpr(
        plan, segment.get(), chunk_num * test_data_count, MAX_TIMESTAMP);
    ASSERT_EQ(10, final.count());

    // query pk in second chunk
    std::vector<proto::plan::GenericValue> filter_data2;
    proto::plan::GenericValue v;
    if (pk_is_string) {
        v.set_string_val("test" + std::to_string(test_data_count + 1));
    } else {
        v.set_int64_val(test_data_count + 1);
    }
    filter_data2.push_back(v);

    pk_term_filter_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(fields.at("pk"),
                         pk_is_string ? DataType::VARCHAR : DataType::INT64),
        filter_data2);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                  pk_term_filter_expr);
    final = query::ExecuteQueryExpr(
        plan, segment.get(), chunk_num * test_data_count, MAX_TIMESTAMP);
    ASSERT_EQ(1, final.count());
}

TEST_P(TestChunkSegment, TestCompareExpr) {
    srand(time(NULL));
    bool pk_is_string = GetParam();
    DataType pk_data_type = pk_is_string ? DataType::VARCHAR : DataType::INT64;
    auto expr = std::make_shared<expr::CompareExpr>(
        pk_is_string ? fields.at("string1") : fields.at("int64"),
        fields.at("pk"),
        pk_data_type,
        pk_data_type,
        proto::plan::OpType::Equal);
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    BitsetType final = query::ExecuteQueryExpr(
        plan, segment.get(), chunk_num * test_data_count, MAX_TIMESTAMP);
    ASSERT_EQ(chunk_num * test_data_count, final.count());

    expr = std::make_shared<expr::CompareExpr>(fields.at("string1"),
                                               fields.at("string2"),
                                               DataType::VARCHAR,
                                               DataType::VARCHAR,
                                               proto::plan::OpType::Equal);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = query::ExecuteQueryExpr(
        plan, segment.get(), chunk_num * test_data_count, MAX_TIMESTAMP);
    ASSERT_EQ(chunk_num * test_data_count, final.count());

    // test with inverted index
    auto fid = fields.at("int64");
    auto file_manager_ctx = storage::FileManagerContext();
    file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
        milvus::proto::schema::Int64);
    file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(fid.get());
    file_manager_ctx.fieldDataMeta.field_id = fid.get();
    milvus::storage::IndexMeta index_meta;
    index_meta.field_id = fid.get();
    index_meta.build_id = rand();
    index_meta.index_version = rand();
    file_manager_ctx.indexMeta = index_meta;
    index::CreateIndexInfo create_index_info;
    create_index_info.field_type = DataType::INT64;
    create_index_info.index_type = index::INVERTED_INDEX_TYPE;
    auto index = index::IndexFactory::GetInstance().CreateScalarIndex(
        create_index_info, file_manager_ctx);
    std::vector<int64_t> data(test_data_count * chunk_num);
    for (int i = 0; i < chunk_num; i++) {
        auto pw = segment->chunk_data<int64_t>(fid, i);
        auto d = pw.get();
        std::copy(d.data(),
                  d.data() + test_data_count,
                  data.begin() + i * test_data_count);
    }

    index->BuildWithRawDataForUT(data.size(), data.data());
    segcore::LoadIndexInfo load_index_info;
    load_index_info.index_params = GenIndexParams(index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test", std::move(index));
    load_index_info.field_id = fid.get();
    segment->LoadIndex(load_index_info);

    expr = std::make_shared<expr::CompareExpr>(
        pk_is_string ? fields.at("string1") : fields.at("int64"),
        fields.at("pk"),
        pk_data_type,
        pk_data_type,
        proto::plan::OpType::Equal);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    final = query::ExecuteQueryExpr(
        plan, segment.get(), chunk_num * test_data_count, MAX_TIMESTAMP);
    ASSERT_EQ(chunk_num * test_data_count, final.count());
}
