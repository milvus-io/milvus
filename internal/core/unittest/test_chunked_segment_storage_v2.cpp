// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <arrow/record_batch.h>
#include <arrow/util/key_value_metadata.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <cstdint>
#include "arrow/table_builder.h"
#include "arrow/type_fwd.h"
#include "common/Consts.h"
#include "common/FieldDataInterface.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "expr/ITypeExpr.h"
#include "gtest/gtest.h"
#include "index/IndexFactory.h"
#include "index/IndexInfo.h"
#include "index/Meta.h"
#include "milvus-storage/common/constants.h"
#include "milvus-storage/common/metadata.h"
#include "mmap/Types.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentSealed.h"
#include "segcore/SegmentSealedImpl.h"
#include "segcore/Types.h"
#include <memory>
#include <numeric>
#include <string>
#include <unordered_map>
#include <vector>

using namespace milvus;

class TestChunkSegmentStorageV2 : public testing::TestWithParam<bool> {
 protected:
    void
    SetUp() override {
        bool pk_is_string = GetParam();
        auto schema = std::make_shared<Schema>();
        auto int64_fid = schema->AddDebugField("int64", DataType::INT64, true);

        auto pk_fid = schema->AddDebugField(
            "pk", pk_is_string ? DataType::VARCHAR : DataType::INT64, true);
        auto str_fid =
            schema->AddDebugField("string1", DataType::VARCHAR, true);
        auto str2_fid =
            schema->AddDebugField("string2", DataType::VARCHAR, true);
        schema->AddField(
            FieldName("ts"), TimestampFieldID, DataType::INT64, true);
        schema->set_primary_field_id(pk_fid);
        segment = segcore::CreateSealedSegment(
            schema,
            nullptr,
            -1,
            segcore::SegcoreConfig::default_config(),
            false,
            true,
            true);
        test_data_count = 10000;

        auto arrow_i64_field = arrow::field(
            "int64",
            arrow::int64(),
            true,
            arrow::key_value_metadata({milvus_storage::ARROW_FIELD_ID_KEY},
                                      {std::to_string(100)}));
        auto arrow_pk_field = arrow::field(
            "pk",
            pk_is_string ? arrow::utf8() : arrow::int64(),
            true,
            arrow::key_value_metadata({milvus_storage::ARROW_FIELD_ID_KEY},
                                      {std::to_string(101)}));
        auto arrow_ts_field = arrow::field(
            "ts",
            arrow::int64(),
            true,
            arrow::key_value_metadata({milvus_storage::ARROW_FIELD_ID_KEY},
                                      {std::to_string(1)}));
        auto arrow_str_field = arrow::field(
            "string1",
            arrow::utf8(),
            true,
            arrow::key_value_metadata({milvus_storage::ARROW_FIELD_ID_KEY},
                                      {std::to_string(102)}));
        auto arrow_str2_field = arrow::field(
            "string2",
            arrow::utf8(),
            true,
            arrow::key_value_metadata({milvus_storage::ARROW_FIELD_ID_KEY},
                                      {std::to_string(103)}));
        std::vector<std::shared_ptr<arrow::Field>> arrow_fields = {
            arrow_ts_field,
            arrow_str2_field,
            arrow_str_field,
            arrow_pk_field,
            arrow_i64_field,
        };
        auto expected_arrow_schema =
            std::make_shared<arrow::Schema>(arrow_fields);
        ASSERT_EQ(schema->ConvertToArrowSchema()->ToString(),
                  expected_arrow_schema->ToString());

        std::vector<FieldId> field_ids = {
            int64_fid, pk_fid, TimestampFieldID, str_fid, str2_fid};
        fields = {{"int64", int64_fid},
                  {"pk", pk_fid},
                  {"ts", TimestampFieldID},
                  {"string1", str_fid},
                  {"string2", str2_fid}};

        int start_id = 0;
        chunk_num = 2;

        std::vector<FieldDataInfo> field_infos;
        for (int i = 0; i < 2; i++) {
            FieldDataInfo field_info;
            field_info.field_id = int64_t(i);
            field_info.row_count = test_data_count * chunk_num;
            field_infos.push_back(field_info);
        }

        std::vector<std::string> str_data;
        for (int i = 0; i < test_data_count * chunk_num; i++) {
            str_data.push_back("test" + std::to_string(i));
        }
        std::sort(str_data.begin(), str_data.end());
        std::vector<bool> validity(test_data_count, true);

        // generate data
        for (int chunk_id = 0; chunk_id < chunk_num;
             chunk_id++, start_id += test_data_count) {
            std::vector<int64_t> test_data(test_data_count);
            std::iota(test_data.begin(), test_data.end(), start_id);

            auto builder = std::make_shared<arrow::Int64Builder>();
            auto status = builder->AppendValues(
                test_data.begin(), test_data.end(), validity.begin());
            ASSERT_TRUE(status.ok());
            auto res = builder->Finish();
            ASSERT_TRUE(res.ok());
            std::shared_ptr<arrow::Array> arrow_int64;
            arrow_int64 = res.ValueOrDie();

            auto str_builder = std::make_shared<arrow::StringBuilder>();
            for (int i = 0; i < test_data_count; i++) {
                auto status = str_builder->Append(str_data[start_id + i]);
                ASSERT_TRUE(status.ok());
            }
            std::shared_ptr<arrow::Array> arrow_str;
            status = str_builder->Finish(&arrow_str);
            ASSERT_TRUE(status.ok());

            auto short_column_schema = arrow::schema(
                {arrow_fields[0], arrow_fields[3], arrow_fields[4]});
            std::shared_ptr<arrow::RecordBatch> short_column_record_batch =
                arrow::RecordBatch::Make(
                    short_column_schema,
                    arrow_int64->length(),
                    {arrow_int64,
                     pk_is_string ? arrow_str : arrow_int64,
                     arrow_int64});

            auto short_column_channel = std::make_shared<ArrowDataWrapper>();
            short_column_channel->record_batches.push_back(
                std::move(short_column_record_batch));

            field_infos[0].arrow_reader_channel->push(short_column_channel);

            auto long_column_schema =
                arrow::schema({arrow_fields[1], arrow_fields[2]});
            std::shared_ptr<arrow::RecordBatch> long_column_record_batch =
                arrow::RecordBatch::Make(long_column_schema,
                                         arrow_int64->length(),
                                         {arrow_str, arrow_str});

            auto long_column_channel = std::make_shared<ArrowDataWrapper>();
            long_column_channel->record_batches.push_back(
                std::move(long_column_record_batch));

            field_infos[1].arrow_reader_channel->push(long_column_channel);
        }

        for (int i = 0; i < field_infos.size(); i++) {
            field_infos[i].arrow_reader_channel->close();
        }

        // load
        segment->LoadColumnGroupData(
            FieldId(0),
            field_infos[0],
            milvus_storage::FieldIDList(
                std::vector<milvus_storage::FieldID>{1, 101, 100}),
            false);
        segment->LoadColumnGroupData(
            FieldId(1),
            field_infos[1],
            milvus_storage::FieldIDList(
                std::vector<milvus_storage::FieldID>{103, 102}),
            false);
    }

    segcore::SegmentSealedUPtr segment;
    int chunk_num;
    int test_data_count;
    std::unordered_map<std::string, FieldId> fields;
};

INSTANTIATE_TEST_SUITE_P(TestChunkSegmentStorageV2,
                         TestChunkSegmentStorageV2,
                         testing::Bool());

TEST_P(TestChunkSegmentStorageV2, TestTermExpr) {
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

TEST_P(TestChunkSegmentStorageV2, TestCompareExpr) {
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
        auto d = segment->chunk_data<int64_t>(fid, i);
        std::copy(d.data(),
                  d.data() + test_data_count,
                  data.begin() + i * test_data_count);
    }

    index->BuildWithRawDataForUT(data.size(), data.data());
    segcore::LoadIndexInfo load_index_info;
    load_index_info.index = std::move(index);
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
