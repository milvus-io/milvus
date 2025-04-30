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

#include <folly/Conv.h>
#include <arrow/record_batch.h>
#include <arrow/util/key_value_metadata.h>
#include <arrow/table_builder.h>
#include <arrow/type_fwd.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "common/Consts.h"
#include "common/FieldDataInterface.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "expr/ITypeExpr.h"
#include "index/IndexFactory.h"
#include "index/IndexInfo.h"
#include "index/Meta.h"
#include "milvus-storage/common/constants.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/packed/writer.h"
#include "milvus-storage/format/parquet/file_reader.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentSealed.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/Utils.h"
#include "segcore/memory_planner.h"
#include "segcore/Types.h"
#include "test_utils/DataGen.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::segcore::storagev1translator;

class TestChunkSegmentStorageV2 : public testing::TestWithParam<bool> {
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

        // Initialize file system
        auto conf = milvus_storage::ArrowFileSystemConfig();
        conf.storage_type = "local";
        conf.root_path = "/tmp";
        milvus_storage::ArrowFileSystemSingleton::GetInstance().Init(conf);
        auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                      .GetArrowFileSystem();

        // Prepare paths and column groups
        std::vector<std::string> paths = {"/tmp/0.parquet", "/tmp/1.parquet"};
        std::vector<std::vector<int>> column_groups = {
            {0, 1, 2}, {3, 4}};  // short columns and long columns
        auto writer_memory = 16 * 1024 * 1024;
        auto storage_config = milvus_storage::StorageConfig();

        // Create writer
        milvus_storage::PackedRecordBatchWriter writer(
            fs,
            paths,
            schema->ConvertToArrowSchema(),
            storage_config,
            column_groups,
            writer_memory);

        // Generate and write data
        int64_t row_count = 0;
        int start_id = 0;

        std::vector<std::string> str_data;
        for (int i = 0; i < test_data_count * chunk_num; i++) {
            str_data.push_back("test" + std::to_string(i));
        }
        std::sort(str_data.begin(), str_data.end());

        fields = {{"int64", schema->get_field_id(FieldName("int64"))},
                  {"pk", schema->get_field_id(FieldName("pk"))},
                  {"ts", TimestampFieldID},
                  {"string1", schema->get_field_id(FieldName("string1"))},
                  {"string2", schema->get_field_id(FieldName("string2"))}};

        auto arrow_schema = schema->ConvertToArrowSchema();
        for (int chunk_id = 0; chunk_id < chunk_num;
             chunk_id++, start_id += test_data_count) {
            std::vector<int64_t> test_data(test_data_count);
            std::iota(test_data.begin(), test_data.end(), start_id);

            // Create arrow arrays for each field
            std::vector<std::shared_ptr<arrow::Array>> arrays;
            for (int i = 0; i < arrow_schema->fields().size(); i++) {
                if (arrow_schema->fields()[i]->type()->id() ==
                    arrow::Type::INT64) {
                    arrow::Int64Builder builder;
                    auto status =
                        builder.AppendValues(test_data.data(), test_data_count);
                    EXPECT_TRUE(status.ok());
                    std::shared_ptr<arrow::Array> array;
                    status = builder.Finish(&array);
                    EXPECT_TRUE(status.ok());
                    arrays.push_back(array);
                } else {
                    arrow::StringBuilder builder;
                    std::vector<std::string> str_values;
                    for (int j = 0; j < test_data_count; j++) {
                        str_values.push_back(str_data[start_id + j]);
                    }
                    auto status = builder.AppendValues(str_values);
                    EXPECT_TRUE(status.ok());
                    std::shared_ptr<arrow::Array> array;
                    status = builder.Finish(&array);
                    EXPECT_TRUE(status.ok());
                    arrays.push_back(array);
                }
            }

            // Create record batch
            auto record_batch = arrow::RecordBatch::Make(
                schema->ConvertToArrowSchema(), test_data_count, arrays);
            row_count += test_data_count;
            EXPECT_TRUE(writer.Write(record_batch).ok());
        }
        EXPECT_TRUE(writer.Close().ok());

        LoadFieldDataInfo load_info;
        load_info.field_infos.emplace(
            int64_t(0),
            FieldBinlogInfo{int64_t(0),
                            static_cast<int64_t>(row_count),
                            std::vector<int64_t>(chunk_num * test_data_count),
                            false,
                            std::vector<std::string>({paths[0]})});
        load_info.field_infos.emplace(
            int64_t(1),
            FieldBinlogInfo{int64_t(1),
                            static_cast<int64_t>(row_count),
                            std::vector<int64_t>(chunk_num * test_data_count),
                            false,
                            std::vector<std::string>({paths[1]})});
        load_info.mmap_dir_path = "";
        load_info.storage_version = 2;
        segment->LoadFieldData(load_info);
    }

    segcore::SegmentSealedUPtr segment;
    int chunk_num = 2;
    int test_data_count = 10000;
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
        expr::ColumnInfo(fields.at("int64"), milvus::DataType::INT64),
        filter_data);
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
        expr::ColumnInfo(
            fields.at("pk"),
            pk_is_string ? milvus::DataType::VARCHAR : milvus::DataType::INT64),
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
        expr::ColumnInfo(
            fields.at("pk"),
            pk_is_string ? milvus::DataType::VARCHAR : milvus::DataType::INT64),
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
    milvus::DataType pk_data_type =
        pk_is_string ? milvus::DataType::VARCHAR : milvus::DataType::INT64;
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
                                               milvus::DataType::VARCHAR,
                                               milvus::DataType::VARCHAR,
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
    create_index_info.field_type = milvus::DataType::INT64;
    create_index_info.index_type = index::INVERTED_INDEX_TYPE;
    auto index = index::IndexFactory::GetInstance().CreateScalarIndex(
        create_index_info, file_manager_ctx);
    std::vector<int64_t> data(test_data_count * chunk_num);
    auto pw = segment->chunk_data<int64_t>(fid, 0);
    auto d = pw.get();
    std::copy(
        d.data(), d.data() + test_data_count, data.begin() + test_data_count);

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
