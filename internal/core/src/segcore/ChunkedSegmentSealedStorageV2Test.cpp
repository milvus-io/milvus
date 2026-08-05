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
#include <numeric>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/Consts.h"
#include "common/FieldDataInterface.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"
#include "index/IndexFactory.h"
#include "index/IndexInfo.h"
#include "index/Meta.h"
#include "index/ScalarIndex.h"
#include "milvus-storage/common/constants.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/packed/writer.h"
#include "milvus-storage/format/parquet/file_reader.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentChunkReader.h"
#include "segcore/SegmentSealed.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentGrowing.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/Utils.h"
#include "segcore/memory_planner.h"
#include "segcore/Types.h"
#include "test_utils/DataGen.h"
#include "test_utils/cachinglayer_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::segcore::storagev1translator;

namespace {
class RawLookupOnlyIndex : public index::ScalarIndex<int64_t> {
 public:
    RawLookupOnlyIndex() : index::ScalarIndex<int64_t>("raw_lookup_only") {
    }

    index::ScalarIndexType
    GetIndexType() const override {
        return index::ScalarIndexType::STLSORT;
    }

    void
    Build(size_t, const int64_t*, const bool* = nullptr) override {
    }

    const TargetBitmap
    In(size_t, const int64_t*) override {
        return {};
    }

    const TargetBitmap
    NotIn(size_t, const int64_t*) override {
        return {};
    }

    const TargetBitmap
    IsNull() override {
        return {};
    }

    TargetBitmap
    IsNotNull() override {
        return {};
    }

    const TargetBitmap
    Range(const int64_t&, OpType) override {
        return {};
    }

    const TargetBitmap
    Range(const int64_t&, bool, const int64_t&, bool) override {
        return {};
    }

    std::optional<int64_t>
    Reverse_Lookup(size_t offset) const override {
        last_lookup_offset = offset;
        return static_cast<int64_t>(offset);
    }

    void
    Build(const Config& = {}) override {
    }

    BinarySet
    Serialize(const Config& = {}) override {
        return {};
    }

    void
    Load(const BinarySet&, const Config& = {}) override {
    }

    void
    Load(milvus::tracer::TraceContext, const Config& = {}) override {
    }

    int64_t
    Count() override {
        return 0;
    }

    int64_t
    Size() override {
        return 0;
    }

    index::IndexStatsPtr
    Upload(const Config& = {}) override {
        return nullptr;
    }

    const bool
    HasRawData() const override {
        return true;
    }

    mutable size_t last_lookup_offset = 0;
};
}  // namespace

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
            true);

        // Use globally initialized ArrowFileSystem
        auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                      .GetArrowFileSystem();

        // Prepare paths and column groups
        std::vector<std::string> paths = {"test_data/0/10000.parquet",
                                          "test_data/102/10001.parquet",
                                          "test_data/103/10002.parquet"};

        // Create directories for the parquet files
        for (const auto& path : paths) {
            auto dir_path = path.substr(0, path.find_last_of('/'));
            auto status = fs->CreateDir(dir_path);
            EXPECT_TRUE(status.ok())
                << "Failed to create directory: " << dir_path;
        }

        std::vector<std::vector<int>> column_groups = {
            {0, 1, 4}, {2}, {3}};  // narrow columns and wide columns
        auto writer_memory = 16 * 1024 * 1024;
        auto storage_config = milvus_storage::StorageConfig();

        // Create writer
        auto result = milvus_storage::PackedRecordBatchWriter::Make(
            fs,
            paths,
            schema->ConvertToArrowSchema(),
            storage_config,
            column_groups,
            writer_memory,
            ::parquet::default_writer_properties());
        EXPECT_TRUE(result.ok());
        auto writer = result.ValueOrDie();

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
            EXPECT_TRUE(writer->Write(record_batch).ok());
        }
        EXPECT_TRUE(writer->Close().ok());

        LoadFieldDataInfo load_info;
        load_info.field_infos.emplace(
            int64_t(0),
            FieldBinlogInfo{
                int64_t(0),
                static_cast<int64_t>(row_count),
                std::vector<int64_t>(chunk_num * test_data_count),
                std::vector<int64_t>(chunk_num * test_data_count * 4),
                false,
                "",
                std::vector<std::string>({paths[0]})});
        load_info.field_infos.emplace(
            int64_t(102),
            FieldBinlogInfo{
                int64_t(102),
                static_cast<int64_t>(row_count),
                std::vector<int64_t>(chunk_num * test_data_count),
                std::vector<int64_t>(chunk_num * test_data_count * 4),
                false,
                "",
                std::vector<std::string>({paths[1]})});
        load_info.field_infos.emplace(
            int64_t(103),
            FieldBinlogInfo{
                int64_t(103),
                static_cast<int64_t>(row_count),
                std::vector<int64_t>(chunk_num * test_data_count),
                std::vector<int64_t>(chunk_num * test_data_count * 4),
                false,
                "",
                std::vector<std::string>({paths[2]})});
        load_info.storage_version = 2;
        segment->AddFieldDataInfoForSealed(load_info);
        for (auto& [id, info] : load_info.field_infos) {
            LoadFieldDataInfo load_field_info;
            load_field_info.storage_version = 2;
            load_field_info.field_infos.emplace(id, info);
            segment->LoadFieldData(load_field_info);
        }
    }

    void
    TearDown() override {
        // Clean up test data directory
        auto fs = milvus_storage::ArrowFileSystemSingleton::GetInstance()
                      .GetArrowFileSystem();
        auto status = fs->DeleteDir("test_data");
        ASSERT_TRUE(status.ok());
    }

    int64_t
    RowCount() const {
        return chunk_num * test_data_count;
    }

    void
    LoadInt64ScalarIndex(const std::string& index_type) {
        auto fid = fields.at("int64");
        auto file_manager_ctx = storage::FileManagerContext();
        file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
            milvus::proto::schema::Int64);
        file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(fid.get());
        file_manager_ctx.fieldDataMeta.field_id = fid.get();
        milvus::storage::IndexMeta index_meta;
        index_meta.field_id = fid.get();
        index_meta.build_id = 1000 + fid.get();
        index_meta.index_version = 2000 + fid.get();
        file_manager_ctx.indexMeta = index_meta;

        index::CreateIndexInfo create_index_info;
        create_index_info.field_type = milvus::DataType::INT64;
        create_index_info.index_type = index_type;
        auto index = index::IndexFactory::GetInstance().CreateScalarIndex(
            create_index_info, file_manager_ctx);

        std::vector<int64_t> data(RowCount());
        std::iota(data.begin(), data.end(), 0);
        index->BuildWithRawDataForUT(data.size(), data.data());

        segcore::LoadIndexInfo load_index_info;
        load_index_info.index_params = GenIndexParams(index.get());
        load_index_info.cache_index =
            CreateTestCacheIndex("int64_scalar_index", std::move(index));
        load_index_info.field_id = fid.get();
        segment->LoadIndex(load_index_info);
    }

    void
    LoadString1ScalarIndex(const std::string& index_type) {
        auto fid = fields.at("string1");
        auto file_manager_ctx = storage::FileManagerContext();
        file_manager_ctx.fieldDataMeta.field_schema.set_data_type(
            milvus::proto::schema::VarChar);
        file_manager_ctx.fieldDataMeta.field_schema.set_fieldid(fid.get());
        file_manager_ctx.fieldDataMeta.field_id = fid.get();
        milvus::storage::IndexMeta index_meta;
        index_meta.field_id = fid.get();
        index_meta.build_id = 1000 + fid.get();
        index_meta.index_version = 2000 + fid.get();
        file_manager_ctx.indexMeta = index_meta;

        index::CreateIndexInfo create_index_info;
        create_index_info.field_type = milvus::DataType::VARCHAR;
        create_index_info.index_type = index_type;
        auto index = index::IndexFactory::GetInstance().CreateScalarIndex(
            create_index_info, file_manager_ctx);

        std::vector<std::string> data;
        data.reserve(RowCount());
        for (int64_t i = 0; i < RowCount(); ++i) {
            data.push_back("test" + std::to_string(i));
        }
        index->BuildWithRawDataForUT(data.size(), data.data());

        segcore::LoadIndexInfo load_index_info;
        load_index_info.index_params = GenIndexParams(index.get());
        load_index_info.cache_index =
            CreateTestCacheIndex("string1_scalar_index", std::move(index));
        load_index_info.field_id = fid.get();
        segment->LoadIndex(load_index_info);
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
    auto pw = segment->chunk_data<int64_t>(nullptr, fid, 0);
    auto d = pw.get();
    std::copy(
        d.data(), d.data() + test_data_count, data.begin() + test_data_count);

    index->BuildWithRawDataForUT(data.size(), data.data());
    segcore::LoadIndexInfo load_index_info;
    load_index_info.index_params = GenIndexParams(index.get());
    load_index_info.cache_index =
        CreateTestCacheIndex("test_index", std::move(index));
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

TEST_P(TestChunkSegmentStorageV2, TestColumnExprWithScalarIndexRawData) {
    LoadInt64ScalarIndex(index::ASCENDING_SORT);
    ASSERT_TRUE(segment->HasRawData(fields.at("int64").get()));

    auto query_config = std::make_shared<exec::QueryConfig>(
        std::unordered_map<std::string, std::string>{
            {exec::QueryConfig::kExprEvalBatchSize, "4096"}});
    exec::QueryContext query_context("column_expr_scalar_index_raw_data",
                                     segment.get(),
                                     RowCount(),
                                     MAX_TIMESTAMP,
                                     0,
                                     0,
                                     query::PlanOptions(),
                                     query_config);
    exec::ExecContext exec_context(&query_context);

    std::vector<expr::TypedExprPtr> exprs{std::make_shared<expr::ColumnExpr>(
        expr::ColumnInfo(fields.at("int64"), milvus::DataType::INT64))};
    exec::ExprSet expr_set(exprs, &exec_context);
    exec::EvalCtx eval_context(&exec_context);

    int64_t offset = 0;
    while (offset < RowCount()) {
        std::vector<VectorPtr> results;
        expr_set.Eval(eval_context, results);
        ASSERT_EQ(1, results.size());

        auto column = std::dynamic_pointer_cast<ColumnVector>(results[0]);
        ASSERT_NE(column, nullptr);
        auto expected_batch_size = std::min<int64_t>(4096, RowCount() - offset);
        ASSERT_EQ(expected_batch_size, column->size());

        auto values = column->RawAsValues<int64_t>();
        for (int64_t i = 0; i < expected_batch_size; ++i) {
            ASSERT_TRUE(column->ValidAt(i));
            ASSERT_EQ(offset + i, values[i]);
        }
        offset += expected_batch_size;
    }
}

TEST_P(TestChunkSegmentStorageV2,
       TestChunkDataAccessorFallsBackWhenPinnedIndexViewIsEmpty) {
    SegmentChunkReader reader(nullptr, segment.get(), RowCount());

    auto accessor = reader.GetChunkDataAccessor(
        milvus::DataType::INT64, fields.at("int64"), 0, {});

    auto value = accessor(7);
    ASSERT_TRUE(value.has_value());
    ASSERT_EQ(7, segcore::get_from_variant<int64_t>(value));
}

TEST_P(TestChunkSegmentStorageV2,
       TestChunkDataAccessorUsesGlobalOffsetForFieldLevelScalarIndex) {
    auto raw_lookup_index = std::make_unique<RawLookupOnlyIndex>();
    std::vector<PinWrapper<const index::IndexBase*>> pinned_indexes;
    pinned_indexes.emplace_back(raw_lookup_index.get());

    SegmentChunkReader reader(nullptr, segment.get(), RowCount());
    auto accessor = reader.GetChunkDataAccessor(
        milvus::DataType::INT64,
        fields.at("int64"),
        1,
        {pinned_indexes.data(), pinned_indexes.size()});

    auto expected_offset =
        segment->num_rows_until_chunk(fields.at("int64"), 1) + 7;
    auto value = accessor(7);
    ASSERT_TRUE(value.has_value());
    ASSERT_EQ(expected_offset, segcore::get_from_variant<int64_t>(value));
    ASSERT_EQ(expected_offset, raw_lookup_index->last_lookup_offset);
}

TEST_P(TestChunkSegmentStorageV2,
       TestChunkDataAccessorThrowsWhenPinnedIndexAndRawDataAreUnavailable) {
    LoadString1ScalarIndex(index::INVERTED_INDEX_TYPE);
    segment->DropFieldData(fields.at("string1"));
    ASSERT_FALSE(segment->HasRawData(fields.at("string1").get()));
    ASSERT_EQ(0, segment->num_chunk_data(fields.at("string1")));

    SegmentChunkReader reader(nullptr, segment.get(), RowCount());
    EXPECT_THROW(reader.GetChunkDataAccessor(
                     milvus::DataType::VARCHAR, fields.at("string1"), 0, {}),
                 SegcoreError);
}

TEST_P(TestChunkSegmentStorageV2,
       TestColumnExprOffsetInputFallsBackWhenScalarIndexHasNoRawData) {
    LoadInt64ScalarIndex(index::INVERTED_INDEX_TYPE);
    ASSERT_FALSE(segment->HasRawData(fields.at("int64").get()));

    auto query_config = std::make_shared<exec::QueryConfig>(
        std::unordered_map<std::string, std::string>{
            {exec::QueryConfig::kExprEvalBatchSize, "4096"}});
    exec::QueryContext query_context("column_expr_offset_input",
                                     segment.get(),
                                     RowCount(),
                                     MAX_TIMESTAMP,
                                     0,
                                     0,
                                     query::PlanOptions(),
                                     query_config);
    exec::ExecContext exec_context(&query_context);

    std::vector<expr::TypedExprPtr> exprs{std::make_shared<expr::ColumnExpr>(
        expr::ColumnInfo(fields.at("int64"), milvus::DataType::INT64))};
    exec::ExprSet expr_set(exprs, &exec_context);

    exec::OffsetVector offsets;
    offsets.push_back(7);
    offsets.push_back(7000);
    exec::EvalCtx eval_context(&exec_context, &expr_set, &offsets);

    std::vector<VectorPtr> results;
    expr_set.Eval(eval_context, results);
    ASSERT_EQ(1, results.size());

    auto column = std::dynamic_pointer_cast<ColumnVector>(results[0]);
    ASSERT_NE(column, nullptr);
    ASSERT_EQ(offsets.size(), column->size());

    auto values = column->RawAsValues<int64_t>();
    for (size_t i = 0; i < offsets.size(); ++i) {
        ASSERT_TRUE(column->ValidAt(i));
        ASSERT_EQ(offsets[i], values[i]);
    }
}

TEST_P(TestChunkSegmentStorageV2,
       TestColumnExprOffsetInputThrowsWhenIndexAndRawDataAreUnavailable) {
    LoadString1ScalarIndex(index::INVERTED_INDEX_TYPE);
    segment->DropFieldData(fields.at("string1"));
    ASSERT_FALSE(segment->HasRawData(fields.at("string1").get()));
    ASSERT_EQ(0, segment->num_chunk_data(fields.at("string1")));

    auto query_config = std::make_shared<exec::QueryConfig>(
        std::unordered_map<std::string, std::string>{
            {exec::QueryConfig::kExprEvalBatchSize, "4096"}});
    exec::QueryContext query_context("column_expr_offset_input_no_raw_data",
                                     segment.get(),
                                     RowCount(),
                                     MAX_TIMESTAMP,
                                     0,
                                     0,
                                     query::PlanOptions(),
                                     query_config);
    exec::ExecContext exec_context(&query_context);

    std::vector<expr::TypedExprPtr> exprs{std::make_shared<expr::ColumnExpr>(
        expr::ColumnInfo(fields.at("string1"), milvus::DataType::VARCHAR))};
    exec::ExprSet expr_set(exprs, &exec_context);

    exec::OffsetVector offsets;
    offsets.push_back(0);
    exec::EvalCtx eval_context(&exec_context, &expr_set, &offsets);

    std::vector<VectorPtr> results;
    EXPECT_THROW(expr_set.Eval(eval_context, results), SegcoreError);
}

TEST_P(TestChunkSegmentStorageV2,
       TestCompareExprSkippedCursorWithScalarIndexWithoutRawData) {
    LoadInt64ScalarIndex(index::INVERTED_INDEX_TYPE);
    ASSERT_FALSE(segment->HasRawData(fields.at("int64").get()));

    proto::plan::GenericValue threshold;
    threshold.set_int64_val(12000);
    auto range_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(fields.at("int64"), milvus::DataType::INT64),
        proto::plan::OpType::GreaterEqual,
        threshold);
    auto right_field = GetParam() ? fields.at("int64") : fields.at("pk");
    auto compare_expr =
        std::make_shared<expr::CompareExpr>(fields.at("int64"),
                                            right_field,
                                            milvus::DataType::INT64,
                                            milvus::DataType::INT64,
                                            proto::plan::OpType::Equal);
    auto conjunct_expr = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::And, range_expr, compare_expr);
    auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                       conjunct_expr);

    auto query_config = std::make_shared<exec::QueryConfig>(
        std::unordered_map<std::string, std::string>{
            {exec::QueryConfig::kExprEvalBatchSize, "6000"}});
    auto query_context =
        std::make_shared<exec::QueryContext>(DEAFULT_QUERY_ID,
                                             segment.get(),
                                             RowCount(),
                                             MAX_TIMESTAMP,
                                             0,
                                             0,
                                             query::PlanOptions(),
                                             query_config);
    auto plan_fragment = plan::PlanFragment(plan);
    auto final =
        query::ExecPlanNodeVisitor::ExecuteTask(plan_fragment, query_context);
    final.flip();
    ASSERT_EQ(RowCount() - threshold.int64_val(), final.count());
}

// Test DropFieldData behavior based on parquet file structure.
// In this test setup, the parquet files are organized as:
//   - paths[0] contains columns {0, 4, 3} = int64, ts, string2 (multi-field column group)
//   - paths[1] contains column {2} = string1 (single-field group)
//   - paths[2] contains column {1} = pk (single-field group)
// When storage_version=2 reads a parquet file with multiple columns, they become
// a multi-field column group, so DropFieldData should be skipped for those fields.
TEST_P(TestChunkSegmentStorageV2, TestDropFieldDataWithColumnGroups) {
    auto segment_impl = dynamic_cast<ChunkedSegmentSealedImpl*>(segment.get());
    ASSERT_NE(segment_impl, nullptr);

    // Verify fields have data initially
    auto int64_fid = fields.at("int64");
    auto string1_fid = fields.at("string1");
    auto string2_fid = fields.at("string2");

    EXPECT_TRUE(segment_impl->HasFieldData(int64_fid))
        << "int64 field data should be ready";
    EXPECT_TRUE(segment_impl->HasFieldData(string1_fid))
        << "string1 field data should be ready";
    EXPECT_TRUE(segment_impl->HasFieldData(string2_fid))
        << "string2 field data should be ready";

    // int64 and string2 are in the same parquet file (multi-field column group)
    // DropFieldData should be SKIPPED for these fields
    segment_impl->DropFieldData(int64_fid);

    // int64 should still have data because it's in a multi-field column group
    EXPECT_TRUE(segment_impl->HasFieldData(int64_fid))
        << "int64 field data should still be ready (multi-field column group)";

    // string2 is in the same group, should also still have data
    EXPECT_TRUE(segment_impl->HasFieldData(string2_fid))
        << "string2 field data should still be ready (same column group as "
           "int64)";

    // string1 is in its own parquet file (single-field group)
    // DropFieldData SHOULD work for this field
    segment_impl->DropFieldData(string1_fid);
    EXPECT_FALSE(segment_impl->HasFieldData(string1_fid))
        << "string1 field data should be dropped (single-field column group)";

    // int64 and string2 should still have data
    EXPECT_TRUE(segment_impl->HasFieldData(int64_fid))
        << "int64 field data should still be ready";
    EXPECT_TRUE(segment_impl->HasFieldData(string2_fid))
        << "string2 field data should still be ready";
}
