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

#include <arrow/api.h>
#include <arrow/array/array_base.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/record_batch.h>
#include <arrow/type_fwd.h>
#include <gtest/gtest.h>
#include <parquet/properties.h>
#include <parquet/schema.h>
#include <parquet/statistics.h>
#include <parquet/types.h>
#include <stdlib.h>
#include <time.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <numeric>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "segcore/default_fs.h"

#include "NamedType/named_type_impl.hpp"
#include "cachinglayer/CacheSlot.h"
#include "cachinglayer/TieredStorageConfig.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/LoadInfo.h"
#include "common/OpContext.h"
#include "common/Schema.h"
#include "common/Span.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "exec/QueryContext.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "monitor/Monitor.h"
#include "expr/ITypeExpr.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "index/Index.h"
#include "index/IndexFactory.h"
#include "index/IndexInfo.h"
#include "index/Meta.h"
#include "index/ScalarIndex.h"
#include "milvus-storage/common/config.h"
#include "milvus-storage/filesystem/fs.h"
#include "milvus-storage/packed/writer.h"
#include "mmap/ChunkedColumnGroup.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/PlanImpl.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentChunkReader.h"
#include "segcore/SegmentSealed.h"
#include "segcore/search_result_export_c.h"
#include "segcore/segment_c.h"
#include "segcore/Types.h"
#include "segcore/storagev2translator/GroupCTMeta.h"
#include "segcore/storagev1translator/ChunkTranslator.h"
#include "storage/FileManager.h"
#include "storage/Types.h"
#include "test_utils/c_api_test_utils.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
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

class StorageV2CellTargetGuard {
 public:
    explicit StorageV2CellTargetGuard(int64_t bytes)
        : old_bytes_(segcore::storagev2translator::GetCellTargetSizeBytes()) {
        segcore::storagev2translator::SetCellTargetSizeBytes(bytes);
    }

    ~StorageV2CellTargetGuard() {
        segcore::storagev2translator::SetCellTargetSizeBytes(old_bytes_);
    }

 private:
    int64_t old_bytes_;
};

class StorageV2TempDirGuard {
 public:
    StorageV2TempDirGuard(milvus_storage::ArrowFileSystemPtr fs,
                          std::string path)
        : fs_(std::move(fs)), path_(std::move(path)) {
        static_cast<void>(fs_->DeleteDir(path_));
    }

    ~StorageV2TempDirGuard() {
        static_cast<void>(fs_->DeleteDir(path_));
    }

 private:
    milvus_storage::ArrowFileSystemPtr fs_;
    std::string path_;
};

void
AddWarmupProperty(milvus::proto::schema::CollectionSchema& schema_proto,
                  const std::string& key,
                  const std::string& value) {
    auto* prop = schema_proto.add_properties();
    prop->set_key(key);
    prop->set_value(value);
}
}  // namespace

TEST(ChunkedSegmentSealedStorageV2,
     DirectLoadFieldDataUsesVectorIndexWarmupForNoIndexVector) {
    constexpr int64_t kPkFieldId = START_USER_FIELDID;
    constexpr int64_t kVectorFieldId = START_USER_FIELDID + 1;
    constexpr int64_t kDim = 4;
    constexpr int64_t kRowCount = 4;

    milvus::proto::schema::CollectionSchema schema_proto;
    auto* pk_field = schema_proto.add_fields();
    pk_field->set_fieldid(kPkFieldId);
    pk_field->set_name("pk");
    pk_field->set_data_type(milvus::proto::schema::DataType::Int64);
    pk_field->set_is_primary_key(true);

    auto* vector_field = schema_proto.add_fields();
    vector_field->set_fieldid(kVectorFieldId);
    vector_field->set_name("vec");
    vector_field->set_data_type(milvus::proto::schema::DataType::FloatVector);
    auto* dim = vector_field->add_type_params();
    dim->set_key("dim");
    dim->set_value(std::to_string(kDim));

    AddWarmupProperty(schema_proto, "warmup.vectorField", "disable");
    AddWarmupProperty(schema_proto, "warmup.vectorIndex", "sync");
    auto schema = Schema::ParseFrom(schema_proto);

    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    const std::string dir = "test_data/storage_v2_direct_warmup";
    StorageV2TempDirGuard dir_guard(fs, dir);
    const std::string path = dir + "/vec.parquet";
    ASSERT_TRUE(fs->CreateDir(dir).ok());

    auto arrow_schema = schema->ConvertToArrowSchema();
    std::vector<std::string> paths{path};
    auto storage_config = milvus_storage::StorageConfig();
    std::vector<std::vector<int>> column_groups{{1}};
    auto writer_result = milvus_storage::PackedRecordBatchWriter::Make(
        fs,
        paths,
        arrow_schema,
        storage_config,
        column_groups,
        16 * 1024 * 1024,
        ::parquet::default_writer_properties());
    ASSERT_TRUE(writer_result.ok()) << writer_result.status().ToString();
    auto writer = writer_result.ValueOrDie();
    auto dataset = DataGen(schema, kRowCount);
    auto record_batch = ConvertToArrowRecordBatch(dataset, kDim, arrow_schema);
    ASSERT_NE(record_batch, nullptr);
    ASSERT_TRUE(writer->Write(record_batch).ok());
    ASSERT_TRUE(writer->Close().ok());

    LoadFieldDataInfo load_info;
    load_info.storage_version = 2;
    FieldBinlogInfo field_info{
        kVectorFieldId,
        kRowCount,
        std::vector<int64_t>{kRowCount},
        std::vector<int64_t>{kRowCount * kDim *
                             static_cast<int64_t>(sizeof(float))},
        false,
        "disable",
        std::vector<std::string>{path},
        std::vector<int64_t>{kVectorFieldId}};
    load_info.field_infos.emplace(kVectorFieldId, std::move(field_info));

    auto segment = segcore::CreateSealedSegment(
        schema, nullptr, -1, segcore::SegcoreConfig::default_config(), true);
    segment->LoadFieldData(load_info);

    auto* sealed = dynamic_cast<ChunkedSegmentSealedImpl*>(segment.get());
    ASSERT_NE(sealed, nullptr);
    auto runtime = sealed->TestCloneMutableRuntimeResourceState();
    auto field = runtime->fields.find(FieldId(kVectorFieldId));
    ASSERT_NE(field, runtime->fields.end());
    auto proxy_column =
        std::dynamic_pointer_cast<ProxyChunkColumn>(field->second);
    ASSERT_NE(proxy_column, nullptr);
    EXPECT_EQ(proxy_column->TestCacheWarmupPolicy(),
              CacheWarmupPolicy::CacheWarmupPolicy_Sync);
}

class TestChunkSegmentStorageV2 : public testing::TestWithParam<bool> {
 protected:
    segcore::SegmentSealedUPtr
    CreateSegment(bool is_sorted_by_pk) {
        auto seg = segcore::CreateSealedSegment(
            schema_,
            nullptr,
            -1,
            segcore::SegcoreConfig::default_config(),
            is_sorted_by_pk);
        seg->AddFieldDataInfoForSealed(load_info_);
        for (auto& [id, info] : load_info_.field_infos) {
            LoadFieldDataInfo load_field_info;
            load_field_info.storage_version = 2;
            load_field_info.field_infos.emplace(id, info);
            seg->LoadFieldData(load_field_info);
        }
        return seg;
    }

    segcore::SegmentSealedUPtr
    CreateSegmentByLoadInfo(proto::segcore::SegmentLoadInfo proto,
                            bool is_sorted_by_pk) {
        auto seg = segcore::CreateSealedSegment(
            schema_,
            nullptr,
            -1,
            segcore::SegcoreConfig::default_config(),
            is_sorted_by_pk);
        auto* sealed = dynamic_cast<ChunkedSegmentSealedImpl*>(seg.get());
        EXPECT_NE(sealed, nullptr);
        if (sealed == nullptr) {
            return seg;
        }
        sealed->SetLoadInfo(std::move(proto));
        milvus::OpContext op_ctx;
        milvus::tracer::TraceContext trace_ctx;
        sealed->Load(trace_ctx, &op_ctx);
        return seg;
    }

    void
    SetUp() override {
        bool pk_is_string = GetParam();
        auto* test_info =
            ::testing::UnitTest::GetInstance()->current_test_info();
        auto test_name = test_info == nullptr ? std::string()
                                              : std::string(test_info->name());
        if (test_name.find("ReduceStringPkWithSimulatedAnnResult") !=
            std::string::npos) {
            if (!pk_is_string) {
                GTEST_SKIP() << "VARCHAR primary key fast path only";
            }
            chunk_num = 10;
            test_data_count = 100000;
            fixed_string_width = 32;
        }

        schema_ = segcore::GenChunkedSegmentTestSchema(pk_is_string);

        // Use globally initialized ArrowFileSystem
        auto fs = milvus::segcore::GetDefaultArrowFileSystem();

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
        test_data_created = true;

        std::vector<std::vector<int>> column_groups = {
            {0, 1, 4}, {2}, {3}};  // narrow columns and wide columns
        auto writer_memory = 16 * 1024 * 1024;
        auto storage_config = milvus_storage::StorageConfig();

        // Create writer
        auto result = milvus_storage::PackedRecordBatchWriter::Make(
            fs,
            paths,
            schema_->ConvertToArrowSchema(),
            storage_config,
            column_groups,
            writer_memory,
            ::parquet::default_writer_properties());
        EXPECT_TRUE(result.ok());
        auto writer = result.ValueOrDie();

        // Generate and write data
        int64_t row_count = 0;
        int start_id = 0;

        string_data.clear();
        string_data.reserve(RowCount());
        for (int64_t i = 0; i < RowCount(); i++) {
            string_data.push_back(MakeStringValue(i));
        }
        std::sort(string_data.begin(), string_data.end());

        fields = {{"int64", schema_->get_field_id(FieldName("int64"))},
                  {"pk", schema_->get_field_id(FieldName("pk"))},
                  {"ts", TimestampFieldID},
                  {"string1", schema_->get_field_id(FieldName("string1"))},
                  {"string2", schema_->get_field_id(FieldName("string2"))}};

        auto arrow_schema = schema_->ConvertToArrowSchema();
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
                    str_values.reserve(test_data_count);
                    for (int j = 0; j < test_data_count; j++) {
                        str_values.push_back(string_data[start_id + j]);
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
                schema_->ConvertToArrowSchema(), test_data_count, arrays);
            row_count += test_data_count;
            EXPECT_TRUE(writer->Write(record_batch).ok());
        }
        EXPECT_TRUE(writer->Close().ok());

        load_info_.field_infos.emplace(
            int64_t(0),
            FieldBinlogInfo{
                int64_t(0),
                static_cast<int64_t>(row_count),
                std::vector<int64_t>(chunk_num * test_data_count),
                std::vector<int64_t>(chunk_num * test_data_count * 4),
                false,
                "",
                std::vector<std::string>({paths[0]})});
        load_info_.field_infos.emplace(
            int64_t(102),
            FieldBinlogInfo{
                int64_t(102),
                static_cast<int64_t>(row_count),
                std::vector<int64_t>(chunk_num * test_data_count),
                std::vector<int64_t>(chunk_num * test_data_count * 4),
                false,
                "",
                std::vector<std::string>({paths[1]})});
        load_info_.field_infos.emplace(
            int64_t(103),
            FieldBinlogInfo{
                int64_t(103),
                static_cast<int64_t>(row_count),
                std::vector<int64_t>(chunk_num * test_data_count),
                std::vector<int64_t>(chunk_num * test_data_count * 4),
                false,
                "",
                std::vector<std::string>({paths[2]})});
        load_info_.storage_version = 2;
        segment = CreateSegment(true);
    }

    void
    TearDown() override {
        if (!test_data_created) {
            return;
        }
        // Clean up test data directory
        auto fs = milvus::segcore::GetDefaultArrowFileSystem();
        auto status = fs->DeleteDir("test_data");
        ASSERT_TRUE(status.ok());
    }

    int64_t
    RowCount() const {
        return chunk_num * test_data_count;
    }

    std::string
    MakeStringValue(int64_t row_id) const {
        if (fixed_string_width == 0) {
            return "test" + std::to_string(row_id);
        }

        auto suffix = std::to_string(row_id);
        AssertInfo(suffix.size() + 2 <= fixed_string_width,
                   "row id is too large for fixed string width");
        std::string value = "pk";
        value.append(fixed_string_width - value.size() - suffix.size(), '0');
        value.append(suffix);
        return value;
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
    SchemaPtr schema_;
    LoadFieldDataInfo load_info_;
    int chunk_num = 2;
    int test_data_count = 10000;
    size_t fixed_string_width = 0;
    bool test_data_created = false;
    std::unordered_map<std::string, FieldId> fields;
    std::vector<std::string> string_data;
};

INSTANTIATE_TEST_SUITE_P(TestChunkSegmentStorageV2,
                         TestChunkSegmentStorageV2,
                         testing::Bool());

TEST_P(TestChunkSegmentStorageV2, ReduceStringPkWithSimulatedAnnResult) {
    constexpr int64_t nq = 4;
    constexpr int64_t candidate_topk = 500;
    constexpr int64_t final_topk = 32;
    constexpr int64_t pk_lookup_count = nq * candidate_topk;
    static_assert(pk_lookup_count == 2000);
    ASSERT_EQ(RowCount(), 1000000);
    ASSERT_EQ(fixed_string_width, 32);

    milvus::query::Plan plan(schema_);
    plan.plan_node_ = std::make_unique<milvus::query::VectorPlanNode>();
    plan.plan_node_->search_info_.topk_ = final_topk;
    plan.plan_node_->search_info_.metric_type_ = knowhere::metric::L2;
    plan.target_entries_.push_back(fields.at("string1"));

    auto* sealed = dynamic_cast<ChunkedSegmentSealedImpl*>(segment.get());
    ASSERT_NE(sealed, nullptr);

    auto offset_at = [this, candidate_topk](int64_t qi, int64_t rank) {
        auto lookup_index = qi * candidate_topk + rank;
        return (lookup_index * 499979 + qi * 9973) % RowCount();
    };

    auto make_result = [&]() {
        SearchResult result;
        result.total_nq_ = nq;
        result.unity_topK_ = candidate_topk;
        result.total_data_cnt_ = RowCount();
        result.segment_ = segment.get();
        result.read_lease_ =
            sealed->AcquireReadLease(folly::CancellationToken());
        result.seg_offsets_.resize(nq * candidate_topk);
        result.distances_.resize(nq * candidate_topk);
        for (int64_t qi = 0; qi < nq; ++qi) {
            for (int64_t rank = 0; rank < candidate_topk; ++rank) {
                auto loc = qi * candidate_topk + rank;
                result.seg_offsets_[loc] = offset_at(qi, rank);
                result.distances_[loc] = static_cast<float>(rank);
            }
        }
        return result;
    };

    auto fast_pk_result = make_result();
    auto generic_pk_result = make_result();
    auto start = std::chrono::steady_clock::now();
    segment->FillPrimaryKeys(&plan, fast_pk_result);
    auto fast_fill_us = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - start)
                            .count();
    start = std::chrono::steady_clock::now();
    static_cast<SegmentInternalInterface*>(segment.get())
        ->SegmentInternalInterface::FillPrimaryKeys(&plan, generic_pk_result);
    auto generic_fill_us =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start)
            .count();
    RecordProperty("fast_pk_fill_us", fast_fill_us);
    RecordProperty("generic_pk_fill_us", generic_fill_us);
    RecordProperty("row_count", std::to_string(RowCount()));
    RecordProperty("varchar_pk_len", std::to_string(fixed_string_width));
    RecordProperty("pk_lookup_count", std::to_string(pk_lookup_count));

    ASSERT_EQ(fast_pk_result.pk_type_, DataType::VARCHAR);
    ASSERT_EQ(generic_pk_result.pk_type_, DataType::VARCHAR);
    ASSERT_EQ(fast_pk_result.primary_keys_.size(),
              generic_pk_result.primary_keys_.size());
    for (size_t i = 0; i < fast_pk_result.primary_keys_.size(); ++i) {
        ASSERT_EQ(std::get<std::string>(fast_pk_result.primary_keys_[i]),
                  std::get<std::string>(generic_pk_result.primary_keys_[i]));
    }

    auto reduce_result = make_result();
    std::vector<CSearchResult> c_search_results{
        reinterpret_cast<CSearchResult>(&reduce_result)};
    std::vector<int64_t> slice_nqs{nq};
    std::vector<int64_t> slice_topks{final_topk};
    int64_t all_search_count = 0;
    CTraceContext trace{0, 0, 0};
    auto status =
        PrepareSearchResultsForExport(trace,
                                      reinterpret_cast<CSearchPlan>(&plan),
                                      nullptr,
                                      c_search_results.data(),
                                      c_search_results.size(),
                                      slice_nqs.data(),
                                      slice_nqs.size(),
                                      slice_topks.data(),
                                      &all_search_count,
                                      nullptr);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    ASSERT_EQ(all_search_count, reduce_result.total_data_cnt_);

    ASSERT_EQ(reduce_result.primary_keys_.size(), nq * candidate_topk);
    ASSERT_EQ(reduce_result.seg_offsets_.size(), nq * candidate_topk);
    ASSERT_EQ(reduce_result.topk_per_nq_prefix_sum_.size(), nq + 1);
    ASSERT_EQ(reduce_result.topk_per_nq_prefix_sum_.back(),
              nq * candidate_topk);

    for (int64_t qi = 0; qi < nq; ++qi) {
        for (int64_t rank = 0; rank < final_topk; ++rank) {
            auto loc = qi * candidate_topk + rank;
            auto expected_offset = offset_at(qi, rank);
            auto expected_string = string_data[expected_offset];
            ASSERT_EQ(reduce_result.seg_offsets_[loc], expected_offset);
            ASSERT_EQ(std::get<std::string>(reduce_result.primary_keys_[loc]),
                      expected_string);
            ASSERT_FLOAT_EQ(reduce_result.distances_[loc],
                            static_cast<float>(rank));
        }
    }

    std::vector<int32_t> result_seg_indices(nq * final_topk, 0);
    std::vector<int64_t> result_seg_offsets;
    result_seg_offsets.reserve(nq * final_topk);
    for (int64_t qi = 0; qi < nq; ++qi) {
        for (int64_t rank = 0; rank < final_topk; ++rank) {
            result_seg_offsets.push_back(offset_at(qi, rank));
        }
    }

    CProto c_proto{};
    status = FillOutputFieldsOrdered(c_search_results.data(),
                                     c_search_results.size(),
                                     reinterpret_cast<CSearchPlan>(&plan),
                                     result_seg_indices.data(),
                                     result_seg_offsets.data(),
                                     result_seg_offsets.size(),
                                     &c_proto,
                                     nullptr);
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    ASSERT_GT(c_proto.proto_size, 0);
    milvus::proto::schema::SearchResultData search_result_data;
    ASSERT_TRUE(search_result_data.ParseFromArray(c_proto.proto_blob,
                                                  c_proto.proto_size));
    ASSERT_EQ(search_result_data.fields_data_size(), 1);
    const auto& marshaled_string_output =
        search_result_data.fields_data(0).scalars().string_data().data();
    ASSERT_EQ(marshaled_string_output.size(), nq * final_topk);
    free(const_cast<void*>(c_proto.proto_blob));

    for (int64_t qi = 0; qi < nq; ++qi) {
        for (int64_t rank = 0; rank < final_topk; ++rank) {
            auto loc = qi * final_topk + rank;
            auto expected_offset = offset_at(qi, rank);
            auto expected_string = string_data[expected_offset];
            ASSERT_EQ(marshaled_string_output.Get(loc), expected_string);
        }
    }
}

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

TEST(TestChunkSegmentStorageV2Regression,
     TestCompareExprFallsBackWhenColumnGroupChunksAreMisaligned) {
    StorageV2CellTargetGuard cell_target_guard(1 * 1024 * 1024);

    auto schema = std::make_shared<Schema>();
    auto left_fid = schema->AddDebugField("left", DataType::INT64, false);
    auto right_fid = schema->AddDebugField("right", DataType::INT64, false);
    schema->AddDebugField("payload", DataType::VARCHAR, false);
    schema->AddField(FieldName("ts"),
                     TimestampFieldID,
                     DataType::INT64,
                     false,
                     std::nullopt);
    schema->set_primary_field_id(right_fid);

    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    const std::string root = "test_compare_expr_misaligned_storage_v2";
    auto cleanup_status = fs->DeleteDir(root);
    (void)cleanup_status;
    ASSERT_TRUE(fs->CreateDir(root + "/0").ok());
    ASSERT_TRUE(
        fs->CreateDir(root + "/" + std::to_string(right_fid.get())).ok());

    std::vector<std::string> paths = {
        root + "/0/10000.parquet",
        root + "/" + std::to_string(right_fid.get()) + "/10001.parquet"};
    std::vector<std::vector<int>> column_groups = {{0, 2, 3}, {1}};
    auto storage_config = milvus_storage::StorageConfig();
    auto result = milvus_storage::PackedRecordBatchWriter::Make(
        fs,
        paths,
        schema->ConvertToArrowSchema(),
        storage_config,
        column_groups,
        16 * 1024 * 1024,
        ::parquet::default_writer_properties());
    ASSERT_TRUE(result.ok());
    auto writer = result.ValueOrDie();

    constexpr int64_t rows_per_batch = 10000;
    constexpr int64_t batch_count = 2;
    auto arrow_schema = schema->ConvertToArrowSchema();
    for (int64_t batch = 0; batch < batch_count; ++batch) {
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        auto start = batch * rows_per_batch;
        for (int i = 0; i < arrow_schema->fields().size(); ++i) {
            if (arrow_schema->fields()[i]->type()->id() == arrow::Type::INT64) {
                std::vector<int64_t> values(rows_per_batch);
                std::iota(values.begin(), values.end(), start);
                arrow::Int64Builder builder;
                ASSERT_TRUE(
                    builder.AppendValues(values.data(), rows_per_batch).ok());
                std::shared_ptr<arrow::Array> array;
                ASSERT_TRUE(builder.Finish(&array).ok());
                arrays.push_back(array);
            } else {
                arrow::StringBuilder builder;
                std::vector<std::string> values;
                values.reserve(rows_per_batch);
                for (int64_t row = 0; row < rows_per_batch; ++row) {
                    values.emplace_back(2048, 'x');
                }
                ASSERT_TRUE(builder.AppendValues(values).ok());
                std::shared_ptr<arrow::Array> array;
                ASSERT_TRUE(builder.Finish(&array).ok());
                arrays.push_back(array);
            }
        }

        auto record_batch =
            arrow::RecordBatch::Make(arrow_schema, rows_per_batch, arrays);
        ASSERT_TRUE(writer->Write(record_batch).ok());
    }
    ASSERT_TRUE(writer->Close().ok());

    const int64_t row_count = rows_per_batch * batch_count;
    LoadFieldDataInfo load_info;
    load_info.storage_version = 2;
    load_info.field_infos.emplace(
        int64_t(0),
        FieldBinlogInfo{int64_t(0),
                        row_count,
                        std::vector<int64_t>(row_count),
                        std::vector<int64_t>(row_count * 4),
                        false,
                        "",
                        std::vector<std::string>({paths[0]})});
    load_info.field_infos.emplace(
        right_fid.get(),
        FieldBinlogInfo{right_fid.get(),
                        row_count,
                        std::vector<int64_t>(row_count),
                        std::vector<int64_t>(row_count * 4),
                        false,
                        "",
                        std::vector<std::string>({paths[1]})});

    auto segment = segcore::CreateSealedSegment(
        schema, nullptr, -1, segcore::SegcoreConfig::default_config(), true);
    segment->AddFieldDataInfoForSealed(load_info);
    for (auto& [id, info] : load_info.field_infos) {
        LoadFieldDataInfo one_field;
        one_field.storage_version = 2;
        one_field.field_infos.emplace(id, info);
        segment->LoadFieldData(one_field);
    }

    ASSERT_GT(segment->num_chunk_data(left_fid),
              segment->num_chunk_data(right_fid));

    auto expr =
        std::make_shared<expr::CompareExpr>(left_fid,
                                            right_fid,
                                            milvus::DataType::INT64,
                                            milvus::DataType::INT64,
                                            proto::plan::OpType::GreaterEqual);
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);

    auto final =
        query::ExecuteQueryExpr(plan, segment.get(), row_count, MAX_TIMESTAMP);
    ASSERT_EQ(row_count, final.count());

    ASSERT_TRUE(fs->DeleteDir(root).ok());
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
    exec::EvalCtx eval_context(&exec_context, &offsets);

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
    exec::EvalCtx eval_context(&exec_context, &offsets);

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
    auto row =
        query::ExecPlanNodeVisitor::ExecuteTask(plan_fragment, query_context);
    ASSERT_NE(row, nullptr);
    ASSERT_EQ(row->childrens().size(), 1);
    auto col_vec = std::dynamic_pointer_cast<ColumnVector>(row->childrens()[0]);
    ASSERT_NE(col_vec, nullptr);
    BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
    BitsetType final(view);
    final.flip();
    ASSERT_EQ(RowCount() - threshold.int64_val(), final.count());
}

TEST_P(TestChunkSegmentStorageV2, LoadGroupedBinlogPreservesChildFieldIds) {
    auto segment_load_info = proto::segcore::SegmentLoadInfo();
    segment_load_info.set_segmentid(100);
    segment_load_info.set_num_of_rows(chunk_num * test_data_count);
    segment_load_info.set_storageversion(2);
    segment_load_info.set_is_sorted(true);

    auto* grouped_binlog = segment_load_info.add_binlog_paths();
    grouped_binlog->set_fieldid(0);
    grouped_binlog->add_child_fields(fields.at("int64").get());
    grouped_binlog->add_child_fields(fields.at("pk").get());
    grouped_binlog->add_child_fields(TimestampFieldID.get());
    auto* grouped_log = grouped_binlog->add_binlogs();
    grouped_log->set_log_path(load_info_.field_infos.at(0).insert_files[0]);
    grouped_log->set_entries_num(chunk_num * test_data_count);
    grouped_log->set_memory_size(
        load_info_.field_infos.at(0).memory_sizes.front());

    auto* string1_binlog = segment_load_info.add_binlog_paths();
    string1_binlog->set_fieldid(102);
    string1_binlog->add_child_fields(fields.at("string1").get());
    auto* string1_log = string1_binlog->add_binlogs();
    string1_log->set_log_path(load_info_.field_infos.at(102).insert_files[0]);
    string1_log->set_entries_num(chunk_num * test_data_count);
    string1_log->set_memory_size(
        load_info_.field_infos.at(102).memory_sizes.front());

    auto* string2_binlog = segment_load_info.add_binlog_paths();
    string2_binlog->set_fieldid(103);
    string2_binlog->add_child_fields(fields.at("string2").get());
    auto* string2_log = string2_binlog->add_binlogs();
    string2_log->set_log_path(load_info_.field_infos.at(103).insert_files[0]);
    string2_log->set_entries_num(chunk_num * test_data_count);
    string2_log->set_memory_size(
        load_info_.field_infos.at(103).memory_sizes.front());

    auto loaded_segment =
        CreateSegmentByLoadInfo(std::move(segment_load_info), true);

    auto int64_chunk =
        loaded_segment->chunk_data<int64_t>(nullptr, fields.at("int64"), 0);
    ASSERT_EQ(int64_chunk.get().row_count(), chunk_num * test_data_count);
    ASSERT_EQ(int64_chunk.get().data()[0], 0);
    ASSERT_EQ(int64_chunk.get().data()[1], 1);
    ASSERT_EQ(int64_chunk.get().data()[test_data_count], test_data_count);

    if (GetParam()) {
        auto pk_chunk = loaded_segment->get_batch_views<std::string_view>(
            nullptr, fields.at("pk"), 0, 0, chunk_num * test_data_count);
        ASSERT_EQ(pk_chunk.get().first.size(), chunk_num * test_data_count);
        ASSERT_EQ(pk_chunk.get().first[0], "test0");
        ASSERT_EQ(pk_chunk.get().first[1], "test1");
        ASSERT_EQ(pk_chunk.get().first[test_data_count],
                  std::string_view("test18999"));
    } else {
        auto pk_chunk =
            loaded_segment->chunk_data<int64_t>(nullptr, fields.at("pk"), 0);
        ASSERT_EQ(pk_chunk.get().row_count(), chunk_num * test_data_count);
        ASSERT_EQ(pk_chunk.get().data()[0], 0);
        ASSERT_EQ(pk_chunk.get().data()[1], 1);
        ASSERT_EQ(pk_chunk.get().data()[test_data_count], test_data_count);
    }

    std::vector<proto::plan::GenericValue> filter_data;
    for (int i = 1; i <= 10; ++i) {
        proto::plan::GenericValue value;
        value.set_int64_val(i);
        filter_data.push_back(value);
    }
    auto term_filter_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(fields.at("int64"), milvus::DataType::INT64),
        filter_data);
    auto plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                       term_filter_expr);
    auto final = query::ExecuteQueryExpr(
        plan, loaded_segment.get(), chunk_num * test_data_count, MAX_TIMESTAMP);
    ASSERT_EQ(10, final.count());

    std::vector<proto::plan::GenericValue> pk_filter_data;
    proto::plan::GenericValue pk_value;
    if (GetParam()) {
        pk_value.set_string_val("test42");
    } else {
        pk_value.set_int64_val(42);
    }
    pk_filter_data.push_back(pk_value);
    auto pk_term_filter_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(
            fields.at("pk"),
            GetParam() ? milvus::DataType::VARCHAR : milvus::DataType::INT64),
        pk_filter_data);
    plan = std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID,
                                                  pk_term_filter_expr);
    final = query::ExecuteQueryExpr(
        plan, loaded_segment.get(), chunk_num * test_data_count, MAX_TIMESTAMP);
    ASSERT_EQ(1, final.count());
}

// Test DropFieldData behavior based on parquet file structure.
// In this test setup, the parquet files are organized as:
//   - paths[0] contains columns {0, 4, 3} = int64, ts, string2 (multi-field column group)
//   - paths[1] contains column {2} = string1 (single-field group)
//   - paths[2] contains column {1} = pk (single-field group)
// When storage_version=2 reads a parquet file with multiple columns, they become
// a multi-field column group, so DropFieldData should be skipped for those fields.

TEST_P(TestChunkSegmentStorageV2, TestLazySystemIndexesOnUnsortedSegment) {
    auto unsorted_segment = CreateSegment(false);
    auto* segment_internal =
        dynamic_cast<SegmentInternalInterface*>(unsorted_segment.get());
    auto* segment_impl =
        dynamic_cast<ChunkedSegmentSealedImpl*>(unsorted_segment.get());
    ASSERT_NE(segment_internal, nullptr);
    ASSERT_NE(segment_impl, nullptr);

    PkType existing_pk;
    PkType missing_pk;
    std::unique_ptr<IdArray> delete_ids = std::make_unique<IdArray>();
    if (GetParam()) {
        existing_pk = std::string("test42");
        missing_pk = std::string("test_missing");
        delete_ids->mutable_str_id()->mutable_data()->Add("test42");
    } else {
        existing_pk = int64_t(42);
        missing_pk = int64_t(-1);
        delete_ids->mutable_int_id()->mutable_data()->Add(42);
    }

    EXPECT_TRUE(segment_impl->Contain(existing_pk));
    EXPECT_FALSE(segment_impl->Contain(missing_pk));

    Timestamp delete_ts = MAX_TIMESTAMP;
    auto status = unsorted_segment->Delete(1, delete_ids.get(), &delete_ts);
    ASSERT_TRUE(status.ok());

    BitsetType timestamp_mask(chunk_num * test_data_count);
    BitsetTypeView timestamp_mask_view(timestamp_mask);
    segment_internal->mask_with_timestamps(timestamp_mask_view, 41, 0);
    ASSERT_FALSE(timestamp_mask[41]);
    ASSERT_TRUE(timestamp_mask[42]);

    timestamp_mask.reset();
    segment_internal->mask_with_timestamps(timestamp_mask_view, 42, 0);
    ASSERT_FALSE(timestamp_mask[42]);
    ASSERT_TRUE(timestamp_mask[43]);

    BitsetType delete_mask(chunk_num * test_data_count);
    BitsetTypeView delete_mask_view(delete_mask);
    segment_internal->mask_with_delete(
        delete_mask_view, chunk_num * test_data_count, MAX_TIMESTAMP);
    ASSERT_EQ(1, delete_mask.count());
    ASSERT_EQ(1, unsorted_segment->get_deleted_count());
    ASSERT_EQ(chunk_num * test_data_count - 1,
              unsorted_segment->get_real_count());
}

// Verify that when delete_ts == insert_ts, the delete does NOT take effect.
// This tests the same-timestamp correctness check in DeletedRecord when
// insert_record_.timestamps_ is empty (StorageV2 lazy-init path).
TEST_P(TestChunkSegmentStorageV2, TestSameTimestampDeleteNotEffective) {
    auto unsorted_segment = CreateSegment(false);

    // Row 42 has insert timestamp = 42 (from sequential int64 data).
    // Deleting with the same timestamp should have no effect.
    std::unique_ptr<IdArray> delete_ids = std::make_unique<IdArray>();
    if (GetParam()) {
        delete_ids->mutable_str_id()->mutable_data()->Add("test42");
    } else {
        delete_ids->mutable_int_id()->mutable_data()->Add(42);
    }

    Timestamp delete_ts = 42;  // same as insert timestamp of row 42
    auto status = unsorted_segment->Delete(1, delete_ids.get(), &delete_ts);
    ASSERT_TRUE(status.ok());

    // The delete should not have taken effect because delete_ts == insert_ts
    ASSERT_EQ(0, unsorted_segment->get_deleted_count());
    ASSERT_EQ(chunk_num * test_data_count, unsorted_segment->get_real_count());
}

TEST_P(TestChunkSegmentStorageV2, TestLazySystemIndexesOnSortedSegment) {
    auto sorted_segment = CreateSegment(true);
    auto* segment_internal =
        dynamic_cast<SegmentInternalInterface*>(sorted_segment.get());
    auto* segment_impl =
        dynamic_cast<ChunkedSegmentSealedImpl*>(sorted_segment.get());
    ASSERT_NE(segment_internal, nullptr);
    ASSERT_NE(segment_impl, nullptr);

    PkType existing_pk;
    PkType missing_pk;
    std::unique_ptr<IdArray> delete_ids = std::make_unique<IdArray>();
    if (GetParam()) {
        existing_pk = std::string("test42");
        missing_pk = std::string("test_missing");
        delete_ids->mutable_str_id()->mutable_data()->Add("test42");
    } else {
        existing_pk = int64_t(42);
        missing_pk = int64_t(-1);
        delete_ids->mutable_int_id()->mutable_data()->Add(42);
    }

    EXPECT_TRUE(segment_impl->Contain(existing_pk));
    EXPECT_FALSE(segment_impl->Contain(missing_pk));

    Timestamp delete_ts = MAX_TIMESTAMP;
    auto status = sorted_segment->Delete(1, delete_ids.get(), &delete_ts);
    ASSERT_TRUE(status.ok());

    BitsetType timestamp_mask(chunk_num * test_data_count);
    BitsetTypeView timestamp_mask_view(timestamp_mask);
    segment_internal->mask_with_timestamps(timestamp_mask_view, 41, 0);
    ASSERT_FALSE(timestamp_mask[41]);
    ASSERT_TRUE(timestamp_mask[42]);

    timestamp_mask.reset();
    segment_internal->mask_with_timestamps(timestamp_mask_view, 42, 0);
    ASSERT_FALSE(timestamp_mask[42]);
    ASSERT_TRUE(timestamp_mask[43]);

    BitsetType delete_mask(chunk_num * test_data_count);
    BitsetTypeView delete_mask_view(delete_mask);
    segment_internal->mask_with_delete(
        delete_mask_view, chunk_num * test_data_count, MAX_TIMESTAMP);
    ASSERT_EQ(1, delete_mask.count());
    ASSERT_EQ(1, sorted_segment->get_deleted_count());
    ASSERT_EQ(chunk_num * test_data_count - 1,
              sorted_segment->get_real_count());

    if (!GetParam()) {
        int64_t seg_offsets[] = {0, 42};
        auto pk_result = sorted_segment->bulk_subscript(
            nullptr, fields.at("pk"), seg_offsets, 2);
        ASSERT_EQ(pk_result->scalars().long_data().data(0), 0);
        ASSERT_EQ(pk_result->scalars().long_data().data(1), 42);
    }
}

// ─────────────────────────────────────────────────────────────────────────
// PR #51441 skip-index regression tests (storage v2). Two dimensions:
//  1. Correctness: real queries return the exact count -- a mis-aligned cell,
//     a use-after-free VARCHAR bound, or a wrong metric would drop rows.
//  2. Skip effect: with the flag ON the footer skip index actually prunes
//     lower cells; with it OFF (default) storage-v2 scalar columns get none.
// ─────────────────────────────────────────────────────────────────────────
namespace {
// The VARCHAR payload of row `i`: a monotonic 8-digit prefix so per-row-group
// min/max discriminate, plus fixed padding so each value is 2048 bytes (bloats
// row groups). Monotonic in the same order as `val`, so row i has val == i and
// payload == SkipMeasurePayloadAt(i).
// Row i of the nullable `nval` column is NULL when i % kNullEvery == 0. No row
// group is entirely null, so every one keeps usable min/max (an all-null row
// group would degrade to NoneFieldChunkMetrics and never prune, which would
// defeat the point of the nullable tests below).
constexpr int64_t kNullEvery = 10;

std::string
SkipMeasurePayloadAt(int64_t i) {
    // Fixed-width zero padding keeps lexicographic order == numeric order, so
    // per-row-group min/max bound a contiguous range of rows.
    auto digits = std::to_string(i);
    auto prefix = digits.size() >= 8
                      ? digits
                      : std::string(8 - digits.size(), '0') + digits;
    return prefix + std::string(2040, 'x');
}

// val(INT64 monotonic 0..N-1) + payload(bloated VARCHAR -> many row groups)
// + ts share one column group; pk sits alone. Writes two parquet files under
// `root`; returns the row count. `writer_mem` tunes row groups per file.
// `nullable_col` is the arrow column index that should be written with NULLs.
// `all_null_first_batch_col` makes that column entirely NULL in the first
// batch, producing at least one footer row group with no usable min/max.
// -1 disables either option and reproduces the original layout byte for byte.
int64_t
WriteSkipMeasureV2Parquet(const std::shared_ptr<Schema>& schema,
                          FieldId pk_fid,
                          const std::string& root,
                          int64_t writer_mem,
                          int nullable_col = -1,
                          int all_null_first_batch_col = -1) {
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    (void)fs->DeleteDir(root);
    EXPECT_TRUE(fs->CreateDir(root + "/0").ok());
    EXPECT_TRUE(fs->CreateDir(root + "/" + std::to_string(pk_fid.get())).ok());
    std::vector<std::string> paths = {
        root + "/0/10000.parquet",
        root + "/" + std::to_string(pk_fid.get()) + "/10001.parquet"};
    // Everything but pk (arrow index 1) shares column group 0; pk sits alone.
    // Derived rather than hardcoded so an added column lands in group 0.
    auto packed_schema = schema->ConvertToArrowSchema();
    std::vector<int> group_zero;
    for (int i = 0; i < packed_schema->num_fields(); ++i) {
        if (i != 1) {
            group_zero.push_back(i);
        }
    }
    std::vector<std::vector<int>> column_groups = {group_zero, {1}};
    auto storage_config = milvus_storage::StorageConfig();
    auto result = milvus_storage::PackedRecordBatchWriter::Make(
        fs,
        paths,
        packed_schema,
        storage_config,
        column_groups,
        writer_mem,
        ::parquet::default_writer_properties());
    EXPECT_TRUE(result.ok());
    auto writer = result.ValueOrDie();

    constexpr int64_t rows_per_batch = 10000;
    constexpr int64_t batch_count = 4;
    const int64_t N = rows_per_batch * batch_count;  // val 0..39999
    auto arrow_schema = schema->ConvertToArrowSchema();
    for (int64_t batch = 0; batch < batch_count; ++batch) {
        const int64_t start = batch * rows_per_batch;
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        for (int i = 0; i < arrow_schema->fields().size(); ++i) {
            if (i == all_null_first_batch_col && batch == 0) {
                arrow::Int64Builder builder;
                for (int64_t r = 0; r < rows_per_batch; ++r) {
                    EXPECT_TRUE(builder.AppendNull().ok());
                }
                std::shared_ptr<arrow::Array> array;
                EXPECT_TRUE(builder.Finish(&array).ok());
                arrays.push_back(array);
            } else if (i == nullable_col) {
                // Nullable twin of `val`: identical monotonic values, so the
                // same thresholds prune the same cells, but every
                // kNullEvery-th row is NULL.
                arrow::Int64Builder builder;
                for (int64_t r = 0; r < rows_per_batch; ++r) {
                    const int64_t v = start + r;
                    if (v % kNullEvery == 0) {
                        EXPECT_TRUE(builder.AppendNull().ok());
                    } else {
                        EXPECT_TRUE(builder.Append(v).ok());
                    }
                }
                std::shared_ptr<arrow::Array> array;
                EXPECT_TRUE(builder.Finish(&array).ok());
                arrays.push_back(array);
            } else if (arrow_schema->fields()[i]->type()->id() ==
                       arrow::Type::INT64) {
                std::vector<int64_t> values(rows_per_batch);
                std::iota(values.begin(), values.end(), start);  // monotonic
                arrow::Int64Builder builder;
                EXPECT_TRUE(
                    builder.AppendValues(values.data(), rows_per_batch).ok());
                std::shared_ptr<arrow::Array> array;
                EXPECT_TRUE(builder.Finish(&array).ok());
                arrays.push_back(array);
            } else if (arrow_schema->fields()[i]->type()->id() ==
                       arrow::Type::INT32) {
                std::vector<int32_t> values(rows_per_batch);
                std::iota(values.begin(),
                          values.end(),
                          static_cast<int32_t>(start));  // monotonic
                arrow::Int32Builder builder;
                EXPECT_TRUE(
                    builder.AppendValues(values.data(), rows_per_batch).ok());
                std::shared_ptr<arrow::Array> array;
                EXPECT_TRUE(builder.Finish(&array).ok());
                arrays.push_back(array);
            } else {
                // Monotonic 8-digit prefix + fixed padding: bloats the row
                // group (many row groups -> many cells) AND gives the VARCHAR
                // column per-row-group min/max that actually discriminate, so
                // the same data exercises string pruning. Length is constant,
                // so the row-group/cell layout matches the numeric-only case.
                arrow::StringBuilder builder;
                std::vector<std::string> values;
                values.reserve(rows_per_batch);
                for (int64_t r = 0; r < rows_per_batch; ++r) {
                    values.push_back(SkipMeasurePayloadAt(start + r));
                }
                EXPECT_TRUE(builder.AppendValues(values).ok());
                std::shared_ptr<arrow::Array> array;
                EXPECT_TRUE(builder.Finish(&array).ok());
                arrays.push_back(array);
            }
        }
        auto rb =
            arrow::RecordBatch::Make(arrow_schema, rows_per_batch, arrays);
        EXPECT_TRUE(writer->Write(rb).ok());
    }
    EXPECT_TRUE(writer->Close().ok());
    return N;
}

// The collection label every metric these tests read is published under.
constexpr const char* kSkipMeasureCollection = "skip_measure_collection";
constexpr const char* kSkipMeasureDb = "skip_measure_db";

std::shared_ptr<Schema>
MakeSkipMeasureSchema(FieldId& val_fid,
                      FieldId& pk_fid,
                      FieldId* payload_fid = nullptr,
                      FieldId* nullable_fid = nullptr,
                      FieldId* int32_fid = nullptr) {
    auto schema = std::make_shared<Schema>();
    val_fid = schema->AddDebugField("val", DataType::INT64, false);
    pk_fid = schema->AddDebugField("pk", DataType::INT64, false);
    auto pl_fid = schema->AddDebugField("payload", DataType::VARCHAR, false);
    if (payload_fid != nullptr) {
        *payload_fid = pl_fid;
    }
    // Only added when asked, so the tests above keep their exact column layout.
    if (nullable_fid != nullptr) {
        *nullable_fid = schema->AddDebugField("nval", DataType::INT64, true);
    }
    if (int32_fid != nullptr) {
        *int32_fid = schema->AddDebugField("ival32", DataType::INT32, false);
    }
    schema->AddField(FieldName("ts"),
                     TimestampFieldID,
                     DataType::INT64,
                     false,
                     std::nullopt);
    schema->set_primary_field_id(pk_fid);
    // Named so the metrics below can be read on the collection label they are
    // published under; a schema assembled field by field has no name of its
    // own, where one parsed from a CollectionSchema carries the real one.
    schema->set_collection_name(kSkipMeasureCollection);
    schema->set_db_name(kSkipMeasureDb);
    return schema;
}

SegmentSealedUPtr
LoadSkipMeasureV2Segment(const std::shared_ptr<Schema>& schema,
                         FieldId pk_fid,
                         int64_t N,
                         const std::string& root) {
    LoadFieldDataInfo load_info;
    load_info.storage_version = 2;
    load_info.field_infos.emplace(
        int64_t(0),
        FieldBinlogInfo{int64_t(0),
                        N,
                        std::vector<int64_t>(N),
                        std::vector<int64_t>(N * 4),
                        false,
                        "",
                        std::vector<std::string>({root + "/0/10000.parquet"})});
    load_info.field_infos.emplace(
        pk_fid.get(),
        FieldBinlogInfo{pk_fid.get(),
                        N,
                        std::vector<int64_t>(N),
                        std::vector<int64_t>(N * 4),
                        false,
                        "",
                        std::vector<std::string>({root + "/" +
                                                  std::to_string(pk_fid.get()) +
                                                  "/10001.parquet"})});
    auto segment = segcore::CreateSealedSegment(
        schema, nullptr, -1, segcore::SegcoreConfig::default_config(), true);
    segment->AddFieldDataInfoForSealed(load_info);
    for (auto& [id, info] : load_info.field_infos) {
        LoadFieldDataInfo one;
        one.storage_version = 2;
        one.field_infos.emplace(id, info);
        segment->LoadFieldData(one);
    }
    return segment;
}
}  // namespace

// Correctness: run real UnaryRange filters end-to-end and compare against the
// exact expected counts. A materialization that dropped rows (mis-alignment,
// VARCHAR use-after-free, wrong metric) would make a count too low.
TEST(SkipIndexPr51441, StorageV2SkipQueryResultsCorrect) {
    // Large target: without the force many row groups pack into few cells;
    // with the flag ON the force makes 1 rg/cell and many cells.
    StorageV2CellTargetGuard cell_target_guard(256 * 1024 * 1024);
    FieldId val_fid, pk_fid;
    auto schema = MakeSkipMeasureSchema(val_fid, pk_fid);
    const std::string root = "skip_pr51441_query_v2";
    const int64_t N =
        WriteSkipMeasureV2Parquet(schema, pk_fid, root, 4 * 1024 * 1024);

    SetDefaultEnableParquetStatsSkipIndex(true);
    auto segment = LoadSkipMeasureV2Segment(schema, pk_fid, N, root);
    const int64_t num_cells = segment->num_chunk_data(val_fid);

    auto run_count = [&](proto::plan::OpType op, int64_t threshold) -> int64_t {
        proto::plan::GenericValue value;
        value.set_int64_val(threshold);
        auto expr = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(val_fid, milvus::DataType::INT64), op, value);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto final =
            query::ExecuteQueryExpr(plan, segment.get(), N, MAX_TIMESTAMP);
        return static_cast<int64_t>(final.count());
    };
    // val holds 0..N-1: #>T = N-1-T, #<T = T, #==T = 1.
    for (int64_t T :
         {int64_t(5000), int64_t(20000), int64_t(30000), int64_t(37777)}) {
        EXPECT_EQ(run_count(proto::plan::OpType::GreaterThan, T), N - 1 - T)
            << "val > " << T << " (cells=" << num_cells << ")";
        EXPECT_EQ(run_count(proto::plan::OpType::LessThan, T), T)
            << "val < " << T;
        EXPECT_EQ(run_count(proto::plan::OpType::Equal, T), int64_t(1))
            << "val == " << T;
    }
    EXPECT_GT(num_cells, 1) << "force 1 rg/cell should yield many cells";

    SetDefaultEnableParquetStatsSkipIndex(false);
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    (void)fs->DeleteDir(root);
}

// Skip effect: flag ON prunes lower cells (but never a cell holding a match);
// flag OFF (default) prunes nothing (storage-v2 scalar columns get no index).
TEST(SkipIndexPr51441, StorageV2CellPruneByFlag) {
    StorageV2CellTargetGuard cell_target_guard(64 * 1024);
    FieldId val_fid, pk_fid;
    auto schema = MakeSkipMeasureSchema(val_fid, pk_fid);
    const std::string root = "skip_pr51441_prune_v2";
    const int64_t N =
        WriteSkipMeasureV2Parquet(schema, pk_fid, root, 16 * 1024 * 1024);
    const int64_t threshold = N - 10000;  // 30000; only the top batch matches

    auto build_and_count = [&](bool flag) -> std::pair<int64_t, int64_t> {
        SetDefaultEnableParquetStatsSkipIndex(flag);
        auto segment = LoadSkipMeasureV2Segment(schema, pk_fid, N, root);
        const int64_t cells = segment->num_chunk_data(val_fid);
        auto skip = segment->GetSkipIndex();
        int64_t skipped = 0;
        for (int64_t c = 0; c < cells; ++c) {
            if (skip->CanSkipUnaryRange<int64_t>(
                    val_fid, c, OpType::GreaterThan, threshold)) {
                ++skipped;
            }
        }
        return {skipped, cells};
    };
    auto [skipped_on, cells_on] = build_and_count(true);
    auto [skipped_off, cells_off] = build_and_count(false);
    SetDefaultEnableParquetStatsSkipIndex(false);

    ASSERT_GT(cells_on, 1) << "need multiple cells to measure pruning";
    EXPECT_GT(skipped_on, 0);
    EXPECT_LT(skipped_on, cells_on);  // never prune a cell that holds a match
    EXPECT_EQ(skipped_off, 0);

    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    (void)fs->DeleteDir(root);
}

// Sealed VARCHAR `IN` pruning. Before this PR the scan path never consulted the
// skip index for string columns (the IN list is owned std::string but the scan
// template is std::string_view, and GetElementValues<string_view> extracts
// nothing from it), so IN never pruned while the prefetch did -- the scan just
// fetched the cell back. Both sides now decide via CanSkipInQuery<std::string>.
// Asserts BOTH that pruning happens and that results are unchanged by it.
TEST(SkipIndexPr51441, StorageV2VarcharInPruneAndResultsCorrect) {
    StorageV2CellTargetGuard cell_target_guard(64 * 1024);
    FieldId val_fid, pk_fid, payload_fid;
    auto schema = MakeSkipMeasureSchema(val_fid, pk_fid, &payload_fid);
    const std::string root = "skip_pr51441_varchar_in_v2";
    const int64_t N =
        WriteSkipMeasureV2Parquet(schema, pk_fid, root, 16 * 1024 * 1024);

    // Two payloads that both live in the last batch, so every cell whose max
    // sorts below them is prunable. Row i has payload SkipMeasurePayloadAt(i).
    const std::vector<std::string> wanted = {SkipMeasurePayloadAt(N - 5000),
                                             SkipMeasurePayloadAt(N - 3000)};

    auto run_in_count = [&](SegmentSealed* segment) -> int64_t {
        std::vector<proto::plan::GenericValue> vals;
        for (const auto& w : wanted) {
            proto::plan::GenericValue v;
            v.set_string_val(w);
            vals.push_back(v);
        }
        auto expr = std::make_shared<expr::TermFilterExpr>(
            expr::ColumnInfo(payload_fid, milvus::DataType::VARCHAR), vals);
        auto plan =
            std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
        auto final = query::ExecuteQueryExpr(plan, segment, N, MAX_TIMESTAMP);
        return static_cast<int64_t>(final.count());
    };

    // Flag ON: the string skip index must actually prune, and the query must
    // still return exactly the two matching rows.
    SetDefaultEnableParquetStatsSkipIndex(true);
    {
        auto segment = LoadSkipMeasureV2Segment(schema, pk_fid, N, root);
        const int64_t cells = segment->num_chunk_data(payload_fid);
        auto skip = segment->GetSkipIndex();
        int64_t pruned = 0;
        for (int64_t c = 0; c < cells; ++c) {
            if (skip->CanSkipInQuery<std::string>(payload_fid, c, wanted)) {
                ++pruned;
            }
        }
        ASSERT_GT(cells, 1) << "need multiple cells to measure IN pruning";
        EXPECT_GT(pruned, 0)
            << "VARCHAR IN must prune (regression: inert skip)";
        // Never prune every cell -- the matching rows live in one of them.
        EXPECT_LT(pruned, cells);
        EXPECT_EQ(run_in_count(segment.get()), int64_t(wanted.size()))
            << "pruning must not drop matching rows";
    }
    // Flag OFF (default): no chunk skip index, same answer.
    SetDefaultEnableParquetStatsSkipIndex(false);
    {
        auto segment = LoadSkipMeasureV2Segment(schema, pk_fid, N, root);
        EXPECT_EQ(run_in_count(segment.get()), int64_t(wanted.size()))
            << "flag off must return the same rows";
    }

    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    (void)fs->DeleteDir(root);
}

// Direct coverage of the SkipIndex statistics-source contract that the storage
// v2 loader relies on: cells are POSITIONAL (cell i describes chunk i), they
// are rebuilt from the source on every (re)load so an evicted cell restores
// itself, a chunk id past the end degrades to "cannot skip" (never a false
// negative), and Erase drops the field so a replaced column is not pruned with
// the previous load's slot.
namespace {
// Test source: two chunks with disjoint int64 ranges, counting how many times
// it is asked so we can prove cells really are rebuilt from it (not retained).
class FakeChunkStatsSource : public milvus::ChunkStatsSource {
 public:
    int64_t
    num_chunks() const override {
        return 2;
    }

    std::unique_ptr<milvus::index::FieldChunkMetrics>
    BuildChunkMetrics(int64_t chunk_id) override {
        ++calls_;
        // chunk 0 spans [0,10]; chunk 1 spans [100,110]
        int64_t lo = chunk_id == 0 ? 0 : 100;
        int64_t hi = chunk_id == 0 ? 10 : 110;
        return std::make_unique<index::IntFieldChunkMetrics<int64_t>>(
            lo, hi, nullptr);
    }

    int
    calls() const {
        return calls_;
    }

 private:
    int calls_{0};
};
}  // namespace

TEST(SkipIndexPr51441, StatsSourcePositionalRebuildAndErase) {
    milvus::SkipIndex skip;
    const FieldId fid(101);
    auto source = std::make_shared<FakeChunkStatsSource>();
    skip.LoadSkipFromStatsSource(/*segment_id=*/1, fid, source);

    // Positional: 105 cannot be in chunk 0, but can be in chunk 1.
    EXPECT_TRUE(
        skip.CanSkipUnaryRange<int64_t>(fid, 0, OpType::Equal, int64_t(105)));
    EXPECT_FALSE(
        skip.CanSkipUnaryRange<int64_t>(fid, 1, OpType::Equal, int64_t(105)));
    // Cells came from the source, i.e. they are rebuildable rather than
    // retained -- this is what lets the cache evict and restore them.
    EXPECT_GT(source->calls(), 0);

    // A field that was never installed never skips.
    EXPECT_FALSE(skip.CanSkipUnaryRange<int64_t>(
        FieldId(999), 0, OpType::Equal, int64_t(105)));

    // Erase drops the slot: nothing prunes afterwards.
    skip.Erase(fid);
    EXPECT_FALSE(
        skip.CanSkipUnaryRange<int64_t>(fid, 0, OpType::Equal, int64_t(105)));
}

// ─────────────────────────────────────────────────────────────────────────
// PR #51441: the IO-saving half of the change.
//
// These three fixes are invisible to a result-count assertion. Each one only
// changes how much data the scan touches; the pre-fix code returned exactly
// the same rows, just after paying for the IO the skip index was supposed to
// have saved. A test that only checks counts therefore passes either way --
// which is why they are verified here through the observability this PR adds
// (the skip-index counters, and OpContext::storage_usage) instead.
//
//  - VarcharInPrunesOnTheExecutedPath  -> TermExpr.cpp: the scan's skip list
//    was built with GetElementValues<std::string_view> from owned std::string
//    containers and came back empty, so sealed VARCHAR IN never pruned.
//  - PrunedCellsAreNotTouchedByTheScan -> Expr.h: the skip branch pinned every
//    pruned chunk anyway, just to read valid_data.
//  - Both of the above also cover Expr.h's fallback prefetch, which now
//    filters by the same skip_func -- with driver prefetch off (the product
//    default) it is the only prefetch that runs.
// ─────────────────────────────────────────────────────────────────────────
namespace {

// common.enableDriverPrefetch defaults to false (configs/milvus.yaml), but the
// core constant DEFAULT_ENABLE_DRIVER_PREFETCH is true, so all_tests would
// otherwise take the skip-aware driver prefetch and never exercise the
// fallback prefetch inside ProcessDataChunksForMultipleChunk -- the only
// prefetch a default deployment runs, and the one that had to learn skip_func.
class DriverPrefetchGuard {
 public:
    explicit DriverPrefetchGuard(bool enabled)
        : old_(milvus::ENABLE_DRIVER_PREFETCH.load()) {
        milvus::SetDefaultDriverPrefetchEnable(enabled);
    }

    ~DriverPrefetchGuard() {
        milvus::SetDefaultDriverPrefetchEnable(old_);
    }

 private:
    const bool old_;
};

// Each CacheSlot freezes storage_usage_tracking_enabled at construction, so
// this has to be held across the segment *load*, not just across the query --
// enabling it afterwards would measure a stream of zeros.
class StorageUsageTrackingGuard {
 public:
    explicit StorageUsageTrackingGuard(bool enabled)
        : old_(milvus::cachinglayer::TieredStorageConfig::GetInstance()
                   .storage_usage_tracking_enabled()) {
        milvus::cachinglayer::TieredStorageConfig::GetInstance()
            .SetStorageUsageTrackingEnabled(enabled);
    }

    ~StorageUsageTrackingGuard() {
        milvus::cachinglayer::TieredStorageConfig::GetInstance()
            .SetStorageUsageTrackingEnabled(old_);
    }

 private:
    const bool old_;
};

struct ScanTraffic {
    int64_t count;
    int64_t total_bytes;  // every cell the scan pinned (hit or miss)
    int64_t cold_bytes;   // the subset that had to be loaded, i.e. real IO
};

// query::ExecuteQueryExpr never installs an OpContext, so the cachinglayer has
// nowhere to accumulate storage_usage. Run the same plan with one attached and
// hand back both the answer and the traffic it cost.
ScanTraffic
RunWithStorageUsage(const std::shared_ptr<milvus::plan::PlanNode>& plannode,
                    const milvus::segcore::SegmentInternalInterface* segment,
                    int64_t active_count) {
    auto plan_fragment = milvus::plan::PlanFragment(plannode);
    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        DEAFULT_QUERY_ID, segment, active_count, MAX_TIMESTAMP);
    milvus::OpContext op_context;
    query_context->set_op_context(&op_context);

    auto row = milvus::query::ExecPlanNodeVisitor::ExecuteTask(plan_fragment,
                                                               query_context);
    auto col_vec = milvus::query::GetColumnVectorForTest(row->childrens()[0]);
    BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
    BitsetType selected(view);
    selected.flip();
    return ScanTraffic{static_cast<int64_t>(selected.count()),
                       op_context.storage_usage.scanned_total_bytes.load(),
                       op_context.storage_usage.scanned_cold_bytes.load()};
}

std::shared_ptr<milvus::plan::PlanNode>
UnaryRangePlan(FieldId fid, proto::plan::OpType op, int64_t threshold) {
    proto::plan::GenericValue value;
    value.set_int64_val(threshold);
    auto expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(fid, milvus::DataType::INT64), op, value);
    return std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
}

std::shared_ptr<milvus::plan::PlanNode>
BinaryRangePlan(FieldId fid, DataType data_type, int64_t lower, int64_t upper) {
    proto::plan::GenericValue lower_value;
    lower_value.set_int64_val(lower);
    proto::plan::GenericValue upper_value;
    upper_value.set_int64_val(upper);
    auto expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(fid, data_type), lower_value, upper_value, true, true);
    return std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
}

std::shared_ptr<milvus::plan::PlanNode>
BinaryArithPlan(FieldId fid,
                proto::plan::OpType op,
                proto::plan::ArithOpType arith_op,
                int64_t value,
                int64_t right_operand) {
    proto::plan::GenericValue value_arg;
    value_arg.set_int64_val(value);
    proto::plan::GenericValue right_arg;
    right_arg.set_int64_val(right_operand);
    auto expr = std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
        expr::ColumnInfo(fid, DataType::INT64),
        op,
        arith_op,
        value_arg,
        right_arg);
    return std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
}

// The same measurement over offset input -- the path iterative filter runs
// (IterativeFilterNode installs an offset vector on the EvalCtx), which reaches
// ProcessDataByOffsets instead of the sequential ProcessDataChunks. test::
// gen_filter_res drives that path but builds its own QueryContext with no
// OpContext, so nothing records traffic; this attaches one.
ScanTraffic
RunByOffsetsWithStorageUsage(
    const std::shared_ptr<milvus::plan::PlanNode>& plannode,
    const milvus::segcore::SegmentInternalInterface* segment,
    int64_t active_count,
    milvus::exec::OffsetVector& offsets,
    int repeats = 1) {
    auto filter_node =
        std::dynamic_pointer_cast<milvus::plan::FilterBitsNode>(plannode);
    AssertInfo(filter_node != nullptr, "expected a FilterBitsNode");
    std::vector<milvus::expr::TypedExprPtr> filters{filter_node->filter()};

    auto query_context = std::make_shared<milvus::exec::QueryContext>(
        DEAFULT_QUERY_ID, segment, active_count, MAX_TIMESTAMP);
    milvus::OpContext op_context;
    query_context->set_op_context(&op_context);

    auto exec_context =
        std::make_unique<milvus::exec::ExecContext>(query_context.get());
    auto exprs =
        std::make_unique<milvus::exec::ExprSet>(filters, exec_context.get());
    int64_t count = 0;
    for (int i = 0; i < repeats; ++i) {
        std::vector<VectorPtr> results;
        milvus::exec::EvalCtx eval_ctx(exec_context.get(), &offsets);
        exprs->Eval(0, 1, true, eval_ctx, results);

        auto col_vec = milvus::test::GetColumnVectorForTest(results[0]);
        // Unlike ExecuteQueryExpr, whose bitmap marks the rows filtered out,
        // an ExprSet evaluated directly returns the match set (this is how
        // ExprArithOpTest reads gen_filter_res), so it is counted as-is.
        BitsetTypeView view(col_vec->GetRawData(), col_vec->size());
        count += static_cast<int64_t>(view.count());
    }
    // exprs is destroyed after the return value is formed and before the
    // caller resumes; ~SegmentExpr publishes the cross-batch unique-chunk
    // effect at that point.
    return ScanTraffic{count,
                       op_context.storage_usage.scanned_total_bytes.load(),
                       op_context.storage_usage.scanned_cold_bytes.load()};
}

}  // namespace

// Arithmetic filters are evaluated exactly as written by the execution
// kernel, without attempting to invert them into a range over the source
// field. Besides avoiding integer overflow/truncation mismatches, an empty
// skip callback means these expressions are not counted as "judged" by the
// skip-index effectiveness metrics. Cover the default in-scan prefetch, driver
// prefetch, and offset/iterative paths; Add is the important control because it
// was the last arithmetic operation that still pruned before this policy.
TEST(SkipIndexPr51441, ArithmeticPredicatesNeverUseSkipIndex) {
    StorageUsageTrackingGuard tracking_guard(true);
    StorageV2CellTargetGuard cell_target_guard(64 * 1024);
    FieldId val_fid, pk_fid;
    auto schema = MakeSkipMeasureSchema(val_fid, pk_fid);
    const std::string root = "skip_pr51441_arithmetic_no_skip_v2";
    const int64_t N =
        WriteSkipMeasureV2Parquet(schema, pk_fid, root, 16 * 1024 * 1024);

    SetDefaultEnableParquetStatsSkipIndex(true);
    auto segment = LoadSkipMeasureV2Segment(schema, pk_fid, N, root);
    ASSERT_GT(segment->num_chunk_data(val_fid), 1)
        << "need multiple cells so the old Add inversion could prune";

    auto& scanned = milvus::monitor::internal_core_skipindex_chunks_scanned(
        kSkipMeasureDb, kSkipMeasureCollection);
    auto& pruned = milvus::monitor::internal_core_skipindex_chunks_pruned(
        kSkipMeasureDb, kSkipMeasureCollection);
    auto& ratio = milvus::monitor::internal_core_skipindex_prune_ratio_expr(
        kSkipMeasureDb, kSkipMeasureCollection);
    struct MetricSnapshot {
        double scanned;
        double pruned;
        uint64_t ratio_samples;
    };
    auto snapshot = [&] {
        return MetricSnapshot{
            scanned.Value(),
            pruned.Value(),
            ratio.Collect().histogram.sample_count,
        };
    };
    auto expect_unchanged = [](const MetricSnapshot& before,
                               const MetricSnapshot& after) {
        EXPECT_DOUBLE_EQ(after.scanned, before.scanned);
        EXPECT_DOUBLE_EQ(after.pruned, before.pruned);
        EXPECT_EQ(after.ratio_samples, before.ratio_samples);
    };

    // val is [0, N). Therefore val + 1 > N matches nothing, while val + 1 > 0
    // matches every row. With arithmetic pruning disabled both must read the
    // same full field, regardless of which prefetch path drives execution.
    auto no_match_plan = BinaryArithPlan(val_fid,
                                         proto::plan::OpType::GreaterThan,
                                         proto::plan::ArithOpType::Add,
                                         N,
                                         1);
    auto full_plan = BinaryArithPlan(val_fid,
                                     proto::plan::OpType::GreaterThan,
                                     proto::plan::ArithOpType::Add,
                                     0,
                                     1);
    auto run_sequential = [&](bool driver_prefetch) {
        DriverPrefetchGuard guard(driver_prefetch);
        const auto before = snapshot();
        auto no_match = RunWithStorageUsage(no_match_plan, segment.get(), N);
        const auto after_no_match = snapshot();
        expect_unchanged(before, after_no_match);

        auto full = RunWithStorageUsage(full_plan, segment.get(), N);
        const auto after_full = snapshot();
        expect_unchanged(after_no_match, after_full);

        EXPECT_EQ(no_match.count, 0);
        EXPECT_EQ(full.count, N);
        ASSERT_GT(full.total_bytes, 0);
        EXPECT_EQ(no_match.total_bytes, full.total_bytes)
            << "arithmetic filter still pruned cells with driver prefetch="
            << driver_prefetch;
    };
    run_sequential(false);
    run_sequential(true);

    milvus::exec::OffsetVector offsets{
        0, static_cast<int32_t>(N / 2), static_cast<int32_t>(N - 1)};
    const auto before_offsets = snapshot();
    auto offset_result = RunByOffsetsWithStorageUsage(
        no_match_plan, segment.get(), N, offsets, 2);
    const auto after_offsets = snapshot();
    expect_unchanged(before_offsets, after_offsets);
    EXPECT_EQ(offset_result.count, 0);
    EXPECT_GT(offset_result.total_bytes, 0);

    SetDefaultEnableParquetStatsSkipIndex(false);
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    (void)fs->DeleteDir(root);
}

// An installed field-level stats source does not mean every chunk has a usable
// metric. All-null row groups deliberately yield NoneFieldChunkMetrics: they
// must remain readable, but they were never judged by min/max and therefore
// must not dilute the effectiveness denominator. Exercise all three reporting
// sites: default in-scan prefetch, driver prefetch, and offset input.
TEST(SkipIndexPr51441, NoneChunkMetricsAreNotJudged) {
    StorageV2CellTargetGuard cell_target_guard(64 * 1024);
    FieldId val_fid, pk_fid, nullable_fid;
    auto schema =
        MakeSkipMeasureSchema(val_fid, pk_fid, nullptr, &nullable_fid);
    const std::string root = "skip_pr51441_none_metrics_v2";
    const int64_t N = WriteSkipMeasureV2Parquet(schema,
                                                pk_fid,
                                                root,
                                                16 * 1024 * 1024,
                                                /*nullable_col=*/3,
                                                /*all_null_first_batch_col=*/3);

    SetDefaultEnableParquetStatsSkipIndex(true);
    auto segment = LoadSkipMeasureV2Segment(schema, pk_fid, N, root);
    const int64_t chunks = segment->num_chunk_data(nullable_fid);
    ASSERT_GT(chunks, 1);

    auto skip_index = segment->GetSkipIndex();
    int64_t available_chunks = 0;
    for (int64_t chunk = 0; chunk < chunks; ++chunk) {
        auto decision = skip_index->EvaluateUnaryRange<int64_t>(
            nullable_fid, chunk, OpType::GreaterThan, N);
        available_chunks += decision.available ? 1 : 0;
        if (decision.available) {
            EXPECT_TRUE(decision.can_skip)
                << "every chunk with min/max is below the impossible "
                   "threshold";
        } else {
            EXPECT_FALSE(decision.can_skip)
                << "a chunk without min/max must remain readable";
        }
    }
    ASSERT_GT(available_chunks, 0);
    ASSERT_LT(available_chunks, chunks)
        << "the mixed parquet file must contain both usable and NONE chunk "
           "metrics";

    auto plan =
        UnaryRangePlan(nullable_fid, proto::plan::OpType::GreaterThan, N);
    auto& scanned = milvus::monitor::internal_core_skipindex_chunks_scanned(
        kSkipMeasureDb, kSkipMeasureCollection);
    auto& pruned = milvus::monitor::internal_core_skipindex_chunks_pruned(
        kSkipMeasureDb, kSkipMeasureCollection);

    auto run_sequential = [&](bool driver_prefetch) {
        DriverPrefetchGuard guard(driver_prefetch);
        const double scanned_before = scanned.Value();
        const double pruned_before = pruned.Value();
        auto result = RunWithStorageUsage(plan, segment.get(), N);
        EXPECT_EQ(result.count, 0);
        EXPECT_DOUBLE_EQ(scanned.Value() - scanned_before,
                         static_cast<double>(available_chunks));
        EXPECT_DOUBLE_EQ(pruned.Value() - pruned_before,
                         static_cast<double>(available_chunks));
    };
    run_sequential(false);
    run_sequential(true);

    milvus::exec::OffsetVector offsets;
    offsets.reserve(chunks);
    for (int64_t chunk = 0; chunk < chunks; ++chunk) {
        offsets.push_back(static_cast<int32_t>(
            segment->num_rows_until_chunk(nullable_fid, chunk)));
    }
    {
        DriverPrefetchGuard guard(false);
        const double scanned_before = scanned.Value();
        const double pruned_before = pruned.Value();
        auto result = RunByOffsetsWithStorageUsage(
            plan, segment.get(), N, offsets, /*repeats=*/2);
        EXPECT_EQ(result.count, 0);
        EXPECT_DOUBLE_EQ(scanned.Value() - scanned_before,
                         static_cast<double>(available_chunks));
        EXPECT_DOUBLE_EQ(pruned.Value() - pruned_before,
                         static_cast<double>(available_chunks));
    }

    SetDefaultEnableParquetStatsSkipIndex(false);
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    (void)fs->DeleteDir(root);
}

// Driver prefetch must ask the skip index with the field's physical type.
// Binary-range literals are represented as int64_t in the plan, but INT32
// footer metrics hold int32_t; passing the promoted literal type makes every
// variant check fail and causes prefetch to touch all cells before the scan
// gets a chance to prune them.
TEST(SkipIndexPr51441, Int32BinaryRangeDriverPrefetchSkipsPrunedCells) {
    DriverPrefetchGuard driver_prefetch_guard(true);
    StorageUsageTrackingGuard tracking_guard(true);
    StorageV2CellTargetGuard cell_target_guard(64 * 1024);
    FieldId val_fid, pk_fid, int32_fid;
    auto schema =
        MakeSkipMeasureSchema(val_fid, pk_fid, nullptr, nullptr, &int32_fid);
    const std::string root = "skip_pr51441_int32_binary_driver_prefetch_v2";
    const int64_t N =
        WriteSkipMeasureV2Parquet(schema, pk_fid, root, 16 * 1024 * 1024);

    SetDefaultEnableParquetStatsSkipIndex(true);
    auto segment = LoadSkipMeasureV2Segment(schema, pk_fid, N, root);
    const int64_t cells = segment->num_chunk_data(int32_fid);
    ASSERT_GT(cells, 1) << "need disjoint INT32 cell ranges to prune";

    auto& scanned = milvus::monitor::internal_core_skipindex_chunks_scanned(
        kSkipMeasureDb, kSkipMeasureCollection);
    auto& pruned = milvus::monitor::internal_core_skipindex_chunks_pruned(
        kSkipMeasureDb, kSkipMeasureCollection);
    const double scanned_before = scanned.Value();
    const double pruned_before = pruned.Value();

    const int64_t lower = N - 1000;
    auto partial = RunWithStorageUsage(
        BinaryRangePlan(int32_fid, DataType::INT32, lower, N - 1),
        segment.get(),
        N);

    const double scanned_delta = scanned.Value() - scanned_before;
    const double pruned_delta = pruned.Value() - pruned_before;
    EXPECT_EQ(partial.count, N - lower);
    EXPECT_DOUBLE_EQ(scanned_delta, static_cast<double>(cells));
    EXPECT_GT(pruned_delta, 0.0);
    EXPECT_LT(pruned_delta, scanned_delta);

    auto full = RunWithStorageUsage(
        BinaryRangePlan(int32_fid, DataType::INT32, 0, N - 1),
        segment.get(),
        N);
    auto all_pruned = RunWithStorageUsage(
        BinaryRangePlan(int32_fid, DataType::INT32, N, N + 1000),
        segment.get(),
        N);

    EXPECT_EQ(full.count, N);
    EXPECT_EQ(all_pruned.count, 0);
    ASSERT_GT(full.total_bytes, 0);
    EXPECT_LT(partial.total_bytes, full.total_bytes / 2)
        << "driver prefetch touched INT32 cells that the skip index pruned ("
        << partial.total_bytes << " vs " << full.total_bytes << ")";
    EXPECT_EQ(all_pruned.total_bytes, 0)
        << "driver prefetch touched an INT32 column even though every cell "
           "was pruned";

    SetDefaultEnableParquetStatsSkipIndex(false);
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    (void)fs->DeleteDir(root);
}

// Sealed VARCHAR IN must prune on the path the query actually takes. The
// existing StorageV2VarcharInPruneAndResultsCorrect asserts pruning by calling
// SkipIndex::CanSkipInQuery directly, which was never the broken part -- the
// break was in PhyTermFilterExpr, and results were correct either way. Read
// the skip-index counters around a real execution instead: they are only
// incremented from inside the executed scan/prefetch.
TEST(SkipIndexPr51441, VarcharInPrunesOnTheExecutedPath) {
    DriverPrefetchGuard driver_prefetch_guard(false);
    StorageV2CellTargetGuard cell_target_guard(64 * 1024);
    FieldId val_fid, pk_fid, payload_fid;
    auto schema = MakeSkipMeasureSchema(val_fid, pk_fid, &payload_fid);
    const std::string root = "skip_pr51441_varchar_in_metrics_v2";
    const int64_t N =
        WriteSkipMeasureV2Parquet(schema, pk_fid, root, 16 * 1024 * 1024);

    // Both wanted payloads live in the last batch, so every cell whose max
    // sorts below them is prunable.
    const std::vector<std::string> wanted = {SkipMeasurePayloadAt(N - 5000),
                                             SkipMeasurePayloadAt(N - 3000)};

    SetDefaultEnableParquetStatsSkipIndex(true);
    auto segment = LoadSkipMeasureV2Segment(schema, pk_fid, N, root);
    ASSERT_GT(segment->num_chunk_data(payload_fid), 1)
        << "need multiple cells for the scan to have anything to prune";

    std::vector<proto::plan::GenericValue> vals;
    for (const auto& w : wanted) {
        proto::plan::GenericValue v;
        v.set_string_val(w);
        vals.push_back(v);
    }
    auto expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(payload_fid, milvus::DataType::VARCHAR), vals);
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);

    auto& scanned = milvus::monitor::internal_core_skipindex_chunks_scanned(
        kSkipMeasureDb, kSkipMeasureCollection);
    auto& pruned = milvus::monitor::internal_core_skipindex_chunks_pruned(
        kSkipMeasureDb, kSkipMeasureCollection);
    const double scanned_before = scanned.Value();
    const double pruned_before = pruned.Value();

    auto bits = query::ExecuteQueryExpr(plan, segment.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(static_cast<int64_t>(bits.count()),
              static_cast<int64_t>(wanted.size()))
        << "pruning must not drop matching rows";

    const double scanned_delta = scanned.Value() - scanned_before;
    const double pruned_delta = pruned.Value() - pruned_before;
    EXPECT_GT(scanned_delta, 0.0)
        << "the executed VARCHAR IN never consulted the skip index at all";
    EXPECT_GT(pruned_delta, 0.0)
        << "the executed VARCHAR IN consulted the skip index but pruned "
           "nothing -- the scan's skip list is empty again";
    EXPECT_LT(pruned_delta, scanned_delta)
        << "a cell holds the matching rows and must never be pruned";

    SetDefaultEnableParquetStatsSkipIndex(false);
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    (void)fs->DeleteDir(root);
}

// A pruned cell must be neither prefetched nor scan-fetched. Measured as
// OpContext::storage_usage on one segment under two predicates: one that the
// skip index can prune three quarters of, and one it can prune nothing of.
// Same segment and same cell layout, so the only variable is pruning -- and
// before the fix the skip branch pinned every pruned chunk anyway to read
// valid_data, which made the two indistinguishable.
TEST(SkipIndexPr51441, PrunedCellsAreNotTouchedByTheScan) {
    DriverPrefetchGuard driver_prefetch_guard(false);
    StorageUsageTrackingGuard tracking_guard(true);
    StorageV2CellTargetGuard cell_target_guard(64 * 1024);
    FieldId val_fid, pk_fid;
    auto schema = MakeSkipMeasureSchema(val_fid, pk_fid);
    const std::string root = "skip_pr51441_traffic_v2";
    const int64_t N =
        WriteSkipMeasureV2Parquet(schema, pk_fid, root, 16 * 1024 * 1024);
    const int64_t threshold = N - 10000;  // only the top quarter matches

    SetDefaultEnableParquetStatsSkipIndex(true);
    auto segment = LoadSkipMeasureV2Segment(schema, pk_fid, N, root);
    ASSERT_GT(segment->num_chunk_data(val_fid), 1)
        << "need multiple cells to measure per-cell traffic";

    // val > threshold: every cell below the threshold is prunable.
    auto pruning = RunWithStorageUsage(
        UnaryRangePlan(val_fid, proto::plan::OpType::GreaterThan, threshold),
        segment.get(),
        N);
    // val > -1: matches everything, so the skip index prunes nothing and this
    // is the cost of touching the whole column.
    auto full = RunWithStorageUsage(
        UnaryRangePlan(val_fid, proto::plan::OpType::GreaterThan, -1),
        segment.get(),
        N);
    // val > N: matches nothing, so EVERY cell is prunable and a scan that
    // touches nothing is the unambiguous signal. This is the assertion that
    // actually pins the fix down: measured at 0 bytes with the gate in place
    // and 87 MB without it, for a query that cannot return a single row.
    auto all_pruned = RunWithStorageUsage(
        UnaryRangePlan(val_fid, proto::plan::OpType::GreaterThan, N),
        segment.get(),
        N);

    EXPECT_EQ(pruning.count, N - 1 - threshold);
    EXPECT_EQ(full.count, N);
    EXPECT_EQ(all_pruned.count, 0);
    ASSERT_GT(full.total_bytes, 0)
        << "storage usage tracking did not accumulate -- the flag must be on "
           "before the segment is loaded, not just before the query";

    EXPECT_EQ(all_pruned.total_bytes, 0)
        << "a query that matches nothing still materialized "
        << all_pruned.total_bytes
        << " bytes: every cell was pruned, yet the scan pinned them anyway "
           "(only to read valid_data), so the skip index saved CPU but no IO";

    // Partial pruning has to scale too. Ratios rather than a bare '<': the
    // full scan pins each cell about twice (prefetch plus per-batch scan)
    // while the skip branch pins once, so 'fewer bytes' alone is satisfied
    // even when every pruned cell is still being fetched.
    EXPECT_LT(pruning.total_bytes, full.total_bytes / 2)
        << "pruning three quarters of the column barely reduced the traffic ("
        << pruning.total_bytes << " vs " << full.total_bytes << ")";

    // Deliberately not asserted on cold_bytes: cells are warmed at load, so
    // every query here is a cache hit and cold_bytes is 0 across the board.
    // total_bytes is what distinguishes "did not touch the cell" from
    // "touched a resident cell", which is exactly the fix under test.

    SetDefaultEnableParquetStatsSkipIndex(false);
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    (void)fs->DeleteDir(root);
}

// The sequential scan is not the only way a filter reads a column. Iterative
// filter evaluates over an offset vector, which reaches ProcessDataByOffsets --
// a path that used to pin the chunk first and consult the skip index after, so
// a pruned cell was materialized anyway and none of the IO the skip index was
// supposed to save materialized either. Measured the same way as the sequential
// case, on offsets spread across every cell so a chunk-at-a-time reader has to
// visit them all.
TEST(SkipIndexPr51441, PrunedCellsAreNotTouchedByOffsetInput) {
    DriverPrefetchGuard driver_prefetch_guard(false);
    StorageV2CellTargetGuard cell_target_guard(64 * 1024);
    StorageUsageTrackingGuard tracking_guard(true);

    FieldId val_fid, pk_fid;
    auto schema = MakeSkipMeasureSchema(val_fid, pk_fid);
    const std::string root = "skip_pr51441_offset_v2";
    const int64_t N =
        WriteSkipMeasureV2Parquet(schema, pk_fid, root, 16 * 1024 * 1024);

    SetDefaultEnableParquetStatsSkipIndex(true);
    auto segment = LoadSkipMeasureV2Segment(schema, pk_fid, N, root);
    ASSERT_GT(segment->num_chunk_data(val_fid), 1)
        << "need multiple cells for offset input to span more than one";

    milvus::exec::OffsetVector offsets;
    const int64_t stride = std::max<int64_t>(1, N / 500);
    for (int64_t i = 0; i < N; i += stride) {
        offsets.push_back(static_cast<int32_t>(i));
    }

    std::unordered_set<int64_t> offset_chunks;
    for (auto offset : offsets) {
        offset_chunks.insert(
            segment->get_chunk_by_offset(val_fid, offset).first);
    }
    auto& scanned = milvus::monitor::internal_core_skipindex_chunks_scanned(
        kSkipMeasureDb, kSkipMeasureCollection);
    auto& pruned = milvus::monitor::internal_core_skipindex_chunks_pruned(
        kSkipMeasureDb, kSkipMeasureCollection);
    const double scanned_before_pruned = scanned.Value();
    const double pruned_before_pruned = pruned.Value();

    // val > N matches nothing, so every cell the offsets land in is prunable.
    auto all_pruned = RunByOffsetsWithStorageUsage(
        UnaryRangePlan(val_fid, proto::plan::OpType::GreaterThan, N),
        segment.get(),
        N,
        offsets,
        2);
    EXPECT_DOUBLE_EQ(scanned.Value() - scanned_before_pruned,
                     static_cast<double>(offset_chunks.size()))
        << "offset metrics must count each judged chunk once, not each row or "
           "non-consecutive run";
    EXPECT_DOUBLE_EQ(pruned.Value() - pruned_before_pruned,
                     static_cast<double>(offset_chunks.size()))
        << "every chunk reached by these offsets is prunable";

    const double scanned_before_full = scanned.Value();
    const double pruned_before_full = pruned.Value();
    // val > -1 matches everything: the cost of reaching those same offsets
    // without pruning.
    auto full = RunByOffsetsWithStorageUsage(
        UnaryRangePlan(val_fid, proto::plan::OpType::GreaterThan, -1),
        segment.get(),
        N,
        offsets);
    EXPECT_DOUBLE_EQ(scanned.Value() - scanned_before_full,
                     static_cast<double>(offset_chunks.size()));
    EXPECT_DOUBLE_EQ(pruned.Value() - pruned_before_full, 0.0);

    EXPECT_EQ(all_pruned.count, 0);
    EXPECT_EQ(full.count, static_cast<int64_t>(offsets.size()));
    ASSERT_GT(full.total_bytes, 0)
        << "storage usage tracking did not accumulate on the offset path -- "
           "the flag must be on before the segment is loaded";

    EXPECT_EQ(all_pruned.total_bytes, 0)
        << "an offset-input filter that prunes every cell still touched "
        << all_pruned.total_bytes << " bytes against " << full.total_bytes
        << " for the unpruned one: the offset path is pinning chunks the skip "
           "index already ruled out, so iterative filter pays IO the "
           "sequential scan no longer does";

    SetDefaultEnableParquetStatsSkipIndex(false);
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    (void)fs->DeleteDir(root);
}

// What the byte histogram publishes, and when. Two properties, and they pull
// in opposite directions -- which is why the reporter asks the segment whether
// it was measured rather than inferring it from the bytes it saw:
//   - a measured operation is published even when it moved few or no bytes.
//     Gating on "did we observe bytes" dropped exactly the well-pruned tail,
//     the samples the metric exists to show;
//   - an operation over a segment whose cache slots were built while tracking
//     was off is not published at all. Those slots never accumulate, so a
//     report would be an unmeasured zero dressed up as a measurement -- and it
//     would read as "the skip index removed all the IO".
TEST(SkipIndexPr51441, ScannedBytesPublishedOnlyForMeasuredSegments) {
    DriverPrefetchGuard driver_prefetch_guard(false);
    StorageV2CellTargetGuard cell_target_guard(64 * 1024);

    FieldId val_fid, pk_fid;
    auto schema = MakeSkipMeasureSchema(val_fid, pk_fid);
    const std::string root = "skip_pr51441_report_v2";
    const int64_t N =
        WriteSkipMeasureV2Parquet(schema, pk_fid, root, 16 * 1024 * 1024);

    SetDefaultEnableParquetStatsSkipIndex(true);

    // The series the async request boundary publishes under. Reading it by
    // label is itself the check that the collection name survives into the
    // metric -- it comes from the schema, which is all a segment keeps of its
    // collection.
    auto observed = [](const char* op) {
        auto metric = milvus::monitor::internal_core_query_scanned_bytes_total(
                          kSkipMeasureDb, kSkipMeasureCollection, op)
                          .Collect();
        return std::make_pair(metric.histogram.sample_count,
                              metric.histogram.sample_sum);
    };

    auto retrieve_plan = [&](int64_t threshold) {
        auto plan = std::make_unique<query::RetrievePlan>(schema);
        proto::plan::GenericValue value;
        value.set_int64_val(threshold);
        auto expr = std::make_shared<expr::UnaryRangeFilterExpr>(
            expr::ColumnInfo(val_fid, milvus::DataType::INT64),
            proto::plan::OpType::GreaterThan,
            value);
        plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
        plan->plan_node_->plannodes_ =
            milvus::test::CreateRetrievePlanByExpr(expr);
        plan->field_ids_ = std::vector<FieldId>{pk_fid};
        return plan;
    };

    auto parse_c_retrieve_result = [](CRetrieveResult* c_result) {
        if (c_result == nullptr) {
            return std::unique_ptr<proto::segcore::RetrieveResults>{};
        }
        auto result = std::make_unique<proto::segcore::RetrieveResults>();
        EXPECT_TRUE(
            result->ParseFromArray(c_result->proto_blob, c_result->proto_size));
        DeleteRetrieveResult(c_result);
        return result;
    };
    auto async_retrieve = [&](SegmentInterface* segment,
                              query::RetrievePlan* plan) {
        CRetrieveResult* c_result = nullptr;
        auto status = CRetrieve(static_cast<CSegmentInterface>(segment),
                                static_cast<CRetrievePlan>(plan),
                                MAX_TIMESTAMP,
                                &c_result);
        EXPECT_EQ(status.error_code, 0);
        return parse_c_retrieve_result(c_result);
    };
    auto async_retrieve_by_offsets = [&](SegmentInterface* segment,
                                         query::RetrievePlan* plan,
                                         int64_t* offsets,
                                         int64_t len) {
        CRetrieveResult* c_result = nullptr;
        auto status =
            CRetrieveByOffsets(static_cast<CSegmentInterface>(segment),
                               static_cast<CRetrievePlan>(plan),
                               offsets,
                               len,
                               &c_result);
        EXPECT_EQ(status.error_code, 0);
        return parse_c_retrieve_result(c_result);
    };

    // val > N matches nothing, so every cell is prunable; val > -1 matches
    // everything and is the cost of not pruning.
    auto all_pruned_plan = retrieve_plan(N);
    auto full_plan = retrieve_plan(-1);

    {
        StorageUsageTrackingGuard tracking_guard(true);
        auto segment = LoadSkipMeasureV2Segment(schema, pk_fid, N, root);
        ASSERT_TRUE(segment->storage_usage_tracked())
            << "the flag was on before the load, so the segment's slots track";

        const auto count_before_fast_path = observed("count");
        EXPECT_EQ(segment->get_real_count(), N);
        EXPECT_EQ(observed("count").first, count_before_fast_path.first)
            << "the no-delete row-count fast path must not create a fake IO "
               "sample";

        const auto query_before_sync = observed("query");
        auto sync_result = segment->Retrieve(nullptr,
                                             all_pruned_plan.get(),
                                             MAX_TIMESTAMP,
                                             DEFAULT_MAX_OUTPUT_SIZE,
                                             false);
        ASSERT_NE(sync_result, nullptr);
        EXPECT_TRUE(sync_result->storage_cost_valid());
        EXPECT_EQ(observed("query").first, query_before_sync.first)
            << "the synchronous primitive must not publish a user-request "
               "sample";

        const auto before_pruned = observed("query");
        const auto count_before_pruned = observed("count");
        auto pruned_result =
            async_retrieve(segment.get(), all_pruned_plan.get());
        ASSERT_NE(pruned_result, nullptr);
        EXPECT_TRUE(pruned_result->storage_cost_valid());
        const auto after_pruned = observed("query");
        EXPECT_EQ(after_pruned.first, before_pruned.first + 1)
            << "a fully pruned query published nothing: the operation was "
               "measured, and dropping it is exactly how the best-pruned "
               "samples disappear from the histogram";
        EXPECT_EQ(observed("count").first, count_before_pruned.first)
            << "a normal retrieve was attributed to count";

        const auto before_full = observed("query");
        auto full_result = async_retrieve(segment.get(), full_plan.get());
        ASSERT_NE(full_result, nullptr);
        EXPECT_TRUE(full_result->storage_cost_valid());
        const auto after_full = observed("query");
        ASSERT_EQ(after_full.first, before_full.first + 1);

        const double pruned_bytes = after_pruned.second - before_pruned.second;
        const double full_bytes = after_full.second - before_full.second;
        EXPECT_DOUBLE_EQ(
            pruned_bytes,
            static_cast<double>(pruned_result->scanned_total_bytes()))
            << "query metric was observed before the final target-field IO "
               "was merged into RetrieveResults";
        EXPECT_DOUBLE_EQ(
            full_bytes, static_cast<double>(full_result->scanned_total_bytes()))
            << "query metric must equal the final cost returned to Proxy";
        ASSERT_GT(full_bytes, 0.0) << "storage usage tracking did not "
                                      "accumulate for the unpruned retrieve";
        // Measured at 0 against ~170 MB for the same segment: a retrieve whose
        // filter prunes every cell touches nothing at all, MVCC included. That
        // exact zero is what the old gate suppressed, so it is asserted rather
        // than merely allowed -- if a future change makes such a retrieve pin
        // something, this metric stops being able to show a clean prune and
        // the assertion above (a sample was published) is what still has to
        // hold.
        EXPECT_EQ(pruned_bytes, 0.0)
            << "a retrieve that prunes every cell reported " << pruned_bytes
            << " bytes";
        EXPECT_LT(pruned_bytes, full_bytes / 2)
            << "the pruned retrieve reported " << pruned_bytes
            << " bytes against " << full_bytes
            << " for the same segment: pruning is not reaching the metric";

        int64_t offset = 0;
        const auto before_offsets = observed("query");
        const auto count_before_offsets = observed("count");
        auto offsets_result = async_retrieve_by_offsets(
            segment.get(), full_plan.get(), &offset, 1);
        ASSERT_NE(offsets_result, nullptr);
        EXPECT_TRUE(offsets_result->storage_cost_valid());
        const auto after_offsets = observed("query");
        EXPECT_EQ(after_offsets.first, before_offsets.first + 1);
        EXPECT_DOUBLE_EQ(
            after_offsets.second - before_offsets.second,
            static_cast<double>(offsets_result->scanned_total_bytes()));
        EXPECT_EQ(observed("count").first, count_before_offsets.first)
            << "RetrieveByOffsets was attributed to count";

        auto count_plan = retrieve_plan(N);
        count_plan->operation_ = query::RetrieveOperation::Count;
        const auto before_count = observed("count");
        const auto query_before_count = observed("query");
        auto count_result = async_retrieve(segment.get(), count_plan.get());
        ASSERT_NE(count_result, nullptr);
        EXPECT_TRUE(count_result->storage_cost_valid());
        const auto after_count = observed("count");
        EXPECT_EQ(after_count.first, before_count.first + 1);
        EXPECT_DOUBLE_EQ(
            after_count.second - before_count.second,
            static_cast<double>(count_result->scanned_total_bytes()));
        EXPECT_EQ(observed("query").first, query_before_count.first)
            << "a count retrieve was attributed to query";

        auto delete_ids = std::make_unique<IdArray>();
        delete_ids->mutable_int_id()->mutable_data()->Add(0);
        Timestamp delete_ts = MAX_TIMESTAMP;
        ASSERT_TRUE(segment->Delete(1, delete_ids.get(), &delete_ts).ok());
        const auto before_internal_count = observed("count");
        const auto query_before_internal_count = observed("query");
        EXPECT_EQ(segment->get_real_count(), N - 1);
        EXPECT_EQ(observed("count").first, before_internal_count.first + 1)
            << "get_real_count with deletes must be attributed to count";
        EXPECT_EQ(observed("query").first, query_before_internal_count.first)
            << "get_real_count polluted the user query histogram";

        auto observed_search = [] {
            auto metric =
                milvus::monitor::internal_core_query_scanned_bytes_total(
                    kSkipMeasureDb, kSkipMeasureCollection, "search")
                    .Collect();
            return std::make_pair(metric.histogram.sample_count,
                                  metric.histogram.sample_sum);
        };
        milvus::SearchResult finalized_search;
        finalized_search.segment_ = segment.get();
        finalized_search.search_storage_cost_ = {123, 456};
        const auto before_search = observed_search();
        ReportSearchResultStorageMetrics(&finalized_search);
        ReportSearchResultStorageMetrics(&finalized_search);
        const auto after_search = observed_search();
        EXPECT_EQ(after_search.first, before_search.first + 1)
            << "explicit finalization must observe each SearchResult once";
        EXPECT_DOUBLE_EQ(after_search.second - before_search.second, 456.0)
            << "search metric must use the final accumulated StorageCost";
        bool has_group_by = false;
        bool search_cost_valid = false;
        int64_t group_size = 0;
        int64_t search_remote_bytes = 0;
        int64_t search_total_bytes = 0;
        GetSearchResultMetadata(&finalized_search,
                                &has_group_by,
                                &group_size,
                                &search_remote_bytes,
                                &search_total_bytes,
                                &search_cost_valid);
        EXPECT_TRUE(search_cost_valid);
        EXPECT_EQ(search_remote_bytes, 123);
        EXPECT_EQ(search_total_bytes, 456);
    }

    {
        // Same segment data, but the slots are created with tracking off, so
        // nothing accumulates and nothing may be published.
        StorageUsageTrackingGuard tracking_guard(false);
        auto segment = LoadSkipMeasureV2Segment(schema, pk_fid, N, root);
        ASSERT_FALSE(segment->storage_usage_tracked());

        const auto before = observed("query");
        auto untracked_result =
            async_retrieve(segment.get(), all_pruned_plan.get());
        ASSERT_NE(untracked_result, nullptr);
        EXPECT_FALSE(untracked_result->storage_cost_valid());
        milvus::SearchResult untracked_search;
        untracked_search.segment_ = segment.get();
        bool has_group_by = false;
        bool search_cost_valid = true;
        int64_t group_size = 0;
        int64_t search_remote_bytes = 0;
        int64_t search_total_bytes = 0;
        GetSearchResultMetadata(&untracked_search,
                                &has_group_by,
                                &group_size,
                                &search_remote_bytes,
                                &search_total_bytes,
                                &search_cost_valid);
        EXPECT_FALSE(search_cost_valid);
        EXPECT_EQ(observed("query").first, before.first)
            << "published a sample for a segment that never measured "
               "anything -- an unmeasured zero reads as 'the skip index "
               "removed all the IO'";
    }

    SetDefaultEnableParquetStatsSkipIndex(false);
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    (void)fs->DeleteDir(root);
}

// The other half of the same gate: eliding the validity fetch on a skipped
// chunk is only sound when nobody observes the result's validity. Both
// directions are checked on a nullable column whose lower cells are prunable:
//   - `nval > T` at top level is null-rejecting, so the fetch IS elided;
//   - `NOT (nval > T)` observes validity (MarkNullRejecting stops at NOT), so
//     the fetch must be kept, or NULL rows get flipped to true.
// Counts are derived from the data, so either mistake shows up as a wrong
// count rather than as a silent extra read.
TEST(SkipIndexPr51441, NullableSkippedChunkKeepsThreeValuedLogic) {
    DriverPrefetchGuard driver_prefetch_guard(false);
    StorageV2CellTargetGuard cell_target_guard(64 * 1024);
    FieldId val_fid, pk_fid, payload_fid, nval_fid;
    auto schema =
        MakeSkipMeasureSchema(val_fid, pk_fid, &payload_fid, &nval_fid);
    const std::string root = "skip_pr51441_nullable_v2";
    // nval is arrow column 3: val(0), pk(1), payload(2), nval(3), ts(4).
    const int64_t N = WriteSkipMeasureV2Parquet(
        schema, pk_fid, root, 16 * 1024 * 1024, /*nullable_col=*/3);
    const int64_t threshold = N - 10000;

    SetDefaultEnableParquetStatsSkipIndex(true);
    auto segment = LoadSkipMeasureV2Segment(schema, pk_fid, N, root);
    ASSERT_GT(segment->num_chunk_data(nval_fid), 1)
        << "need multiple cells so some are skipped";

    int64_t expect_gt = 0;   // nval > T, NULL excluded
    int64_t expect_not = 0;  // NOT (nval > T), NULL still excluded
    for (int64_t i = 0; i < N; ++i) {
        if (i % kNullEvery == 0) {
            continue;  // NULL: `nval > T` is NULL, and so is its negation
        }
        if (i > threshold) {
            ++expect_gt;
        } else {
            ++expect_not;
        }
    }
    ASSERT_GT(expect_not, 0);

    // Null-rejecting: the skipped cells contribute nothing, and the elided
    // validity fetch must not change the answer.
    auto gt_plan =
        UnaryRangePlan(nval_fid, proto::plan::OpType::GreaterThan, threshold);
    auto gt_bits =
        query::ExecuteQueryExpr(gt_plan, segment.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(static_cast<int64_t>(gt_bits.count()), expect_gt)
        << "nullable scan under a null-rejecting consumer returned the wrong "
           "rows";

    // Validity IS observed here: a skipped cell that skipped its validity
    // fetch would flip its NULL rows to true and overshoot by exactly the
    // number of NULLs in the skipped cells.
    proto::plan::GenericValue value;
    value.set_int64_val(threshold);
    auto inner = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(nval_fid, milvus::DataType::INT64),
        proto::plan::OpType::GreaterThan,
        value);
    auto not_expr = std::make_shared<expr::LogicalUnaryExpr>(
        expr::LogicalUnaryExpr::OpType::LogicalNot, inner);
    auto not_plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, not_expr);
    auto not_bits =
        query::ExecuteQueryExpr(not_plan, segment.get(), N, MAX_TIMESTAMP);
    EXPECT_EQ(static_cast<int64_t>(not_bits.count()), expect_not)
        << "NOT over a skipped nullable cell leaked NULL rows -- the validity "
           "fetch must not be elided when validity is observed";

    SetDefaultEnableParquetStatsSkipIndex(false);
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    (void)fs->DeleteDir(root);
}
