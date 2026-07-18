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
#include <stdlib.h>
#include <time.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
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
#include "common/Consts.h"
#include "common/LoadInfo.h"
#include "common/Schema.h"
#include "common/Span.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
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
#include "segcore/Types.h"
#include "segcore/storagev2translator/GroupCTMeta.h"
#include "segcore/storagev1translator/ChunkTranslator.h"
#include "storage/FileManager.h"
#include "storage/Types.h"
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
