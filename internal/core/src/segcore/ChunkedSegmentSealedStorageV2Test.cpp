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
#include <folly/CancellationToken.h>
#include <folly/ScopeGuard.h>
#include <gtest/gtest.h>
#include <boost/filesystem/operations.hpp>
#include <parquet/properties.h>
#include <stdlib.h>
#include <time.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include "segcore/default_fs.h"

#include "NamedType/named_type_impl.hpp"
#include "cachinglayer/Manager.h"
#include "cachinglayer/CacheSlot.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/JsonCastType.h"
#include "common/LoadInfo.h"
#include "common/Schema.h"
#include "common/Span.h"
#include "common/SystemProperty.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "exec/QueryContext.h"
#include "exec/Task.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"
#include "filemanager/InputStream.h"
#include "gtest/gtest.h"
#include "index/Index.h"
#include "index/IndexFactory.h"
#include "index/IndexInfo.h"
#include "index/JsonFlatIndex.h"
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
#include "segcore/storagev2translator/StorageV2Config.h"
#include "segcore/SegmentChunkReader.h"
#include "segcore/SegmentSealed.h"
#include "segcore/search_result_export_c.h"
#include "segcore/Types.h"
#include "segcore/storagev2translator/GroupCTMeta.h"
#include "segcore/storagev2translator/SystemIndexTranslator.h"
#include "segcore/storagev1translator/ChunkTranslator.h"
#include "storage/FileManager.h"
#include "storage/Types.h"
#include "storage/loon_ffi/property_singleton.h"
#include "test_utils/Constants.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/ManifestTestUtil.h"
#include "test_utils/cachinglayer_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;
using namespace milvus::segcore::storagev1translator;

namespace {
std::string
UniqueManifestTestPath(const std::string& prefix) {
    return boost::filesystem::unique_path(prefix + "_%%%%-%%%%-%%%%-%%%%")
        .string();
}

bool
IsLazyColumnForTest(const std::shared_ptr<ChunkedColumnInterface>& column) {
    auto proxy = std::dynamic_pointer_cast<ProxyChunkColumn>(column);
    EXPECT_NE(proxy, nullptr);
    return proxy != nullptr && proxy->IsLazy();
}

class LazyColumnGroupGuard {
 public:
    explicit LazyColumnGroupGuard(bool enabled)
        : previous_(SegcoreConfig::default_config()
                        .get_lazy_column_group_enabled()) {
        SegcoreConfig::default_config().set_lazy_column_group_enabled(
            enabled);
    }

    ~LazyColumnGroupGuard() {
        SegcoreConfig::default_config().set_lazy_column_group_enabled(
            previous_);
    }

 private:
    bool previous_;
};

class CacheWarmupPolicyGuard {
 public:
    explicit CacheWarmupPolicyGuard(
        milvus::cachinglayer::CacheWarmupPolicies warmup_policies)
        : previous_(milvus::cachinglayer::TieredStorageConfig::GetInstance()
                        .GetSnapshot()) {
        milvus::cachinglayer::Manager::UpdateConfig(
            previous_.loading_timeout,
            previous_.warmup_loading_timeout,
            previous_.storage_usage_tracking_enabled,
            warmup_policies);
    }

    ~CacheWarmupPolicyGuard() {
        milvus::cachinglayer::Manager::UpdateConfig(
            previous_.loading_timeout,
            previous_.warmup_loading_timeout,
            previous_.storage_usage_tracking_enabled,
            previous_.warmup_policies);
    }

 private:
    milvus::cachinglayer::TieredStorageConfig::Snapshot previous_;
};

SchemaPtr
CreateTextMatchManifestSchema(bool pk_is_string) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField("int64", DataType::INT64, true);
    auto pk_fid = schema->AddDebugField(
        "pk", pk_is_string ? DataType::VARCHAR : DataType::INT64, false);
    std::map<std::string, std::string> analyzer_params;
    schema->AddDebugVarcharField(FieldName("string1"),
                                 DataType::VARCHAR,
                                 65535,
                                 true,
                                 true,
                                 true,
                                 analyzer_params,
                                 std::nullopt);
    schema->AddDebugField("string2", DataType::VARCHAR, true);
    schema->AddField(FieldName("ts"),
                     TimestampFieldID,
                     DataType::INT64,
                     false,
                     std::nullopt);
    schema->set_primary_field_id(pk_fid);
    return schema;
}

// Keep system fields and PK in separate physical groups so ordinary user-field
// Tasks can exercise lazy materialization independently of those blockers.
std::string
LazyManifestTestGroupPattern(const SchemaPtr& schema) {
    std::string user_fields;
    std::string other_groups;
    auto primary_field_id = schema->get_primary_field_id();
    for (const auto& field_id : schema->get_field_ids()) {
        if (SystemProperty::Instance().IsSystem(field_id) ||
            (primary_field_id.has_value() &&
             primary_field_id.value() == field_id) ||
            IsVectorDataType((*schema)[field_id].get_data_type())) {
            other_groups += "," + std::to_string(field_id.get());
        } else {
            if (!user_fields.empty()) {
                user_fields += "|";
            }
            user_fields += std::to_string(field_id.get());
        }
    }
    return user_fields.empty() ? other_groups.substr(1)
                               : user_fields + other_groups;
}

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
                            const SchemaPtr& schema,
                            bool is_sorted_by_pk,
                            Timestamp commit_ts = 0) {
        auto seg = segcore::CreateSealedSegment(
            schema,
            nullptr,
            proto.segmentid(),
            segcore::SegcoreConfig::default_config(),
            is_sorted_by_pk);
        auto* sealed = dynamic_cast<ChunkedSegmentSealedImpl*>(seg.get());
        EXPECT_NE(sealed, nullptr);
        if (sealed == nullptr) {
            return seg;
        }
        sealed->SetLoadInfo(std::move(proto));
        if (commit_ts != 0) {
            sealed->SetCommitTimestamp(commit_ts);
        }
        milvus::OpContext op_ctx;
        milvus::tracer::TraceContext trace_ctx;
        sealed->Load(trace_ctx, &op_ctx);
        return seg;
    }

    segcore::SegmentSealedUPtr
    CreateSegmentByLoadInfo(proto::segcore::SegmentLoadInfo proto,
                            bool is_sorted_by_pk,
                            Timestamp commit_ts = 0) {
        return CreateSegmentByLoadInfo(
            std::move(proto), schema_, is_sorted_by_pk, commit_ts);
    }

    proto::segcore::SegmentLoadInfo
    MakeV3ManifestLoadInfo(const milvus::test::V3SegmentTestData& test_data,
                           int64_t segment_id) {
        proto::segcore::SegmentLoadInfo load_info;
        load_info.set_segmentid(segment_id);
        load_info.set_partitionid(1);
        load_info.set_collectionid(1);
        load_info.set_num_of_rows(test_data.TotalRows());
        load_info.set_storageversion(STORAGE_V3);
        load_info.set_manifest_path(test_data.ManifestPathJson());
        load_info.set_priority(proto::common::LoadPriority::LOW);
        return load_info;
    }

    segcore::SegmentSealedUPtr
    CreateV3ManifestSegment(const milvus::test::V3SegmentTestData& test_data,
                            int64_t segment_id,
                            Timestamp commit_ts = 0,
                            const SchemaPtr& schema = nullptr) {
        return CreateSegmentByLoadInfo(
            MakeV3ManifestLoadInfo(test_data, segment_id),
            schema != nullptr ? schema : schema_,
            false,
            commit_ts);
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

class TestLazyManifest : public TestChunkSegmentStorageV2 {
 protected:
    void
    SetUp() override {
        // Manifest tests create their own V3 data; no V2 segment is needed.
        schema_ = segcore::GenChunkedSegmentTestSchema(GetParam());
        fields = {{"int64", schema_->get_field_id(FieldName("int64"))},
                  {"pk", schema_->get_field_id(FieldName("pk"))},
                  {"ts", TimestampFieldID},
                  {"string1", schema_->get_field_id(FieldName("string1"))},
                  {"string2", schema_->get_field_id(FieldName("string2"))}};
    }
};

INSTANTIATE_TEST_SUITE_P(TestLazyManifest,
                         TestLazyManifest,
                         testing::Bool());

TEST_P(TestLazyManifest, LazyManifestPreparesInterimIndex) {
    LazyColumnGroupGuard lazy_group_guard(true);
    auto schema = GenChunkedSegmentTestSchema(GetParam());
    auto vec = schema->AddDebugField(
        "vec", DataType::VECTOR_FLOAT, 4, knowhere::metric::L2);
    auto schema_proto = schema->ToProto();
    for (auto& field : *schema_proto.mutable_fields()) {
        if (field.fieldid() == TimestampFieldID.get()) {
            field.set_name("Timestamp");
        }
    }
    // Admit a lazy raw-vector Task independently of interim index warmup.
    AddWarmupProperty(schema_proto, "warmup.vectorIndex", "disable");
    schema = Schema::ParseFrom(schema_proto);

    FieldIndexMeta field_index_meta(
        vec,
        {{"index_type", knowhere::IndexEnum::INDEX_FAISS_IVFFLAT},
         {"metric_type", knowhere::metric::L2},
         {"nlist", "64"}},
        {{"dim", "4"}});
    auto index_meta = std::make_shared<CollectionIndexMeta>(
        100000, std::map<FieldId, FieldIndexMeta>{{vec, field_index_meta}});
    auto& config = SegcoreConfig::default_config();
    const auto previous_interim = config.get_enable_interim_segment_index();
    const auto previous_chunk_rows = config.get_chunk_rows();
    const auto previous_index_type =
        config.get_dense_vector_intermin_index_type();
    const auto previous_nlist = config.get_nlist();
    const auto previous_nprobe = config.get_nprobe();
    auto restore_config = folly::makeGuard([&]() {
        config.set_enable_interim_segment_index(previous_interim);
        config.set_chunk_rows(previous_chunk_rows);
        config.set_dense_vector_intermin_index_type(previous_index_type);
        config.set_nlist(previous_nlist);
        config.set_nprobe(previous_nprobe);
    });
    config.set_enable_interim_segment_index(true);
    config.set_chunk_rows(1024);
    config.set_dense_vector_intermin_index_type(
        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC);
    config.set_nlist(16);
    config.set_nprobe(16);

    for (int64_t num_rows : {1, 1000}) {
        const auto base_path = UniqueManifestTestPath(
            std::string("lazy_manifest_interim_") +
            (GetParam() ? "varchar_" : "int64_") +
            std::to_string(num_rows));
        auto fs = milvus::segcore::GetDefaultArrowFileSystem();
        StorageV2TempDirGuard dir_guard(fs, base_path);
        milvus::test::V3SegmentTestData test_data(
            schema, 1, num_rows, 4, TestLocalPath, base_path);

        for (auto warmup : {CacheWarmupPolicy::CacheWarmupPolicy_Disable,
                            CacheWarmupPolicy::CacheWarmupPolicy_Sync,
                            CacheWarmupPolicy::CacheWarmupPolicy_Async}) {
            SCOPED_TRACE(::testing::Message()
                         << "rows=" << num_rows
                         << ", warmup=" << static_cast<int>(warmup));
            auto policies = cachinglayer::TieredStorageConfig::GetInstance()
                                .warmup_policies();
            policies.vectorIndexCacheWarmupPolicy = warmup;
            CacheWarmupPolicyGuard warmup_guard(policies);

            auto seg = CreateSealedSegment(schema, index_meta, 3190, config);
            auto* sealed = dynamic_cast<ChunkedSegmentSealedImpl*>(seg.get());
            ASSERT_NE(sealed, nullptr);
            sealed->SetLoadInfo(MakeV3ManifestLoadInfo(test_data, 3190));
            milvus::OpContext op_ctx;
            milvus::tracer::TraceContext trace_ctx;
            sealed->Load(trace_ctx, &op_ctx);

            auto snapshot = sealed->TestGetPublishedStateSnapshot();
            auto column = std::dynamic_pointer_cast<ProxyChunkColumn>(
                snapshot->runtime->fields.at(vec));
            ASSERT_NE(column, nullptr);
            ASSERT_TRUE(column->IsLazy());
            if (num_rows == 1) {
                EXPECT_FALSE(column->IsMaterialized());
                EXPECT_EQ(snapshot->runtime->vector_indexings.count(vec), 0);
                EXPECT_FALSE(snapshot->binlog_index_bitset[vec.get() -
                                                           START_USER_FIELDID]);
                continue;
            }

            ASSERT_TRUE(snapshot->runtime->vector_indexings.count(vec));
            EXPECT_TRUE(snapshot->binlog_index_bitset[vec.get() -
                                                      START_USER_FIELDID]);
            auto slot = snapshot->runtime->vector_indexings.at(vec)->indexing_;
            ASSERT_NE(slot, nullptr);
            if (warmup == CacheWarmupPolicy::CacheWarmupPolicy_Disable) {
                EXPECT_FALSE(column->IsMaterialized());
                EXPECT_FALSE(slot->IsCached(0));
            } else if (warmup == CacheWarmupPolicy::CacheWarmupPolicy_Sync) {
                EXPECT_TRUE(column->IsMaterialized());
                EXPECT_TRUE(slot->IsCached(0));
            }

            auto first =
                cachinglayer::SemiInlineGet(slot->PinCells(&op_ctx, {0}));
            EXPECT_TRUE(column->IsMaterialized());
            EXPECT_EQ(first->get_cell_of(0)->Count(), num_rows);
            auto second =
                cachinglayer::SemiInlineGet(slot->PinCells(&op_ctx, {0}));
            EXPECT_EQ(first->get_cell_of(0), second->get_cell_of(0));
        }
    }
}

TEST_P(TestLazyManifest, LazyManifestPreservesInitialMultiFieldTask) {
    LazyColumnGroupGuard lazy_group_guard(true);
    auto warmup_policies =
        milvus::cachinglayer::TieredStorageConfig::GetInstance()
            .warmup_policies();
    warmup_policies.scalarFieldCacheWarmupPolicy =
        CacheWarmupPolicy::CacheWarmupPolicy_Disable;
    CacheWarmupPolicyGuard warmup_guard(warmup_policies);

    const auto base_path = UniqueManifestTestPath(
        std::string("lazy_manifest_initial_task_") +
        (GetParam() ? "varchar" : "int64"));
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    StorageV2TempDirGuard dir_guard(fs, base_path);
    milvus::test::V3SegmentTestData test_data(
        schema_,
        1,
        64,
        128,
        TestLocalPath,
        base_path,
        LazyManifestTestGroupPattern(schema_));
    auto manifest_segment =
        CreateV3ManifestSegment(test_data, 3150 + (GetParam() ? 100 : 0));
    auto* segment_impl =
        dynamic_cast<ChunkedSegmentSealedImpl*>(manifest_segment.get());
    ASSERT_NE(segment_impl, nullptr);

    auto int64_column =
        segment_impl->TestGetPublishedStateSnapshot()->runtime->fields.at(
            fields.at("int64"));
    auto string_column =
        segment_impl->TestGetPublishedStateSnapshot()->runtime->fields.at(
            fields.at("string1"));
    ASSERT_NE(int64_column, nullptr);
    ASSERT_NE(string_column, nullptr);
    auto snapshot = segment_impl->TestGetPublishedStateSnapshot();

    const std::vector<
        std::pair<FieldId, std::shared_ptr<ChunkedColumnInterface>>>
        lazy_columns = {
            {fields.at("int64"), int64_column},
            {fields.at("string1"), string_column},
        };
    for (const auto& [field_id, column] : lazy_columns) {
        (void)field_id;
        EXPECT_TRUE(column->IsInMultiFieldColumnGroup());
        int64_t offset = 0;
        EXPECT_FALSE(column->CellsLoaded(&offset, 1));
        EXPECT_TRUE(column->CellsLoaded(nullptr, 0));
        EXPECT_TRUE(IsLazyColumnForTest(column));
    }

    auto memory_before_materialize = segment_impl->GetMemoryUsageInBytes();
    constexpr int kThreadCount = 16;
    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::atomic<bool> failed{false};
    std::vector<std::thread> workers;
    workers.reserve(kThreadCount);
    for (int i = 0; i < kThreadCount; ++i) {
        auto column = i % 2 == 0 ? int64_column : string_column;
        workers.emplace_back([column, &ready, &start, &failed]() {
            ready.fetch_add(1, std::memory_order_acq_rel);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            try {
                auto data = column->DataOfChunk(nullptr, 0);
                if (data.get() == nullptr) {
                    failed.store(true, std::memory_order_release);
                }
            } catch (...) {
                failed.store(true, std::memory_order_release);
            }
        });
    }
    while (ready.load(std::memory_order_acquire) != kThreadCount) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    for (auto& worker : workers) {
        worker.join();
    }

    EXPECT_FALSE(failed.load(std::memory_order_acquire));
    EXPECT_GT(int64_column->DataByteSize(), 0);
    EXPECT_GT(string_column->DataByteSize(), 0);
    // The untouched user-field sibling shares the same Task.
    int64_t first_offset = 0;
    auto sibling_column = snapshot->runtime->fields.at(fields.at("string2"));
    EXPECT_TRUE(sibling_column->CellsLoaded(&first_offset, 1));
    EXPECT_EQ(segment_impl->GetMemoryUsageInBytes(), memory_before_materialize);

    segment_impl->DropFieldData(fields.at("int64"));
    EXPECT_EQ(segment_impl->GetMemoryUsageInBytes(), memory_before_materialize);
    auto dropped_column_snapshot =
        segment_impl->TestGetPublishedStateSnapshot();
    EXPECT_EQ(
        dropped_column_snapshot->runtime->fields.count(fields.at("int64")), 0);

}

TEST_P(TestLazyManifest,
       LazyManifestDefersTextMatchAfterIndexCreation) {
    LazyColumnGroupGuard lazy_group_guard(true);
    auto warmup_policies =
        milvus::cachinglayer::TieredStorageConfig::GetInstance()
            .warmup_policies();
    warmup_policies.scalarFieldCacheWarmupPolicy =
        CacheWarmupPolicy::CacheWarmupPolicy_Disable;
    CacheWarmupPolicyGuard warmup_guard(warmup_policies);

    auto text_schema = CreateTextMatchManifestSchema(GetParam());
    auto text_field = text_schema->get_field_id(FieldName("string1"));
    const auto suffix = GetParam() ? "varchar" : "int64";
    const auto base_path_a = UniqueManifestTestPath(
        std::string("lazy_manifest_text_match_a_") + suffix);
    const auto base_path_b = UniqueManifestTestPath(
        std::string("lazy_manifest_text_match_b_") + suffix);
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    StorageV2TempDirGuard dir_guard_a(fs, base_path_a);
    StorageV2TempDirGuard dir_guard_b(fs, base_path_b);
    milvus::test::V3SegmentTestData test_data_a(
        text_schema,
        1,
        64,
        128,
        TestLocalPath,
        base_path_a,
        LazyManifestTestGroupPattern(text_schema));
    milvus::test::V3SegmentTestData test_data_b(
        text_schema,
        1,
        64,
        128,
        TestLocalPath,
        base_path_b,
        LazyManifestTestGroupPattern(text_schema));

    constexpr int64_t segment_id = 3178;
    auto manifest_segment =
        CreateV3ManifestSegment(test_data_a, segment_id, 0, text_schema);
    auto* segment_impl =
        dynamic_cast<ChunkedSegmentSealedImpl*>(manifest_segment.get());
    ASSERT_NE(segment_impl, nullptr);

    auto initial_snapshot = segment_impl->TestGetPublishedStateSnapshot();
    auto initial_column = initial_snapshot->runtime->fields.at(text_field);
    ASSERT_NE(initial_column, nullptr);
    EXPECT_FALSE(IsLazyColumnForTest(initial_column));
    ASSERT_NE(initial_snapshot->load_info, nullptr);
    EXPECT_TRUE(initial_snapshot->load_info->HasTextIndexCreated(text_field));

    milvus::OpContext op_ctx;
    segment_impl->Reopen(&op_ctx,
                         MakeV3ManifestLoadInfo(test_data_b, segment_id));

    auto reopened_snapshot = segment_impl->TestGetPublishedStateSnapshot();
    auto reopened_column = reopened_snapshot->runtime->fields.at(text_field);
    ASSERT_NE(reopened_column, nullptr);
    EXPECT_TRUE(IsLazyColumnForTest(reopened_column));
    int64_t offset = 0;
    EXPECT_FALSE(reopened_column->CellsLoaded(&offset, 1));

    auto data = reopened_column->DataOfChunk(nullptr, 0);
    EXPECT_NE(data.get(), nullptr);
    EXPECT_TRUE(reopened_column->CellsLoaded(&offset, 1));
}

TEST_P(TestLazyManifest,
       LazyManifestFieldPreCancellationAllowsFreshContextRetry) {
    LazyColumnGroupGuard lazy_group_guard(true);
    auto warmup_policies =
        milvus::cachinglayer::TieredStorageConfig::GetInstance()
            .warmup_policies();
    warmup_policies.scalarFieldCacheWarmupPolicy =
        CacheWarmupPolicy::CacheWarmupPolicy_Disable;
    CacheWarmupPolicyGuard warmup_guard(warmup_policies);

    const auto base_path = UniqueManifestTestPath(
        std::string("lazy_manifest_field_cancel_") +
        (GetParam() ? "varchar" : "int64"));
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    StorageV2TempDirGuard dir_guard(fs, base_path);
    constexpr int64_t kNumRows = 64;
    milvus::test::V3SegmentTestData test_data(
        schema_,
        1,
        kNumRows,
        128,
        TestLocalPath,
        base_path,
        LazyManifestTestGroupPattern(schema_));
    auto manifest_segment =
        CreateV3ManifestSegment(test_data, 3171 + (GetParam() ? 100 : 0));
    auto* segment_impl =
        dynamic_cast<ChunkedSegmentSealedImpl*>(manifest_segment.get());
    ASSERT_NE(segment_impl, nullptr);

    auto snapshot = segment_impl->TestGetPublishedStateSnapshot();
    ASSERT_NE(snapshot->runtime, nullptr);
    auto column = snapshot->runtime->fields.at(fields.at("int64"));
    ASSERT_NE(column, nullptr);
    EXPECT_TRUE(IsLazyColumnForTest(column));
    int64_t first_offset = 0;
    EXPECT_FALSE(column->CellsLoaded(&first_offset, 1));

    folly::CancellationSource source;
    source.requestCancellation();
    milvus::OpContext cancelled_ctx(source.getToken());
    try {
        (void)column->DataOfChunk(&cancelled_ctx, 0);
        FAIL() << "expected cancelled field materialization";
    } catch (const SegcoreError& err) {
        EXPECT_EQ(err.get_error_code(), ErrorCode::FollyCancel);
    }
    EXPECT_FALSE(column->CellsLoaded(&first_offset, 1));

    milvus::OpContext fresh_ctx;
    auto data = column->DataOfChunk(&fresh_ctx, 0);
    ASSERT_NE(data.get(), nullptr);
    EXPECT_TRUE(column->CellsLoaded(&first_offset, 1));
    EXPECT_GT(column->DataByteSize(), 0);
}

TEST_P(TestLazyManifest,
       LazyManifestFirstNonCancellationFailureIsRetryable) {
    LazyColumnGroupGuard lazy_group_guard(true);
    auto warmup_policies =
        milvus::cachinglayer::TieredStorageConfig::GetInstance()
            .warmup_policies();
    warmup_policies.scalarFieldCacheWarmupPolicy =
        CacheWarmupPolicy::CacheWarmupPolicy_Disable;
    CacheWarmupPolicyGuard warmup_guard(warmup_policies);

    const auto base_path = UniqueManifestTestPath(
        std::string("lazy_manifest_failure_retry_") +
        (GetParam() ? "varchar" : "int64"));
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    StorageV2TempDirGuard dir_guard(fs, base_path);
    constexpr int64_t kNumRows = 64;
    milvus::test::V3SegmentTestData test_data(
        schema_,
        1,
        kNumRows,
        128,
        TestLocalPath,
        base_path,
        LazyManifestTestGroupPattern(schema_));
    auto manifest_segment =
        CreateV3ManifestSegment(test_data, 3174 + (GetParam() ? 100 : 0));
    auto* segment_impl =
        dynamic_cast<ChunkedSegmentSealedImpl*>(manifest_segment.get());
    ASSERT_NE(segment_impl, nullptr);
    auto snapshot = segment_impl->TestGetPublishedStateSnapshot();
    ASSERT_NE(snapshot->runtime, nullptr);
    ASSERT_NE(snapshot->runtime->reader, nullptr);

    auto column_groups = snapshot->runtime->reader->get_column_groups();
    ASSERT_NE(column_groups, nullptr);
    auto field_id = fields.at("int64");
    auto storage_name = schema_->get_storage_column_name(field_id);
    auto group_it = std::find_if(
        column_groups->begin(), column_groups->end(), [&](const auto& group) {
            return group != nullptr &&
                   std::find(group->columns.begin(),
                             group->columns.end(),
                             storage_name) != group->columns.end();
        });
    ASSERT_NE(group_it, column_groups->end());
    auto target_group = *group_it;
    ASSERT_FALSE(target_group->files.empty());
    const auto original_path = target_group->files.front().path;
    const auto unavailable_path = original_path + ".lazy-retry-unavailable";
    ASSERT_TRUE(fs->Move(original_path, unavailable_path).ok());
    auto restore_file = folly::makeGuard([&]() {
        static_cast<void>(fs->Move(unavailable_path, original_path));
    });

    auto column =
        segment_impl->TestGetPublishedStateSnapshot()->runtime->fields.at(
            field_id);
    ASSERT_NE(column, nullptr);
    int64_t first_offset = 0;
    EXPECT_FALSE(column->CellsLoaded(&first_offset, 1));

    std::optional<ErrorCode> first_error;
    try {
        (void)column->DataOfChunk(nullptr, 0);
    } catch (const SegcoreError& err) {
        first_error = err.get_error_code();
    }

    proto::plan::GenericValue value;
    value.set_int64_val(17);
    auto term_expr = std::make_shared<expr::TermFilterExpr>(
        expr::ColumnInfo(field_id, DataType::INT64),
        std::vector<proto::plan::GenericValue>{value});
    auto filter_node =
        std::make_shared<plan::FilterBitsNode>("filter_1", term_expr);
    auto query_context =
        std::make_shared<exec::QueryContext>("lazy_manifest_failure_retry",
                                             manifest_segment.get(),
                                             kNumRows,
                                             MAX_TIMESTAMP);
    auto task = exec::Task::Create("lazy_manifest_failure_retry_task",
                                   plan::PlanFragment(filter_node),
                                   0,
                                   query_context);
    std::optional<ErrorCode> operator_error;
    std::exception_ptr unexpected_error;
    try {
        while (task->Next()) {
        }
    } catch (const SegcoreError& err) {
        operator_error = err.get_error_code();
    } catch (...) {
        unexpected_error = std::current_exception();
    }

    if (unexpected_error != nullptr) {
        std::rethrow_exception(unexpected_error);
    }

    ASSERT_TRUE(first_error.has_value());
    EXPECT_NE(*first_error, ErrorCode::FollyCancel);
    ASSERT_TRUE(operator_error.has_value());
    EXPECT_EQ(*operator_error, *first_error);
    EXPECT_NE(*operator_error, ErrorCode::UnexpectedError);
    EXPECT_FALSE(column->CellsLoaded(&first_offset, 1));

    ASSERT_TRUE(fs->Move(unavailable_path, original_path).ok());
    restore_file.dismiss();
    auto data = column->DataOfChunk(nullptr, 0);
    ASSERT_NE(data.get(), nullptr);
    EXPECT_GT(column->DataByteSize(), 0);
}

TEST_P(TestLazyManifest,
       LazyManifestKeepsWarmupDisabledAfterGlobalConfigChanges) {
    LazyColumnGroupGuard lazy_group_guard(true);
    StorageV2CellTargetGuard cell_target_guard(1);
    auto warmup_policies =
        milvus::cachinglayer::TieredStorageConfig::GetInstance()
            .warmup_policies();
    warmup_policies.scalarFieldCacheWarmupPolicy =
        CacheWarmupPolicy::CacheWarmupPolicy_Disable;
    CacheWarmupPolicyGuard disable_warmup_guard(warmup_policies);

    const auto base_path = UniqueManifestTestPath(
        std::string("lazy_manifest_fixed_warmup_") +
        (GetParam() ? "varchar" : "int64"));
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    StorageV2TempDirGuard dir_guard(fs, base_path);
    constexpr int64_t kRowsPerBatch = 64;
    milvus::test::V3SegmentTestData test_data(
        schema_,
        2,
        kRowsPerBatch,
        128,
        TestLocalPath,
        base_path,
        LazyManifestTestGroupPattern(schema_));
    auto manifest_segment =
        CreateV3ManifestSegment(test_data, 3154 + (GetParam() ? 100 : 0));
    auto* segment_impl =
        dynamic_cast<ChunkedSegmentSealedImpl*>(manifest_segment.get());
    ASSERT_NE(segment_impl, nullptr);
    auto column =
        segment_impl->TestGetPublishedStateSnapshot()->runtime->fields.at(
            fields.at("int64"));
    ASSERT_NE(column, nullptr);
    int64_t first_offset = 0;
    EXPECT_FALSE(column->CellsLoaded(&first_offset, 1));

    auto sync_policies = warmup_policies;
    sync_policies.scalarFieldCacheWarmupPolicy =
        CacheWarmupPolicy::CacheWarmupPolicy_Sync;
    CacheWarmupPolicyGuard sync_warmup_guard(sync_policies);

    ASSERT_GT(column->num_chunks(), 0);
    EXPECT_FALSE(column->CellsLoaded(&first_offset, 1));

    int64_t value = 0;
    column->BulkPrimitiveValueAt(nullptr, &value, &first_offset, 1);
    EXPECT_TRUE(column->CellsLoaded(&first_offset, 1));
}

TEST_P(TestLazyManifest, LazyManifestRetrieveSizeAndValuesAreCorrect) {
    LazyColumnGroupGuard lazy_group_guard(true);
    auto warmup_policies =
        milvus::cachinglayer::TieredStorageConfig::GetInstance()
            .warmup_policies();
    warmup_policies.scalarFieldCacheWarmupPolicy =
        CacheWarmupPolicy::CacheWarmupPolicy_Disable;
    CacheWarmupPolicyGuard warmup_guard(warmup_policies);

    const auto base_path = UniqueManifestTestPath(
        std::string("lazy_manifest_retrieve_size_") +
        (GetParam() ? "varchar" : "int64"));
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    StorageV2TempDirGuard dir_guard(fs, base_path);
    constexpr int64_t kNumRows = 64;
    constexpr Timestamp kCommitTs = 1000;
    milvus::test::V3SegmentTestData test_data(
        schema_,
        1,
        kNumRows,
        128,
        TestLocalPath,
        base_path,
        LazyManifestTestGroupPattern(schema_));
    int64_t eager_avg_size = 0;
    {
        LazyColumnGroupGuard eager_group_guard(false);
        auto eager_segment = CreateSegmentByLoadInfo(
            MakeV3ManifestLoadInfo(test_data, 3156 + (GetParam() ? 100 : 0)),
            true,
            kCommitTs);
        eager_avg_size = static_cast<SegmentInterface*>(eager_segment.get())
                             ->get_field_avg_size(fields.at("string1"));
    }
    ASSERT_GT(eager_avg_size, 0);

    auto manifest_segment = CreateSegmentByLoadInfo(
        MakeV3ManifestLoadInfo(test_data, 3155 + (GetParam() ? 100 : 0)),
        true,
        kCommitTs);
    auto* segment_impl =
        dynamic_cast<ChunkedSegmentSealedImpl*>(manifest_segment.get());
    ASSERT_NE(segment_impl, nullptr);

    auto string_column =
        segment_impl->TestGetPublishedStateSnapshot()->runtime->fields.at(
            fields.at("string1"));
    ASSERT_NE(string_column, nullptr);
    auto proxy = std::dynamic_pointer_cast<ProxyChunkColumn>(string_column);
    ASSERT_NE(proxy, nullptr);
    ASSERT_TRUE(proxy->IsLazy());
    ASSERT_FALSE(proxy->IsMaterialized());

    auto* segment_interface =
        static_cast<SegmentInterface*>(manifest_segment.get());

    auto plan = std::make_unique<query::RetrievePlan>(schema_);
    plan->plan_node_ = std::make_unique<query::RetrievePlanNode>();
    plan->plan_node_->plannodes_ = milvus::test::CreateRetrievePlanByExpr(
        std::make_shared<expr::AlwaysTrueExpr>());
    plan->field_ids_ = {fields.at("string1")};

    segment_interface->set_field_avg_size(
        fields.at("string1"), kNumRows, kNumRows);
    try {
        auto unexpected = manifest_segment->Retrieve(
            nullptr, plan.get(), MAX_TIMESTAMP, 1, false);
        (void)unexpected;
        FAIL() << "expected Retrieve size guard to reject the result";
    } catch (const SegcoreError& err) {
        EXPECT_EQ(err.get_error_code(), RetrieveError);
        EXPECT_NE(std::string(err.what()).find("query results exceed"),
                  std::string::npos);
    }
    EXPECT_GT(string_column->DataByteSize(), 0);
    EXPECT_EQ(segment_interface->get_field_avg_size(fields.at("string1")),
              eager_avg_size);

    auto results = manifest_segment->Retrieve(
        nullptr, plan.get(), MAX_TIMESTAMP, DEFAULT_MAX_OUTPUT_SIZE, false);
    EXPECT_EQ(manifest_segment->get_max_timestamp(), kCommitTs);
    ASSERT_EQ(results->fields_data_size(), 1);
    const auto& values = results->fields_data(0).scalars().string_data();
    ASSERT_EQ(values.data_size(), kNumRows);
    auto expected_data = DataGen(schema_, kNumRows, 42);
    const auto expected =
        expected_data.get_col<std::string>(fields.at("string1"));
    const auto expected_valid = expected_data.get_col_valid(fields.at("string1"));
    const auto& actual_valid = GetFieldDataRowValidData(results->fields_data(0));
    ASSERT_EQ(actual_valid.size(), kNumRows);
    for (int64_t i = 0; i < kNumRows; ++i) {
        EXPECT_EQ(actual_valid[i], expected_valid[i]) << "row " << i;
        if (expected_valid[i]) {
            EXPECT_EQ(values.data(i), expected[i]) << "row " << i;
        }
    }
}

TEST_P(TestLazyManifest, LazyManifestSystemFieldTaskUsesRegularLoad) {
    LazyColumnGroupGuard lazy_group_guard(true);
    auto warmup_policies =
        milvus::cachinglayer::TieredStorageConfig::GetInstance()
            .warmup_policies();
    warmup_policies.scalarFieldCacheWarmupPolicy =
        CacheWarmupPolicy::CacheWarmupPolicy_Disable;
    CacheWarmupPolicyGuard warmup_guard(warmup_policies);

    auto schema = std::make_shared<Schema>();
    schema->AddField(
        FieldName("RowID"), RowFieldID, DataType::INT64, false, std::nullopt);
    auto pk = schema->AddDebugField(
        "pk", GetParam() ? DataType::VARCHAR : DataType::INT64, false);
    schema->AddField(FieldName("Timestamp"),
                     TimestampFieldID,
                     DataType::INT64,
                     false,
                     std::nullopt);
    schema->set_primary_field_id(pk);

    for (bool separate_timestamp : {false, true}) {
        SCOPED_TRACE(separate_timestamp);
        const auto base_path = UniqueManifestTestPath(
            std::string("lazy_manifest_system_fields_") +
            (GetParam() ? "varchar" : "int64") +
            (separate_timestamp ? "_split_ts" : "_shared_ts"));
        auto fs = milvus::segcore::GetDefaultArrowFileSystem();
        StorageV2TempDirGuard dir_guard(fs, base_path);
        constexpr int64_t kNumRows = 64;
        milvus::test::V3SegmentTestData test_data(
            schema,
            1,
            kNumRows,
            1,
            TestLocalPath,
            base_path,
            separate_timestamp ? fmt::format("{}|{},{}",
                                             RowFieldID.get(),
                                             pk.get(),
                                             TimestampFieldID.get())
                               : "");
        ASSERT_EQ(test_data.NumColumnGroups(), separate_timestamp ? 2 : 1);

        auto manifest_segment = CreateV3ManifestSegment(
            test_data,
            3161 + (GetParam() ? 100 : 0) + (separate_timestamp ? 1000 : 0),
            0,
            schema);
        auto* segment_impl =
            dynamic_cast<ChunkedSegmentSealedImpl*>(manifest_segment.get());
        ASSERT_NE(segment_impl, nullptr);
        auto snapshot = segment_impl->TestGetPublishedStateSnapshot();
        ASSERT_NE(snapshot->runtime, nullptr);
        ASSERT_TRUE(snapshot->system_field_ready);
        ASSERT_NE(snapshot->runtime->pk_index_slot, nullptr);
        ASSERT_NE(snapshot->runtime->timestamp_index_slot, nullptr);
        EXPECT_NE(snapshot->runtime->timestamps, nullptr);
        EXPECT_NE(snapshot->runtime->timestamp_index, nullptr);
        EXPECT_FALSE(snapshot->runtime->pk_index_slot->IsCached(0));
        EXPECT_TRUE(snapshot->runtime->timestamp_index_slot->IsCached(0));

        auto row_id_column = snapshot->runtime->fields.at(RowFieldID);
        auto pk_column = snapshot->runtime->fields.at(pk);
        auto timestamp_column = snapshot->runtime->fields.at(TimestampFieldID);
        for (const auto& [field_id, column] : std::vector<
                 std::pair<FieldId, std::shared_ptr<ChunkedColumnInterface>>>{
                 {RowFieldID, row_id_column},
                 {pk, pk_column},
                 {TimestampFieldID, timestamp_column}}) {
            ASSERT_NE(column, nullptr);
            EXPECT_EQ(column->IsInMultiFieldColumnGroup(),
                      !separate_timestamp || field_id != TimestampFieldID);
            EXPECT_FALSE(IsLazyColumnForTest(column));
        }

        int64_t offsets[] = {0, kNumRows - 1};
        auto timestamps = manifest_segment->bulk_subscript(
            nullptr, TimestampFieldID, offsets, 2);
        ASSERT_EQ(timestamps->scalars().long_data().data_size(), 2);
        EXPECT_EQ(timestamps->scalars().long_data().data(0), 0);
        EXPECT_EQ(timestamps->scalars().long_data().data(1), kNumRows - 1);

        BitsetType timestamp_mask(kNumRows);
        BitsetTypeView timestamp_mask_view(timestamp_mask);
        static_cast<SegmentInternalInterface*>(manifest_segment.get())
            ->mask_with_timestamps(timestamp_mask_view, 31, 0);
        for (int64_t i = 0; i < kNumRows; ++i) {
            EXPECT_EQ(timestamp_mask[i], i >= kNumRows / 2);
        }

        // PK shares the regular-load Task; delete application must still work.
        PkType missing_pk = GetParam() ? PkType(std::string("__missing__"))
                                       : PkType(int64_t{-1});
        EXPECT_FALSE(segment_impl->Contain(missing_pk));
        EXPECT_TRUE(snapshot->runtime->pk_index_slot->IsCached(0));
        auto pk_values =
            manifest_segment->bulk_subscript(nullptr, pk, offsets, 1);
        IdArray delete_ids;
        if (GetParam()) {
            delete_ids.mutable_str_id()->add_data(
                pk_values->scalars().string_data().data(0));
        } else {
            delete_ids.mutable_int_id()->add_data(
                pk_values->scalars().long_data().data(0));
        }
        Timestamp delete_ts = MAX_TIMESTAMP;
        ASSERT_TRUE(manifest_segment->Delete(1, &delete_ids, &delete_ts).ok());
        auto* segment_internal =
            dynamic_cast<SegmentInternalInterface*>(manifest_segment.get());
        ASSERT_NE(segment_internal, nullptr);
        BitsetType delete_mask(kNumRows);
        BitsetTypeView delete_mask_view(delete_mask);
        segment_internal->mask_with_delete(
            delete_mask_view, kNumRows, MAX_TIMESTAMP);
        EXPECT_EQ(delete_mask.count(), 1);
        EXPECT_TRUE(delete_mask[0]);
        EXPECT_GT(pk_column->DataByteSize(), 0);
        EXPECT_GT(timestamp_column->DataByteSize(), 0);
    }
}

TEST_P(TestLazyManifest, LazyManifestPkTasksUseRegularLoad) {
    LazyColumnGroupGuard lazy_group_guard(true);
    auto warmup_policies =
        milvus::cachinglayer::TieredStorageConfig::GetInstance()
            .warmup_policies();
    warmup_policies.scalarFieldCacheWarmupPolicy =
        CacheWarmupPolicy::CacheWarmupPolicy_Disable;
    warmup_policies.scalarIndexCacheWarmupPolicy =
        CacheWarmupPolicy::CacheWarmupPolicy_Disable;
    CacheWarmupPolicyGuard warmup_guard(warmup_policies);

    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField(
        "pk", GetParam() ? DataType::VARCHAR : DataType::INT64, false);
    auto value = schema->AddDebugField("value", DataType::INT64, false);
    schema->AddField(FieldName("Timestamp"),
                     TimestampFieldID,
                     DataType::INT64,
                     false,
                     std::nullopt);
    schema->set_primary_field_id(pk);
    constexpr int64_t kNumRows = 64;

    for (bool share_pk_task : {false, true}) {
        SCOPED_TRACE(share_pk_task);
        const auto base_path = UniqueManifestTestPath(
            std::string("lazy_manifest_pk_regular_") +
            (GetParam() ? "varchar" : "int64") +
            (share_pk_task ? "_shared" : "_single"));
        auto fs = milvus::segcore::GetDefaultArrowFileSystem();
        StorageV2TempDirGuard dir_guard_a(fs, base_path + "_a");
        StorageV2TempDirGuard dir_guard_b(fs, base_path + "_b");
        auto pattern =
            share_pk_task
                ? fmt::format(
                      "{}|{},{}", pk.get(), value.get(), TimestampFieldID.get())
                : fmt::format("{},{},{}",
                              pk.get(),
                              value.get(),
                              TimestampFieldID.get());
        milvus::test::V3SegmentTestData test_data_a(
            schema, 1, kNumRows, 1, TestLocalPath, base_path + "_a", pattern);
        milvus::test::V3SegmentTestData test_data_b(
            schema, 1, kNumRows, 1, TestLocalPath, base_path + "_b", pattern);
        ASSERT_EQ(test_data_a.NumColumnGroups(), share_pk_task ? 2 : 3);
        const auto segment_id =
            3180 + (GetParam() ? 100 : 0) + (share_pk_task ? 1000 : 0);
        auto segment =
            CreateV3ManifestSegment(test_data_a, segment_id, 0, schema);
        auto* impl = dynamic_cast<ChunkedSegmentSealedImpl*>(segment.get());
        ASSERT_NE(impl, nullptr);
        auto old_pk_column =
            impl->TestGetPublishedStateSnapshot()->runtime->fields.at(pk);

        for (bool reopen : {false, true}) {
            SCOPED_TRACE(reopen);
            if (reopen) {
                milvus::OpContext ctx;
                impl->Reopen(&ctx,
                             MakeV3ManifestLoadInfo(test_data_b, segment_id));
            }
            auto snapshot = impl->TestGetPublishedStateSnapshot();
            auto pk_column = snapshot->runtime->fields.at(pk);
            auto value_column = snapshot->runtime->fields.at(value);
            EXPECT_FALSE(IsLazyColumnForTest(pk_column));
            EXPECT_EQ(IsLazyColumnForTest(value_column), !share_pk_task);
            EXPECT_EQ(pk_column->IsInMultiFieldColumnGroup(), share_pk_task);
            ASSERT_NE(snapshot->runtime->pk_index_slot, nullptr);
            // Excluding PK from Lazy Manifest does not force its index to build.
            EXPECT_FALSE(snapshot->runtime->pk_index_slot->IsCached(0));
            if (reopen) {
                EXPECT_NE(pk_column, old_pk_column);
            }
        }

        int64_t offset = 0;
        auto pk_values = segment->bulk_subscript(nullptr, pk, &offset, 1);
        IdArray delete_ids;
        if (GetParam()) {
            delete_ids.mutable_str_id()->add_data(
                pk_values->scalars().string_data().data(0));
        } else {
            delete_ids.mutable_int_id()->add_data(
                pk_values->scalars().long_data().data(0));
        }
        Timestamp delete_ts = MAX_TIMESTAMP;
        ASSERT_TRUE(segment->Delete(1, &delete_ids, &delete_ts).ok());
        auto* internal = dynamic_cast<SegmentInternalInterface*>(segment.get());
        ASSERT_NE(internal, nullptr);
        BitsetType delete_mask(kNumRows);
        BitsetTypeView delete_mask_view(delete_mask);
        internal->mask_with_delete(delete_mask_view, kNumRows, MAX_TIMESTAMP);
        EXPECT_EQ(delete_mask.count(), 1);
        EXPECT_TRUE(delete_mask[0]);
    }
}

TEST_P(TestLazyManifest, LazyManifestReopenRebindsGeneration) {
    LazyColumnGroupGuard lazy_group_guard(true);
    auto warmup_policies =
        milvus::cachinglayer::TieredStorageConfig::GetInstance()
            .warmup_policies();
    warmup_policies.scalarFieldCacheWarmupPolicy =
        CacheWarmupPolicy::CacheWarmupPolicy_Disable;
    CacheWarmupPolicyGuard warmup_guard(warmup_policies);

    const auto suffix = GetParam() ? "varchar" : "int64";
    const auto base_path_a = UniqueManifestTestPath(
        std::string("lazy_manifest_reopen_a_") + suffix);
    const auto base_path_b = UniqueManifestTestPath(
        std::string("lazy_manifest_reopen_b_") + suffix);
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    StorageV2TempDirGuard dir_guard_a(fs, base_path_a);
    StorageV2TempDirGuard dir_guard_b(fs, base_path_b);
    milvus::test::V3SegmentTestData test_data_a(
        schema_,
        1,
        64,
        128,
        TestLocalPath,
        base_path_a,
        LazyManifestTestGroupPattern(schema_));
    milvus::test::V3SegmentTestData test_data_b(
        schema_,
        2,
        32,
        128,
        TestLocalPath,
        base_path_b,
        LazyManifestTestGroupPattern(schema_));

    auto manifest_segment =
        CreateV3ManifestSegment(test_data_a, 3164 + (GetParam() ? 100 : 0));
    auto* segment_impl =
        dynamic_cast<ChunkedSegmentSealedImpl*>(manifest_segment.get());
    ASSERT_NE(segment_impl, nullptr);
    auto old_snapshot = segment_impl->TestGetPublishedStateSnapshot();
    auto old_column = old_snapshot->runtime->fields.at(fields.at("int64"));
    ASSERT_NE(old_column, nullptr);
    ASSERT_NE(old_snapshot->runtime->pk_index_slot, nullptr);
    ASSERT_NE(old_snapshot->runtime->timestamp_index_slot, nullptr);

    const auto string_field = fields.at("string1");
    static_cast<SegmentInterface*>(manifest_segment.get())
        ->set_field_avg_size(string_field, 64, 64 * 123);
    ASSERT_GT(segment_impl->TestGetPublishedStateSnapshot()
                  ->runtime->variable_fields_avg_size.count(string_field),
              0);
    auto mmap_proto = schema_->ToProto();
    for (auto& field : *mmap_proto.mutable_fields()) {
        if (field.fieldid() == TimestampFieldID.get()) {
            field.set_name("Timestamp");
        }
        if (field.fieldid() == string_field.get()) {
            auto* mmap = field.add_type_params();
            mmap->set_key(MMAP_ENABLED_KEY);
            mmap->set_value("true");
        }
    }
    auto mmap_schema = Schema::ParseFrom(mmap_proto);
    milvus::OpContext op_ctx;
    segment_impl->Reopen(
        &op_ctx,
        MakeV3ManifestLoadInfo(test_data_b, 3164 + (GetParam() ? 100 : 0)),
        mmap_schema);
    auto new_snapshot = segment_impl->TestGetPublishedStateSnapshot();
    ASSERT_NE(new_snapshot, old_snapshot);
    auto new_column = new_snapshot->runtime->fields.at(fields.at("int64"));
    ASSERT_NE(new_column, nullptr);
    EXPECT_NE(new_column, old_column);
    EXPECT_EQ(new_snapshot->runtime->mmap_field_ids.count(string_field), 1);
    EXPECT_EQ(new_snapshot->runtime->variable_fields_avg_size.count(string_field),
              0);
    auto mmap_column = std::dynamic_pointer_cast<ProxyChunkColumn>(
        new_snapshot->runtime->fields.at(string_field));
    ASSERT_NE(mmap_column, nullptr);
    ASSERT_TRUE(mmap_column->IsLazy());
    ASSERT_FALSE(mmap_column->IsMaterialized());
    auto* segment_interface =
        static_cast<SegmentInterface*>(manifest_segment.get());
    EXPECT_EQ(segment_interface->get_field_avg_size(string_field), 0);
    EXPECT_FALSE(mmap_column->IsMaterialized());
    ASSERT_NE(new_snapshot->runtime->pk_index_slot, nullptr);
    ASSERT_NE(new_snapshot->runtime->timestamp_index_slot, nullptr);
    EXPECT_NE(new_snapshot->runtime->pk_index_slot,
              old_snapshot->runtime->pk_index_slot);
    EXPECT_NE(new_snapshot->runtime->timestamp_index_slot,
              old_snapshot->runtime->timestamp_index_slot);

    auto read_value = [](const std::shared_ptr<ChunkedColumnInterface>& column,
                         int64_t offset) {
        int64_t value = -1;
        column->BulkPrimitiveValueAt(nullptr, &value, &offset, 1);
        return value;
    };
    EXPECT_EQ(read_value(old_column, 40), 40);

    EXPECT_EQ(read_value(new_column, 40), 8);
    EXPECT_TRUE(mmap_column->IsMaterialized());
    EXPECT_EQ(segment_interface->get_field_avg_size(string_field), 0);
    segment_impl->DropFieldData(fields.at("string1"));
    auto dropped_column_snapshot =
        segment_impl->TestGetPublishedStateSnapshot();
    EXPECT_EQ(
        dropped_column_snapshot->runtime->fields.count(fields.at("string1")),
        0);

    EXPECT_EQ(read_value(new_column, 40), 8);
    EXPECT_EQ(read_value(old_column, 40), 40);
    EXPECT_EQ(old_snapshot->runtime->fields.at(fields.at("int64")), old_column);
}

TEST_P(TestLazyManifest, LazyManifestSchemaOnlyDropKeepsSurvivorTask) {
    LazyColumnGroupGuard lazy_group_guard(true);
    auto warmup_policies =
        milvus::cachinglayer::TieredStorageConfig::GetInstance()
            .warmup_policies();
    warmup_policies.scalarFieldCacheWarmupPolicy =
        CacheWarmupPolicy::CacheWarmupPolicy_Disable;
    CacheWarmupPolicyGuard warmup_guard(warmup_policies);

    auto old_schema = std::make_shared<Schema>(*schema_);
    old_schema->set_schema_version(1);

    const auto suffix = GetParam() ? "varchar" : "int64";
    const auto base_path = UniqueManifestTestPath(
        std::string("lazy_manifest_schema_drop_") + suffix);
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    StorageV2TempDirGuard dir_guard(fs, base_path);

    constexpr int64_t kNumRows = 64;
    constexpr int64_t kOffset = 40;
    const auto segment_id = 3167 + (GetParam() ? 100 : 0);
    milvus::test::V3SegmentTestData test_data(
        old_schema,
        1,
        kNumRows,
        128,
        TestLocalPath,
        base_path,
        LazyManifestTestGroupPattern(old_schema));

    auto manifest_segment =
        CreateV3ManifestSegment(test_data, segment_id, 0, old_schema);
    auto* segment_impl =
        dynamic_cast<ChunkedSegmentSealedImpl*>(manifest_segment.get());
    ASSERT_NE(segment_impl, nullptr);

    const auto survivor_field = fields.at("int64");
    const auto dropped_field = fields.at("string1");
    const auto peer_field = fields.at("string2");

    auto old_snapshot = segment_impl->TestGetPublishedStateSnapshot();
    ASSERT_NE(old_snapshot, nullptr);
    ASSERT_NE(old_snapshot->runtime, nullptr);
    auto old_survivor = old_snapshot->runtime->fields.at(survivor_field);
    auto old_dropped = old_snapshot->runtime->fields.at(dropped_field);
    auto old_peer = old_snapshot->runtime->fields.at(peer_field);
    ASSERT_NE(old_survivor, nullptr);
    ASSERT_NE(old_dropped, nullptr);
    ASSERT_NE(old_peer, nullptr);
    EXPECT_TRUE(old_survivor->IsInMultiFieldColumnGroup());

    auto schema_proto = old_schema->ToProto();
    bool removed = false;
    for (int i = 0; i < schema_proto.fields_size(); ++i) {
        if (schema_proto.fields(i).fieldid() == dropped_field.get()) {
            schema_proto.mutable_fields()->DeleteSubrange(i, 1);
            removed = true;
            break;
        }
    }
    ASSERT_TRUE(removed);
    for (auto& field : *schema_proto.mutable_fields()) {
        if (field.fieldid() == TimestampFieldID.get()) {
            // GenChunkedSegmentTestSchema uses the debug-only name "ts";
            // Schema::ParseFrom validates production system-field names.
            field.set_name("Timestamp");
        }
    }
    auto new_schema = Schema::ParseFrom(schema_proto);
    new_schema->set_schema_version(2);

    segment_impl->Reopen(new_schema);

    auto new_snapshot = segment_impl->TestGetPublishedStateSnapshot();
    ASSERT_NE(new_snapshot, old_snapshot);
    ASSERT_NE(new_snapshot->runtime, nullptr);
    EXPECT_TRUE(old_snapshot->schema->has_field(dropped_field));
    EXPECT_FALSE(new_snapshot->schema->has_field(dropped_field));
    EXPECT_EQ(new_snapshot->runtime->fields.count(dropped_field), 0);

    auto new_survivor = new_snapshot->runtime->fields.at(survivor_field);
    auto new_peer = new_snapshot->runtime->fields.at(peer_field);
    ASSERT_NE(new_survivor, nullptr);
    ASSERT_NE(new_peer, nullptr);
    EXPECT_EQ(new_survivor, old_survivor);
    EXPECT_EQ(new_peer, old_peer);
    EXPECT_TRUE(new_survivor->IsInMultiFieldColumnGroup());

    // A schema-only drop removes the dropped facade from the new runtime but
    // keeps the unchanged physical Task for surviving fields.
    auto read_int64 = [](const std::shared_ptr<ChunkedColumnInterface>& column,
                         int64_t offset) {
        int64_t value = -1;
        column->BulkPrimitiveValueAt(nullptr, &value, &offset, 1);
        return value;
    };

    EXPECT_EQ(read_int64(new_survivor, kOffset), kOffset);
    auto old_dropped_data = old_dropped->DataOfChunk(nullptr, 0);
    EXPECT_NE(old_dropped_data.get(), nullptr);

    auto current_dropped_snapshot =
        segment_impl->TestGetPublishedStateSnapshot();
    EXPECT_EQ(current_dropped_snapshot->runtime->fields.count(dropped_field),
              0);
}

TEST_P(TestLazyManifest, LazyManifestDisabledKeepsEagerColumnGroup) {
    LazyColumnGroupGuard lazy_group_guard(false);
    auto warmup_policies =
        milvus::cachinglayer::TieredStorageConfig::GetInstance()
            .warmup_policies();
    warmup_policies.scalarFieldCacheWarmupPolicy =
        CacheWarmupPolicy::CacheWarmupPolicy_Disable;
    CacheWarmupPolicyGuard warmup_guard(warmup_policies);

    const auto base_path = UniqueManifestTestPath(
        std::string("lazy_manifest_disabled_") +
        (GetParam() ? "varchar" : "int64"));
    auto fs = milvus::segcore::GetDefaultArrowFileSystem();
    StorageV2TempDirGuard dir_guard(fs, base_path);
    milvus::test::V3SegmentTestData test_data(
        schema_,
        1,
        32,
        128,
        TestLocalPath,
        base_path,
        LazyManifestTestGroupPattern(schema_));
    auto manifest_segment =
        CreateV3ManifestSegment(test_data, 3170 + (GetParam() ? 100 : 0));
    auto* segment_impl =
        dynamic_cast<ChunkedSegmentSealedImpl*>(manifest_segment.get());
    ASSERT_NE(segment_impl, nullptr);

    auto int64_column =
        segment_impl->TestGetPublishedStateSnapshot()->runtime->fields.at(
            fields.at("int64"));
    auto pk_column =
        segment_impl->TestGetPublishedStateSnapshot()->runtime->fields.at(
            fields.at("pk"));
    ASSERT_NE(int64_column, nullptr);
    ASSERT_NE(pk_column, nullptr);
    EXPECT_TRUE(int64_column->IsInMultiFieldColumnGroup());
    // The test layout keeps PK in its own physical group.
    EXPECT_FALSE(pk_column->IsInMultiFieldColumnGroup());
    EXPECT_FALSE(IsLazyColumnForTest(int64_column));
    EXPECT_FALSE(IsLazyColumnForTest(pk_column));

    auto& config = SegcoreConfig::default_config();
    config.set_lazy_column_group_enabled(true);
    auto lazy_segment =
        CreateV3ManifestSegment(test_data, 3180 + (GetParam() ? 100 : 0));
    auto* lazy_impl =
        dynamic_cast<ChunkedSegmentSealedImpl*>(lazy_segment.get());
    ASSERT_NE(lazy_impl, nullptr);
    auto lazy_column = std::dynamic_pointer_cast<ProxyChunkColumn>(
        lazy_impl->TestGetPublishedStateSnapshot()->runtime->fields.at(
            fields.at("int64")));
    ASSERT_NE(lazy_column, nullptr);
    EXPECT_TRUE(lazy_column->IsLazy());
    EXPECT_FALSE(lazy_column->IsMaterialized());
    EXPECT_FALSE(IsLazyColumnForTest(int64_column));

    config.set_lazy_column_group_enabled(false);
    auto next_segment =
        CreateV3ManifestSegment(test_data, 3181 + (GetParam() ? 100 : 0));
    auto* next_impl =
        dynamic_cast<ChunkedSegmentSealedImpl*>(next_segment.get());
    ASSERT_NE(next_impl, nullptr);
    EXPECT_FALSE(IsLazyColumnForTest(
        next_impl->TestGetPublishedStateSnapshot()->runtime->fields.at(
            fields.at("int64"))));
    EXPECT_FALSE(lazy_column->IsMaterialized());
    EXPECT_TRUE(lazy_column->IsLazy());
}

TEST(LazyManifest, ExternalMappedColumnsStayColdUntilRead) {
    for (bool async_enabled : {false, true}) {
        SCOPED_TRACE(async_enabled);
        const auto previous_async =
            storagev2translator::StorageV2AsyncLoadEnabled();
        auto restore_async = folly::makeGuard([previous_async] {
            storagev2translator::SetStorageV2AsyncLoadEnabled(previous_async);
        });
        storagev2translator::SetStorageV2AsyncLoadEnabled(async_enabled);
        LazyColumnGroupGuard lazy_group_guard(true);
        auto policies =
            cachinglayer::TieredStorageConfig::GetInstance().warmup_policies();
        policies.scalarFieldCacheWarmupPolicy =
            CacheWarmupPolicy::CacheWarmupPolicy_Disable;
        CacheWarmupPolicyGuard warmup_guard(policies);

        const FieldId number(100), text(101), pk(102);
        auto source_schema = std::make_shared<Schema>();
        source_schema->AddField(FieldMeta(FieldName("number"),
                                          number,
                                          DataType::INT64,
                                          true,
                                          std::nullopt,
                                          "source_number"));
        source_schema->AddField(FieldMeta(FieldName("text"),
                                          text,
                                          DataType::VARCHAR,
                                          65535,
                                          true,
                                          std::nullopt,
                                          "source_text"));
        source_schema->set_external_source("s3://test-bucket/data");
        source_schema->set_external_spec(R"({"format":"parquet"})");
        const auto path = UniqueManifestTestPath("lazy_manifest_external");
        auto fs = GetDefaultArrowFileSystem();
        StorageV2TempDirGuard dir_guard(fs, path);
        constexpr int64_t rows = 8;
        milvus::test::V3SegmentTestData data(source_schema,
                                             1,
                                             rows,
                                             1,
                                             TestLocalPath,
                                             path,
                                             "source_number|source_text");
        ASSERT_EQ(data.NumColumnGroups(), 1);

        auto schema = std::make_shared<Schema>(*source_schema);
        schema->AddField(FieldMeta(
            FieldName("virtual_pk"), pk, DataType::INT64, false, std::nullopt));
        schema->AddField(FieldName("RowID"),
                         RowFieldID,
                         DataType::INT64,
                         false,
                         std::nullopt);
        schema->AddField(FieldName("Timestamp"),
                         TimestampFieldID,
                         DataType::INT64,
                         false,
                         std::nullopt);
        schema->set_primary_field_id(pk);

        // A cold external load must not need even the Parquet footer.
        const auto parquet_path =
            data.GetColumnGroups()->at(0)->files.front().path;
        const auto hidden_path = parquet_path + ".hidden";
        ASSERT_TRUE(fs->Move(parquet_path, hidden_path).ok());
        auto restore_file = folly::makeGuard(
            [&] { static_cast<void>(fs->Move(hidden_path, parquet_path)); });
        constexpr int64_t segment_id = 3190;
        auto segment = CreateSealedSegment(schema, nullptr, segment_id);
        proto::segcore::SegmentLoadInfo info;
        info.set_segmentid(segment_id);
        info.set_collectionid(1);
        info.set_partitionid(1);
        info.set_num_of_rows(rows);
        info.set_storageversion(STORAGE_V3);
        info.set_manifest_path(data.ManifestPathJson());
        segment->SetLoadInfo(info);
        milvus::tracer::TraceContext trace_ctx;
        ASSERT_NO_THROW(segment->Load(trace_ctx, nullptr));
        auto* sealed = dynamic_cast<ChunkedSegmentSealedImpl*>(segment.get());
        ASSERT_NE(sealed, nullptr);
        auto state = sealed->TestGetPublishedStateSnapshot();
        auto number_column = std::dynamic_pointer_cast<ProxyChunkColumn>(
            state->runtime->fields.at(number));
        auto text_column = std::dynamic_pointer_cast<ProxyChunkColumn>(
            state->runtime->fields.at(text));
        ASSERT_NE(number_column, nullptr);
        ASSERT_NE(text_column, nullptr);
        ASSERT_TRUE(number_column->IsLazy());
        ASSERT_TRUE(text_column->IsLazy());
        EXPECT_FALSE(number_column->IsMaterialized());
        EXPECT_FALSE(text_column->IsMaterialized());
        EXPECT_NE(state->runtime->virtual_pk2offset, nullptr);
        EXPECT_EQ(state->runtime->pk_index_slot, nullptr);
        EXPECT_EQ(state->runtime->timestamp_index_slot, nullptr);

        int64_t offsets[] = {6, 0, 2, 6};
        auto pks = segment->bulk_subscript(nullptr, pk, offsets, 4);
        ASSERT_EQ(pks->scalars().long_data().data_size(), 4);
        Timestamp timestamps[4];
        static_cast<SegmentInternalInterface*>(segment.get())
            ->bulk_subscript(
                nullptr, SystemFieldType::Timestamp, offsets, 4, timestamps);
        for (int i = 0; i < 4; ++i) {
            EXPECT_EQ(pks->scalars().long_data().data(i),
                      (segment_id << 32) | offsets[i]);
            EXPECT_EQ(timestamps[i], 0);
        }
        EXPECT_FALSE(number_column->IsMaterialized());
        EXPECT_FALSE(text_column->IsMaterialized());

        ASSERT_TRUE(fs->Move(hidden_path, parquet_path).ok());
        restore_file.dismiss();
        int64_t values[4];
        number_column->BulkPrimitiveValueAt(
            nullptr, values, offsets, 4, /*small_int_raw_type=*/false);
        for (int i = 0; i < 4; ++i) {
            EXPECT_EQ(values[i], offsets[i]);
        }
        EXPECT_TRUE(number_column->IsMaterialized());
        // Separate Tasks from the same physical CG materialize independently.
        EXPECT_FALSE(text_column->IsMaterialized());
        auto expected =
            DataGen(source_schema, rows, 42).get_col<std::string>(text);
        size_t seen = 0;
        text_column->BulkRawStringAt(
            nullptr,
            [&](std::string_view value, size_t i, bool valid) {
                ASSERT_LT(i, 4);
                EXPECT_TRUE(valid);
                EXPECT_EQ(value, expected[offsets[i]]);
                ++seen;
            },
            offsets,
            4);
        EXPECT_EQ(seen, 4);
        EXPECT_TRUE(text_column->IsMaterialized());
    }
}

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
