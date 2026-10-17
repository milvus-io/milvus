// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#ifdef BUILD_DISK_ANN

#include <arrow/api.h>
#include <parquet/properties.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "common/Schema.h"
#include "index/contracts/query/IVectorReader.h"
#include "indexbuilder/VectorDiskBuildMaterializer.h"
#include "indexbuilder/test_utils/SourceBuildTestUtils.h"
#include "milvus-storage/common/constants.h"
#include "milvus-storage/common/layout.h"
#include "milvus-storage/manifest.h"
#include "milvus-storage/packed/writer.h"
#include "storage/loon_ffi/util.h"

namespace milvus::indexbuilder::test {
namespace {

constexpr int64_t kDim = 4;
constexpr int64_t kPrefix = 3;

class VectorDiskBuildSessionTest : public SourceBuildTest,
                                   public ::testing::WithParamInterface<bool> {
 protected:
    PreparedBuild
    PrepareColumnar(int64_t expected_rows, bool nullable = true) {
        auto prepared =
            Prepare(DataType::VECTOR_FLOAT,
                    DataType::NONE,
                    nullable,
                    knowhere::IndexEnum::INDEX_DISKANN,
                    expected_rows,
                    kDim,
                    {{knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
                     {index::DISK_ANN_MAX_DEGREE, 24},
                     {index::DISK_ANN_SEARCH_LIST_SIZE, 56},
                     {index::DISK_ANN_PQ_CODE_BUDGET, 0.001},
                     {index::DISK_ANN_BUILD_DRAM_BUDGET, 2},
                     {index::DISK_ANN_BUILD_THREAD_NUM, 2},
                     {index::DISK_ANN_LOAD_THREAD_NUM, 2},
                     {index::DISK_ANN_SEARCH_CACHE_BUDGET, 0.0}});
        prepared.request.source = StorageV2BuildSource{};
        return prepared;
    }

    void
    SetSource(PreparedBuild& prepared, int64_t rows, bool has_field) {
        if (!GetParam()) {
            prepared.request.source = has_field
                                          ? StorageV2BuildSource{{{DataPath()}}}
                                          : StorageV2BuildSource{};
            return;
        }
        storage::StorageConfig config;
        config.storage_type = "local";
        config.root_path = directory_->Path();
        prepared.file_manager_context.set_loon_ffi_properties(
            MakeInternalPropertiesFromStorageConfig(ToCStorageConfig(config)));
        const auto base = directory_->Path() + "/segment";
        const auto fs = prepared.file_manager_context.fs;
        ASSERT_TRUE(
            fs->CreateDir(milvus_storage::get_manifest_path(base)).ok());
        milvus_storage::api::ColumnGroups groups;
        if (has_field) {
            auto group = std::make_shared<milvus_storage::api::ColumnGroup>();
            group->columns = {"100"};
            group->format = "parquet";
            group->files.push_back(
                {.path = DataPath(), .start_index = 0, .end_index = rows});
            groups.push_back(std::move(group));
        }
        const auto status = milvus_storage::api::Manifest::WriteTo(
            fs,
            milvus_storage::get_manifest_filepath(base, 1),
            milvus_storage::api::Manifest(std::move(groups)));
        ASSERT_TRUE(status.ok()) << status.ToString();
        prepared.request.source =
            ManifestBuildSource{Config{{"base_path", base}, {"ver", 1}}.dump()};
    }

    std::string
    DataPath() const {
        return directory_->Path() + "/segment/100/1";
    }

    void
    WriteColumnar(const PreparedBuild& prepared, int64_t rows, bool all_null) {
        auto schema = std::make_shared<Schema>();
        EXPECT_EQ(schema->AddDebugField("100",
                                        DataType::VECTOR_FLOAT,
                                        kDim,
                                        knowhere::metric::L2,
                                        true),
                  FieldId(100));
        const auto field = schema->ConvertToLoonArrowSchema()->field(0);
        // Packed storage needs a field ID in addition to Loon's nullable
        // binary representation and dimension metadata, as in native flush.
        auto metadata = field->metadata()->Copy();
        metadata->Append(milvus_storage::ARROW_FIELD_ID_KEY,
                         std::to_string(prepared.request.field_id.get()));
        const auto arrow_schema =
            arrow::schema({field->WithMetadata(metadata)});
        const auto fs = prepared.file_manager_context.fs;
        ASSERT_TRUE(fs->CreateDir(directory_->Path() + "/segment/100").ok());
        std::vector<std::string> paths{DataPath()};
        milvus_storage::StorageConfig config;
        std::vector<std::vector<int>> groups{{0}};
        const auto result = milvus_storage::PackedRecordBatchWriter::Make(
            fs,
            paths,
            arrow_schema,
            config,
            groups,
            1,
            ::parquet::default_writer_properties());
        ASSERT_TRUE(result.ok()) << result.status().ToString();
        auto writer = result.ValueOrDie();
        // Multiple groups exercise the complete streaming/replay path.
        for (int64_t begin = 0; begin < rows; begin += 500) {
            const auto count = std::min<int64_t>(500, rows - begin);
            arrow::BinaryBuilder builder;
            for (int64_t row = begin; row < begin + count; ++row) {
                if (all_null) {
                    ASSERT_TRUE(builder.AppendNull().ok());
                } else {
                    const auto value = Row(row);
                    ASSERT_TRUE(builder
                                    .Append(reinterpret_cast<const uint8_t*>(
                                                value.data()),
                                            kDim * sizeof(float))
                                    .ok());
                }
            }
            std::shared_ptr<arrow::Array> values;
            ASSERT_TRUE(builder.Finish(&values).ok());
            const auto batch =
                arrow::RecordBatch::Make(arrow_schema, count, {values});
            ASSERT_TRUE(batch->ValidateFull().ok());
            ASSERT_TRUE(writer->Write(batch).ok());
        }
        ASSERT_TRUE(writer->Close().ok());
    }

    static std::array<float, kDim>
    Row(int64_t row) {
        std::array<float, kDim> value;
        for (int64_t col = 0; col < kDim; ++col) {
            value[col] = static_cast<float>(
                ((row * (col * 73 + 41) + col * 11) % 997) / 997.0);
        }
        return value;
    }

    void
    ExpectAllNull(const PreparedBuild& prepared, int64_t rows) {
        const auto stats = Publish(prepared);
        ASSERT_FALSE(stats.Files().empty());
        auto owner = Open(prepared, stats);
        ASSERT_NE(owner, nullptr);
        const auto* reader =
            dynamic_cast<const index::IVectorReader*>(owner.get());
        ASSERT_NE(reader, nullptr);
        EXPECT_EQ(owner->Count(), 0);
        EXPECT_EQ(reader->ValidCount(), 0);
        EXPECT_TRUE(reader->HasValidData());
        for (int64_t row = 0; row < rows; ++row) {
            EXPECT_FALSE(reader->IsRowValid(row));
        }
    }
};

TEST_P(VectorDiskBuildSessionTest, MissingColumnBuildsCompleteNullPrefix) {
    auto prepared = PrepareColumnar(kPrefix);
    ASSERT_NO_FATAL_FAILURE(SetSource(prepared, 0, false));
    ExpectAllNull(prepared, kPrefix);
}

TEST_P(VectorDiskBuildSessionTest, CompleteShortColumnGetsLeadingNullPrefix) {
    constexpr int64_t rows = 7;
    auto prepared = PrepareColumnar(kPrefix + rows);
    ASSERT_NO_FATAL_FAILURE(WriteColumnar(prepared, rows, true));
    ASSERT_NO_FATAL_FAILURE(SetSource(prepared, rows, true));
    ExpectAllNull(prepared, kPrefix + rows);
}

TEST_P(VectorDiskBuildSessionTest, ValidRowsKeepOffsetsAfterMissingPrefix) {
    constexpr int64_t rows = 1000;
    auto prepared = PrepareColumnar(kPrefix + rows);
    ASSERT_NO_FATAL_FAILURE(WriteColumnar(prepared, rows, false));
    ASSERT_NO_FATAL_FAILURE(SetSource(prepared, rows, true));
    const auto stats = Publish(prepared);
    auto owner = Open(prepared, stats);
    ASSERT_NE(owner, nullptr);
    const auto* reader = dynamic_cast<const index::IVectorReader*>(owner.get());
    ASSERT_NE(reader, nullptr);
    EXPECT_EQ(owner->Count(), rows);
    EXPECT_EQ(reader->ValidCount(), rows);
    for (int64_t row = 0; row < kPrefix + rows; ++row) {
        EXPECT_EQ(reader->IsRowValid(row), row >= kPrefix);
    }
    const int64_t id = kPrefix + 20;
    const auto query = Row(20);
    index::VectorSearchParams params{
        .search_params_ = {{index::DISK_ANN_QUERY_LIST, rows}},
        .metric_type_ = knowhere::metric::L2,
        .topk_ = 1};
    SearchResult result;
    reader->Search(GenDataset(1, kDim, query.data()),
                   params,
                   BitsetView{},
                   nullptr,
                   result);
    ASSERT_EQ(result.seg_offsets_.size(), 1);
    EXPECT_EQ(result.seg_offsets_.front(), id);
    if (reader->HasRawData()) {
        const auto bytes = reader->GetVector(GenIdsDataset(1, &id));
        ASSERT_EQ(bytes.size(), kDim * sizeof(float));
        std::array<float, kDim> actual;
        std::memcpy(actual.data(), bytes.data(), bytes.size());
        EXPECT_EQ(actual, query);
    }
}

TEST_P(VectorDiskBuildSessionTest, TruncatedListedFileIsNotMissingPrefix) {
    constexpr int64_t rows = 7;
    auto prepared = PrepareColumnar(kPrefix + rows);
    ASSERT_NO_FATAL_FAILURE(WriteColumnar(prepared, rows, false));
    ASSERT_NO_FATAL_FAILURE(SetSource(prepared, rows, true));
    const auto size = std::filesystem::file_size(DataPath());
    ASSERT_GT(size, 16);
    std::filesystem::resize_file(DataPath(), size - 16);
    BuildSession session(prepared.request, prepared.file_manager_context);
    EXPECT_ANY_THROW(session.BuildFromSource());
    EXPECT_ANY_THROW(static_cast<void>(session.Publish()));
}

TEST_P(VectorDiskBuildSessionTest, MissingNonNullableColumnIsRejected) {
    auto prepared = PrepareColumnar(kPrefix, false);
    ASSERT_NO_FATAL_FAILURE(SetSource(prepared, 0, false));
    BuildSession session(prepared.request, prepared.file_manager_context);
    EXPECT_ANY_THROW(session.BuildFromSource());
    EXPECT_ANY_THROW(static_cast<void>(session.Publish()));
}

TEST_P(VectorDiskBuildSessionTest, MoreDecodedRowsThanExpectedIsRejected) {
    constexpr int64_t rows = 7;
    auto prepared = PrepareColumnar(rows - 1);
    ASSERT_NO_FATAL_FAILURE(WriteColumnar(prepared, rows, true));
    ASSERT_NO_FATAL_FAILURE(SetSource(prepared, rows, true));
    BuildSession session(prepared.request, prepared.file_manager_context);
    try {
        session.BuildFromSource();
        FAIL() << "expected excess decoded rows to be rejected";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::DataFormatBroken);
        EXPECT_NE(std::string(error.what()).find("source decoded more than"),
                  std::string::npos);
    }
    EXPECT_ANY_THROW(static_cast<void>(session.Publish()));
}

TEST_P(VectorDiskBuildSessionTest, IncompleteMaterializerInputIsStillRejected) {
    auto prepared = PrepareColumnar(kPrefix);
    auto params = prepared.request.params;
    params["local_dir"] = directory_->Path();
    params["field_type"] = static_cast<int>(DataType::VECTOR_FLOAT);
    params["value_type"] = static_cast<int>(DataType::VECTOR_FLOAT);
    VectorDiskBuildMaterializer materializer(directory_->Path(),
                                             DataType::VECTOR_FLOAT,
                                             DataType::VECTOR_FLOAT,
                                             kDim,
                                             true,
                                             kPrefix,
                                             index::families::kVectorDisk,
                                             params);
    // Only BuildSession may establish and synthesize a legitimate prefix.
    // FinishPrimary must never reinterpret incomplete input as null rows.
    EXPECT_ANY_THROW(materializer.FinishPrimary());
}

INSTANTIATE_TEST_SUITE_P(ColumnarSources,
                         VectorDiskBuildSessionTest,
                         ::testing::Bool(),
                         [](const ::testing::TestParamInfo<bool>& info) {
                             return info.param ? "Manifest" : "StorageV2";
                         });

}  // namespace
}  // namespace milvus::indexbuilder::test

#endif  // BUILD_DISK_ANN
