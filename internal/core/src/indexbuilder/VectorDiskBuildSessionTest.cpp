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
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "common/Schema.h"
#include "index/contracts/query/IVectorReader.h"
#include "indexbuilder/test_utils/SourceBuildTestUtils.h"
#include "milvus-storage/common/constants.h"
#include "milvus-storage/packed/writer.h"

namespace milvus::indexbuilder::test {
namespace {

constexpr int64_t kDim = 4;
constexpr int64_t kPrefix = 3;

class VectorDiskBuildSessionTest : public SourceBuildTest {
 protected:
    PreparedBuild
    PrepareColumnar(int64_t expected_rows) {
        auto prepared =
            Prepare(DataType::VECTOR_FLOAT,
                    DataType::NONE,
                    true,
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

    std::string
    DataPath() const {
        return directory_->Path() + "/segment/100/1";
    }

    void
    WriteColumnar(const PreparedBuild& prepared, int64_t rows) {
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
                const auto value = Row(row);
                ASSERT_TRUE(
                    builder
                        .Append(reinterpret_cast<const uint8_t*>(value.data()),
                                kDim * sizeof(float))
                        .ok());
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
};

TEST_F(VectorDiskBuildSessionTest, ValidRowsKeepOffsetsAfterMissingPrefix) {
    constexpr int64_t rows = 1000;
    auto prepared = PrepareColumnar(kPrefix + rows);
    ASSERT_NO_FATAL_FAILURE(WriteColumnar(prepared, rows));
    prepared.request.source = StorageV2BuildSource{{{DataPath()}}};
    const auto stats = Publish(prepared);
    auto owner = Open(prepared, stats);
    ASSERT_NE(owner, nullptr);
    const auto* reader = dynamic_cast<const index::IVectorReader*>(owner.get());
    ASSERT_NE(reader, nullptr);
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
}

}  // namespace
}  // namespace milvus::indexbuilder::test

#endif  // BUILD_DISK_ANN
