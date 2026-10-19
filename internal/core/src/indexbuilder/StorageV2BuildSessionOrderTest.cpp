// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <arrow/api.h>
#include <parquet/arrow/writer.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "index/contracts/query/IScalarPredicateReader.h"
#include "index/contracts/query/IVectorReader.h"
#include "indexbuilder/test_utils/SourceBuildTestUtils.h"
#include "knowhere/comp/index_param.h"
#include "milvus-storage/common/constants.h"
#include "milvus-storage/common/metadata.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/MemFileManagerImpl.h"

namespace milvus::indexbuilder::test {
namespace {

class StorageV2BuildSessionOrderTest : public SourceBuildTest {
 protected:
    std::string
    Path(int64_t field_id, int64_t log_id) const {
        return directory_->Path() + "/" + std::to_string(field_id) + "/" +
               std::to_string(log_id);
    }

    void
    WriteColumn(const PreparedBuild& prepared,
                int64_t field_id,
                int64_t log_id,
                const std::shared_ptr<arrow::Array>& values) const {
        const auto& fs = prepared.file_manager_context.fs;
        ASSERT_TRUE(
            fs->CreateDir(directory_->Path() + "/" + std::to_string(field_id))
                .ok());
        const auto schema = arrow::schema({arrow::field(
            "values",
            values->type(),
            false,
            arrow::key_value_metadata({milvus_storage::ARROW_FIELD_ID_KEY},
                                      {std::to_string(field_id)}))});
        auto sink_result = fs->OpenOutputStream(Path(field_id, log_id));
        ASSERT_TRUE(sink_result.ok()) << sink_result.status().ToString();
        auto sink = sink_result.ValueOrDie();
        auto writer_result = ::parquet::arrow::FileWriter::Open(
            *schema, arrow::default_memory_pool(), sink);
        ASSERT_TRUE(writer_result.ok()) << writer_result.status().ToString();
        auto writer = std::move(writer_result).ValueOrDie();
        ASSERT_TRUE(writer->NewBufferedRowGroup().ok());
        ASSERT_TRUE(writer
                        ->WriteRecordBatch(*arrow::RecordBatch::Make(
                            schema, values->length(), {values}))
                        .ok());
        milvus_storage::GroupFieldIDList groups;
        groups.AddFieldIDList(milvus_storage::FieldIDList({field_id}));
        milvus_storage::RowGroupMetadataVector row_groups;
        row_groups.Add(milvus_storage::RowGroupMetadata(
            values->length() * 16, values->length(), 0));
        ASSERT_TRUE(
            writer
                ->AddKeyValueMetadata(arrow::key_value_metadata(
                    {milvus_storage::GROUP_FIELD_ID_LIST_META_KEY,
                     milvus_storage::ROW_GROUP_META_KEY,
                     milvus_storage::STORAGE_VERSION_KEY},
                    {groups.Serialize(), row_groups.Serialize(), "1.0.0"}))
                .ok());
        ASSERT_TRUE(writer->Close().ok());
        ASSERT_TRUE(sink->Close().ok());
    }

    void
    WriteIntegers(const PreparedBuild& prepared,
                  int64_t field_id,
                  int64_t log_id,
                  const std::vector<int64_t>& values) const {
        arrow::Int64Builder builder;
        ASSERT_TRUE(builder.AppendValues(values).ok());
        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        ASSERT_NO_FATAL_FAILURE(WriteColumn(prepared, field_id, log_id, array));
    }
};

TEST_F(StorageV2BuildSessionOrderTest, BothReadersSortNumericBinlogIds) {
    const auto prepared = Prepare(
        DataType::INT64, DataType::NONE, false, index::ASCENDING_SORT, 4, 1);
    ASSERT_NO_FATAL_FAILURE(WriteIntegers(prepared, 100, 10, {20, 21}));
    ASSERT_NO_FATAL_FAILURE(WriteIntegers(prepared, 100, 2, {10, 11}));
    std::vector<std::vector<std::string>> files{{Path(100, 10), Path(100, 2)}};
    const auto original_files = files;
    const std::vector<int64_t> expected{10, 11, 20, 21};
    const auto values = [](const std::vector<FieldDataPtr>& batches) {
        std::vector<int64_t> output;
        for (const auto& batch : batches) {
            const auto* data = static_cast<const int64_t*>(batch->Data());
            output.insert(output.end(), data, data + batch->get_num_rows());
        }
        return output;
    };
    std::vector<FieldDataPtr> streamed;
    const auto outcome = storage::VisitFieldDataFromStorageV2(
        files,
        100,
        DataType::INT64,
        DataType::NONE,
        1,
        prepared.file_manager_context.fs,
        [&](FieldDataPtr batch) {
            streamed.push_back(std::move(batch));
            return storage::VisitControl::Continue;
        });
    EXPECT_EQ(outcome, storage::VisitOutcome::Exhausted);
    EXPECT_EQ(values(streamed), expected);
    EXPECT_EQ(files, original_files);
    const auto collected =
        storage::GetFieldDatasFromStorageV2(files,
                                            100,
                                            DataType::INT64,
                                            DataType::NONE,
                                            1,
                                            prepared.file_manager_context.fs);
    EXPECT_EQ(values(collected), expected);
    // Keep all caller-owned source metadata unchanged for another field/pass.
    EXPECT_EQ(files, original_files);
}

TEST_F(StorageV2BuildSessionOrderTest, ScalarPredicateUsesNumericBinlogOrder) {
    auto prepared = Prepare(
        DataType::INT64, DataType::NONE, false, index::ASCENDING_SORT, 4, 1);
    ASSERT_NO_FATAL_FAILURE(WriteIntegers(prepared, 100, 2, {10, 11}));
    ASSERT_NO_FATAL_FAILURE(WriteIntegers(prepared, 100, 10, {20, 21}));
    // Lexicographic ordering and request ordering both put log 10 first.
    prepared.request.source =
        StorageV2BuildSource{{{Path(100, 10), Path(100, 2)}}};
    const auto stats = Publish(prepared);
    auto reader = Open(prepared, stats);
    ASSERT_NE(reader, nullptr);
    const auto* predicate =
        dynamic_cast<const index::IScalarPredicateReader<int64_t>*>(
            reader.get());
    ASSERT_NE(predicate, nullptr);
    for (const auto [value, row] :
         {std::pair<int64_t, size_t>{10, 0}, {11, 1}, {20, 2}, {21, 3}}) {
        const auto hits = predicate->In(1, &value);
        ASSERT_EQ(hits.size(), 4);
        EXPECT_EQ(hits.count(), 1);
        EXPECT_TRUE(hits[row]) << "value=" << value;
    }
}

TEST_F(StorageV2BuildSessionOrderTest, VectorAndOptionalFieldKeepRowIds) {
    constexpr int64_t rows = 1024;
    constexpr int64_t dim = 4;
    constexpr int64_t side_field_id = 101;
    const OptFieldT opt_fields{{side_field_id,
                                std::make_tuple("tenant",
                                                DataType::INT64,
                                                DataType::NONE,
                                                std::vector<std::string>{})}};
    auto prepared = Prepare(DataType::VECTOR_FLOAT,
                            DataType::NONE,
                            false,
                            knowhere::IndexEnum::INDEX_HNSW,
                            rows,
                            dim,
                            {{index::METRIC_TYPE, "L2"},
                             {knowhere::indexparam::M, 16},
                             {knowhere::indexparam::EFCONSTRUCTION, 96},
                             {"mv_build_minimum_category_size", 128}});
    // Prepare serializes ordinary index params as strings. Optional-field
    // metadata is a typed config value, as produced by AdaptOptionalFields.
    prepared.request.params[PARTITION_KEY_ISOLATION_KEY] = true;
    prepared.request.params[VEC_OPT_FIELDS] = opt_fields;
    for (const auto [log_id, begin] :
         {std::pair<int64_t, int64_t>{2, 0}, {10, rows / 2}}) {
        arrow::FixedSizeBinaryBuilder builder(
            arrow::fixed_size_binary(dim * sizeof(float)));
        for (int64_t row = begin; row < begin + rows / 2; ++row) {
            const std::array<float, dim> vector{static_cast<float>(row),
                                                static_cast<float>(2 * row),
                                                static_cast<float>(3 * row),
                                                static_cast<float>(4 * row)};
            ASSERT_TRUE(
                builder.Append(reinterpret_cast<const uint8_t*>(vector.data()))
                    .ok());
        }
        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        ASSERT_NO_FATAL_FAILURE(WriteColumn(prepared, 100, log_id, array));
        std::vector<int64_t> tenants(rows / 2);
        for (int64_t i = 0; i < rows / 2; ++i) {
            const auto row = begin + i;
            tenants[i] =
                row < rows / 4 || (row >= rows / 2 && row < 5 * rows / 8) ? 2
                : row < rows / 2                                          ? 3
                                                                          : 4;
        }
        ASSERT_NO_FATAL_FAILURE(
            WriteIntegers(prepared, side_field_id, log_id, tenants));
    }
    const std::vector<std::vector<std::string>> files{
        {Path(side_field_id, 10), Path(side_field_id, 2)},
        {Path(100, 10), Path(100, 2)}};
    prepared.request.source = StorageV2BuildSource{files};
    const auto stats = Publish(prepared);
    auto owner = Open(prepared, stats);
    ASSERT_NE(owner, nullptr);
    const auto* vectors =
        dynamic_cast<const index::IVectorReader*>(owner.get());
    ASSERT_NE(vectors, nullptr);
    const std::array<int64_t, 4> ids{0, rows / 2 - 1, rows / 2, rows - 1};
    const auto bytes =
        vectors->GetVector(GenIdsDataset(ids.size(), ids.data()));
    ASSERT_EQ(bytes.size(), ids.size() * dim * sizeof(float));
    std::array<float, 4 * dim> decoded{};
    std::memcpy(decoded.data(), bytes.data(), bytes.size());
    for (size_t i = 0; i < ids.size(); ++i) {
        for (int64_t col = 0; col < dim; ++col) {
            EXPECT_FLOAT_EQ(decoded[i * dim + col],
                            static_cast<float>(ids[i] * (col + 1)));
        }
    }
    const std::array<float, dim> query{0, 0, 0, 0};
    // Query tenant == 2, which includes row 0 and the second log's first
    // quarter. Knowhere selects the isolated partition from the first
    // unmasked row, so pass the full category filter instead of an empty view.
    std::vector<uint8_t> excluded((rows + 7) / 8, 0xff);
    for (int64_t row = 0; row < rows; ++row) {
        if (row < rows / 4 || (row >= rows / 2 && row < 5 * rows / 8)) {
            excluded[row / 8] &= static_cast<uint8_t>(~(1U << (row % 8)));
        }
    }
    SearchResult result;
    vectors->Search(GenDataset(1, dim, query.data()),
                    index::VectorSearchParams{
                        .search_params_ = {{knowhere::indexparam::EF, 64}},
                        .metric_type_ = "L2",
                        .topk_ = 1},
                    BitsetView(excluded.data(), rows),
                    nullptr,
                    result);
    ASSERT_EQ(result.seg_offsets_.size(), 1);
    EXPECT_EQ(result.seg_offsets_[0], 0);

    // Check the optional scalar's physical row IDs independently of HNSW's
    // category ordering, which is not part of its public reader contract.
    storage::MemFileManagerImpl manager(prepared.file_manager_context);
    Config config{{STORAGE_VERSION_KEY, STORAGE_V2},
                  {SEGMENT_INSERT_FILES_KEY, files},
                  {VEC_OPT_FIELDS, opt_fields}};
    auto categories = manager.CacheOptFieldToMemory(config).at(side_field_id);
    ASSERT_EQ(categories.size(), 3);
    std::sort(categories.begin(), categories.end());
    std::vector<uint32_t> first, second, third;
    for (uint32_t row = 0; row < rows; ++row) {
        if (row < rows / 4 || (row >= rows / 2 && row < 5 * rows / 8)) {
            first.push_back(row);
        } else if (row < rows / 2) {
            second.push_back(row);
        } else {
            third.push_back(row);
        }
    }
    EXPECT_EQ(categories[0], first);
    EXPECT_EQ(categories[1], second);
    EXPECT_EQ(categories[2], third);
}

TEST_F(StorageV2BuildSessionOrderTest, DenseDiskCacheUsesNumericBinlogOrder) {
    constexpr int64_t rows = 4;
    constexpr int64_t dim = 4;
    const auto prepared = Prepare(DataType::VECTOR_FLOAT,
                                  DataType::NONE,
                                  false,
                                  "FLAT",
                                  rows,
                                  dim,
                                  {{index::METRIC_TYPE, "L2"}});
    for (const auto [log_id, begin] :
         {std::pair<int64_t, int64_t>{2, 0}, {10, 2}}) {
        arrow::FixedSizeBinaryBuilder builder(
            arrow::fixed_size_binary(dim * sizeof(float)));
        for (int64_t row = begin; row < begin + 2; ++row) {
            const std::array<float, dim> vector{static_cast<float>(row),
                                                static_cast<float>(2 * row),
                                                static_cast<float>(3 * row),
                                                static_cast<float>(4 * row)};
            ASSERT_TRUE(
                builder.Append(reinterpret_cast<const uint8_t*>(vector.data()))
                    .ok());
        }
        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        ASSERT_NO_FATAL_FAILURE(WriteColumn(prepared, 100, log_id, array));
    }
    storage::DiskFileManagerImpl manager(prepared.file_manager_context);
    Config config{
        {STORAGE_VERSION_KEY, STORAGE_V2},
        {SEGMENT_INSERT_FILES_KEY,
         std::vector<std::vector<std::string>>{{Path(100, 10), Path(100, 2)}}},
        {DATA_TYPE_KEY, DataType::VECTOR_FLOAT},
        {ELEMENT_TYPE_KEY, DataType::NONE},
        {DIM_KEY, dim}};
    const auto path = manager.CacheRawDataToDisk<float>(config);
    std::ifstream raw(path, std::ios::binary);
    ASSERT_TRUE(raw.good());
    uint32_t actual_rows = 0, actual_dim = 0;
    raw.read(reinterpret_cast<char*>(&actual_rows), sizeof(actual_rows));
    raw.read(reinterpret_cast<char*>(&actual_dim), sizeof(actual_dim));
    EXPECT_EQ(actual_rows, rows);
    EXPECT_EQ(actual_dim, dim);
    std::array<float, rows * dim> values{};
    raw.read(reinterpret_cast<char*>(values.data()), sizeof(values));
    ASSERT_TRUE(raw.good());
    for (int64_t row = 0; row < rows; ++row) {
        for (int64_t col = 0; col < dim; ++col) {
            EXPECT_FLOAT_EQ(values[row * dim + col],
                            static_cast<float>(row * (col + 1)));
        }
    }
}

TEST_F(StorageV2BuildSessionOrderTest, RejectsNonNumericBinlogBasenames) {
    const auto prepared = Prepare(
        DataType::INT64, DataType::NONE, false, index::ASCENDING_SORT, 1, 1);
    for (const auto name :
         {"not-a-log", "2.parquet", "10suffix", "", "9223372036854775808"}) {
        SCOPED_TRACE(name);
        auto request = prepared.request;
        // A one-file group must be validated even though sorting it needs no
        // comparisons. No source file exists, so validation must precede I/O.
        const std::vector<std::vector<std::string>> files{
            {directory_->Path() + "/100/" + name}};
        request.source = StorageV2BuildSource{files};
        BuildSession session(std::move(request), prepared.file_manager_context);
        try {
            session.BuildFromSource();
            FAIL() << "expected malformed binlog basename to be rejected";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), DataFormatBroken);
        }
        auto mutable_files = files;
        try {
            storage::GetFieldDatasFromStorageV2(
                mutable_files,
                100,
                DataType::INT64,
                DataType::NONE,
                1,
                prepared.file_manager_context.fs);
            FAIL() << "expected the collecting V2 reader to reject the path";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), DataFormatBroken);
        }
    }
}

}  // namespace
}  // namespace milvus::indexbuilder::test
