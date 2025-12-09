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

#include <gtest/gtest.h>
#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <memory>
#include <vector>

#include "segcore/storagev2translator/AggregateTranslator.h"
#include "segcore/storagev2translator/ManifestGroupTranslator.h"
#include "common/Chunk.h"
#include "common/ChunkWriter.h"
#include "common/GroupChunk.h"
#include "common/Types.h"
#include "common/FieldMeta.h"
#include "milvus-storage/reader.h"

using milvus::Chunk;
using milvus::DataType;
using milvus::FieldId;
using milvus::FieldMeta;
using milvus::FieldName;
using milvus::GroupChunk;
using milvus::segcore::storagev2translator::AggregateTranslator;
using milvus::segcore::storagev2translator::ManifestGroupTranslator;
namespace cachinglayer = milvus::cachinglayer;

// Minimal mock ChunkReader for testing
class MockChunkReader : public milvus_storage::api::ChunkReader {
 public:
    explicit MockChunkReader(size_t num_cells, const std::vector<int64_t>& rows_per_cell)
        : num_cells_(num_cells), rows_per_cell_(rows_per_cell) {}

    size_t
    total_number_of_chunks() const override {
        return num_cells_;
    }

    arrow::Result<std::vector<int64_t>>
    get_chunk_indices(const std::vector<int64_t>& row_indices) override {
        return arrow::Status::NotImplemented("Mock");
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>>
    get_chunk(int64_t chunk_index) override {
        return arrow::Status::NotImplemented("Mock");
    }

    arrow::Result<std::vector<std::shared_ptr<arrow::RecordBatch>>>
    get_chunks(const std::vector<int64_t>& indices, int64_t parallel_degree) override {
        return arrow::Status::NotImplemented("Mock - use read_cells override");
    }

    arrow::Result<std::vector<uint64_t>>
    get_chunk_size() override {
        // Return reasonable sizes (1KB per cell)
        return std::vector<uint64_t>(num_cells_, 1024);
    }

    arrow::Result<std::vector<uint64_t>>
    get_chunk_rows() override {
        std::vector<uint64_t> rows;
        rows.reserve(rows_per_cell_.size());
        for (auto r : rows_per_cell_) {
            rows.push_back(static_cast<uint64_t>(r));
        }
        return rows;
    }

 private:
    size_t num_cells_;
    std::vector<int64_t> rows_per_cell_;
};

// Mock ManifestGroupTranslator that overrides virtual methods for testing
class MockManifestGroupTranslator : public ManifestGroupTranslator {
 public:
    MockManifestGroupTranslator(
        size_t num_cells,
        const std::vector<int64_t>& rows_per_cell,
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& test_batches)
        : ManifestGroupTranslator(
              0,  // segment_id
              0,  // column_group_index
              std::make_unique<MockChunkReader>(num_cells, rows_per_cell),
              CreateFieldMetas(),
              false,  // use_mmap
              2,      // num_fields
              milvus::proto::common::LoadPriority::HIGH),
          num_cells_(num_cells),
          rows_per_cell_(rows_per_cell),
          test_batches_(test_batches) {
    }

    // Override num_cells since chunk_reader is nullptr
    size_t
    num_cells() const override {
        return num_cells_;
    }

    // Override read_cells to return test RecordBatches
    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::shared_ptr<arrow::RecordBatch>>>
    read_cells(const std::vector<milvus::cachinglayer::cid_t>& cids) override {
        std::vector<std::pair<milvus::cachinglayer::cid_t,
                              std::shared_ptr<arrow::RecordBatch>>>
            result;
        result.reserve(cids.size());
        for (auto cid : cids) {
            result.emplace_back(cid, test_batches_[cid]);
        }
        return result;
    }

    // Override load_group_chunk to create test GroupChunks from RecordBatches
    std::unique_ptr<milvus::GroupChunk>
    load_group_chunk(
        const std::vector<std::shared_ptr<arrow::RecordBatch>>& record_batches,
        milvus::cachinglayer::cid_t base_cid) override {
        // Concatenate arrays for each field
        std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;

        // Get field metas
        auto field_metas = CreateFieldMetas();

        // Field 1 (int64)
        arrow::ArrayVector int64_arrays;
        for (const auto& batch : record_batches) {
            int64_arrays.push_back(batch->column(0));
        }
        chunks[FieldId(1)] = create_chunk(field_metas.at(FieldId(1)), int64_arrays);

        // Field 2 (float)
        arrow::ArrayVector float_arrays;
        for (const auto& batch : record_batches) {
            float_arrays.push_back(batch->column(1));
        }
        chunks[FieldId(2)] = create_chunk(field_metas.at(FieldId(2)), float_arrays);

        return std::make_unique<GroupChunk>(chunks);
    }

 private:
    size_t num_cells_;
    std::vector<int64_t> rows_per_cell_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> test_batches_;

    static std::unordered_map<FieldId, FieldMeta>
    CreateFieldMetas() {
        std::unordered_map<FieldId, FieldMeta> field_metas;
        field_metas.emplace(FieldId(1), FieldMeta(FieldName("field1"),
                                                  FieldId(1),
                                                  DataType::INT64,
                                                  false,
                                                  std::nullopt));
        field_metas.emplace(FieldId(2), FieldMeta(FieldName("field2"),
                                                  FieldId(2),
                                                  DataType::FLOAT,
                                                  false,
                                                  std::nullopt));
        return field_metas;
    }
};

// Test fixture
class AggregateTranslatorTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        // Create test data - 6 cells with different row counts
        num_rows_per_cell_ = {100, 200, 150, 300, 250, 50};

        // Create RecordBatches for each cell
        for (size_t i = 0; i < num_rows_per_cell_.size(); ++i) {
            test_batches_.push_back(
                CreateTestRecordBatch(num_rows_per_cell_[i], i));
        }
    }

    std::shared_ptr<arrow::RecordBatch>
    CreateTestRecordBatch(int64_t num_rows, size_t cell_id) {
        // Create int64 array
        arrow::Int64Builder int64_builder;
        for (int64_t i = 0; i < num_rows; ++i) {
            EXPECT_TRUE(int64_builder.Append(cell_id * 1000 + i).ok());
        }
        std::shared_ptr<arrow::Array> int64_array;
        EXPECT_TRUE(int64_builder.Finish(&int64_array).ok());

        // Create float array
        arrow::FloatBuilder float_builder;
        for (int64_t i = 0; i < num_rows; ++i) {
            EXPECT_TRUE(
                float_builder.Append(static_cast<float>(cell_id) + i * 0.1f)
                    .ok());
        }
        std::shared_ptr<arrow::Array> float_array;
        EXPECT_TRUE(float_builder.Finish(&float_array).ok());

        // Create schema
        auto schema = arrow::schema({arrow::field("1", arrow::int64()),
                                     arrow::field("2", arrow::float32())});

        // Create RecordBatch
        return arrow::RecordBatch::Make(
            schema, num_rows, {int64_array, float_array});
    }

    std::vector<int64_t> num_rows_per_cell_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> test_batches_;
};

TEST_F(AggregateTranslatorTest, BasicProperties) {
    // Create mock translator with 6 cells
    auto mock_translator = std::make_unique<MockManifestGroupTranslator>(
        num_rows_per_cell_.size(), num_rows_per_cell_, test_batches_);

    // Create aggregate translator with factor 2
    auto aggregate_translator =
        std::make_unique<AggregateTranslator>(std::move(mock_translator), 2);

    // Test num_cells: 6 cells with factor 2 should give 3 aggregate cells
    EXPECT_EQ(aggregate_translator->num_cells(), 3);

    // Test key
    EXPECT_EQ(aggregate_translator->key(), "agg_2_seg_0_cg_0");
}

TEST_F(AggregateTranslatorTest, CellIdMapping) {
    auto mock_translator = std::make_unique<MockManifestGroupTranslator>(
        num_rows_per_cell_.size(), num_rows_per_cell_, test_batches_);

    auto aggregate_translator =
        std::make_unique<AggregateTranslator>(std::move(mock_translator), 2);

    // Test cell_id_of mapping
    // Units 0,1 -> Aggregate cell 0
    EXPECT_EQ(aggregate_translator->cell_id_of(0), 0);
    EXPECT_EQ(aggregate_translator->cell_id_of(1), 0);

    // Units 2,3 -> Aggregate cell 1
    EXPECT_EQ(aggregate_translator->cell_id_of(2), 1);
    EXPECT_EQ(aggregate_translator->cell_id_of(3), 1);

    // Units 4,5 -> Aggregate cell 2
    EXPECT_EQ(aggregate_translator->cell_id_of(4), 2);
    EXPECT_EQ(aggregate_translator->cell_id_of(5), 2);
}

TEST_F(AggregateTranslatorTest, GetCells) {
    auto mock_translator = std::make_unique<MockManifestGroupTranslator>(
        num_rows_per_cell_.size(), num_rows_per_cell_, test_batches_);

    auto aggregate_translator =
        std::make_unique<AggregateTranslator>(std::move(mock_translator), 2);

    // Request aggregate cell 0 (which merges underlying cells 0 and 1)
    std::vector<cachinglayer::cid_t> cids = {0};
    auto cells = aggregate_translator->get_cells(cids);

    EXPECT_EQ(cells.size(), 1);
    EXPECT_EQ(cells[0].first, 0);

    auto& merged_chunk = cells[0].second;
    EXPECT_NE(merged_chunk, nullptr);

    // Verify the merged chunk has the right number of rows
    // Cell 0 has 100 rows, cell 1 has 200 rows -> merged should have 300
    EXPECT_EQ(merged_chunk->RowNums(), 300);

    // Verify both fields are present
    EXPECT_TRUE(merged_chunk->HasChunk(FieldId(1)));
    EXPECT_TRUE(merged_chunk->HasChunk(FieldId(2)));

    // Verify field 1 (int64) data
    auto int64_chunk = merged_chunk->GetChunk(FieldId(1));
    EXPECT_NE(int64_chunk, nullptr);
    EXPECT_EQ(int64_chunk->RowNums(), 300);
}

TEST_F(AggregateTranslatorTest, GetMultipleCells) {
    auto mock_translator = std::make_unique<MockManifestGroupTranslator>(
        num_rows_per_cell_.size(), num_rows_per_cell_, test_batches_);

    auto aggregate_translator =
        std::make_unique<AggregateTranslator>(std::move(mock_translator), 2);

    // Request multiple aggregate cells
    std::vector<cachinglayer::cid_t> cids = {0, 1, 2};
    auto cells = aggregate_translator->get_cells(cids);

    EXPECT_EQ(cells.size(), 3);

    // Verify row counts
    // Aggregate cell 0: cells 0(100) + 1(200) = 300 rows
    EXPECT_EQ(cells[0].second->RowNums(), 300);

    // Aggregate cell 1: cells 2(150) + 3(300) = 450 rows
    EXPECT_EQ(cells[1].second->RowNums(), 450);

    // Aggregate cell 2: cells 4(250) + 5(50) = 300 rows
    EXPECT_EQ(cells[2].second->RowNums(), 300);
}

TEST_F(AggregateTranslatorTest, OddNumberOfCells) {
    // Create 5 cells (odd number)
    std::vector<int64_t> odd_num_rows = {100, 200, 150, 300, 250};
    std::vector<std::shared_ptr<arrow::RecordBatch>> odd_batches;

    for (size_t i = 0; i < odd_num_rows.size(); ++i) {
        odd_batches.push_back(CreateTestRecordBatch(odd_num_rows[i], i));
    }

    auto mock_translator = std::make_unique<MockManifestGroupTranslator>(
        odd_num_rows.size(), odd_num_rows, odd_batches);

    auto aggregate_translator =
        std::make_unique<AggregateTranslator>(std::move(mock_translator), 2);

    // 5 cells with factor 2 should give ceil(5/2) = 3 aggregate cells
    EXPECT_EQ(aggregate_translator->num_cells(), 3);

    // Get all cells
    std::vector<cachinglayer::cid_t> cids = {0, 1, 2};
    auto cells = aggregate_translator->get_cells(cids);

    EXPECT_EQ(cells.size(), 3);

    // Last aggregate cell should have only 1 underlying cell
    // Aggregate cell 2: cell 4(250) only = 250 rows
    EXPECT_EQ(cells[2].second->RowNums(), 250);
}

TEST_F(AggregateTranslatorTest, LargeAggregationFactor) {
    auto mock_translator = std::make_unique<MockManifestGroupTranslator>(
        num_rows_per_cell_.size(), num_rows_per_cell_, test_batches_);

    // Factor of 10 (larger than num cells) should aggregate everything
    auto aggregate_translator =
        std::make_unique<AggregateTranslator>(std::move(mock_translator), 10);

    // Should have 1 aggregate cell
    EXPECT_EQ(aggregate_translator->num_cells(), 1);

    // Get the single aggregate cell
    std::vector<cachinglayer::cid_t> cids = {0};
    auto cells = aggregate_translator->get_cells(cids);

    EXPECT_EQ(cells.size(), 1);

    // Should have sum of all rows: 100+200+150+300+250+50 = 1050
    EXPECT_EQ(cells[0].second->RowNums(), 1050);
}

TEST_F(AggregateTranslatorTest, FactorFour) {
    // Test with factor 4 (the default used in production)
    auto mock_translator = std::make_unique<MockManifestGroupTranslator>(
        num_rows_per_cell_.size(), num_rows_per_cell_, test_batches_);

    auto aggregate_translator =
        std::make_unique<AggregateTranslator>(std::move(mock_translator), 4);

    // 6 cells with factor 4 should give ceil(6/4) = 2 aggregate cells
    EXPECT_EQ(aggregate_translator->num_cells(), 2);

    // Get both cells
    std::vector<cachinglayer::cid_t> cids = {0, 1};
    auto cells = aggregate_translator->get_cells(cids);

    EXPECT_EQ(cells.size(), 2);

    // Aggregate cell 0: cells 0(100) + 1(200) + 2(150) + 3(300) = 750 rows
    EXPECT_EQ(cells[0].second->RowNums(), 750);

    // Aggregate cell 1: cells 4(250) + 5(50) = 300 rows
    EXPECT_EQ(cells[1].second->RowNums(), 300);
}

TEST_F(AggregateTranslatorTest, EstimatedByteSize) {
    auto mock_translator = std::make_unique<MockManifestGroupTranslator>(
        num_rows_per_cell_.size(), num_rows_per_cell_, test_batches_);

    auto aggregate_translator =
        std::make_unique<AggregateTranslator>(std::move(mock_translator), 2);

    // Get the size of aggregate cell 0 (should be sum of cells 0 and 1)
    auto [loading_usage, loaded_usage] =
        aggregate_translator->estimated_byte_size_of_cell(0);

    // Each cell should have some size (exact values depend on MockChunkReader)
    EXPECT_GT(loading_usage.memory_bytes, 0);
    EXPECT_GT(loaded_usage.memory_bytes, 0);
}
