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

// Correctness and micro-benchmark coverage for ProxyChunkColumn bulk
// accessors with random (search-reduce style) row offsets spanning many
// chunks — the FillPrimaryKeys / FillTargetEntry hot path.

#include <gtest/gtest.h>
#include <arrow/array.h>
#include <arrow/io/memory.h>
#include <parquet/arrow/reader.h>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <random>
#include <vector>

#include "common/Chunk.h"
#include "common/ChunkWriter.h"
#include "common/FieldData.h"
#include "common/FieldMeta.h"
#include "common/GroupChunk.h"
#include "common/Types.h"
#include "mmap/ChunkedColumnGroup.h"
#include "storage/Event.h"
#include "storage/Util.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::storage;

namespace {

std::shared_ptr<Chunk>
make_int64_chunk(const FixedVector<int64_t>& data, const FieldMeta& meta) {
    auto field_data = milvus::storage::CreateFieldData(storage::DataType::INT64,
                                                       DataType::NONE);
    field_data->FillFieldData(data.data(), data.size());
    storage::InsertEventData event_data;
    event_data.payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    auto ser_data = event_data.Serialize();
    auto buffer = std::make_shared<arrow::io::BufferReader>(
        ser_data.data() + 2 * sizeof(milvus::Timestamp),
        ser_data.size() - 2 * sizeof(milvus::Timestamp));

    parquet::arrow::FileReaderBuilder reader_builder;
    auto s = reader_builder.Open(buffer);
    EXPECT_TRUE(s.ok());
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    s = reader_builder.Build(&arrow_reader);
    EXPECT_TRUE(s.ok());
    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    s = arrow_reader->GetRecordBatchReader(&rb_reader);
    EXPECT_TRUE(s.ok());

    arrow::ArrayVector array_vec = read_single_column_batches(rb_reader);
    return create_chunk(meta, array_vec);
}

struct GroupSetup {
    std::shared_ptr<ChunkedColumnGroup> column_group;
    std::shared_ptr<ProxyChunkColumn> target_column;
    int64_t num_rows = 0;
};

// Builds a column group of `num_fields` INT64 fields split into
// `num_chunks` chunks of `rows_per_chunk` rows. Every field stores
// value == global row id, so ground truth for any offset is the offset
// itself. The target column is the first field.
GroupSetup
BuildInt64Group(int64_t num_chunks, int64_t rows_per_chunk, int num_fields) {
    GroupSetup setup;
    setup.num_rows = num_chunks * rows_per_chunk;

    std::vector<FieldMeta> metas;
    metas.reserve(num_fields);
    for (int f = 0; f < num_fields; ++f) {
        metas.emplace_back(FieldName("f" + std::to_string(f)),
                           FieldId(100 + f),
                           DataType::INT64,
                           false,
                           std::nullopt);
    }

    std::vector<std::unique_ptr<GroupChunk>> group_chunks;
    group_chunks.reserve(num_chunks);
    for (int64_t c = 0; c < num_chunks; ++c) {
        FixedVector<int64_t> data(rows_per_chunk);
        for (int64_t r = 0; r < rows_per_chunk; ++r) {
            data[r] = c * rows_per_chunk + r;
        }
        std::unordered_map<FieldId, std::shared_ptr<Chunk>> chunks;
        for (int f = 0; f < num_fields; ++f) {
            chunks[FieldId(100 + f)] = make_int64_chunk(data, metas[f]);
        }
        group_chunks.push_back(std::make_unique<GroupChunk>(chunks));
    }

    auto translator = std::make_unique<TestGroupChunkTranslator>(
        num_fields,
        std::vector<int64_t>(num_chunks, rows_per_chunk),
        "proxy_bulk_access_test",
        std::move(group_chunks));
    setup.column_group =
        std::make_shared<ChunkedColumnGroup>(std::move(translator));
    setup.target_column = std::make_shared<ProxyChunkColumn>(
        setup.column_group, FieldId(100), metas[0]);
    return setup;
}

std::vector<int64_t>
RandomOffsets(int64_t count, int64_t num_rows, uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int64_t> dist(0, num_rows - 1);
    std::vector<int64_t> offsets(count);
    for (auto& o : offsets) {
        o = dist(rng);
    }
    return offsets;
}

}  // namespace

TEST(ProxyChunkColumnBulkAccess, PrimitiveValueAtMatchesOffsets) {
    auto setup = BuildInt64Group(/*num_chunks=*/7, /*rows_per_chunk=*/33, 3);

    // Random offsets with duplicates, spanning all chunks.
    auto offsets = RandomOffsets(500, setup.num_rows, /*seed=*/42);
    offsets.push_back(0);
    offsets.push_back(setup.num_rows - 1);
    offsets.push_back(offsets.front());  // duplicate

    std::vector<int64_t> dst(offsets.size(), -1);
    setup.target_column->BulkPrimitiveValueAt(
        nullptr, dst.data(), offsets.data(), offsets.size(), false);
    for (size_t i = 0; i < offsets.size(); ++i) {
        ASSERT_EQ(dst[i], offsets[i]) << "row " << i;
    }

    // Single row and empty input.
    int64_t one = setup.num_rows / 2;
    int64_t single = -1;
    setup.target_column->BulkPrimitiveValueAt(nullptr, &single, &one, 1, false);
    EXPECT_EQ(single, one);
    setup.target_column->BulkPrimitiveValueAt(
        nullptr, dst.data(), offsets.data(), 0, false);
}

TEST(ProxyChunkColumnBulkAccess, IsValidNonNullableInvokedOncePerRow) {
    auto setup = BuildInt64Group(/*num_chunks=*/3, /*rows_per_chunk=*/10, 1);
    auto offsets = RandomOffsets(64, setup.num_rows, /*seed=*/7);
    std::vector<int> hit(offsets.size(), 0);
    setup.target_column->BulkIsValid(
        nullptr,
        [&](bool valid, size_t row) {
            EXPECT_TRUE(valid);
            ASSERT_LT(row, hit.size());
            hit[row]++;
        },
        offsets.data(),
        offsets.size());
    for (size_t i = 0; i < hit.size(); ++i) {
        EXPECT_EQ(hit[i], 1) << "row " << i;
    }
}

TEST(ProxyChunkColumnBulkAccess, BenchPrimitiveValueAtRandomOffsets) {
    struct Config {
        int64_t num_chunks;
        int64_t rows_per_chunk;
        int num_fields;
    };
    // Chunk counts mirror fragmented vs compacted segments; field counts
    // mirror narrow vs wide column groups. The last config exceeds LLC so
    // per-row access pays real cache misses like a production segment.
    std::vector<Config> configs = {
        {4, 65536, 4},
        {32, 8192, 4},
        {32, 8192, 16},
        {128, 2048, 4},
        {64, 65536, 8},
    };
    constexpr int64_t kCount = 10000;  // ~nq(100) x topk(100) candidates
    constexpr int kWarmup = 5;
    constexpr int kIters = 100;

    for (const auto& cfg : configs) {
        auto setup = BuildInt64Group(
            cfg.num_chunks, cfg.rows_per_chunk, cfg.num_fields);
        // Fresh offsets per iteration, as in production reduce — reusing
        // one offset set would keep the touched cache lines hot and hide
        // the random-access cost on large segments.
        std::vector<std::vector<int64_t>> offset_sets;
        offset_sets.reserve(kWarmup + kIters);
        for (int it = 0; it < kWarmup + kIters; ++it) {
            offset_sets.push_back(
                RandomOffsets(kCount, setup.num_rows, /*seed=*/1234 + it));
        }
        std::vector<int64_t> dst(kCount);

        for (int w = 0; w < kWarmup; ++w) {
            setup.target_column->BulkPrimitiveValueAt(
                nullptr, dst.data(), offset_sets[w].data(), kCount, false);
        }
        auto start = std::chrono::steady_clock::now();
        for (int it = 0; it < kIters; ++it) {
            setup.target_column->BulkPrimitiveValueAt(
                nullptr,
                dst.data(),
                offset_sets[kWarmup + it].data(),
                kCount,
                false);
        }
        auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::steady_clock::now() - start)
                           .count();
        // Keep the result observable so the loop cannot be optimized out.
        volatile int64_t sink = dst[kCount / 2];
        (void)sink;

        double ns_per_row =
            static_cast<double>(elapsed) / (kIters * kCount);
        std::cout << "[bench] chunks=" << cfg.num_chunks
                  << " rows_per_chunk=" << cfg.rows_per_chunk
                  << " fields=" << cfg.num_fields << " count=" << kCount
                  << " iters=" << kIters << " -> " << ns_per_row
                  << " ns/row, total "
                  << (elapsed / 1e6) << " ms" << std::endl;
    }
}
