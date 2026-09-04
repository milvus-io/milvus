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

#include <array>
#include <cstring>
#include <iterator>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/Chunk.h"
#include "common/GroupChunk.h"
#include "mmap/ChunkedColumn.h"
#include "mmap/ChunkedColumnFilter.h"
#include "mmap/ChunkedColumnGroup.h"
#include "test_utils/cachinglayer_test_utils.h"

namespace milvus {
namespace {

constexpr int32_t kElementSize = 4;
constexpr int64_t kTestFieldId = 1;
constexpr int64_t kVectorArrayFieldId = 2;
constexpr int64_t kVectorArrayRows = 4;
constexpr int64_t kVectorArrayDim = 2;

struct ColumnFixture {
    std::shared_ptr<ChunkedColumnInterface> column;
    std::shared_ptr<std::set<cachinglayer::cid_t>> fetched;
    std::shared_ptr<std::vector<std::vector<int64_t>>> pin_requests;
    // Keeps chunk data buffers alive for the column's lifetime.
    std::shared_ptr<void> buffer_holder;
    // Proxy columns delegate cache ownership to their shared group.
    std::shared_ptr<ChunkedColumnGroup> group;
};

struct ColumnSpec {
    std::vector<int64_t> rows_per_chunk;
    std::vector<std::vector<bool>> valid_patterns;  // empty => all valid
    bool nullable{true};
    DataType data_type{DataType::VECTOR_INT8};
    // Primitive ChunkWriter keeps one payload slot per logical row. Some
    // positional-access tests intentionally exercise the legacy compact
    // fixture, while scan tests use the production dense layout.
    bool dense_nullable_payload{false};
};

FieldMeta
MakeTestFieldMeta(const ColumnSpec& spec) {
    if (IsVectorDataType(spec.data_type)) {
        return FieldMeta(FieldName("t"),
                         FieldId(kTestFieldId),
                         spec.data_type,
                         kElementSize,
                         std::nullopt,
                         spec.nullable,
                         std::nullopt);
    }
    return FieldMeta(FieldName("t"),
                     FieldId(kTestFieldId),
                     spec.data_type,
                     spec.nullable,
                     std::nullopt);
}

struct VectorArrayColumnFixture {
    std::shared_ptr<ChunkedColumnInterface> column;
    std::shared_ptr<void> buffer_holder;
};

std::vector<char>
BuildChunkBuffer(int64_t row_num,
                 const std::vector<bool>* pattern,
                 bool nullable,
                 int64_t start_logical_offset,
                 bool dense_nullable_payload = false) {
    const int32_t bitmap_bytes = nullable ? (row_num + 7) / 8 : 0;
    std::vector<char> buf(bitmap_bytes + row_num * kElementSize, 0);
    int64_t physical_offset = 0;
    if (nullable) {
        for (int64_t j = 0; j < row_num; ++j) {
            const bool v = pattern ? (*pattern)[j] : true;
            if (v) {
                buf[j >> 3] |= (1 << (j & 0x07));
            }
            const int32_t value = start_logical_offset + j;
            const auto value_offset =
                dense_nullable_payload ? j : physical_offset;
            if (dense_nullable_payload || v) {
                std::memcpy(
                    buf.data() + bitmap_bytes + value_offset * kElementSize,
                    &value,
                    sizeof(value));
            }
            physical_offset += v ? 1 : 0;
        }
    } else {
        for (int64_t j = 0; j < row_num; ++j) {
            const int32_t value = start_logical_offset + j;
            std::memcpy(buf.data() + bitmap_bytes + j * kElementSize,
                        &value,
                        sizeof(value));
        }
    }
    return buf;
}

std::vector<char>
BuildNullableVectorArrayChunkBuffer() {
    const auto bitmap_bytes = (kVectorArrayRows + 7) / 8;
    const auto header_bytes = sizeof(uint32_t) * (kVectorArrayRows * 2 + 1);
    const std::array<float, 6> payload{1.0F, 2.0F, 3.0F, 4.0F, 5.0F, 6.0F};
    const auto payload_bytes = payload.size() * sizeof(float);

    std::vector<char> buf(
        bitmap_bytes + header_bytes + payload_bytes + MMAP_ARRAY_PADDING, 0);
    buf[0] = 0b00001101;  // rows 0, 2, and 3 are valid; row 1 is null.

    const auto payload_offset =
        static_cast<uint32_t>(bitmap_bytes + header_bytes);
    const auto row0_end =
        payload_offset + static_cast<uint32_t>(kVectorArrayDim * sizeof(float));
    const auto row3_end =
        row0_end + static_cast<uint32_t>(2 * kVectorArrayDim * sizeof(float));
    const std::array<uint32_t, kVectorArrayRows * 2 + 1> header{
        payload_offset,
        1,
        row0_end,
        0,
        row0_end,
        0,
        row0_end,
        2,
        row3_end,
    };
    std::memcpy(buf.data() + bitmap_bytes,
                header.data(),
                header.size() * sizeof(uint32_t));
    std::memcpy(buf.data() + payload_offset, payload.data(), payload_bytes);
    return buf;
}

std::unique_ptr<FixedWidthChunk>
MakeFixedChunk(int64_t row_num, bool nullable, char* data, size_t size) {
    auto guard = std::make_shared<ChunkMmapGuard>(nullptr, 0, "");
    return std::make_unique<FixedWidthChunk>(
        row_num, 1, data, size, kElementSize, nullable, guard);
}

std::unique_ptr<VectorArrayChunk>
MakeVectorArrayChunk(char* data, size_t size) {
    auto guard = std::make_shared<ChunkMmapGuard>(nullptr, 0, "");
    return std::make_unique<VectorArrayChunk>(kVectorArrayDim,
                                              kVectorArrayRows,
                                              data,
                                              size,
                                              DataType::VECTOR_FLOAT,
                                              guard,
                                              /*nullable=*/true);
}

std::vector<char>
BuildNullableEmptyArrayChunkBuffer() {
    constexpr int64_t row_count = 2;
    constexpr size_t bitmap_bytes = 1;
    constexpr size_t header_entries = row_count * 2 + 1;
    constexpr size_t header_bytes = header_entries * sizeof(uint32_t);
    constexpr uint32_t payload_offset = bitmap_bytes + header_bytes;

    // Keep one padding byte so an empty ArrayView has a stable, non-null data
    // pointer. Row 0 is a valid empty INT64 array; row 1 is null.
    std::vector<char> buffer(payload_offset + 1, 0);
    buffer[0] = 0b00000001;
    const std::array<uint32_t, header_entries> header{
        payload_offset, 0, payload_offset, 0, payload_offset};
    std::memcpy(buffer.data() + bitmap_bytes,
                header.data(),
                header.size() * sizeof(uint32_t));
    return buffer;
}

std::vector<char>
BuildStringChunkBuffer(const std::vector<std::string>& values) {
    const auto header_bytes = sizeof(uint32_t) * (values.size() + 1);
    size_t payload_bytes = 0;
    for (const auto& value : values) {
        payload_bytes += value.size();
    }

    std::vector<char> buffer(header_bytes + payload_bytes, 0);
    auto* offsets = reinterpret_cast<uint32_t*>(buffer.data());
    uint32_t next_offset = static_cast<uint32_t>(header_bytes);
    offsets[0] = next_offset;
    for (size_t i = 0; i < values.size(); ++i) {
        std::memcpy(
            buffer.data() + next_offset, values[i].data(), values[i].size());
        next_offset += static_cast<uint32_t>(values[i].size());
        offsets[i + 1] = next_offset;
    }
    return buffer;
}

class CountingChunkTranslator : public TestChunkTranslator {
 public:
    CountingChunkTranslator(
        std::vector<int64_t> rows,
        std::string key,
        std::vector<std::unique_ptr<Chunk>>&& chunks,
        std::shared_ptr<std::set<cachinglayer::cid_t>> fetched)
        : TestChunkTranslator(
              std::move(rows), std::move(key), std::move(chunks)),
          fetched_(std::move(fetched)) {
    }

    std::vector<std::pair<cachinglayer::cid_t, std::unique_ptr<Chunk>>>
    get_cells(milvus::OpContext* ctx,
              const std::vector<cachinglayer::cid_t>& cids) override {
        for (auto c : cids) {
            fetched_->insert(c);
        }
        return TestChunkTranslator::get_cells(ctx, cids);
    }

 private:
    std::shared_ptr<std::set<cachinglayer::cid_t>> fetched_;
};

class CountingGroupChunkTranslator : public TestGroupChunkTranslator {
 public:
    CountingGroupChunkTranslator(
        size_t num_fields,
        std::vector<int64_t> rows,
        std::string key,
        std::vector<std::unique_ptr<GroupChunk>>&& chunks,
        std::shared_ptr<std::set<cachinglayer::cid_t>> fetched)
        : TestGroupChunkTranslator(
              num_fields, std::move(rows), std::move(key), std::move(chunks)),
          fetched_(std::move(fetched)) {
    }

    std::vector<std::pair<cachinglayer::cid_t, std::unique_ptr<GroupChunk>>>
    get_cells(milvus::OpContext* ctx,
              const std::vector<cachinglayer::cid_t>& cids) override {
        for (auto c : cids) {
            fetched_->insert(c);
        }
        return TestGroupChunkTranslator::get_cells(ctx, cids);
    }

 private:
    std::shared_ptr<std::set<cachinglayer::cid_t>> fetched_;
};

class ScanCountingChunkedColumn : public ChunkedColumn {
 public:
    ScanCountingChunkedColumn(
        std::shared_ptr<CacheSlot<Chunk>> slot,
        const FieldMeta& field_meta,
        std::shared_ptr<std::vector<std::vector<int64_t>>> pin_requests)
        : ChunkedColumn(std::move(slot), field_meta),
          pin_requests_(std::move(pin_requests)) {
    }

    PinWrapper<Chunk*>
    GetChunk(milvus::OpContext* op_ctx, int64_t chunk_id) const override {
        pin_requests_->emplace_back(std::vector<int64_t>{chunk_id});
        return ChunkedColumn::GetChunk(op_ctx, chunk_id);
    }

    TakeCellPin
    MakeTakeCellPin(milvus::OpContext* op_ctx) const override {
        auto pin_cell = ChunkedColumn::MakeTakeCellPin(op_ctx);
        auto pin_requests = pin_requests_;
        return [pin_cell = std::move(pin_cell),
                pin_requests = std::move(pin_requests)](int64_t chunk_id) {
            pin_requests->emplace_back(std::vector<int64_t>{chunk_id});
            return pin_cell(chunk_id);
        };
    }

 private:
    std::shared_ptr<std::vector<std::vector<int64_t>>> pin_requests_;
};

class ScanCountingProxyChunkColumn : public ProxyChunkColumn {
 public:
    ScanCountingProxyChunkColumn(
        std::shared_ptr<ChunkedColumnGroup> group,
        FieldId field_id,
        const FieldMeta& field_meta,
        std::shared_ptr<std::vector<std::vector<int64_t>>> pin_requests)
        : ProxyChunkColumn(std::move(group), field_id, field_meta),
          pin_requests_(std::move(pin_requests)) {
    }

    PinWrapper<Chunk*>
    GetChunk(milvus::OpContext* op_ctx, int64_t chunk_id) const override {
        pin_requests_->emplace_back(std::vector<int64_t>{chunk_id});
        return ProxyChunkColumn::GetChunk(op_ctx, chunk_id);
    }

    TakeCellPin
    MakeTakeCellPin(milvus::OpContext* op_ctx) const override {
        auto pin_cell = ProxyChunkColumn::MakeTakeCellPin(op_ctx);
        auto pin_requests = pin_requests_;
        return [pin_cell = std::move(pin_cell),
                pin_requests = std::move(pin_requests)](int64_t chunk_id) {
            pin_requests->emplace_back(std::vector<int64_t>{chunk_id});
            return pin_cell(chunk_id);
        };
    }

 private:
    std::shared_ptr<std::vector<std::vector<int64_t>>> pin_requests_;
};

class ScanCountingStringColumn : public ChunkedVariableColumn<std::string> {
 public:
    ScanCountingStringColumn(
        std::shared_ptr<cachinglayer::CacheSlot<Chunk>> slot,
        const FieldMeta& field_meta,
        std::shared_ptr<std::vector<std::vector<int64_t>>> pin_requests)
        : ChunkedVariableColumn<std::string>(std::move(slot), field_meta),
          pin_requests_(std::move(pin_requests)) {
    }

    PinWrapper<Chunk*>
    GetChunk(milvus::OpContext* op_ctx, int64_t chunk_id) const override {
        pin_requests_->emplace_back(std::vector<int64_t>{chunk_id});
        return ChunkedVariableColumn<std::string>::GetChunk(op_ctx, chunk_id);
    }

    TakeCellPin
    MakeTakeCellPin(milvus::OpContext* op_ctx) const override {
        auto pin_cell =
            ChunkedVariableColumn<std::string>::MakeTakeCellPin(op_ctx);
        auto pin_requests = pin_requests_;
        return [pin_cell = std::move(pin_cell),
                pin_requests = std::move(pin_requests)](int64_t chunk_id) {
            pin_requests->emplace_back(std::vector<int64_t>{chunk_id});
            return pin_cell(chunk_id);
        };
    }

 private:
    std::shared_ptr<std::vector<std::vector<int64_t>>> pin_requests_;
};

ColumnFixture
CreateNullableEmptyArrayColumn() {
    auto buffers = std::make_shared<std::vector<std::vector<char>>>(1);
    (*buffers)[0] = BuildNullableEmptyArrayChunkBuffer();
    auto guard = std::make_shared<ChunkMmapGuard>(nullptr, 0, "");
    std::vector<std::unique_ptr<Chunk>> chunks;
    chunks.emplace_back(std::make_unique<ArrayChunk>(2,
                                                     (*buffers)[0].data(),
                                                     (*buffers)[0].size(),
                                                     DataType::INT64,
                                                     /*nullable=*/true,
                                                     guard));

    auto fetched = std::make_shared<std::set<cachinglayer::cid_t>>();
    auto translator =
        std::make_unique<CountingChunkTranslator>(std::vector<int64_t>{2},
                                                  "cc_empty_array_iface",
                                                  std::move(chunks),
                                                  fetched);
    FieldMeta field_meta(FieldName("array"),
                         FieldId(kTestFieldId),
                         DataType::ARRAY,
                         DataType::INT64,
                         /*nullable=*/true,
                         std::nullopt);
    auto slot = cachinglayer::Manager::GetInstance().CreateCacheSlot<Chunk>(
        std::move(translator), nullptr);
    auto column =
        MakeChunkedColumnBase(DataType::ARRAY, std::move(slot), field_meta);
    return {std::move(column),
            std::move(fetched),
            std::make_shared<std::vector<std::vector<int64_t>>>(),
            std::static_pointer_cast<void>(buffers)};
}

struct ChunkedColumnFactory {
    static ColumnFixture
    Create(const ColumnSpec& spec) {
        const auto n = spec.rows_per_chunk.size();
        auto buffers = std::make_shared<std::vector<std::vector<char>>>(n);
        std::vector<std::unique_ptr<Chunk>> chunks;
        chunks.reserve(n);
        int64_t start_logical_offset = 0;
        for (size_t i = 0; i < n; ++i) {
            (*buffers)[i] = BuildChunkBuffer(
                spec.rows_per_chunk[i],
                spec.valid_patterns.empty() ? nullptr : &spec.valid_patterns[i],
                spec.nullable,
                start_logical_offset,
                spec.dense_nullable_payload);
            chunks.push_back(MakeFixedChunk(spec.rows_per_chunk[i],
                                            spec.nullable,
                                            (*buffers)[i].data(),
                                            (*buffers)[i].size()));
            start_logical_offset += spec.rows_per_chunk[i];
        }
        auto fetched = std::make_shared<std::set<cachinglayer::cid_t>>();
        auto translator = std::make_unique<CountingChunkTranslator>(
            spec.rows_per_chunk, "cc_iface", std::move(chunks), fetched);
        auto fm = MakeTestFieldMeta(spec);
        auto slot = cachinglayer::Manager::GetInstance().CreateCacheSlot<Chunk>(
            std::move(translator), nullptr);
        auto pin_requests =
            std::make_shared<std::vector<std::vector<int64_t>>>();
        auto column = std::make_shared<ScanCountingChunkedColumn>(
            std::move(slot), fm, pin_requests);
        return {std::static_pointer_cast<ChunkedColumnInterface>(column),
                std::move(fetched),
                std::move(pin_requests),
                std::static_pointer_cast<void>(buffers)};
    }
};

struct ProxyChunkColumnFactory {
    static ColumnFixture
    Create(const ColumnSpec& spec) {
        const auto n = spec.rows_per_chunk.size();
        auto buffers = std::make_shared<std::vector<std::vector<char>>>(n);
        std::vector<std::unique_ptr<GroupChunk>> group_chunks;
        group_chunks.reserve(n);
        int64_t start_logical_offset = 0;
        for (size_t i = 0; i < n; ++i) {
            (*buffers)[i] = BuildChunkBuffer(
                spec.rows_per_chunk[i],
                spec.valid_patterns.empty() ? nullptr : &spec.valid_patterns[i],
                spec.nullable,
                start_logical_offset,
                spec.dense_nullable_payload);
            std::shared_ptr<Chunk> chunk =
                MakeFixedChunk(spec.rows_per_chunk[i],
                               spec.nullable,
                               (*buffers)[i].data(),
                               (*buffers)[i].size());
            std::unordered_map<FieldId, std::shared_ptr<Chunk>> fields;
            fields[FieldId(kTestFieldId)] = std::move(chunk);
            group_chunks.push_back(std::make_unique<GroupChunk>(fields));
            start_logical_offset += spec.rows_per_chunk[i];
        }
        auto fetched = std::make_shared<std::set<cachinglayer::cid_t>>();
        auto translator = std::make_unique<CountingGroupChunkTranslator>(
            /*num_fields=*/1,
            spec.rows_per_chunk,
            "pcc_iface",
            std::move(group_chunks),
            fetched);
        auto group =
            std::make_shared<ChunkedColumnGroup>(std::move(translator));
        auto fm = MakeTestFieldMeta(spec);
        auto pin_requests =
            std::make_shared<std::vector<std::vector<int64_t>>>();
        auto column = std::make_shared<ScanCountingProxyChunkColumn>(
            group, FieldId(kTestFieldId), fm, pin_requests);
        return {std::static_pointer_cast<ChunkedColumnInterface>(column),
                std::move(fetched),
                std::move(pin_requests),
                std::static_pointer_cast<void>(buffers),
                std::move(group)};
    }
};

struct ChunkedVectorArrayColumnFactory {
    static VectorArrayColumnFixture
    Create() {
        auto buffers = std::make_shared<std::vector<std::vector<char>>>(1);
        (*buffers)[0] = BuildNullableVectorArrayChunkBuffer();
        std::vector<std::unique_ptr<Chunk>> chunks;
        chunks.push_back(
            MakeVectorArrayChunk((*buffers)[0].data(), (*buffers)[0].size()));

        auto translator = std::make_unique<TestChunkTranslator>(
            std::vector<int64_t>{kVectorArrayRows},
            "cc_vector_array_iface",
            std::move(chunks));
        FieldMeta fm(FieldName("va"),
                     FieldId(kVectorArrayFieldId),
                     DataType::VECTOR_ARRAY,
                     kVectorArrayDim,
                     knowhere::metric::L2,
                     /*nullable=*/true,
                     std::nullopt);
        auto slot = cachinglayer::Manager::GetInstance().CreateCacheSlot<Chunk>(
            std::move(translator), nullptr);
        auto column =
            MakeChunkedColumnBase(DataType::VECTOR_ARRAY, std::move(slot), fm);
        return {column, std::static_pointer_cast<void>(buffers)};
    }
};

struct ProxyVectorArrayColumnFactory {
    static VectorArrayColumnFixture
    Create() {
        auto buffers = std::make_shared<std::vector<std::vector<char>>>(1);
        (*buffers)[0] = BuildNullableVectorArrayChunkBuffer();
        std::unordered_map<FieldId, std::shared_ptr<Chunk>> fields;
        fields[FieldId(kVectorArrayFieldId)] =
            MakeVectorArrayChunk((*buffers)[0].data(), (*buffers)[0].size());

        std::vector<std::unique_ptr<GroupChunk>> group_chunks;
        group_chunks.push_back(std::make_unique<GroupChunk>(fields));
        auto translator = std::make_unique<TestGroupChunkTranslator>(
            /*num_fields=*/1,
            std::vector<int64_t>{kVectorArrayRows},
            "pcc_vector_array_iface",
            std::move(group_chunks));
        auto group =
            std::make_shared<ChunkedColumnGroup>(std::move(translator));
        FieldMeta fm(FieldName("va"),
                     FieldId(kVectorArrayFieldId),
                     DataType::VECTOR_ARRAY,
                     kVectorArrayDim,
                     knowhere::metric::L2,
                     /*nullable=*/true,
                     std::nullopt);
        auto column = std::make_shared<ProxyChunkColumn>(
            group, FieldId(kVectorArrayFieldId), fm);
        return {std::static_pointer_cast<ChunkedColumnInterface>(column),
                std::static_pointer_cast<void>(buffers)};
    }
};

bool
IsScanRowValid(const ChunkedColumnInterface::ScanBatch& batch, int64_t offset) {
    return !batch.validity || batch.validity[offset];
}

}  // namespace

template <typename Factory>
class ChunkedColumnInterfaceTest : public ::testing::Test {};

using Factories =
    ::testing::Types<ChunkedColumnFactory, ProxyChunkColumnFactory>;
TYPED_TEST_SUITE(ChunkedColumnInterfaceTest, Factories);

TYPED_TEST(ChunkedColumnInterfaceTest, BuildValidRowIdsBuildsFullMapping) {
    ColumnSpec spec{{5, 3, 4},
                    {{true, false, true, true, false},
                     {false, false, false},
                     {true, true, true, true}},
                    true};

    auto fx = TypeParam::Create(spec);
    EXPECT_TRUE(fx.fetched->empty());

    fx.column->BuildValidRowIds(nullptr);
    EXPECT_EQ(fx.fetched->size(), 3u);
    EXPECT_EQ(fx.fetched->count(0), 1u);
    EXPECT_EQ(fx.fetched->count(1), 1u);
    EXPECT_EQ(fx.fetched->count(2), 1u);

    EXPECT_EQ(fx.column->GetValidCountInChunk(0), 3);
    EXPECT_EQ(fx.column->GetValidCountInChunk(1), 0);
    EXPECT_EQ(fx.column->GetValidCountInChunk(2), 4);

    const auto& m = fx.column->GetOffsetMapping();
    EXPECT_TRUE(m.IsEnabled());
    EXPECT_EQ(m.GetTotalCount(), 12);
    EXPECT_EQ(m.GetValidCount(), 7);
    EXPECT_EQ(m.GetPhysicalOffset(0), 0);
    EXPECT_EQ(m.GetPhysicalOffset(2), 1);
    EXPECT_EQ(m.GetPhysicalOffset(3), 2);
    EXPECT_EQ(m.GetPhysicalOffset(1), -1);
    EXPECT_EQ(m.GetPhysicalOffset(4), -1);
    EXPECT_EQ(m.GetPhysicalOffset(8), 3);
    EXPECT_EQ(m.GetPhysicalOffset(9), 4);
    EXPECT_EQ(m.GetPhysicalOffset(10), 5);
    EXPECT_EQ(m.GetPhysicalOffset(11), 6);
}

TYPED_TEST(ChunkedColumnInterfaceTest, CellsLoadedDoesNotFetchCells) {
    ColumnSpec spec{{5, 3, 4}, {}, /*nullable=*/false};
    auto fx = TypeParam::Create(spec);

    const int64_t offsets[] = {0, 6};

    EXPECT_FALSE(fx.column->CellsLoaded(offsets, std::size(offsets)));
    EXPECT_TRUE(fx.fetched->empty());
}

TYPED_TEST(ChunkedColumnInterfaceTest, CellsLoadedTracksFetchedCells) {
    ColumnSpec spec{{5, 3, 4}, {}, /*nullable=*/false};
    auto fx = TypeParam::Create(spec);

    const int64_t chunk0_offsets[] = {0, 4};
    const int64_t chunk1_offsets[] = {6};
    const int64_t mixed_offsets[] = {0, 6};

    std::vector<int32_t> values(std::size(chunk1_offsets));
    fx.column->BulkVectorValueAt(nullptr,
                                 values.data(),
                                 chunk1_offsets,
                                 kElementSize,
                                 std::size(chunk1_offsets));

    EXPECT_TRUE(
        fx.column->CellsLoaded(chunk1_offsets, std::size(chunk1_offsets)));
    EXPECT_FALSE(
        fx.column->CellsLoaded(chunk0_offsets, std::size(chunk0_offsets)));
    EXPECT_FALSE(
        fx.column->CellsLoaded(mixed_offsets, std::size(mixed_offsets)));
    EXPECT_EQ(fx.fetched->size(), 1u);
    EXPECT_EQ(fx.fetched->count(1), 1u);
}

TYPED_TEST(ChunkedColumnInterfaceTest, BuildValidRowIdsNonNullableIsNoop) {
    ColumnSpec spec{{5, 5}, {}, /*nullable=*/false};
    auto fx = TypeParam::Create(spec);

    fx.column->BuildValidRowIds(nullptr);

    EXPECT_FALSE(fx.column->GetOffsetMapping().IsEnabled());
    EXPECT_EQ(fx.column->GetValidCountInChunk(0), 5);
    EXPECT_TRUE(fx.fetched->empty());
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           BulkIsValidWithoutOffsetsTraversesNullableColumn) {
    ColumnSpec spec{{3, 2},
                    {{true, false, true}, {false, true}},
                    /*nullable=*/true};
    auto fx = TypeParam::Create(spec);

    std::vector<std::pair<size_t, bool>> validity;
    fx.column->BulkIsValid(
        nullptr,
        [&](bool valid, size_t offset) {
            validity.emplace_back(offset, valid);
        },
        nullptr,
        0);

    EXPECT_EQ(validity,
              (std::vector<std::pair<size_t, bool>>{
                  {0, true},
                  {1, false},
                  {2, true},
                  {3, false},
                  {4, true},
              }));
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           BulkVectorValueAtDefaultsToLogicalOffsetsForNullableColumn) {
    ColumnSpec spec{{5, 3, 4},
                    {{true, false, true, true, false},
                     {false, false, false},
                     {true, true, true, true}},
                    true};
    auto fx = TypeParam::Create(spec);

    const int64_t offsets[] = {0, 2, 3, 8, 11};
    std::vector<int32_t> values(std::size(offsets));
    fx.column->BulkVectorValueAt(
        nullptr, values.data(), offsets, kElementSize, std::size(offsets));

    EXPECT_EQ(values, (std::vector<int32_t>{0, 2, 3, 8, 11}));
    EXPECT_FALSE(fx.column->GetOffsetMapping().IsEnabled());
    EXPECT_TRUE(fx.column->GetValidData().empty());
    EXPECT_EQ(fx.fetched->size(), 2u);
    EXPECT_EQ(fx.fetched->count(0), 1u);
    EXPECT_EQ(fx.fetched->count(2), 1u);
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           BulkValueAtDefaultsToLogicalOffsetsForNullableColumn) {
    ColumnSpec spec{{5, 3, 4},
                    {{true, false, true, true, false},
                     {false, false, false},
                     {true, true, true, true}},
                    true};
    auto fx = TypeParam::Create(spec);

    const int64_t offsets[] = {0, 2, 3, 8, 11};
    std::vector<int32_t> values;
    fx.column->BulkValueAt(
        nullptr,
        [&](const char* value, size_t i) {
            int32_t decoded = 0;
            std::memcpy(&decoded, value, sizeof(decoded));
            values.push_back(decoded);
        },
        offsets,
        std::size(offsets));

    EXPECT_EQ(values, (std::vector<int32_t>{0, 2, 3, 8, 11}));
    EXPECT_FALSE(fx.column->GetOffsetMapping().IsEnabled());
    EXPECT_TRUE(fx.column->GetValidData().empty());
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           BulkVectorValueAtLogicalOffsetRejectsNullRow) {
    ColumnSpec spec{{5}, {{true, false, true, true, false}}, true};
    auto fx = TypeParam::Create(spec);

    const int64_t null_offset[] = {1};
    int32_t value = 0;
    EXPECT_THROW(fx.column->BulkVectorValueAt(
                     nullptr, &value, null_offset, kElementSize, 1),
                 std::exception);
    EXPECT_FALSE(fx.column->GetOffsetMapping().IsEnabled());
}

template <typename Factory>
class VectorArrayColumnInterfaceTest : public ::testing::Test {};

using VectorArrayFactories = ::testing::Types<ChunkedVectorArrayColumnFactory,
                                              ProxyVectorArrayColumnFactory>;
TYPED_TEST_SUITE(VectorArrayColumnInterfaceTest, VectorArrayFactories);

TYPED_TEST(VectorArrayColumnInterfaceTest,
           BulkVectorArrayAtUsesLogicalOffsetsForNullableRows) {
    auto fx = TypeParam::Create();

    const int64_t offsets[] = {0, 2, 3};
    std::vector<VectorFieldProto> values(std::size(offsets));
    fx.column->BulkVectorArrayAt(
        nullptr,
        [&](VectorFieldProto&& value, size_t i) {
            values[i] = std::move(value);
        },
        offsets,
        std::size(offsets));

    ASSERT_EQ(values[0].float_vector().data().size(), 2);
    EXPECT_FLOAT_EQ(values[0].float_vector().data(0), 1.0F);
    EXPECT_FLOAT_EQ(values[0].float_vector().data(1), 2.0F);
    EXPECT_EQ(values[1].float_vector().data().size(), 0);
    ASSERT_EQ(values[2].float_vector().data().size(), 4);
    EXPECT_FLOAT_EQ(values[2].float_vector().data(0), 3.0F);
    EXPECT_FLOAT_EQ(values[2].float_vector().data(1), 4.0F);
    EXPECT_FLOAT_EQ(values[2].float_vector().data(2), 5.0F);
    EXPECT_FLOAT_EQ(values[2].float_vector().data(3), 6.0F);

    bool valid = true;
    fx.column->BulkIsValid(
        nullptr,
        [&](bool is_valid, size_t) { valid = is_valid; },
        offsets + 1,
        1);
    EXPECT_TRUE(valid);

    const int64_t null_offset[] = {1};
    EXPECT_THROW(
        fx.column->BulkVectorArrayAt(
            nullptr, [](VectorFieldProto&&, size_t) {}, null_offset, 1),
        std::exception);
}

TYPED_TEST(VectorArrayColumnInterfaceTest,
           DataScanReturnsLogicalVectorArrayViews) {
    auto fx = TypeParam::Create();

    auto cursor = fx.column->Scan(
        nullptr,
        ChunkedColumnInterface::ScanOptions::ForData(
            0, ChunkedColumnInterface::TargetType::VectorArrayView));
    ASSERT_NE(cursor, nullptr);

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(
        4, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_EQ(batch.row_id_start, 0);
    EXPECT_EQ(batch.size, 4);
    EXPECT_EQ(batch.values.target_type,
              ChunkedColumnInterface::TargetType::VectorArrayView);

    const auto* views = batch.values.data_as<VectorArrayView>();
    EXPECT_EQ(views[0].length(), 1);
    EXPECT_EQ(views[2].length(), 0);
    EXPECT_EQ(views[3].length(), 2);
    EXPECT_TRUE(IsScanRowValid(batch, 0));
    EXPECT_FALSE(IsScanRowValid(batch, 1));
    EXPECT_TRUE(IsScanRowValid(batch, 2));
    EXPECT_TRUE(IsScanRowValid(batch, 3));
}

TYPED_TEST(VectorArrayColumnInterfaceTest,
           ValidityOnlyScanUsesColumnTypeWithoutBuildingValues) {
    auto fx = TypeParam::Create();

    auto cursor =
        fx.column->Scan(nullptr,
                        ChunkedColumnInterface::ScanOptions::ForData(
                            0, ChunkedColumnInterface::TargetType::None));
    ASSERT_NE(cursor, nullptr);

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(
        2, ChunkedColumnInterface::ScanReadMode::ValidityOnly, &batch));
    EXPECT_TRUE(batch.values.empty());
    EXPECT_EQ(batch.size, 2);
    EXPECT_TRUE(IsScanRowValid(batch, 0));
    EXPECT_FALSE(IsScanRowValid(batch, 1));
    ASSERT_NE(batch.owner, nullptr);

    ASSERT_TRUE(cursor->Next(
        2, ChunkedColumnInterface::ScanReadMode::ValidityOnly, &batch));
    EXPECT_EQ(batch.row_id_start, 2);
    EXPECT_EQ(batch.size, 2);
    EXPECT_TRUE(IsScanRowValid(batch, 0));
    EXPECT_TRUE(IsScanRowValid(batch, 1));
    ASSERT_NE(batch.owner, nullptr);
}

TYPED_TEST(VectorArrayColumnInterfaceTest,
           DataScanRejectsMismatchedTargetType) {
    auto fx = TypeParam::Create();

    EXPECT_THROW(
        fx.column->Scan(nullptr,
                        ChunkedColumnInterface::ScanOptions::ForData(
                            0, ChunkedColumnInterface::TargetType::Int32)),
        std::exception);
}

TYPED_TEST(ChunkedColumnInterfaceTest, RawFormatScanDoesNotSupportUnaryRowIds) {
    ColumnSpec spec{{5}, {{true, false, true, true, false}}, true};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    proto::plan::GenericValue value;
    value.set_int64_val(3);
    auto options = ChunkedColumnInterface::ScanOptions::ForUnary(
        0, proto::plan::OpType::Equal, value);

    EXPECT_FALSE(fx.column->SupportsScanPushdown(options));
    EXPECT_EQ(fx.column->Scan(nullptr, options), nullptr);
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           RawFormatScanDoesNotSupportBinaryRangeRowIds) {
    ColumnSpec spec{{5}, {{true, false, true, true, false}}, true};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    proto::plan::GenericValue lower;
    proto::plan::GenericValue upper;
    lower.set_int64_val(1);
    upper.set_int64_val(10);
    auto options = ChunkedColumnInterface::ScanOptions::ForBinaryRange(
        0, lower, true, upper, true);

    EXPECT_FALSE(fx.column->SupportsScanPushdown(options));
    EXPECT_EQ(fx.column->Scan(nullptr, options), nullptr);
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           FixedWidthDataScanReturnsNaturalChunkBatches) {
    ColumnSpec spec{{3, 2},
                    {{true, false, true}, {false, true}},
                    /*nullable=*/true};
    spec.data_type = DataType::INT32;
    spec.dense_nullable_payload = true;
    auto fx = TypeParam::Create(spec);

    auto cursor =
        fx.column->Scan(nullptr,
                        ChunkedColumnInterface::ScanOptions::ForData(
                            1, ChunkedColumnInterface::TargetType::Int32));
    ASSERT_NE(cursor, nullptr);
    EXPECT_TRUE(fx.pin_requests->empty());
    EXPECT_TRUE(fx.fetched->empty());

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(
        4, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_EQ(batch.row_id_start, 1);
    EXPECT_EQ(batch.size, 2);
    ASSERT_FALSE(batch.values.empty());
    EXPECT_EQ(batch.values.target_type,
              ChunkedColumnInterface::TargetType::Int32);
    EXPECT_EQ(batch.values.data_as<int32_t>()[1], 2);
    EXPECT_FALSE(IsScanRowValid(batch, 0));
    EXPECT_TRUE(IsScanRowValid(batch, 1));
    ASSERT_TRUE(cursor->Next(
        2, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_EQ(batch.row_id_start, 3);
    EXPECT_EQ(batch.size, 2);
    EXPECT_EQ(batch.values.data_as<int32_t>()[1], 4);
    EXPECT_FALSE(IsScanRowValid(batch, 0));
    EXPECT_TRUE(IsScanRowValid(batch, 1));

    EXPECT_EQ(fx.pin_requests->size(), 2u);
    EXPECT_EQ((*fx.pin_requests)[0], (std::vector<int64_t>{0}));
    EXPECT_EQ((*fx.pin_requests)[1], (std::vector<int64_t>{1}));
    EXPECT_EQ(*fx.fetched, (std::set<cachinglayer::cid_t>{0, 1}));
}

TYPED_TEST(ChunkedColumnInterfaceTest, PrefetchOptionBatchWarmsRemainingCells) {
    ColumnSpec spec{{3, 2},
                    {{true, false, true}, {false, true}},
                    /*nullable=*/true};
    spec.data_type = DataType::INT32;
    spec.dense_nullable_payload = true;
    auto fx = TypeParam::Create(spec);

    // Without prefetch, creation loads nothing.
    auto plain =
        fx.column->Scan(nullptr,
                        ChunkedColumnInterface::ScanOptions::ForData(
                            0, ChunkedColumnInterface::TargetType::Int32));
    ASSERT_NE(plain, nullptr);
    EXPECT_TRUE(fx.fetched->empty());

    // With prefetch, creation batch-warms every remaining cell.
    auto cursor =
        fx.column->Scan(nullptr,
                        ChunkedColumnInterface::ScanOptions::ForData(
                            0,
                            ChunkedColumnInterface::TargetType::Int32,
                            ChunkedColumnInterface::ScanPinPolicy::ResultOwned,
                            /*prefetch=*/true));
    ASSERT_NE(cursor, nullptr);
    EXPECT_EQ(*fx.fetched, (std::set<cachinglayer::cid_t>{0, 1}));
    // Warmup loads cells but does not retain pins; reads still pin per Cell.
    EXPECT_TRUE(fx.pin_requests->empty());

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(
        5, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_EQ(batch.row_id_start, 0);
    EXPECT_EQ(batch.size, 3);
    EXPECT_EQ(fx.pin_requests->size(), 1u);
    EXPECT_EQ(fx.pin_requests->front(), (std::vector<int64_t>{0}));

    ASSERT_TRUE(cursor->Next(
        2, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_EQ(batch.row_id_start, 3);
    EXPECT_EQ(batch.size, 2);
    EXPECT_EQ(fx.pin_requests->size(), 2u);
    EXPECT_EQ(fx.pin_requests->back(), (std::vector<int64_t>{1}));
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           PrefetchRetainsFilterDecisionsAndWarmsNullableSkippedCells) {
    auto make_filter = [](const std::shared_ptr<std::vector<int>>& decisions) {
        return std::make_shared<const detail::ColumnFilter>(
            detail::ColumnFilter::MetricsSource::PreloadedStatistics,
            [decisions](int64_t cell_id) {
                ++(*decisions)[cell_id];
                return cell_id == 0;
            });
    };

    {
        ColumnSpec spec{{2, 2}, {}, /*nullable=*/false};
        spec.data_type = DataType::INT32;
        auto fx = TypeParam::Create(spec);
        auto decisions = std::make_shared<std::vector<int>>(2, 0);
        auto options = ChunkedColumnInterface::ScanOptions::ForData(
            0,
            ChunkedColumnInterface::TargetType::Int32,
            ChunkedColumnInterface::ScanPinPolicy::ResultOwned,
            /*prefetch=*/true);
        options.filter = make_filter(decisions);

        auto cursor = fx.column->Scan(nullptr, options);
        ASSERT_NE(cursor, nullptr);
        EXPECT_EQ(*decisions, (std::vector<int>{1, 1}));
        EXPECT_EQ(*fx.fetched, (std::set<cachinglayer::cid_t>{1}));

        ChunkedColumnInterface::ScanBatch batch;
        ASSERT_TRUE(cursor->Next(
            4, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
        EXPECT_TRUE(batch.data_skipped);
        ASSERT_TRUE(cursor->Next(
            2, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
        EXPECT_FALSE(batch.data_skipped);
        EXPECT_EQ(*decisions, (std::vector<int>{1, 1}));
    }

    {
        ColumnSpec spec{{2, 2},
                        {{false, true}, {true, true}},
                        /*nullable=*/true};
        spec.data_type = DataType::INT32;
        spec.dense_nullable_payload = true;
        auto fx = TypeParam::Create(spec);
        auto decisions = std::make_shared<std::vector<int>>(2, 0);
        auto options = ChunkedColumnInterface::ScanOptions::ForData(
            0,
            ChunkedColumnInterface::TargetType::Int32,
            ChunkedColumnInterface::ScanPinPolicy::ResultOwned,
            /*prefetch=*/true);
        options.filter = make_filter(decisions);

        auto cursor = fx.column->Scan(nullptr, options);
        ASSERT_NE(cursor, nullptr);
        EXPECT_EQ(*decisions, (std::vector<int>{1, 1}));
        EXPECT_EQ(*fx.fetched, (std::set<cachinglayer::cid_t>{0, 1}));

        ChunkedColumnInterface::ScanBatch batch;
        ASSERT_TRUE(cursor->Next(
            4, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
        EXPECT_TRUE(batch.data_skipped);
        EXPECT_FALSE(IsScanRowValid(batch, 0));
        EXPECT_TRUE(IsScanRowValid(batch, 1));
        EXPECT_EQ(*decisions, (std::vector<int>{1, 1}));
    }
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           ScanNextClampsLengthToRemainingSegmentRows) {
    ColumnSpec spec{{3, 2}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    auto cursor =
        fx.column->Scan(nullptr,
                        ChunkedColumnInterface::ScanOptions::ForData(
                            4, ChunkedColumnInterface::TargetType::Int32));
    ASSERT_NE(cursor, nullptr);

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(
        1024, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_EQ(batch.row_id_start, 4);
    EXPECT_EQ(batch.size, 1);
    EXPECT_EQ(batch.values.data_as<int32_t>()[0], 4);
    EXPECT_EQ(cursor->Position(), 5);
    EXPECT_FALSE(cursor->Next(
        1024, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
}

TYPED_TEST(ChunkedColumnInterfaceTest, PrefetchOptionSkipsZeroRowCells) {
    ColumnSpec spec{{2, 0, 2},
                    {{true, true}, {}, {true, true}},
                    /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    auto cursor =
        fx.column->Scan(nullptr,
                        ChunkedColumnInterface::ScanOptions::ForData(
                            0,
                            ChunkedColumnInterface::TargetType::Int32,
                            ChunkedColumnInterface::ScanPinPolicy::ResultOwned,
                            /*prefetch=*/true));
    ASSERT_NE(cursor, nullptr);
    // Only non-empty remaining cells are warmed; the zero-row cell is skipped.
    EXPECT_EQ(*fx.fetched, (std::set<cachinglayer::cid_t>{0, 2}));
    EXPECT_TRUE(fx.pin_requests->empty());
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           FixedWidthValidityOnlyScanReturnsOnlyValidity) {
    ColumnSpec spec{{4},
                    {{true, false, true, false}},
                    /*nullable=*/true};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    auto cursor =
        fx.column->Scan(nullptr,
                        ChunkedColumnInterface::ScanOptions::ForData(
                            0, ChunkedColumnInterface::TargetType::None));
    ASSERT_NE(cursor, nullptr);
    EXPECT_TRUE(fx.pin_requests->empty());

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(
        4, ChunkedColumnInterface::ScanReadMode::ValidityOnly, &batch));
    EXPECT_EQ(batch.row_id_start, 0);
    EXPECT_EQ(batch.size, 4);
    EXPECT_TRUE(batch.values.empty());
    EXPECT_TRUE(IsScanRowValid(batch, 0));
    EXPECT_FALSE(IsScanRowValid(batch, 1));
    EXPECT_TRUE(IsScanRowValid(batch, 2));
    EXPECT_FALSE(IsScanRowValid(batch, 3));
    EXPECT_EQ(fx.pin_requests->size(), 1u);
    EXPECT_EQ(fx.pin_requests->front(), (std::vector<int64_t>{0}));
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           FixedWidthDataScanPinIsOwnedByResultByDefault) {
    ColumnSpec spec{{5}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    auto cursor =
        fx.column->Scan(nullptr,
                        ChunkedColumnInterface::ScanOptions::ForData(
                            1, ChunkedColumnInterface::TargetType::Int32));
    ASSERT_NE(cursor, nullptr);
    EXPECT_TRUE(fx.pin_requests->empty());

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(
        2, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_EQ(batch.row_id_start, 1);
    EXPECT_EQ(batch.size, 2);
    EXPECT_NE(batch.owner, nullptr);

    ASSERT_TRUE(cursor->Next(
        1, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_EQ(batch.row_id_start, 3);
    EXPECT_EQ(batch.size, 1);

    ASSERT_TRUE(cursor->Next(
        1, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_EQ(batch.row_id_start, 4);
    EXPECT_EQ(batch.size, 1);
    EXPECT_EQ(fx.pin_requests->size(), 3u);
    EXPECT_EQ((*fx.pin_requests)[0], (std::vector<int64_t>{0}));
    EXPECT_EQ((*fx.pin_requests)[1], (std::vector<int64_t>{0}));
    EXPECT_EQ((*fx.pin_requests)[2], (std::vector<int64_t>{0}));
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           FixedWidthDataScanPinCanBeOwnedByCursor) {
    ColumnSpec spec{{5}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    auto cursor = fx.column->Scan(
        nullptr,
        ChunkedColumnInterface::ScanOptions::ForData(
            0,
            ChunkedColumnInterface::TargetType::Int32,
            ChunkedColumnInterface::ScanPinPolicy::CursorOwned));
    ASSERT_NE(cursor, nullptr);
    EXPECT_TRUE(fx.pin_requests->empty());

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(
        2, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_EQ(batch.owner, nullptr);
    ASSERT_EQ(fx.pin_requests->size(), 1u);
    EXPECT_EQ(fx.pin_requests->front(), (std::vector<int64_t>{0}));

    ASSERT_TRUE(cursor->Next(
        2, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_EQ(fx.pin_requests->size(), 1u);

    ASSERT_TRUE(cursor->Next(
        1, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_EQ(fx.pin_requests->size(), 1u);
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           CursorOwnedScanReleasesPreviousPinForSkippedCell) {
    ColumnSpec spec{{2, 2}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);
    auto filter = std::make_shared<const detail::ColumnFilter>(
        detail::ColumnFilter::MetricsSource::PreloadedStatistics,
        [](int64_t cell_id) { return cell_id == 1; });
    auto options = ChunkedColumnInterface::ScanOptions::ForData(
        0,
        ChunkedColumnInterface::TargetType::Int32,
        ChunkedColumnInterface::ScanPinPolicy::CursorOwned);
    options.filter = std::move(filter);
    auto cursor = fx.column->Scan(nullptr, options);
    ASSERT_NE(cursor, nullptr);

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(
        2, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_FALSE(batch.data_skipped);
    const int64_t first_cell_offset[] = {0};
    ASSERT_TRUE(fx.column->CellsLoaded(first_cell_offset, 1));
    const auto evict = [&] {
        if (fx.group != nullptr) {
            fx.group->ManualEvictCache();
        } else {
            fx.column->ManualEvictCache();
        }
    };
    evict();
    EXPECT_TRUE(fx.column->CellsLoaded(first_cell_offset, 1));

    ASSERT_TRUE(cursor->Next(
        2, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_TRUE(batch.data_skipped);
    evict();
    EXPECT_FALSE(fx.column->CellsLoaded(first_cell_offset, 1));
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           CursorOwnedScanReturnsBeforeCrossingCells) {
    ColumnSpec spec{{3, 4}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    auto cursor = fx.column->Scan(
        nullptr,
        ChunkedColumnInterface::ScanOptions::ForData(
            0,
            ChunkedColumnInterface::TargetType::Int32,
            ChunkedColumnInterface::ScanPinPolicy::CursorOwned));
    ASSERT_NE(cursor, nullptr);

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(
        4, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_EQ(batch.row_id_start, 0);
    EXPECT_EQ(batch.size, 3);
    ASSERT_EQ(fx.pin_requests->size(), 1u);
    EXPECT_EQ(fx.pin_requests->front(), (std::vector<int64_t>{0}));

    ASSERT_TRUE(cursor->Next(
        3, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_EQ(batch.row_id_start, 3);
    EXPECT_EQ(batch.size, 3);
    EXPECT_EQ(fx.pin_requests->size(), 2u);
    EXPECT_EQ((*fx.pin_requests)[1], (std::vector<int64_t>{1}));
}

TYPED_TEST(ChunkedColumnInterfaceTest, ScanSkipsZeroRowCellsAtBoundaries) {
    ColumnSpec spec{{0, 0, 2, 0, 0, 3, 0, 0}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    auto cursor =
        fx.column->Scan(nullptr,
                        ChunkedColumnInterface::ScanOptions::ForData(
                            0, ChunkedColumnInterface::TargetType::Int32));
    ASSERT_NE(cursor, nullptr);

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(
        5, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_EQ(batch.row_id_start, 0);
    EXPECT_EQ(batch.size, 2);
    EXPECT_EQ(batch.values.data_as<int32_t>()[0], 0);
    EXPECT_EQ(batch.values.data_as<int32_t>()[1], 1);

    ASSERT_TRUE(cursor->Next(
        3, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_EQ(batch.row_id_start, 2);
    EXPECT_EQ(batch.size, 3);
    EXPECT_EQ(batch.values.data_as<int32_t>()[0], 2);
    EXPECT_EQ(batch.values.data_as<int32_t>()[2], 4);

    ASSERT_EQ(fx.pin_requests->size(), 2u);
    EXPECT_EQ((*fx.pin_requests)[0], (std::vector<int64_t>{2}));
    EXPECT_EQ((*fx.pin_requests)[1], (std::vector<int64_t>{5}));
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           CursorOwnedScanSkipsZeroRowCellsWithoutRepinning) {
    ColumnSpec spec{{0, 0, 2, 0, 0, 3, 0, 0}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    auto cursor = fx.column->Scan(
        nullptr,
        ChunkedColumnInterface::ScanOptions::ForData(
            0,
            ChunkedColumnInterface::TargetType::Int32,
            ChunkedColumnInterface::ScanPinPolicy::CursorOwned));
    ASSERT_NE(cursor, nullptr);

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(
        1, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    ASSERT_TRUE(cursor->Next(
        1, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    ASSERT_TRUE(cursor->Next(
        3, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_EQ(batch.row_id_start, 2);
    EXPECT_EQ(batch.size, 3);
    EXPECT_EQ(batch.values.data_as<int32_t>()[2], 4);

    ASSERT_EQ(fx.pin_requests->size(), 2u);
    EXPECT_EQ((*fx.pin_requests)[0], (std::vector<int64_t>{2}));
    EXPECT_EQ((*fx.pin_requests)[1], (std::vector<int64_t>{5}));
}

TYPED_TEST(ChunkedColumnInterfaceTest, SeekSkipsWithoutPinningSkippedCells) {
    ColumnSpec spec{{4, 4}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    auto cursor = fx.column->Scan(
        nullptr,
        ChunkedColumnInterface::ScanOptions::ForData(
            0,
            ChunkedColumnInterface::TargetType::Int32,
            ChunkedColumnInterface::ScanPinPolicy::CursorOwned));
    ASSERT_NE(cursor, nullptr);

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(
        2, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    ASSERT_EQ(fx.pin_requests->size(), 1u);
    EXPECT_EQ(fx.pin_requests->front(), (std::vector<int64_t>{0}));

    cursor->Seek(5);
    ASSERT_TRUE(cursor->Next(
        1, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    ASSERT_EQ(fx.pin_requests->size(), 2u);
    EXPECT_EQ((*fx.pin_requests)[1], (std::vector<int64_t>{1}));
}

TYPED_TEST(ChunkedColumnInterfaceTest, DataScanRejectsBackwardSeek) {
    ColumnSpec spec{{4}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    auto cursor =
        fx.column->Scan(nullptr,
                        ChunkedColumnInterface::ScanOptions::ForData(
                            0, ChunkedColumnInterface::TargetType::Int32));
    ASSERT_NE(cursor, nullptr);

    ChunkedColumnInterface::ScanBatch batch;
    cursor->Seek(1);
    ASSERT_TRUE(cursor->Next(
        2, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_THROW(cursor->Seek(2), std::exception);
}

TYPED_TEST(ChunkedColumnInterfaceTest, NonNullableScanRejectsValidityOnlyMode) {
    ColumnSpec spec{{3, 2}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    auto cursor =
        fx.column->Scan(nullptr,
                        ChunkedColumnInterface::ScanOptions::ForData(
                            0, ChunkedColumnInterface::TargetType::Int32));
    ASSERT_NE(cursor, nullptr);
    EXPECT_TRUE(fx.fetched->empty());
    EXPECT_TRUE(fx.pin_requests->empty());

    ChunkedColumnInterface::ScanBatch batch;
    EXPECT_THROW(
        cursor->Next(
            3, ChunkedColumnInterface::ScanReadMode::ValidityOnly, &batch),
        std::exception);
    EXPECT_TRUE(fx.fetched->empty());
    EXPECT_TRUE(fx.pin_requests->empty());
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           FixedWidthDataScanReportsAllValidForNonNullableColumn) {
    ColumnSpec spec{{3}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    auto cursor =
        fx.column->Scan(nullptr,
                        ChunkedColumnInterface::ScanOptions::ForData(
                            0, ChunkedColumnInterface::TargetType::Int32));
    ASSERT_NE(cursor, nullptr);

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(
        3, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &batch));
    EXPECT_FALSE(batch.validity);
    for (int64_t i = 0; i < batch.size; ++i) {
        EXPECT_TRUE(IsScanRowValid(batch, i));
    }
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           FixedWidthTakePreservesOrderDuplicatesAcrossChunks) {
    ColumnSpec spec{{3, 2}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    const FixedVector<int32_t> offsets{2, 0, 0, 4, 3, 1};
    auto take = fx.column->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            ChunkedColumnInterface::OffsetView::From(
                offsets.data(), static_cast<int64_t>(offsets.size())),
            ChunkedColumnInterface::TargetType::Int32});
    ASSERT_NE(take, nullptr);
    ASSERT_EQ(take->Size(), static_cast<int64_t>(offsets.size()));
    EXPECT_TRUE(fx.pin_requests->empty());

    std::vector<int32_t> actual;
    for (int64_t i = 0; i < take->Size(); ++i) {
        const auto item = take->template Get<int32_t>(i);
        ASSERT_TRUE(item.value.has_value());
        actual.emplace_back(*item.value);
        EXPECT_TRUE(item.is_valid);
        EXPECT_FALSE(item.data_skipped);
    }

    EXPECT_EQ(actual, (std::vector<int32_t>{2, 0, 0, 4, 3, 1}));
    ASSERT_EQ(fx.pin_requests->size(), 3u);
    EXPECT_EQ((*fx.pin_requests)[0], (std::vector<int64_t>{0}));
    EXPECT_EQ((*fx.pin_requests)[1], (std::vector<int64_t>{1}));
    EXPECT_EQ((*fx.pin_requests)[2], (std::vector<int64_t>{0}));
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           TakeKeepsSkippedRowsAlignedWithoutPinningNonNullableData) {
    ColumnSpec spec{{2, 2}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);
    auto filter = std::make_shared<const detail::ColumnFilter>(
        detail::ColumnFilter::MetricsSource::PreloadedStatistics,
        [](int64_t cell_id) { return cell_id == 0; });

    const FixedVector<int32_t> offsets{0, 3, 1};
    auto take = fx.column->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            ChunkedColumnInterface::OffsetView::From(
                offsets.data(), static_cast<int64_t>(offsets.size())),
            ChunkedColumnInterface::TargetType::Int32,
            std::move(filter)});
    ASSERT_NE(take, nullptr);

    const auto skipped_first = take->template Get<int32_t>(0);
    EXPECT_TRUE(skipped_first.is_valid);
    EXPECT_TRUE(skipped_first.data_skipped);
    EXPECT_FALSE(skipped_first.value.has_value());
    EXPECT_TRUE(fx.pin_requests->empty());

    const auto active = take->template Get<int32_t>(1);
    ASSERT_TRUE(active.value.has_value());
    EXPECT_EQ(*active.value, 3);
    EXPECT_FALSE(active.data_skipped);

    const auto skipped_last = take->template Get<int32_t>(2);
    EXPECT_TRUE(skipped_last.data_skipped);
    EXPECT_FALSE(skipped_last.value.has_value());
    ASSERT_EQ(fx.pin_requests->size(), 1u);
    EXPECT_EQ((*fx.pin_requests)[0], (std::vector<int64_t>{1}));

    const auto owned = take->GetOwn();
    ASSERT_TRUE(owned.data_skipped);
    EXPECT_TRUE(owned.data_skipped[0]);
    EXPECT_FALSE(owned.data_skipped[1]);
    EXPECT_TRUE(owned.data_skipped[2]);
    // Materializing the aligned result still must not pin the skipped Cell.
    ASSERT_EQ(fx.pin_requests->size(), 1u);
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           TakeSkippedNullableRowsStillReadRealValidity) {
    ColumnSpec spec{{2, 2}, {{false, true}, {true, true}}, /*nullable=*/true};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);
    auto filter = std::make_shared<const detail::ColumnFilter>(
        detail::ColumnFilter::MetricsSource::PreloadedStatistics,
        [](int64_t cell_id) { return cell_id == 0; });

    const FixedVector<int32_t> offsets{0, 1, 3};
    auto take = fx.column->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            ChunkedColumnInterface::OffsetView::From(
                offsets.data(), static_cast<int64_t>(offsets.size())),
            ChunkedColumnInterface::TargetType::Int32,
            std::move(filter)});
    ASSERT_NE(take, nullptr);

    const auto null_skipped = take->template Get<int32_t>(0);
    EXPECT_FALSE(null_skipped.is_valid);
    EXPECT_TRUE(null_skipped.data_skipped);
    EXPECT_FALSE(null_skipped.value.has_value());

    const auto valid_skipped = take->template Get<int32_t>(1);
    EXPECT_TRUE(valid_skipped.is_valid);
    EXPECT_TRUE(valid_skipped.data_skipped);
    EXPECT_FALSE(valid_skipped.value.has_value());

    const auto active = take->template Get<int32_t>(2);
    ASSERT_TRUE(active.value.has_value());
    EXPECT_EQ(*active.value, 3);
    ASSERT_EQ(fx.pin_requests->size(), 2u);
    EXPECT_EQ((*fx.pin_requests)[0], (std::vector<int64_t>{0}));
    EXPECT_EQ((*fx.pin_requests)[1], (std::vector<int64_t>{1}));
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           ScanReturnsExplicitSkippedBatchWithoutExposingCellPlan) {
    ColumnSpec spec{{2, 2}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);
    auto options = ChunkedColumnInterface::ScanOptions::ForData(
        0, ChunkedColumnInterface::TargetType::Int32);
    options.filter = std::make_shared<const detail::ColumnFilter>(
        detail::ColumnFilter::MetricsSource::PreloadedStatistics,
        [](int64_t cell_id) { return cell_id == 0; });
    auto cursor = fx.column->Scan(nullptr, options);
    ASSERT_NE(cursor, nullptr);

    ChunkedColumnInterface::ScanBatch skipped;
    ASSERT_TRUE(cursor->Next(
        4, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &skipped));
    EXPECT_EQ(skipped.row_id_start, 0);
    EXPECT_EQ(skipped.size, 2);
    EXPECT_TRUE(skipped.data_skipped);
    EXPECT_TRUE(skipped.values.empty());
    EXPECT_TRUE(fx.pin_requests->empty());

    ChunkedColumnInterface::ScanBatch active;
    ASSERT_TRUE(cursor->Next(
        2, ChunkedColumnInterface::ScanReadMode::DataAndValidity, &active));
    EXPECT_EQ(active.row_id_start, 2);
    EXPECT_EQ(active.size, 2);
    EXPECT_FALSE(active.data_skipped);
    EXPECT_EQ(active.values.data_as<int32_t>()[0], 2);
    ASSERT_EQ(fx.pin_requests->size(), 1u);
    EXPECT_EQ((*fx.pin_requests)[0], (std::vector<int64_t>{1}));
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           TakePreservesRequestedOrderWithoutEagerPin) {
    ColumnSpec spec{{3, 2}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    const FixedVector<int32_t> offsets{4, 1, 3, 1};
    EXPECT_TRUE(fx.pin_requests->empty());

    auto take = fx.column->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            ChunkedColumnInterface::OffsetView::From(
                offsets.data(), static_cast<int64_t>(offsets.size())),
            ChunkedColumnInterface::TargetType::Int32});
    ASSERT_NE(take, nullptr);
    ASSERT_EQ(take->Size(), static_cast<int64_t>(offsets.size()));
    EXPECT_TRUE(fx.pin_requests->empty());

    const auto items = take->template Access<int32_t>();
    ASSERT_EQ(items.Size(), static_cast<int64_t>(offsets.size()));
    std::vector<int32_t> actual;
    for (int64_t i = 0; i < items.Size(); ++i) {
        actual.emplace_back(*items[i].value);
    }
    EXPECT_EQ(actual, (std::vector<int32_t>{4, 1, 3, 1}));
}

TEST(ColumnPlannerTest, LocatesSegmentOffsets) {
    ColumnPlanner planner(std::vector<int64_t>{0, 3, 5});
    EXPECT_EQ(planner.NumCells(), 2);
    EXPECT_EQ(planner.NumRows(), 5);
    EXPECT_EQ(planner.CellBoundaries(), (std::vector<int64_t>{0, 3, 5}));

    const auto last = planner.Locate(4);
    EXPECT_EQ(last.cell_id, 1);
    EXPECT_EQ(last.cell_offset, 1);
    const auto first = planner.Locate(1);
    EXPECT_EQ(first.cell_id, 0);
    EXPECT_EQ(first.cell_offset, 1);
    const auto boundary = planner.Locate(3);
    EXPECT_EQ(boundary.cell_id, 1);
    EXPECT_EQ(boundary.cell_offset, 0);
}

TEST(ColumnPlannerTest, BorrowsStableBoundariesAndOwnsTemporaryBoundaries) {
    const std::vector<int64_t> stable_boundaries{0, 3, 5};
    ColumnPlanner borrowed(stable_boundaries);
    EXPECT_EQ(borrowed.CellBoundaries().data(), stable_boundaries.data());

    ColumnPlanner owned(std::vector<int64_t>{0, 2, 5});
    EXPECT_EQ(owned.CellBoundaries(), (std::vector<int64_t>{0, 2, 5}));
    EXPECT_EQ(owned.NumRows(), 5);
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           FixedWidthTakeReadsValidityByLogicalPosition) {
    ColumnSpec spec{{4}, {{true, false, true, false}}, /*nullable=*/true};
    spec.data_type = DataType::INT32;
    spec.dense_nullable_payload = true;
    auto fx = TypeParam::Create(spec);

    const FixedVector<int32_t> offsets{3, 0, 1, 2};
    auto take = fx.column->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            ChunkedColumnInterface::OffsetView::From(
                offsets.data(), static_cast<int64_t>(offsets.size())),
            ChunkedColumnInterface::TargetType::Int32});
    ASSERT_NE(take, nullptr);
    ASSERT_EQ(take->Size(), 4);
    EXPECT_FALSE(take->IsValid(0));
    EXPECT_TRUE(take->IsValid(1));
    EXPECT_FALSE(take->IsValid(2));
    EXPECT_TRUE(take->IsValid(3));
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           RawTakeLazyPinAccessorKeepsColumnGenerationAlive) {
    ColumnSpec spec{{2, 2}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    const FixedVector<int32_t> offsets{3, 2, 0};
    auto take = fx.column->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            ChunkedColumnInterface::OffsetView::From(
                offsets.data(), static_cast<int64_t>(offsets.size())),
            ChunkedColumnInterface::TargetType::Int32});
    ASSERT_NE(take, nullptr);
    EXPECT_TRUE(fx.pin_requests->empty());

    fx.column.reset();
    EXPECT_EQ(*take->template Get<int32_t>(0).value, 3);
    EXPECT_EQ(*take->template Get<int32_t>(1).value, 2);
    EXPECT_EQ(*take->template Get<int32_t>(2).value, 0);
    ASSERT_EQ(fx.pin_requests->size(), 2u);
    EXPECT_EQ((*fx.pin_requests)[0], (std::vector<int64_t>{1}));
    EXPECT_EQ((*fx.pin_requests)[1], (std::vector<int64_t>{0}));
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           RawTakeDefersCellPlanningAndOwnsLogicalOffsets) {
    ColumnSpec spec{{2, 2}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);
    auto filter_calls = std::make_shared<std::vector<int>>(2, 0);
    auto filter = std::make_shared<const detail::ColumnFilter>(
        detail::ColumnFilter::MetricsSource::PreloadedStatistics,
        [filter_calls](int64_t cell_id) {
            ++(*filter_calls)[cell_id];
            return false;
        });

    auto offsets = FixedVector<int32_t>{0, 3, 1};
    auto take = fx.column->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            ChunkedColumnInterface::OffsetView::From(
                offsets.data(), static_cast<int64_t>(offsets.size())),
            ChunkedColumnInterface::TargetType::Int32,
            std::move(filter)});
    ASSERT_NE(take, nullptr);
    EXPECT_EQ(*filter_calls, (std::vector<int>{0, 0}));
    EXPECT_TRUE(fx.pin_requests->empty());

    // The result owns the original logical offsets, while physical planning is
    // performed only for positions that are actually consumed.
    offsets = FixedVector<int32_t>{2, 2, 2};
    EXPECT_EQ(*take->template Get<int32_t>(1).value, 3);
    EXPECT_EQ(*filter_calls, (std::vector<int>{0, 1}));
    EXPECT_EQ(*take->template Get<int32_t>(1).value, 3);
    EXPECT_EQ(*filter_calls, (std::vector<int>{0, 1}));
    EXPECT_EQ(*take->template Get<int32_t>(0).value, 0);
    EXPECT_EQ(*filter_calls, (std::vector<int>{1, 1}));
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           RawTakeGetOwnMaterializesOrderedIndependentData) {
    ColumnSpec spec{{3, 2},
                    {{true, false, true}, {false, true}},
                    /*nullable=*/true};
    spec.data_type = DataType::INT32;
    spec.dense_nullable_payload = true;
    auto fx = TypeParam::Create(spec);

    const FixedVector<int32_t> offsets{4, 0, 3, 2};
    auto take = fx.column->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            ChunkedColumnInterface::OffsetView::From(
                offsets.data(), static_cast<int64_t>(offsets.size())),
            ChunkedColumnInterface::TargetType::Int32});
    ASSERT_NE(take, nullptr);
    EXPECT_FALSE(take->IsOwned());
    EXPECT_TRUE(fx.pin_requests->empty());
    auto owned = take->GetOwn();
    EXPECT_TRUE(take->IsOwned());
    ASSERT_EQ(owned.size, 4);
    ASSERT_NE(owned.owner, nullptr);
    ASSERT_TRUE(owned.validity);
    take.reset();

    const auto* values = owned.values.template data_as<int32_t>();
    EXPECT_EQ(values[0], 4);
    EXPECT_EQ(values[1], 0);
    EXPECT_EQ(values[2], 3);
    EXPECT_EQ(values[3], 2);
    EXPECT_TRUE(owned.validity[0]);
    EXPECT_TRUE(owned.validity[1]);
    EXPECT_FALSE(owned.validity[2]);
    EXPECT_TRUE(owned.validity[3]);
    ASSERT_EQ(fx.pin_requests->size(), 2u);
    EXPECT_EQ((*fx.pin_requests)[0], (std::vector<int64_t>{1}));
    EXPECT_EQ((*fx.pin_requests)[1], (std::vector<int64_t>{0}));
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           RawTakeGetOwnOmitsSkipMaskWhenFilterMatchesNoCell) {
    ColumnSpec spec{{3, 2}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);
    auto filter = std::make_shared<const detail::ColumnFilter>(
        detail::ColumnFilter::MetricsSource::PreloadedStatistics,
        [](int64_t) { return false; });

    const FixedVector<int32_t> offsets{4, 0, 3, 2};
    auto take = fx.column->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            ChunkedColumnInterface::OffsetView::From(
                offsets.data(), static_cast<int64_t>(offsets.size())),
            ChunkedColumnInterface::TargetType::Int32,
            std::move(filter)});
    ASSERT_NE(take, nullptr);

    const auto owned = take->GetOwn();
    ASSERT_EQ(owned.size, static_cast<int64_t>(offsets.size()));
    EXPECT_FALSE(owned.data_skipped);
    const auto* values = owned.values.template data_as<int32_t>();
    EXPECT_EQ(values[0], 4);
    EXPECT_EQ(values[1], 0);
    EXPECT_EQ(values[2], 3);
    EXPECT_EQ(values[3], 2);
}

TEST(ChunkedColumnInterfaceTest,
     RawTakeGetOwnPreservesValidEmptyArrayMetadata) {
    auto fx = CreateNullableEmptyArrayColumn();
    const FixedVector<int32_t> offsets{0, 1};
    auto take = fx.column->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            ChunkedColumnInterface::OffsetView::From(offsets.data(), 2),
            ChunkedColumnInterface::TargetType::ArrayView});
    ASSERT_NE(take, nullptr);
    ASSERT_TRUE(take->IsValid(0));
    ASSERT_FALSE(take->IsValid(1));

    auto owned = take->GetOwn();
    ASSERT_EQ(owned.size, 2);
    ASSERT_TRUE(owned.validity);
    const auto* arrays = owned.values.data_as<ArrayView>();
    EXPECT_EQ(arrays[0].length(), 0);
    EXPECT_EQ(arrays[0].get_element_type(), DataType::INT64);
    EXPECT_NE(arrays[0].data(), nullptr);
    EXPECT_EQ(arrays[1].get_element_type(), DataType::NONE);
    EXPECT_EQ(arrays[1].data(), nullptr);
}

TYPED_TEST(ChunkedColumnInterfaceTest, TakeGetRejectsMismatchedFixedType) {
    ColumnSpec spec{{2}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    const FixedVector<int32_t> offsets{0};
    auto take = fx.column->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            ChunkedColumnInterface::OffsetView::From(offsets.data(), 1),
            ChunkedColumnInterface::TargetType::Int32});
    ASSERT_NE(take, nullptr);
    EXPECT_THROW(take->template Get<int64_t>(0), std::exception);
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           FixedWidthDataScanRejectsMismatchedTargetType) {
    ColumnSpec spec{{3}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    EXPECT_THROW(
        fx.column->Scan(nullptr,
                        ChunkedColumnInterface::ScanOptions::ForData(
                            0, ChunkedColumnInterface::TargetType::StringView)),
        std::exception);
}

TEST(ChunkedColumnInterfaceTest, VarcharPrimaryKeyUsesStringViewScanCursor) {
    // Primary-key metadata does not change the underlying column
    // representation: a user-defined VARCHAR PK uses the regular string
    // column and must therefore resolve to a StringView scan cursor.
    const std::vector<std::string> values{"alpha", "beta", "gamma"};
    auto buffer = BuildStringChunkBuffer(values);
    auto guard = std::make_shared<ChunkMmapGuard>(nullptr, 0, "");
    std::vector<std::unique_ptr<Chunk>> chunks;
    chunks.push_back(
        std::make_unique<StringChunk>(static_cast<int32_t>(values.size()),
                                      buffer.data(),
                                      buffer.size(),
                                      /*nullable=*/false,
                                      std::move(guard)));
    auto translator = std::make_unique<TestChunkTranslator>(
        std::vector<int64_t>{static_cast<int64_t>(values.size())},
        "varchar_pk_scan",
        std::move(chunks));
    FieldMeta field_meta(FieldName("varchar_pk"),
                         FieldId(kTestFieldId),
                         DataType::VARCHAR,
                         /*nullable=*/false,
                         std::nullopt);
    auto slot = cachinglayer::Manager::GetInstance().CreateCacheSlot<Chunk>(
        std::move(translator), nullptr);
    auto pin_requests = std::make_shared<std::vector<std::vector<int64_t>>>();
    auto column = std::make_shared<ScanCountingStringColumn>(
        std::move(slot), field_meta, pin_requests);

    auto cursor =
        column->Scan(nullptr,
                     ChunkedColumnInterface::ScanOptions::ForData(
                         0, ChunkedColumnInterface::TargetType::StringView));
    ASSERT_NE(cursor, nullptr);
    EXPECT_TRUE(pin_requests->empty());

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(
        cursor->Next(static_cast<int64_t>(values.size()),
                     ChunkedColumnInterface::ScanReadMode::DataAndValidity,
                     &batch));
    EXPECT_EQ(batch.values.target_type,
              ChunkedColumnInterface::TargetType::StringView);
    const auto* scanned = batch.values.data_as<std::string_view>();
    ASSERT_EQ(batch.size, static_cast<int64_t>(values.size()));
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(scanned[i], values[i]);
    }
    ASSERT_EQ(pin_requests->size(), 1u);
    EXPECT_EQ(pin_requests->front(), (std::vector<int64_t>{0}));
}

TEST(ChunkedColumnInterfaceTest, VarcharTakeBuildsOnlyRequestedOrderedViews) {
    const std::vector<std::string> values{"alpha", "beta", "gamma", "delta"};
    auto buffer = BuildStringChunkBuffer(values);
    auto guard = std::make_shared<ChunkMmapGuard>(nullptr, 0, "");
    std::vector<std::unique_ptr<Chunk>> chunks;
    chunks.push_back(
        std::make_unique<StringChunk>(static_cast<int32_t>(values.size()),
                                      buffer.data(),
                                      buffer.size(),
                                      /*nullable=*/false,
                                      std::move(guard)));
    auto translator = std::make_unique<TestChunkTranslator>(
        std::vector<int64_t>{static_cast<int64_t>(values.size())},
        "varchar_take",
        std::move(chunks));
    FieldMeta field_meta(FieldName("varchar"),
                         FieldId(kTestFieldId),
                         DataType::VARCHAR,
                         /*nullable=*/false,
                         std::nullopt);
    auto slot = cachinglayer::Manager::GetInstance().CreateCacheSlot<Chunk>(
        std::move(translator), nullptr);
    auto pin_requests = std::make_shared<std::vector<std::vector<int64_t>>>();
    auto column = std::make_shared<ScanCountingStringColumn>(
        std::move(slot), field_meta, pin_requests);

    const FixedVector<int32_t> offsets{3, 1, 1, 0};
    auto take = column->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            ChunkedColumnInterface::OffsetView::From(
                offsets.data(), static_cast<int64_t>(offsets.size())),
            ChunkedColumnInterface::TargetType::StringView});
    ASSERT_NE(take, nullptr);
    ASSERT_EQ(take->Size(), 4);
    EXPECT_TRUE(pin_requests->empty());
    EXPECT_EQ(*take->Get<std::string_view>(0).value, "delta");
    EXPECT_EQ(*take->Get<std::string_view>(1).value, "beta");
    EXPECT_EQ(*take->Get<std::string_view>(2).value, "beta");
    EXPECT_EQ(*take->Get<std::string_view>(3).value, "alpha");
    ASSERT_EQ(pin_requests->size(), 1u);
    EXPECT_EQ(pin_requests->front(), (std::vector<int64_t>{0}));
}

TEST(ChunkedColumnInterfaceTest, MaterializedViewDataScanUsesWindowBounds) {
    const std::vector<std::string> values{
        "alpha", "beta", "gamma", "delta", "epsilon"};
    auto buffer = BuildStringChunkBuffer(values);
    auto guard = std::make_shared<ChunkMmapGuard>(nullptr, 0, "");
    std::vector<std::unique_ptr<Chunk>> chunks;
    chunks.push_back(
        std::make_unique<StringChunk>(static_cast<int32_t>(values.size()),
                                      buffer.data(),
                                      buffer.size(),
                                      /*nullable=*/false,
                                      std::move(guard)));
    auto translator = std::make_unique<TestChunkTranslator>(
        std::vector<int64_t>{static_cast<int64_t>(values.size())},
        "bounded_varchar_scan",
        std::move(chunks));
    FieldMeta field_meta(FieldName("varchar"),
                         FieldId(kTestFieldId),
                         DataType::VARCHAR,
                         /*nullable=*/false,
                         std::nullopt);
    auto slot = cachinglayer::Manager::GetInstance().CreateCacheSlot<Chunk>(
        std::move(translator), nullptr);
    auto pin_requests = std::make_shared<std::vector<std::vector<int64_t>>>();
    auto column = std::make_shared<ScanCountingStringColumn>(
        std::move(slot), field_meta, pin_requests);

    constexpr int64_t kMaxBatchRows = 2;
    auto cursor =
        column->Scan(nullptr,
                     ChunkedColumnInterface::ScanOptions::ForData(
                         0, ChunkedColumnInterface::TargetType::StringView));
    ASSERT_NE(cursor, nullptr);
    EXPECT_TRUE(pin_requests->empty());

    std::vector<std::string> scanned;
    std::vector<int64_t> batch_starts;
    std::vector<int64_t> batch_sizes;
    for (int64_t start = 0; start < static_cast<int64_t>(values.size());
         start += kMaxBatchRows) {
        const auto length = std::min<int64_t>(
            kMaxBatchRows, static_cast<int64_t>(values.size()) - start);
        ChunkedColumnInterface::ScanBatch batch;
        ASSERT_TRUE(
            cursor->Next(length,
                         ChunkedColumnInterface::ScanReadMode::DataAndValidity,
                         &batch));
        batch_starts.emplace_back(batch.row_id_start);
        batch_sizes.emplace_back(batch.size);
        EXPECT_LE(batch.size, kMaxBatchRows);
        const auto* batch_values = batch.values.data_as<std::string_view>();
        for (int64_t i = 0; i < batch.size; ++i) {
            scanned.emplace_back(batch_values[i]);
        }
    }

    EXPECT_EQ(batch_starts, (std::vector<int64_t>{0, 2, 4}));
    EXPECT_EQ(batch_sizes, (std::vector<int64_t>{2, 2, 1}));
    EXPECT_EQ(scanned, values);
    EXPECT_EQ(pin_requests->size(), 3u);
}

}  // namespace milvus
