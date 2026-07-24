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
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/Chunk.h"
#include "common/GroupChunk.h"
#include "mmap/ChunkedColumn.h"
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

    std::vector<PinWrapper<Chunk*>>
    PinChunks(milvus::OpContext* op_ctx,
              const std::vector<int64_t>& chunk_ids) const override {
        pin_requests_->emplace_back(chunk_ids);
        return ChunkedColumn::PinChunks(op_ctx, chunk_ids);
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

    std::vector<PinWrapper<Chunk*>>
    PinChunks(milvus::OpContext* op_ctx,
              const std::vector<int64_t>& chunk_ids) const override {
        pin_requests_->emplace_back(chunk_ids);
        return ProxyChunkColumn::PinChunks(op_ctx, chunk_ids);
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

    std::vector<PinWrapper<Chunk*>>
    PinChunks(milvus::OpContext* op_ctx,
              const std::vector<int64_t>& chunk_ids) const override {
        pin_requests_->emplace_back(chunk_ids);
        return ChunkedVariableColumn<std::string>::PinChunks(op_ctx, chunk_ids);
    }

 private:
    std::shared_ptr<std::vector<std::vector<int64_t>>> pin_requests_;
};

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
                std::static_pointer_cast<void>(buffers)};
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
    return batch.validity == nullptr || batch.validity[offset];
}

int64_t
TakeValueOffset(const ChunkedColumnInterface::TakeBatch& batch,
                int64_t logical_offset) {
    return batch.selection == nullptr ? logical_offset
                                      : batch.selection[logical_offset];
}

bool
IsTakeRowValid(const ChunkedColumnInterface::TakeBatch& batch,
               int64_t logical_offset) {
    return batch.validity == nullptr ||
           batch.validity[TakeValueOffset(batch, logical_offset)];
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
        nullptr, ChunkedColumnInterface::ScanOptions::ForData(0, 4));
    ASSERT_NE(cursor, nullptr);
    EXPECT_EQ(cursor->Position(), 0);

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(4, &batch));
    EXPECT_EQ(batch.row_id_start, 0);
    EXPECT_EQ(batch.size, 4);
    EXPECT_EQ(batch.values.encoding,
              ChunkedColumnInterface::ValueEncoding::VectorArrayView);
    EXPECT_EQ(batch.values.kind,
              ChunkedColumnInterface::ScanValueKind::VectorArrayView);

    const auto* views = batch.values.data_as<VectorArrayView>();
    EXPECT_EQ(views[0].length(), 1);
    EXPECT_EQ(views[2].length(), 0);
    EXPECT_EQ(views[3].length(), 2);
    EXPECT_TRUE(IsScanRowValid(batch, 0));
    EXPECT_FALSE(IsScanRowValid(batch, 1));
    EXPECT_TRUE(IsScanRowValid(batch, 2));
    EXPECT_TRUE(IsScanRowValid(batch, 3));
    EXPECT_EQ(cursor->Position(), 4);
    EXPECT_FALSE(cursor->Next(4, &batch));
}

TYPED_TEST(VectorArrayColumnInterfaceTest,
           NoDataScanUsesColumnTypeInsteadOfRequestedValueKind) {
    auto fx = TypeParam::Create();

    auto cursor =
        fx.column->Scan(nullptr,
                        ChunkedColumnInterface::ScanOptions::ForNoData(
                            0,
                            kVectorArrayRows,
                            ChunkedColumnInterface::ScanValueKind::FixedWidth));
    ASSERT_NE(cursor, nullptr);

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(2, &batch));
    EXPECT_TRUE(batch.values.empty());
    EXPECT_EQ(batch.size, 2);
    EXPECT_TRUE(IsScanRowValid(batch, 0));
    EXPECT_FALSE(IsScanRowValid(batch, 1));
    EXPECT_EQ(cursor->Position(), 2);
    ASSERT_NE(batch.owner, nullptr);
    EXPECT_EQ(batch.owner.use_count(), 2);

    ASSERT_TRUE(cursor->Next(2, &batch));
    EXPECT_EQ(batch.row_id_start, 2);
    EXPECT_EQ(batch.size, 2);
    EXPECT_TRUE(IsScanRowValid(batch, 0));
    EXPECT_TRUE(IsScanRowValid(batch, 1));
    ASSERT_NE(batch.owner, nullptr);
    EXPECT_EQ(batch.owner.use_count(), 2);
    EXPECT_EQ(cursor->Position(), kVectorArrayRows);
}

TYPED_TEST(VectorArrayColumnInterfaceTest, DataScanRejectsMismatchedValueKind) {
    auto fx = TypeParam::Create();

    EXPECT_THROW(
        fx.column->Scan(nullptr,
                        ChunkedColumnInterface::ScanOptions::ForData(
                            0,
                            kVectorArrayRows,
                            ChunkedColumnInterface::ScanProjection::Data,
                            ChunkedColumnInterface::ScanValueKind::FixedWidth)),
        std::exception);
}

TYPED_TEST(ChunkedColumnInterfaceTest, RawFormatScanDoesNotSupportUnaryRowIds) {
    ColumnSpec spec{{5}, {{true, false, true, true, false}}, true};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    proto::plan::GenericValue value;
    value.set_int64_val(3);
    auto options = ChunkedColumnInterface::ScanOptions::ForUnary(
        0, 5, proto::plan::OpType::Equal, value);

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
        0, 5, lower, true, upper, true);

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
                            1,
                            4,
                            ChunkedColumnInterface::ScanProjection::Data,
                            ChunkedColumnInterface::ScanValueKind::FixedWidth));
    ASSERT_NE(cursor, nullptr);
    EXPECT_EQ(cursor->Position(), 1);
    ASSERT_EQ(fx.pin_requests->size(), 1u);
    EXPECT_EQ(fx.pin_requests->front(), (std::vector<int64_t>{0, 1}));
    EXPECT_EQ(*fx.fetched, (std::set<cachinglayer::cid_t>{0, 1}));

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(10, &batch));
    EXPECT_EQ(batch.row_id_start, 1);
    EXPECT_EQ(batch.size, 2);
    ASSERT_FALSE(batch.values.empty());
    EXPECT_EQ(batch.values.encoding,
              ChunkedColumnInterface::ValueEncoding::FixedWidth);
    EXPECT_EQ(batch.values.data_as<int32_t>()[1], 2);
    EXPECT_FALSE(IsScanRowValid(batch, 0));
    EXPECT_TRUE(IsScanRowValid(batch, 1));
    EXPECT_EQ(cursor->Position(), 3);

    ASSERT_TRUE(cursor->Next(10, &batch));
    EXPECT_EQ(batch.row_id_start, 3);
    EXPECT_EQ(batch.size, 2);
    EXPECT_EQ(batch.values.data_as<int32_t>()[1], 4);
    EXPECT_FALSE(IsScanRowValid(batch, 0));
    EXPECT_TRUE(IsScanRowValid(batch, 1));
    EXPECT_EQ(cursor->Position(), 5);

    EXPECT_FALSE(cursor->Next(10, &batch));
    EXPECT_EQ(fx.pin_requests->size(), 1u);
    EXPECT_EQ(*fx.fetched, (std::set<cachinglayer::cid_t>{0, 1}));
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           FixedWidthNoDataScanReturnsOnlyValidity) {
    ColumnSpec spec{{4},
                    {{true, false, true, false}},
                    /*nullable=*/true};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    auto cursor = fx.column->Scan(
        nullptr, ChunkedColumnInterface::ScanOptions::ForNoData(0, 4));
    ASSERT_NE(cursor, nullptr);
    ASSERT_EQ(fx.pin_requests->size(), 1u);
    EXPECT_EQ(fx.pin_requests->front(), (std::vector<int64_t>{0}));

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(4, &batch));
    EXPECT_EQ(batch.row_id_start, 0);
    EXPECT_EQ(batch.size, 4);
    EXPECT_TRUE(batch.values.empty());
    EXPECT_TRUE(IsScanRowValid(batch, 0));
    EXPECT_FALSE(IsScanRowValid(batch, 1));
    EXPECT_TRUE(IsScanRowValid(batch, 2));
    EXPECT_FALSE(IsScanRowValid(batch, 3));
    EXPECT_FALSE(cursor->Next(4, &batch));
    EXPECT_EQ(fx.pin_requests->size(), 1u);
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           FixedWidthDataScanHonorsCallerBatchLimit) {
    ColumnSpec spec{{5}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    auto cursor = fx.column->Scan(
        nullptr, ChunkedColumnInterface::ScanOptions::ForData(1, 4));
    ASSERT_NE(cursor, nullptr);
    EXPECT_EQ(cursor->Position(), 1);
    ASSERT_EQ(fx.pin_requests->size(), 1u);
    EXPECT_EQ(fx.pin_requests->front(), (std::vector<int64_t>{0}));

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(2, &batch));
    EXPECT_EQ(batch.row_id_start, 1);
    EXPECT_EQ(batch.size, 2);
    EXPECT_EQ(cursor->Position(), 3);

    ASSERT_TRUE(cursor->Next(1, &batch));
    EXPECT_EQ(batch.row_id_start, 3);
    EXPECT_EQ(batch.size, 1);
    EXPECT_EQ(cursor->Position(), 4);

    ASSERT_TRUE(cursor->Next(2, &batch));
    EXPECT_EQ(batch.row_id_start, 4);
    EXPECT_EQ(batch.size, 1);
    EXPECT_EQ(cursor->Position(), 5);
    EXPECT_FALSE(cursor->Next(2, &batch));
    EXPECT_EQ(fx.pin_requests->size(), 1u);
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           PreparedScanSkipsPlannedRangesAndReusesPinsForValidity) {
    ColumnSpec spec{{3, 2},
                    {{true, false, true}, {false, true}},
                    /*nullable=*/true};
    spec.data_type = DataType::INT32;
    spec.dense_nullable_payload = true;
    auto fx = TypeParam::Create(spec);

    auto prepared = fx.column->PrepareScan(
        nullptr, ChunkedColumnInterface::ScanOptions::ForData(0, 5));
    ASSERT_NE(prepared, nullptr);
    ASSERT_EQ(fx.pin_requests->size(), 1u);
    EXPECT_EQ(fx.pin_requests->front(), (std::vector<int64_t>{0, 1}));

    auto plan = ChunkedColumnInterface::ScanPlan::Full(0, 5);
    plan.skip_ranges = {
        ChunkedColumnInterface::ScanRowRange{1, 4},
    };
    auto cursor =
        prepared->Open(plan, ChunkedColumnInterface::ScanProjection::Data);
    ASSERT_NE(cursor, nullptr);
    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(5, &batch));
    EXPECT_EQ(batch.row_id_start, 0);
    EXPECT_EQ(batch.size, 1);
    EXPECT_TRUE(IsScanRowValid(batch, 0));

    ASSERT_TRUE(cursor->Next(5, &batch));
    EXPECT_EQ(batch.row_id_start, 1);
    EXPECT_EQ(batch.size, 2);
    EXPECT_TRUE(batch.values.empty());
    EXPECT_FALSE(IsScanRowValid(batch, 0));
    EXPECT_TRUE(IsScanRowValid(batch, 1));

    ASSERT_TRUE(cursor->Next(5, &batch));
    EXPECT_EQ(batch.row_id_start, 3);
    EXPECT_EQ(batch.size, 1);
    EXPECT_TRUE(batch.values.empty());
    EXPECT_FALSE(IsScanRowValid(batch, 0));

    ASSERT_TRUE(cursor->Next(5, &batch));
    EXPECT_EQ(batch.row_id_start, 4);
    EXPECT_EQ(batch.size, 1);
    EXPECT_FALSE(batch.values.empty());
    EXPECT_TRUE(IsScanRowValid(batch, 0));
    EXPECT_FALSE(cursor->Next(5, &batch));
    EXPECT_EQ(cursor->Position(), 5);

    auto validity_cursor =
        prepared->Open(ChunkedColumnInterface::ScanPlan::Full(1, 3),
                       ChunkedColumnInterface::ScanProjection::NoData);
    ASSERT_NE(validity_cursor, nullptr);
    ASSERT_TRUE(validity_cursor->Next(5, &batch));
    EXPECT_EQ(batch.row_id_start, 1);
    EXPECT_EQ(batch.size, 2);
    EXPECT_FALSE(IsScanRowValid(batch, 0));
    EXPECT_TRUE(IsScanRowValid(batch, 1));
    ASSERT_TRUE(validity_cursor->Next(5, &batch));
    EXPECT_EQ(batch.row_id_start, 3);
    EXPECT_EQ(batch.size, 1);
    EXPECT_FALSE(IsScanRowValid(batch, 0));
    EXPECT_FALSE(validity_cursor->Next(5, &batch));

    EXPECT_EQ(fx.pin_requests->size(), 1u);
    EXPECT_EQ(*fx.fetched, (std::set<cachinglayer::cid_t>{0, 1}));
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           CellSkipPlanningRunsBeforeAndAfterPinThenBuildsRanges) {
    ColumnSpec spec{{2, 3, 2}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    std::vector<int64_t> metadata_calls;
    std::vector<int64_t> loaded_calls;
    auto options = ChunkedColumnInterface::ScanOptions::ForData(0, 7);
    options.metadata_skip_cell = [&](int64_t cell_id) {
        metadata_calls.emplace_back(cell_id);
        EXPECT_TRUE(fx.pin_requests->empty());
        return cell_id == 1;
    };
    options.loaded_skip_cell = [&](int64_t cell_id) {
        loaded_calls.emplace_back(cell_id);
        EXPECT_EQ(fx.pin_requests->size(), 1u);
        EXPECT_EQ(fx.pin_requests->front(), (std::vector<int64_t>{0, 2}));
        return cell_id == 2;
    };

    auto prepared = fx.column->PrepareScan(nullptr, options);
    ASSERT_NE(prepared, nullptr);
    EXPECT_EQ(metadata_calls, (std::vector<int64_t>{0, 1, 2}));
    EXPECT_EQ(loaded_calls, (std::vector<int64_t>{0, 2}));
    ASSERT_EQ(prepared->Plan().skip_ranges.size(), 1u);
    EXPECT_EQ(prepared->Plan().skip_ranges[0].start, 2);
    EXPECT_EQ(prepared->Plan().skip_ranges[0].end, 7);

    auto cursor = prepared->Open(prepared->Plan(),
                                 ChunkedColumnInterface::ScanProjection::Data);
    ASSERT_NE(cursor, nullptr);
    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(7, &batch));
    EXPECT_EQ(batch.row_id_start, 0);
    EXPECT_EQ(batch.size, 2);
    EXPECT_FALSE(batch.values.empty());
    EXPECT_FALSE(cursor->Next(7, &batch));
    EXPECT_EQ(cursor->Position(), 7);

    // Reopening a subrange keeps the planner skips selected before pinning.
    // The caller does not need to repeat those ranges, and the cursor must not
    // try to access the unpinned middle Cells.
    cursor = prepared->Open(
        ChunkedColumnInterface::ScanPlan::Full(1, 5),
        ChunkedColumnInterface::ScanProjection::Data);
    ASSERT_NE(cursor, nullptr);
    ASSERT_TRUE(cursor->Next(5, &batch));
    EXPECT_EQ(batch.row_id_start, 1);
    EXPECT_EQ(batch.size, 1);
    EXPECT_FALSE(cursor->Next(5, &batch));
    EXPECT_EQ(cursor->Position(), 6);
    EXPECT_EQ(fx.pin_requests->size(), 1u);
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           NullableSkippedCellsStayPinnedAndReturnValidityOnlyBatches) {
    ColumnSpec spec{{2, 2},
                    {{true, true}, {false, true}},
                    /*nullable=*/true};
    spec.data_type = DataType::INT32;
    spec.dense_nullable_payload = true;
    auto fx = TypeParam::Create(spec);

    auto options = ChunkedColumnInterface::ScanOptions::ForData(0, 4);
    options.metadata_skip_cell = [](int64_t cell_id) { return cell_id == 1; };
    auto prepared = fx.column->PrepareScan(nullptr, options);
    ASSERT_NE(prepared, nullptr);
    ASSERT_EQ(fx.pin_requests->size(), 1u);
    EXPECT_EQ(fx.pin_requests->front(), (std::vector<int64_t>{0, 1}));
    ASSERT_EQ(prepared->Plan().skip_ranges.size(), 1u);
    EXPECT_EQ(prepared->Plan().skip_ranges[0].start, 2);
    EXPECT_EQ(prepared->Plan().skip_ranges[0].end, 4);

    auto cursor = prepared->Open(prepared->Plan(),
                                 ChunkedColumnInterface::ScanProjection::Data);
    ASSERT_NE(cursor, nullptr);
    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(4, &batch));
    EXPECT_EQ(batch.row_id_start, 0);
    EXPECT_EQ(batch.size, 2);
    EXPECT_FALSE(batch.values.empty());

    ASSERT_TRUE(cursor->Next(4, &batch));
    EXPECT_EQ(batch.row_id_start, 2);
    EXPECT_EQ(batch.size, 2);
    EXPECT_TRUE(batch.values.empty());
    EXPECT_FALSE(IsScanRowValid(batch, 0));
    EXPECT_TRUE(IsScanRowValid(batch, 1));
    EXPECT_FALSE(cursor->Next(4, &batch));
    EXPECT_EQ(cursor->Position(), 4);
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           PreparedScanAllSkippedAdvancesWithoutReturningData) {
    ColumnSpec spec{{3, 2}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    auto prepared = fx.column->PrepareScan(
        nullptr, ChunkedColumnInterface::ScanOptions::ForData(0, 5));
    ASSERT_NE(prepared, nullptr);
    auto plan = ChunkedColumnInterface::ScanPlan::Full(0, 5);
    plan.skip_ranges = {
        ChunkedColumnInterface::ScanRowRange{0, 5},
    };
    auto cursor =
        prepared->Open(plan, ChunkedColumnInterface::ScanProjection::Data);
    ASSERT_NE(cursor, nullptr);

    ChunkedColumnInterface::ScanBatch batch;
    EXPECT_FALSE(cursor->Next(5, &batch));
    EXPECT_EQ(cursor->Position(), 5);
    EXPECT_EQ(fx.pin_requests->size(), 1u);
}

TEST(ChunkedColumnInterfaceTest,
     VarcharPreparedScanStopsAtLeadingMiddleAndTrailingSkipRanges) {
    const std::vector<std::string> values{
        "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"};
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
        "varchar_skip_ranges",
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

    auto prepared = column->PrepareScan(
        nullptr,
        ChunkedColumnInterface::ScanOptions::ForData(
            0,
            static_cast<int64_t>(values.size()),
            ChunkedColumnInterface::ScanProjection::Data,
            ChunkedColumnInterface::ScanValueKind::StringView));
    ASSERT_NE(prepared, nullptr);
    const auto row_count = static_cast<int64_t>(values.size());
    auto plan = ChunkedColumnInterface::ScanPlan::Full(0, row_count);
    plan.skip_ranges = {
        ChunkedColumnInterface::ScanRowRange{0, 1},
        ChunkedColumnInterface::ScanRowRange{3, 5},
        ChunkedColumnInterface::ScanRowRange{7, 8},
    };
    auto cursor =
        prepared->Open(plan, ChunkedColumnInterface::ScanProjection::Data);
    ASSERT_NE(cursor, nullptr);

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(row_count, &batch));
    EXPECT_EQ(batch.row_id_start, 1);
    EXPECT_EQ(batch.size, 2);
    const auto* first = batch.values.data_as<std::string_view>();
    EXPECT_EQ(first[0], "beta");
    EXPECT_EQ(first[1], "gamma");

    ASSERT_TRUE(cursor->Next(row_count, &batch));
    EXPECT_EQ(batch.row_id_start, 5);
    EXPECT_EQ(batch.size, 2);
    const auto* second = batch.values.data_as<std::string_view>();
    EXPECT_EQ(second[0], "zeta");
    EXPECT_EQ(second[1], "eta");

    EXPECT_FALSE(cursor->Next(row_count, &batch));
    EXPECT_EQ(cursor->Position(), row_count);
    ASSERT_EQ(pin_requests->size(), 1u);
    EXPECT_EQ(pin_requests->front(), (std::vector<int64_t>{0}));
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           NonNullableNoDataScanDoesNotFetchPayload) {
    ColumnSpec spec{{3, 2}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    auto cursor = fx.column->Scan(
        nullptr, ChunkedColumnInterface::ScanOptions::ForNoData(0, 5));
    ASSERT_NE(cursor, nullptr);
    EXPECT_TRUE(fx.fetched->empty());
    EXPECT_TRUE(fx.pin_requests->empty());

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(5, &batch));
    EXPECT_EQ(batch.size, 3);
    EXPECT_TRUE(batch.values.empty());
    EXPECT_EQ(batch.validity, nullptr);
    EXPECT_TRUE(fx.fetched->empty());

    ASSERT_TRUE(cursor->Next(5, &batch));
    EXPECT_EQ(batch.size, 2);
    EXPECT_TRUE(batch.values.empty());
    EXPECT_EQ(batch.validity, nullptr);
    EXPECT_TRUE(fx.fetched->empty());
    EXPECT_TRUE(fx.pin_requests->empty());
    EXPECT_FALSE(cursor->Next(5, &batch));
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           FixedWidthDataScanReportsAllValidForNonNullableColumn) {
    ColumnSpec spec{{3}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    auto cursor = fx.column->Scan(
        nullptr, ChunkedColumnInterface::ScanOptions::ForData(0, 3));
    ASSERT_NE(cursor, nullptr);

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(3, &batch));
    EXPECT_EQ(batch.validity, nullptr);
    for (int64_t i = 0; i < batch.size; ++i) {
        EXPECT_TRUE(IsScanRowValid(batch, i));
    }
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           FixedWidthTakePreservesOrderDuplicatesAndChunkRuns) {
    ColumnSpec spec{{3, 2}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    const FixedVector<int32_t> offsets{2, 0, 0, 4, 3, 1};
    auto cursor = fx.column->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            ChunkedColumnInterface::OffsetView::From(
                offsets.data(), static_cast<int64_t>(offsets.size())),
            ChunkedColumnInterface::ScanValueKind::FixedWidth});
    ASSERT_NE(cursor, nullptr);

    std::vector<int32_t> actual;
    std::vector<int64_t> batch_positions;
    std::vector<int64_t> batch_sizes;
    std::vector<int64_t> source_chunks;
    ChunkedColumnInterface::TakeBatch batch;
    while (cursor->Next(4, &batch)) {
        batch_positions.emplace_back(batch.position);
        batch_sizes.emplace_back(batch.size);
        source_chunks.emplace_back(batch.source_chunk_id);
        ASSERT_NE(batch.owner, nullptr);
        ASSERT_NE(batch.selection, nullptr);
        const auto* values = batch.values.data_as<int32_t>();
        for (int64_t i = 0; i < batch.size; ++i) {
            actual.emplace_back(values[TakeValueOffset(batch, i)]);
            EXPECT_TRUE(IsTakeRowValid(batch, i));
        }
    }

    EXPECT_EQ(actual, (std::vector<int32_t>{2, 0, 0, 4, 3, 1}));
    EXPECT_EQ(batch_positions, (std::vector<int64_t>{0, 3, 5}));
    EXPECT_EQ(batch_sizes, (std::vector<int64_t>{3, 2, 1}));
    EXPECT_EQ(source_chunks, (std::vector<int64_t>{0, 1, 0}));
    EXPECT_EQ(cursor->Position(), static_cast<int64_t>(offsets.size()));
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           FixedWidthTakeIndexesValidityThroughSelection) {
    ColumnSpec spec{{4}, {{true, false, true, false}}, /*nullable=*/true};
    spec.data_type = DataType::INT32;
    spec.dense_nullable_payload = true;
    auto fx = TypeParam::Create(spec);

    const FixedVector<int32_t> offsets{3, 0, 1, 2};
    auto cursor = fx.column->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            ChunkedColumnInterface::OffsetView::From(
                offsets.data(), static_cast<int64_t>(offsets.size())),
            ChunkedColumnInterface::ScanValueKind::FixedWidth});
    ASSERT_NE(cursor, nullptr);

    ChunkedColumnInterface::TakeBatch batch;
    ASSERT_TRUE(cursor->Next(4, &batch));
    ASSERT_EQ(batch.size, 4);
    EXPECT_FALSE(IsTakeRowValid(batch, 0));
    EXPECT_TRUE(IsTakeRowValid(batch, 1));
    EXPECT_FALSE(IsTakeRowValid(batch, 2));
    EXPECT_TRUE(IsTakeRowValid(batch, 3));
}

TYPED_TEST(ChunkedColumnInterfaceTest,
           FixedWidthDataScanRejectsMismatchedValueKind) {
    ColumnSpec spec{{3}, {}, /*nullable=*/false};
    spec.data_type = DataType::INT32;
    auto fx = TypeParam::Create(spec);

    EXPECT_THROW(
        fx.column->Scan(nullptr,
                        ChunkedColumnInterface::ScanOptions::ForData(
                            0,
                            3,
                            ChunkedColumnInterface::ScanProjection::Data,
                            ChunkedColumnInterface::ScanValueKind::StringView)),
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

    auto cursor = column->Scan(nullptr,
                               ChunkedColumnInterface::ScanOptions::ForData(
                                   0, static_cast<int64_t>(values.size())));
    ASSERT_NE(cursor, nullptr);
    ASSERT_EQ(pin_requests->size(), 1u);
    EXPECT_EQ(pin_requests->front(), (std::vector<int64_t>{0}));

    ChunkedColumnInterface::ScanBatch batch;
    ASSERT_TRUE(cursor->Next(static_cast<int64_t>(values.size()), &batch));
    EXPECT_EQ(batch.values.encoding,
              ChunkedColumnInterface::ValueEncoding::StringView);
    EXPECT_EQ(batch.values.kind,
              ChunkedColumnInterface::ScanValueKind::StringView);
    const auto* scanned = batch.values.data_as<std::string_view>();
    ASSERT_EQ(batch.size, static_cast<int64_t>(values.size()));
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(scanned[i], values[i]);
    }
    EXPECT_FALSE(cursor->Next(static_cast<int64_t>(values.size()), &batch));
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
    auto cursor = column->Take(
        nullptr,
        ChunkedColumnInterface::TakeOptions{
            ChunkedColumnInterface::OffsetView::From(
                offsets.data(), static_cast<int64_t>(offsets.size())),
            ChunkedColumnInterface::ScanValueKind::StringView});
    ASSERT_NE(cursor, nullptr);

    ChunkedColumnInterface::TakeBatch batch;
    ASSERT_TRUE(cursor->Next(10, &batch));
    EXPECT_EQ(batch.position, 0);
    EXPECT_EQ(batch.size, 4);
    EXPECT_EQ(batch.selection, nullptr);
    const auto* taken = batch.values.data_as<std::string_view>();
    EXPECT_EQ(taken[0], "delta");
    EXPECT_EQ(taken[1], "beta");
    EXPECT_EQ(taken[2], "beta");
    EXPECT_EQ(taken[3], "alpha");
    EXPECT_FALSE(cursor->Next(10, &batch));
}

TEST(ChunkedColumnInterfaceTest,
     MaterializedViewDataScanStreamsBoundedBatches) {
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
                         0,
                         static_cast<int64_t>(values.size()),
                         ChunkedColumnInterface::ScanProjection::Data,
                         ChunkedColumnInterface::ScanValueKind::StringView));
    ASSERT_NE(cursor, nullptr);
    ASSERT_EQ(pin_requests->size(), 1u);
    EXPECT_EQ(pin_requests->front(), (std::vector<int64_t>{0}));

    std::vector<std::string> scanned;
    std::vector<int64_t> batch_starts;
    std::vector<int64_t> batch_sizes;
    ChunkedColumnInterface::ScanBatch batch;
    while (cursor->Next(kMaxBatchRows, &batch)) {
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
    EXPECT_EQ(pin_requests->size(), 1u);
}

}  // namespace milvus
