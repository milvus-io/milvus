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
    // Keeps chunk data buffers alive for the column's lifetime.
    std::shared_ptr<void> buffer_holder;
};

struct ColumnSpec {
    std::vector<int64_t> rows_per_chunk;
    std::vector<std::vector<bool>> valid_patterns;  // empty => all valid
    bool nullable{true};
    DataType data_type{DataType::VECTOR_INT8};
};

struct VectorArrayColumnFixture {
    std::shared_ptr<ChunkedColumnInterface> column;
    std::shared_ptr<void> buffer_holder;
};

std::vector<char>
BuildChunkBuffer(int64_t row_num,
                 const std::vector<bool>* pattern,
                 bool nullable,
                 int64_t start_logical_offset) {
    const int32_t bitmap_bytes = nullable ? (row_num + 7) / 8 : 0;
    std::vector<char> buf(bitmap_bytes + row_num * kElementSize, 0);
    int64_t physical_offset = 0;
    if (nullable) {
        for (int64_t j = 0; j < row_num; ++j) {
            const bool v = pattern ? (*pattern)[j] : true;
            if (v) {
                buf[j >> 3] |= (1 << (j & 0x07));
                const int32_t value = start_logical_offset + j;
                std::memcpy(
                    buf.data() + bitmap_bytes + physical_offset * kElementSize,
                    &value,
                    sizeof(value));
                ++physical_offset;
            }
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
                start_logical_offset);
            chunks.push_back(MakeFixedChunk(spec.rows_per_chunk[i],
                                            spec.nullable,
                                            (*buffers)[i].data(),
                                            (*buffers)[i].size()));
            start_logical_offset += spec.rows_per_chunk[i];
        }
        auto fetched = std::make_shared<std::set<cachinglayer::cid_t>>();
        auto translator = std::make_unique<CountingChunkTranslator>(
            spec.rows_per_chunk, "cc_iface", std::move(chunks), fetched);
        FieldMeta fm(FieldName("t"),
                     FieldId(kTestFieldId),
                     spec.data_type,
                     kElementSize,
                     std::nullopt,
                     spec.nullable,
                     std::nullopt);
        auto slot = cachinglayer::Manager::GetInstance().CreateCacheSlot<Chunk>(
            std::move(translator), nullptr);
        auto column = std::make_shared<ChunkedColumn>(std::move(slot), fm);
        return {std::static_pointer_cast<ChunkedColumnInterface>(column),
                std::move(fetched),
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
                start_logical_offset);
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
        FieldMeta fm(FieldName("t"),
                     FieldId(kTestFieldId),
                     spec.data_type,
                     kElementSize,
                     std::nullopt,
                     spec.nullable,
                     std::nullopt);
        auto column = std::make_shared<ProxyChunkColumn>(
            group, FieldId(kTestFieldId), fm);
        return {std::static_pointer_cast<ChunkedColumnInterface>(column),
                std::move(fetched),
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

TYPED_TEST(ChunkedColumnInterfaceTest, BuildValidRowIdsNonNullableIsNoop) {
    ColumnSpec spec{{5, 5}, {}, /*nullable=*/false};
    auto fx = TypeParam::Create(spec);

    fx.column->BuildValidRowIds(nullptr);

    EXPECT_FALSE(fx.column->GetOffsetMapping().IsEnabled());
    EXPECT_EQ(fx.column->GetValidCountInChunk(0), 5);
    EXPECT_TRUE(fx.fetched->empty());
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

}  // namespace milvus
