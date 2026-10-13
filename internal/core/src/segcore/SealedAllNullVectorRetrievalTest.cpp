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

#include <array>
#include <cstdint>
#include <memory>
#include <string>

#include "common/Schema.h"
#include "common/protobuf_utils.h"
#include "index/contracts/query/IVectorReader.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegmentSealed.h"
#include "segcore/test_utils/ConsumerIndexTestUtils.h"

namespace milvus::segcore::test {
namespace {

template <typename T>
void
CheckAllNullOutput(DataType field_type,
                   const std::string& index_type,
                   const std::string& metric) {
    constexpr int64_t rows = 8;
    constexpr int64_t dim = 16;
    const std::array<bool, rows> valid{};
    auto schema = std::make_shared<Schema>();
    const auto vector_id =
        schema->AddDebugField("vector", field_type, dim, metric, true);
    const auto pk_id = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk_id);
    auto opened = milvus::test::consumer::BuildVectorReader<T>(
        field_type,
        index_type,
        metric,
        dim,
        rows,
        nullptr,
        Config::object(),
        true,
        ValidityView::FromExpanded(valid.data()),
        0);
    ASSERT_NE(opened.reader, nullptr);
    const auto* reader =
        dynamic_cast<const index::IVectorReader*>(opened.reader.get());
    ASSERT_NE(reader, nullptr);
    ASSERT_TRUE(reader->HasValidData());
    ASSERT_EQ(reader->ValidCount(), 0);
    const bool reader_has_raw_data = reader->HasRawData();
    ASSERT_TRUE(reader_has_raw_data);

    auto segment = CreateSealedSegment(schema);
    auto info = milvus::test::consumer::MakeLoadIndexInfo(
        std::move(opened), field_type, vector_id.get());
    // The preloaded reader supplies the capability for this fixture; the
    // helper's zero resource request does not describe its all-NULL state.
    ASSERT_TRUE(info.load_resource_request.has_value());
    info.load_resource_request->has_raw_data = reader_has_raw_data;
    segment->LoadIndex(info);
    auto* sealed = dynamic_cast<ChunkedSegmentSealedImpl*>(segment.get());
    ASSERT_NE(sealed, nullptr);
    ASSERT_TRUE(sealed->HasIndex(vector_id));
    ASSERT_FALSE(sealed->HasFieldData(vector_id));
    ASSERT_TRUE(sealed->HasRawData(vector_id.get()));
    // No raw column is installed. The nonempty output request has zero valid
    // IDs after the sealed consumer applies the index's validity map.
    const std::array<int64_t, 4> offsets{rows - 1, 0, 3, 3};
    const auto output = sealed->bulk_subscript(
        nullptr, vector_id, offsets.data(), offsets.size());
    ASSERT_NE(output, nullptr);
    EXPECT_EQ(output->type(), static_cast<proto::schema::DataType>(field_type));
    const auto& output_valid = GetFieldDataRowValidData(*output);
    ASSERT_EQ(output_valid.size(), offsets.size());
    for (const bool is_valid : output_valid) {
        EXPECT_FALSE(is_valid);
    }
    if (field_type == DataType::VECTOR_SPARSE_U32_F32) {
        EXPECT_EQ(output->vectors().sparse_float_vector().contents_size(), 0);
    } else {
        EXPECT_EQ(output->vectors().dim(), dim);
        switch (field_type) {
            case DataType::VECTOR_FLOAT:
                EXPECT_EQ(output->vectors().float_vector().data_size(), 0);
                break;
            case DataType::VECTOR_FLOAT16:
                EXPECT_TRUE(output->vectors().float16_vector().empty());
                break;
            case DataType::VECTOR_BFLOAT16:
                EXPECT_TRUE(output->vectors().bfloat16_vector().empty());
                break;
            case DataType::VECTOR_INT8:
                EXPECT_TRUE(output->vectors().int8_vector().empty());
                break;
            case DataType::VECTOR_BINARY:
                EXPECT_TRUE(output->vectors().binary_vector().empty());
                break;
            default:
                FAIL() << "unexpected dense vector type";
        }
    }
}

TEST(SealedAllNullVectorRetrievalTest, DenseFieldsReturnNullOutputRows) {
    CheckAllNullOutput<float>(DataType::VECTOR_FLOAT, "FLAT", "L2");
    CheckAllNullOutput<float16>(DataType::VECTOR_FLOAT16, "FLAT", "L2");
    CheckAllNullOutput<bfloat16>(DataType::VECTOR_BFLOAT16, "FLAT", "L2");
    CheckAllNullOutput<int8>(DataType::VECTOR_INT8, "FLAT", "L2");
    CheckAllNullOutput<bin1>(DataType::VECTOR_BINARY, "BIN_FLAT", "HAMMING");
}

TEST(SealedAllNullVectorRetrievalTest, SparseFieldReturnsNullOutputRows) {
    CheckAllNullOutput<sparse_u32_f32>(
        DataType::VECTOR_SPARSE_U32_F32, "SPARSE_INVERTED_INDEX", "IP");
}

}  // namespace
}  // namespace milvus::segcore::test
