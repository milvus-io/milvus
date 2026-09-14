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
#include <algorithm>
#include <numeric>
#include <optional>
#include <string>
#include <vector>

#include "common/Utils.h"
#include "segcore/SegmentGrowingImpl.h"
#include "test_utils/DataGen.h"
#include "test_utils/SegcoreConfigUtils.h"

using namespace milvus;
using namespace milvus::segcore;

namespace {

// Compare the returned payload against the original insert batch, independently
// of either the segment's offset mapping or its interim index.
std::vector<std::string>
VectorRows(const DataArray& field, int64_t dim) {
    const auto& vectors = field.vectors();
    if (field.type() == proto::schema::SparseFloatVector) {
        const auto& rows = vectors.sparse_float_vector().contents();
        return {rows.begin(), rows.end()};
    }
    std::string bytes;
    int64_t width = 0;
    switch (field.type()) {
        case proto::schema::FloatVector:
            bytes.assign(reinterpret_cast<const char*>(
                             vectors.float_vector().data().data()),
                         vectors.float_vector().data_size() * sizeof(float));
            width = dim * sizeof(float);
            break;
        case proto::schema::Float16Vector:
            bytes = vectors.float16_vector();
            width = dim * sizeof(float16);
            break;
        case proto::schema::BFloat16Vector:
            bytes = vectors.bfloat16_vector();
            width = dim * sizeof(bfloat16);
            break;
        case proto::schema::BinaryVector:
            bytes = vectors.binary_vector();
            width = dim / 8;
            break;
        case proto::schema::Int8Vector:
            bytes = vectors.int8_vector();
            width = dim;
            break;
        default:
            throw std::runtime_error("unsupported test vector type");
    }
    std::vector<std::string> rows;
    for (size_t i = 0; i < bytes.size(); i += width) {
        rows.push_back(bytes.substr(i, width));
    }
    return rows;
}

class GrowingNullableVectorTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        auto& config = SegcoreConfig::default_config();
        restore_ = std::make_unique<ScopedSegcoreConfigRestore>(config);
        build_ratio_ = config.get_build_ratio();
        config.set_build_ratio(0);
        InterimIndexConfigForTest options;
        options.nlist = 16;
        options.nprobe = 16;
        options.chunk_rows = 128;
        options.dense_vector_interim_index_type =
            knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC;
        ApplyInterimIndexConfigForTest(options, config);
    }

    void
    TearDown() override {
        segment_.reset();
        SegcoreConfig::default_config().set_build_ratio(build_ratio_);
        restore_.reset();
    }

    void
    MakeSegment(DataType type, bool nullable, const std::string& interim) {
        segment_.reset();
        expected_.clear();
        auto& config = SegcoreConfig::default_config();
        config.set_enable_interim_segment_index(!interim.empty());
        if (!interim.empty()) {
            config.set_dense_vector_intermin_index_type(interim);
        }
        schema_ = std::make_shared<Schema>();
        const auto metric =
            type == DataType::VECTOR_SPARSE_U32_F32 ? knowhere::metric::IP
            : type == DataType::VECTOR_BINARY       ? knowhere::metric::HAMMING
                                                    : knowhere::metric::L2;
        vec_ = schema_->AddDebugField("vec", type, kDim, metric, nullable);
        auto pk = schema_->AddDebugField("pk", DataType::INT64);
        schema_->set_primary_field_id(pk);
        std::map<std::string, std::string> index_params = {
            {"index_type",
             type == DataType::VECTOR_SPARSE_U32_F32
                 ? knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX
                 : knowhere::IndexEnum::INDEX_FAISS_IVFFLAT},
            {"metric_type", metric}};
        std::map<FieldId, FieldIndexMeta> fields;
        fields.emplace(vec_,
                       FieldIndexMeta(vec_,
                                      std::move(index_params),
                                      {{"dim", std::to_string(kDim)}}));
        auto meta =
            std::make_shared<CollectionIndexMeta>(20000, std::move(fields));
        segment_ = CreateGrowingSegment(schema_, meta);
        impl_ = dynamic_cast<SegmentGrowingImpl*>(segment_.get());
        ASSERT_NE(impl_, nullptr);
    }

    void
    Insert(int64_t count, int null_percent = 10, bool nulls_at_end = false) {
        const auto start = expected_.size();
        auto data = DataGen(schema_,
                            count,
                            42 + start,
                            start,
                            1,
                            10,
                            1,
                            false,
                            true,
                            false,
                            null_percent);
        for (auto& field : *data.raw_->mutable_fields_data()) {
            if (field.field_id() != vec_.get()) {
                continue;
            }
            if (nulls_at_end) {
                auto reversed_valid_data = GetFieldDataRowValidData(field);
                std::reverse(reversed_valid_data.begin(),
                             reversed_valid_data.end());
                MutableFieldDataRowValidData(&field)->Swap(
                    &reversed_valid_data);
            }
            const auto& valid_data = GetFieldDataRowValidData(field);
            auto rows = VectorRows(field, kDim);
            size_t physical = 0;
            for (int64_t i = 0; i < count; ++i) {
                if (valid_data.empty() || valid_data[i]) {
                    expected_.emplace_back(rows.at(physical++));
                } else {
                    expected_.emplace_back(std::nullopt);
                }
            }
            ASSERT_EQ(physical, rows.size());
        }
        auto offset = segment_->PreInsert(count);
        ASSERT_EQ(offset, start);
        segment_->Insert(offset,
                         count,
                         data.row_ids_.data(),
                         data.timestamps_.data(),
                         data.raw_);
    }

    void
    Check(const std::vector<int64_t>& offsets) {
        auto output = impl_->bulk_subscript(
            nullptr, vec_, offsets.data(), offsets.size());
        auto rows = VectorRows(*output, kDim);
        const bool nullable = (*schema_)[vec_].is_nullable();
        const auto& valid_data = GetFieldDataRowValidData(*output);
        ASSERT_EQ(valid_data.size(), nullable ? offsets.size() : 0);
        size_t physical = 0;
        for (size_t i = 0; i < offsets.size(); ++i) {
            const auto offset = offsets[i];
            const bool valid = offset >= 0 && offset < expected_.size() &&
                               expected_[offset].has_value();
            if (nullable) {
                EXPECT_EQ(valid_data[i], valid) << offset;
            }
            if (valid) {
                ASSERT_LT(physical, rows.size());
                EXPECT_EQ(rows[physical++], expected_[offset].value())
                    << "logical offset " << offset;
            }
        }
        EXPECT_EQ(physical, rows.size());
    }

    static constexpr int64_t kDim = 8;
    std::unique_ptr<ScopedSegcoreConfigRestore> restore_;
    float build_ratio_;
    SchemaPtr schema_;
    FieldId vec_{100};
    SegmentGrowingPtr segment_;
    SegmentGrowingImpl* impl_ = nullptr;
    std::vector<std::optional<std::string>> expected_;
};

TEST_F(GrowingNullableVectorTest, RawStorageTypesAndNullPatterns) {
    for (auto type : {DataType::VECTOR_FLOAT,
                      DataType::VECTOR_FLOAT16,
                      DataType::VECTOR_BFLOAT16,
                      DataType::VECTOR_BINARY,
                      DataType::VECTOR_INT8,
                      DataType::VECTOR_SPARSE_U32_F32}) {
        for (int null_percent : {0, 10, 100}) {
            for (bool reverse : {false, true}) {
                SCOPED_TRACE(::testing::Message()
                             << int(type) << "/" << null_percent << "/"
                             << reverse);
                MakeSegment(type, true, "");
                Check({});
                Check({-1, 0});  // Empty validity bitmap: no valid rows.
                Insert(300, null_percent, reverse);
                std::vector<int64_t> offsets(300);
                std::iota(offsets.begin(), offsets.end(), 0);
                Check(offsets);
                Check({299, 14, 0, 150, 14, 10, -1, 300});
            }
        }
    }
}

TEST_F(GrowingNullableVectorTest, IndexedTypesKeepLogicalOffsets) {
    for (auto type : {DataType::VECTOR_FLOAT,
                      DataType::VECTOR_FLOAT16,
                      DataType::VECTOR_BFLOAT16,
                      DataType::VECTOR_SPARSE_U32_F32}) {
        for (const std::string interim : {"IVF_FLAT_CC", "SCANN_DVR"}) {
            if (type == DataType::VECTOR_SPARSE_U32_F32 &&
                interim == "SCANN_DVR") {
                continue;
            }
            for (bool nullable : {false, true}) {
                SCOPED_TRACE(::testing::Message()
                             << int(type) << "/" << interim << "/" << nullable);
                MakeSegment(type, nullable, interim);
                Insert(100);
                ASSERT_FALSE(
                    impl_->get_indexing_record().SyncDataWithIndex(vec_));
                Check({99, 14, 0, 14, 10});
                Insert(2000);
                ASSERT_TRUE(
                    impl_->get_indexing_record().SyncDataWithIndex(vec_));
                ASSERT_EQ(impl_->get_indexing_record().HasRawData(vec_),
                          interim == "IVF_FLAT_CC");
                std::vector<int64_t> offsets(expected_.size());
                std::iota(offsets.begin(), offsets.end(), 0);
                Check(offsets);
                Check({2099, 14, 0, 1999, 14, 10, 100});
                // Validity must still be maintained when subsequent inserts
                // bypass raw chunks because the index already owns the data.
                Insert(100);
                Check({2199, 2114, 2100, 14, 2114});
            }
        }
    }
}

TEST_F(GrowingNullableVectorTest, IndexTakesOverBetweenFilteringAndFetch) {
    for (auto type : {DataType::VECTOR_FLOAT,
                      DataType::VECTOR_FLOAT16,
                      DataType::VECTOR_BFLOAT16,
                      DataType::VECTOR_SPARSE_U32_F32}) {
        SCOPED_TRACE(int(type));
        MakeSegment(type, true, "IVF_FLAT_CC");
        Insert(100);
        const auto& index = impl_->get_indexing_record();
        ASSERT_FALSE(index.HasRawData(vec_));
        const std::vector<int64_t> offsets = {14, 99, 12, 14, 0, -1, 100};
        auto filtered = impl_->FilterVectorValidOffsets(
            nullptr, vec_, offsets.data(), offsets.size());
        auto* vec = impl_->get_insert_record().get_data_base(vec_);
        const auto pinned_chunks = vec->acquire_chunks();
        ASSERT_FALSE(pinned_chunks.empty());

        // Deterministically schedule the writer between the two actual read
        // stages. Insert builds the index and reclaims the segment's chunks.
        // No sleeps or product-only test hooks are needed at this boundary.
        Insert(2000);
        ASSERT_TRUE(index.HasRawData(vec_));
        ASSERT_TRUE(vec->acquire_chunks().empty());
        ASSERT_FALSE(pinned_chunks.empty());
        const std::vector<int64_t> expected_offsets = {14, 99, 12, 14};
        ASSERT_EQ(filtered.valid_logical_offsets, expected_offsets);
        auto output = CreateEmptyVectorDataArray(offsets.size(),
                                                 filtered.valid_count,
                                                 filtered.valid_data.get(),
                                                 (*schema_)[vec_]);
        auto* ids = filtered.valid_logical_offsets.data();
        auto count = filtered.valid_count;
        // This is the same index consumer selected by the vector fetch path
        // once HasRawData becomes true. Its IDs must be the logical IDs saved
        // before the writer built the index, not raw-column physical offsets.
        void* raw_output = nullptr;
        int64_t element_size = 0;
        if (type == DataType::VECTOR_SPARSE_U32_F32) {
            raw_output =
                output->mutable_vectors()->mutable_sparse_float_vector();
        } else {
            element_size = (*schema_)[vec_].get_sizeof();
            if (type == DataType::VECTOR_FLOAT) {
                raw_output = output->mutable_vectors()
                                 ->mutable_float_vector()
                                 ->mutable_data()
                                 ->mutable_data();
            } else if (type == DataType::VECTOR_FLOAT16) {
                raw_output =
                    output->mutable_vectors()->mutable_float16_vector()->data();
            } else {
                raw_output = output->mutable_vectors()
                                 ->mutable_bfloat16_vector()
                                 ->data();
            }
        }
        index.GetDataFromIndex(vec_, ids, count, element_size, raw_output);
        auto rows = VectorRows(*output, kDim);
        ASSERT_EQ(rows.size(), expected_offsets.size());
        for (size_t i = 0; i < rows.size(); ++i) {
            EXPECT_EQ(rows[i], expected_[expected_offsets[i]].value());
        }
        Check(offsets);
    }
}

}  // namespace
