// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <string>
#include <thread>

#include "common/EasyAssert.h"
#include "fmt/format.h"
#include "index/ScalarIndexSort.h"
#include "index/StringIndexSort.h"

#include "common/SystemProperty.h"
#include "segcore/FieldIndexing.h"
#include "index/VectorMemIndex.h"
#include "IndexConfigGenerator.h"

namespace milvus::segcore {
using std::unique_ptr;

VectorFieldIndexing::VectorFieldIndexing(const FieldMeta& field_meta,
                                         const FieldIndexMeta& field_index_meta,
                                         int64_t segment_max_row_count,
                                         const SegcoreConfig& segcore_config,
                                         const VectorBase* field_raw_data)
    : FieldIndexing(field_meta, segcore_config),
      built_(false),
      sync_with_index_(false),
      config_(std::make_unique<VecIndexConfig>(
          segment_max_row_count,
          field_index_meta,
          segcore_config,
          SegmentType::Growing,
          IsSparseFloatVectorDataType(field_meta.get_data_type()))) {
    recreate_index(field_meta.get_data_type(), field_raw_data);
}

void
VectorFieldIndexing::recreate_index(DataType data_type,
                                    const VectorBase* field_raw_data) {
    if (IsSparseFloatVectorDataType(data_type)) {
        index_ = std::make_unique<index::VectorMemIndex<float>>(
            config_->GetIndexType(),
            config_->GetMetricType(),
            knowhere::Version::GetCurrentVersion().VersionNumber());
    } else if (data_type == DataType::VECTOR_FLOAT) {
        auto concurrent_fp32_vec =
            reinterpret_cast<const ConcurrentVector<FloatVector>*>(
                field_raw_data);
        AssertInfo(concurrent_fp32_vec != nullptr,
                   "Fail to generate a cocurrent vector when recreate_index in "
                   "growing segment.");
        knowhere::ViewDataOp view_data = [field_raw_data_ptr =
                                              concurrent_fp32_vec](size_t id) {
            return (const void*)field_raw_data_ptr->get_element(id);
        };
        index_ = std::make_unique<index::VectorMemIndex<float>>(
            config_->GetIndexType(),
            config_->GetMetricType(),
            knowhere::Version::GetCurrentVersion().VersionNumber(),
            view_data);
    } else if (data_type == DataType::VECTOR_FLOAT16) {
        auto concurrent_fp16_vec =
            reinterpret_cast<const ConcurrentVector<Float16Vector>*>(
                field_raw_data);
        AssertInfo(concurrent_fp16_vec != nullptr,
                   "Fail to generate a cocurrent vector when    recreate_index "
                   "in growing segment.");
        knowhere::ViewDataOp view_data = [field_raw_data_ptr =
                                              concurrent_fp16_vec](size_t id) {
            return (const void*)field_raw_data_ptr->get_element(id);
        };
        index_ = std::make_unique<index::VectorMemIndex<float16>>(
            config_->GetIndexType(),
            config_->GetMetricType(),
            knowhere::Version::GetCurrentVersion().VersionNumber(),
            view_data);
    } else if (data_type == DataType::VECTOR_BFLOAT16) {
        auto concurrent_bf16_vec =
            reinterpret_cast<const ConcurrentVector<BFloat16Vector>*>(
                field_raw_data);
        AssertInfo(concurrent_bf16_vec != nullptr,
                   "Fail to generate a cocurrent vector when    recreate_index "
                   "in growing segment.");
        knowhere::ViewDataOp view_data = [field_raw_data_ptr =
                                              concurrent_bf16_vec](size_t id) {
            return (const void*)field_raw_data_ptr->get_element(id);
        };
        index_ = std::make_unique<index::VectorMemIndex<bfloat16>>(
            config_->GetIndexType(),
            config_->GetMetricType(),
            knowhere::Version::GetCurrentVersion().VersionNumber(),
            view_data);
    }
}

void
VectorFieldIndexing::BuildIndexRange(int64_t ack_beg,
                                     int64_t ack_end,
                                     const VectorBase* vec_base) {
    // No BuildIndexRange support for sparse vector.
    AssertInfo(field_meta_.get_data_type() == DataType::VECTOR_FLOAT ||
                   field_meta_.get_data_type() == DataType::VECTOR_FLOAT16 ||
                   field_meta_.get_data_type() == DataType::VECTOR_BFLOAT16,
               "Data type of vector field is not in (VECTOR_FLOAT, "
               "VECTOR_FLOAT16,VECTOR_BFLOAT16)");
    auto dim = field_meta_.get_dim();
    AssertInfo(
        ConcurrentDenseVectorCheck(vec_base, field_meta_.get_data_type()),
        "vec_base can't cast to ConcurrentVector type");
    auto num_chunk = vec_base->num_chunk();
    AssertInfo(ack_end <= num_chunk, "ack_end is bigger than num_chunk");
    auto conf = get_build_params();
    data_.grow_to_at_least(ack_end);
    for (int chunk_id = ack_beg; chunk_id < ack_end; chunk_id++) {
        const auto& chunk_data = vec_base->get_chunk_data(chunk_id);
        std::unique_ptr<index::VectorIndex> indexing = nullptr;
        if (field_meta_.get_data_type() == DataType::VECTOR_FLOAT) {
            indexing = std::make_unique<index::VectorMemIndex<float>>(
                knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                knowhere::metric::L2,
                knowhere::Version::GetCurrentVersion().VersionNumber());
        } else if (field_meta_.get_data_type() == DataType::VECTOR_FLOAT16) {
            indexing = std::make_unique<index::VectorMemIndex<float16>>(
                knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                knowhere::metric::L2,
                knowhere::Version::GetCurrentVersion().VersionNumber());
        } else {
            indexing = std::make_unique<index::VectorMemIndex<bfloat16>>(
                knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
                knowhere::metric::L2,
                knowhere::Version::GetCurrentVersion().VersionNumber());
        }
        auto dataset = knowhere::GenDataSet(
            vec_base->get_size_per_chunk(), dim, chunk_data);
        indexing->BuildWithDataset(dataset, conf);
        data_[chunk_id] = std::move(indexing);
    }
}

// for sparse float vector:
//   * element_size is not used
//   * output_raw pooints at a milvus::schema::proto::SparseFloatArray.
void
VectorFieldIndexing::GetDataFromIndex(const int64_t* seg_offsets,
                                      int64_t count,
                                      int64_t element_size,
                                      void* output) {
    auto ids_ds = std::make_shared<knowhere::DataSet>();
    ids_ds->SetRows(count);
    ids_ds->SetDim(1);
    ids_ds->SetIds(seg_offsets);
    ids_ds->SetIsOwner(false);
    if (field_meta_.get_data_type() == DataType::VECTOR_SPARSE_FLOAT) {
        auto vector = index_->GetSparseVector(ids_ds);
        SparseRowsToProto(
            [vec_ptr = vector.get()](size_t i) { return vec_ptr + i; },
            count,
            reinterpret_cast<milvus::proto::schema::SparseFloatArray*>(output));
    } else {
        auto vector = index_->GetVector(ids_ds);
        std::memcpy(output, vector.data(), count * element_size);
    }
}

void
VectorFieldIndexing::AppendSegmentIndexSparse(int64_t reserved_offset,
                                              int64_t size,
                                              int64_t new_data_dim,
                                              const VectorBase* field_raw_data,
                                              const void* data_source) {
    auto conf = get_build_params();
    auto source = dynamic_cast<const ConcurrentVector<SparseFloatVector>*>(
        field_raw_data);
    AssertInfo(source,
               "field_raw_data can't cast to "
               "ConcurrentVector<SparseFloatVector> type");
    AssertInfo(size > 0, "append 0 sparse rows to index is not allowed");
    if (!built_) {
        AssertInfo(!sync_with_index_, "index marked synced before built");
        idx_t total_rows = reserved_offset + size;
        idx_t chunk_id = 0;
        auto dim = source->Dim();

        while (total_rows > 0) {
            auto mat = static_cast<const knowhere::sparse::SparseRow<float>*>(
                source->get_chunk_data(chunk_id));
            auto rows = std::min(source->get_size_per_chunk(), total_rows);
            auto dataset = knowhere::GenDataSet(rows, dim, mat);
            dataset->SetIsSparse(true);
            try {
                if (chunk_id == 0) {
                    index_->BuildWithDataset(dataset, conf);
                } else {
                    index_->AddWithDataset(dataset, conf);
                }
            } catch (SegcoreError& error) {
                LOG_ERROR("growing sparse index build error: {}", error.what());
                recreate_index(field_meta_.get_data_type(), nullptr);
                index_cur_ = 0;
                return;
            }
            index_cur_.fetch_add(rows);
            total_rows -= rows;
            chunk_id++;
        }
        built_ = true;
        sync_with_index_ = true;
        // if not built_, new rows in data_source have already been added to
        // source(ConcurrentVector<SparseFloatVector>) and thus added to the
        // index, thus no need to add again.
        return;
    }

    auto dataset = knowhere::GenDataSet(size, new_data_dim, data_source);
    dataset->SetIsSparse(true);
    index_->AddWithDataset(dataset, conf);
    index_cur_.fetch_add(size);
}

void
VectorFieldIndexing::AppendSegmentIndexDense(int64_t reserved_offset,
                                             int64_t size,
                                             const VectorBase* field_raw_data,
                                             const void* data_source) {
    AssertInfo(field_meta_.get_data_type() == DataType::VECTOR_FLOAT ||
                   field_meta_.get_data_type() == DataType::VECTOR_FLOAT16 ||
                   field_meta_.get_data_type() == DataType::VECTOR_BFLOAT16,
               "Data type of vector field is not in (VECTOR_FLOAT, "
               "VECTOR_FLOAT16,VECTOR_BFLOAT16)");
    auto dim = field_meta_.get_dim();
    auto conf = get_build_params();
    auto size_per_chunk = field_raw_data->get_size_per_chunk();
    //append vector [vector_id_beg, vector_id_end] into index
    //build index [vector_id_beg, build_threshold) when index not exist
    AssertInfo(
        ConcurrentDenseVectorCheck(field_raw_data, field_meta_.get_data_type()),
        "vec_base can't cast to ConcurrentVector type");
    size_t vec_length;
    if (field_meta_.get_data_type() == DataType::VECTOR_FLOAT) {
        vec_length = dim * sizeof(float);
    } else if (field_meta_.get_data_type() == DataType::VECTOR_FLOAT16) {
        vec_length = dim * sizeof(float16);
    } else {
        vec_length = dim * sizeof(bfloat16);
    }
    if (!built_) {
        idx_t vector_id_beg = index_cur_.load();
        Assert(vector_id_beg == 0);
        idx_t vector_id_end = get_build_threshold() - 1;
        auto chunk_id_beg = vector_id_beg / size_per_chunk;
        auto chunk_id_end = vector_id_end / size_per_chunk;

        int64_t vec_num = vector_id_end - vector_id_beg + 1;
        // for train index
        const void* data_addr;
        unique_ptr<char[]> vec_data;
        //all train data in one chunk
        if (chunk_id_beg == chunk_id_end) {
            data_addr = field_raw_data->get_chunk_data(chunk_id_beg);
        } else {
            //merge data from multiple chunks together
            vec_data = std::make_unique<char[]>(vec_num * vec_length);
            int64_t offset = 0;
            //copy vector data [vector_id_beg, vector_id_end]
            for (int chunk_id = chunk_id_beg; chunk_id <= chunk_id_end;
                 chunk_id++) {
                int chunk_offset = 0;
                int chunk_copysz =
                    chunk_id == chunk_id_end
                        ? vector_id_end - chunk_id * size_per_chunk + 1
                        : size_per_chunk;
                std::memcpy(
                    (void*)((const char*)vec_data.get() + offset * vec_length),
                    (void*)((const char*)field_raw_data->get_chunk_data(
                                chunk_id) +
                            chunk_offset * vec_length),
                    chunk_copysz * vec_length);
                offset += chunk_copysz;
            }
            data_addr = vec_data.get();
        }
        auto dataset = knowhere::GenDataSet(vec_num, dim, data_addr);
        dataset->SetIsOwner(false);
        try {
            index_->BuildWithDataset(dataset, conf);
        } catch (SegcoreError& error) {
            LOG_ERROR("growing index build error: {}", error.what());
            recreate_index(field_meta_.get_data_type(), field_raw_data);
            return;
        }
        index_cur_.fetch_add(vec_num);
        built_ = true;
    }
    //append rest data when index has built
    idx_t vector_id_beg = index_cur_.load();
    idx_t vector_id_end = reserved_offset + size - 1;
    auto chunk_id_beg = vector_id_beg / size_per_chunk;
    auto chunk_id_end = vector_id_end / size_per_chunk;
    int64_t vec_num = vector_id_end - vector_id_beg + 1;

    if (vec_num <= 0) {
        sync_with_index_.store(true);
        return;
    }

    if (sync_with_index_.load()) {
        Assert(size == vec_num);
        auto dataset = knowhere::GenDataSet(vec_num, dim, data_source);
        index_->AddWithDataset(dataset, conf);
        index_cur_.fetch_add(vec_num);
    } else {
        for (int chunk_id = chunk_id_beg; chunk_id <= chunk_id_end;
             chunk_id++) {
            int chunk_offset = chunk_id == chunk_id_beg
                                   ? index_cur_ - chunk_id * size_per_chunk
                                   : 0;
            int chunk_sz =
                chunk_id == chunk_id_end
                    ? vector_id_end % size_per_chunk - chunk_offset + 1
                    : size_per_chunk - chunk_offset;
            auto dataset = knowhere::GenDataSet(
                chunk_sz,
                dim,
                (const char*)field_raw_data->get_chunk_data(chunk_id) +
                    chunk_offset * vec_length);
            index_->AddWithDataset(dataset, conf);
            index_cur_.fetch_add(chunk_sz);
        }
        sync_with_index_.store(true);
    }
}

knowhere::Json
VectorFieldIndexing::get_build_params() const {
    auto config = config_->GetBuildBaseParams();
    if (!IsSparseFloatVectorDataType(field_meta_.get_data_type())) {
        config[knowhere::meta::DIM] = std::to_string(field_meta_.get_dim());
    }
    config[knowhere::meta::NUM_BUILD_THREAD] = std::to_string(1);
    // for sparse float vector: drop_ratio_build config is not allowed to be set
    // on growing segment index.
    return config;
}

SearchInfo
VectorFieldIndexing::get_search_params(const SearchInfo& searchInfo) const {
    auto conf = config_->GetSearchConf(searchInfo);
    return conf;
}

bool
VectorFieldIndexing::sync_data_with_index() const {
    return sync_with_index_.load();
}

bool
VectorFieldIndexing::has_raw_data() const {
    return index_->HasRawData();
}

template <typename T>
void
ScalarFieldIndexing<T>::BuildIndexRange(int64_t ack_beg,
                                        int64_t ack_end,
                                        const VectorBase* vec_base) {
    auto source = dynamic_cast<const ConcurrentVector<T>*>(vec_base);
    AssertInfo(source, "vec_base can't cast to ConcurrentVector type");
    auto num_chunk = source->num_chunk();
    AssertInfo(ack_end <= num_chunk, "Ack_end is bigger than num_chunk");
    data_.grow_to_at_least(ack_end);
    for (int chunk_id = ack_beg; chunk_id < ack_end; chunk_id++) {
        auto chunk_data = source->get_chunk_data(chunk_id);
        // build index for chunk
        // seem no lint, not pass valid_data here
        // TODO
        if constexpr (std::is_same_v<T, std::string>) {
            auto indexing = index::CreateStringIndexSort();
            indexing->Build(vec_base->get_size_per_chunk(),
                            static_cast<const T*>(chunk_data));
            data_[chunk_id] = std::move(indexing);
        } else {
            auto indexing = index::CreateScalarIndexSort<T>();
            indexing->Build(vec_base->get_size_per_chunk(),
                            static_cast<const T*>(chunk_data));
            data_[chunk_id] = std::move(indexing);
        }
    }
}

std::unique_ptr<FieldIndexing>
CreateIndex(const FieldMeta& field_meta,
            const FieldIndexMeta& field_index_meta,
            int64_t segment_max_row_count,
            const SegcoreConfig& segcore_config,
            const VectorBase* field_raw_data) {
    if (field_meta.is_vector()) {
        if (field_meta.get_data_type() == DataType::VECTOR_FLOAT ||
            field_meta.get_data_type() == DataType::VECTOR_FLOAT16 ||
            field_meta.get_data_type() == DataType::VECTOR_BFLOAT16 ||
            field_meta.get_data_type() == DataType::VECTOR_SPARSE_FLOAT) {
            return std::make_unique<VectorFieldIndexing>(field_meta,
                                                         field_index_meta,
                                                         segment_max_row_count,
                                                         segcore_config,
                                                         field_raw_data);
        } else {
            PanicInfo(DataTypeInvalid,
                      fmt::format("unsupported vector type in index: {}",
                                  field_meta.get_data_type()));
        }
    }
    switch (field_meta.get_data_type()) {
        case DataType::BOOL:
            return std::make_unique<ScalarFieldIndexing<bool>>(field_meta,
                                                               segcore_config);
        case DataType::INT8:
            return std::make_unique<ScalarFieldIndexing<int8_t>>(
                field_meta, segcore_config);
        case DataType::INT16:
            return std::make_unique<ScalarFieldIndexing<int16_t>>(
                field_meta, segcore_config);
        case DataType::INT32:
            return std::make_unique<ScalarFieldIndexing<int32_t>>(
                field_meta, segcore_config);
        case DataType::INT64:
            return std::make_unique<ScalarFieldIndexing<int64_t>>(
                field_meta, segcore_config);
        case DataType::FLOAT:
            return std::make_unique<ScalarFieldIndexing<float>>(field_meta,
                                                                segcore_config);
        case DataType::DOUBLE:
            return std::make_unique<ScalarFieldIndexing<double>>(
                field_meta, segcore_config);
        case DataType::VARCHAR:
            return std::make_unique<ScalarFieldIndexing<std::string>>(
                field_meta, segcore_config);
        default:
            PanicInfo(DataTypeInvalid,
                      fmt::format("unsupported scalar type in index: {}",
                                  field_meta.get_data_type()));
    }
}

}  // namespace milvus::segcore
