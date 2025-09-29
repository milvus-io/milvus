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
#include "common/Types.h"
#include "fmt/format.h"
#include "index/ScalarIndexSort.h"
#include "index/StringIndexMarisa.h"

#include "common/SystemProperty.h"
#include "segcore/FieldIndexing.h"
#include "index/VectorMemIndex.h"
#include "IndexConfigGenerator.h"
#include "index/RTreeIndex.h"
#include "storage/FileManager.h"
#include "storage/LocalChunkManagerSingleton.h"

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
        index_ = std::make_unique<index::VectorMemIndex<sparse_u32_f32>>(
            DataType::NONE,
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
            DataType::NONE,
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
            DataType::NONE,
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
            DataType::NONE,
            config_->GetIndexType(),
            config_->GetMetricType(),
            knowhere::Version::GetCurrentVersion().VersionNumber(),
            view_data);
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
    if (IsSparseFloatVectorDataType(get_data_type())) {
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
    auto conf = get_build_params(get_data_type());
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
            auto mat = static_cast<
                const knowhere::sparse::SparseRow<SparseValueType>*>(
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
                recreate_index(get_data_type(), nullptr);
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
    AssertInfo(get_data_type() == DataType::VECTOR_FLOAT ||
                   get_data_type() == DataType::VECTOR_FLOAT16 ||
                   get_data_type() == DataType::VECTOR_BFLOAT16,
               "Data type of vector field is not in (VECTOR_FLOAT, "
               "VECTOR_FLOAT16,VECTOR_BFLOAT16)");
    auto dim = get_dim();
    auto conf = get_build_params(get_data_type());
    auto size_per_chunk = field_raw_data->get_size_per_chunk();
    //append vector [vector_id_beg, vector_id_end] into index
    //build index [vector_id_beg, build_threshold) when index not exist
    AssertInfo(ConcurrentDenseVectorCheck(field_raw_data, get_data_type()),
               "vec_base can't cast to ConcurrentVector type");
    size_t vec_length;
    if (get_data_type() == DataType::VECTOR_FLOAT) {
        vec_length = dim * sizeof(float);
    } else if (get_data_type() == DataType::VECTOR_FLOAT16) {
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
            recreate_index(get_data_type(), field_raw_data);
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
VectorFieldIndexing::get_build_params(DataType data_type) const {
    auto config = config_->GetBuildBaseParams(data_type);
    if (!IsSparseFloatVectorDataType(get_data_type())) {
        config[knowhere::meta::DIM] = std::to_string(get_dim());
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
ScalarFieldIndexing<T>::ScalarFieldIndexing(
    const FieldMeta& field_meta,
    const FieldIndexMeta& field_index_meta,
    int64_t segment_max_row_count,
    const SegcoreConfig& segcore_config,
    const VectorBase* field_raw_data)
    : FieldIndexing(field_meta, segcore_config),
      built_(false),
      sync_with_index_(false),
      config_(std::make_unique<FieldIndexMeta>(field_index_meta)) {
    recreate_index(field_meta, field_raw_data);
}

template <typename T>
void
ScalarFieldIndexing<T>::recreate_index(const FieldMeta& field_meta,
                                       const VectorBase* field_raw_data) {
    if constexpr (std::is_same_v<T, std::string>) {
        if (field_meta.get_data_type() == DataType::GEOMETRY) {
            // Create chunk manager for file operations
            auto chunk_manager =
                milvus::storage::LocalChunkManagerSingleton::GetInstance()
                    .GetChunkManager();

            // Create FieldDataMeta for RTree index
            storage::FieldDataMeta field_data_meta;
            field_data_meta.field_id = field_meta.get_id().get();

            // Create a minimal field schema from FieldMeta
            field_data_meta.field_schema.set_fieldid(field_meta.get_id().get());
            field_data_meta.field_schema.set_name(field_meta.get_name().get());
            field_data_meta.field_schema.set_data_type(
                static_cast<proto::schema::DataType>(
                    field_meta.get_data_type()));
            field_data_meta.field_schema.set_nullable(field_meta.is_nullable());

            // Create IndexMeta for RTree index
            storage::IndexMeta index_meta;
            index_meta.segment_id = 0;
            index_meta.field_id = field_meta.get_id().get();
            index_meta.build_id = 0;
            index_meta.index_version = 1;
            index_meta.key = "rtree_index";
            index_meta.field_name = field_meta.get_name().get();
            index_meta.field_type = field_meta.get_data_type();
            index_meta.index_non_encoding = false;

            // Create FileManagerContext with all required components
            storage::FileManagerContext ctx(
                field_data_meta, index_meta, chunk_manager);

            index_ = std::make_unique<index::RTreeIndex<std::string>>(ctx);
            built_ = false;
            sync_with_index_ = false;
            index_cur_ = 0;
            LOG_INFO(
                "Created R-Tree index for geometry data type: {} with "
                "FileManagerContext",
                field_meta.get_data_type());
            return;
        }
        index_ = index::CreateStringIndexMarisa();
    } else {
        index_ = index::CreateScalarIndexSort<T>();
    }

    built_ = false;
    sync_with_index_ = false;
    index_cur_ = 0;

    LOG_INFO("Created scalar index for data type: {}",
             field_meta.get_data_type());
}

template <typename T>
void
ScalarFieldIndexing<T>::AppendSegmentIndex(int64_t reserved_offset,
                                           int64_t size,
                                           const VectorBase* vec_base,
                                           const DataArray* stream_data) {
    // Special handling for geometry fields (stored as std::string)
    if constexpr (std::is_same_v<T, std::string>) {
        if (get_data_type() == DataType::GEOMETRY) {
            // Extract geometry data from stream_data
            if (stream_data->has_scalars() &&
                stream_data->scalars().has_geometry_data()) {
                const auto& geometry_array =
                    stream_data->scalars().geometry_data();
                const auto& valid_data = stream_data->valid_data();

                // Create accessor for DataArray
                auto accessor = [&geometry_array, &valid_data](
                                    int64_t i) -> std::pair<std::string, bool> {
                    bool is_valid = valid_data.empty() || valid_data[i];
                    if (is_valid && i < geometry_array.data_size()) {
                        return {geometry_array.data(i), true};
                    }
                    return {"", false};
                };

                process_geometry_data(
                    reserved_offset, size, vec_base, accessor, "DataArray");
            }
            return;
        }
    }

    // For other scalar fields, not implemented yet
    ThrowInfo(Unsupported,
              "ScalarFieldIndexing::AppendSegmentIndex from DataArray not "
              "implemented for non-geometry scalar fields. Type: {}",
              get_data_type());
}

template <typename T>
void
ScalarFieldIndexing<T>::AppendSegmentIndex(int64_t reserved_offset,
                                           int64_t size,
                                           const VectorBase* vec_base,
                                           const FieldDataPtr& field_data) {
    // Special handling for geometry fields (stored as std::string)
    if constexpr (std::is_same_v<T, std::string>) {
        if (get_data_type() == DataType::GEOMETRY) {
            // Extract geometry data from field_data
            const void* raw_data = field_data->Data();
            if (raw_data) {
                const auto* string_array =
                    static_cast<const std::string*>(raw_data);

                // Create accessor for FieldDataPtr
                auto accessor = [field_data, string_array](
                                    int64_t i) -> std::pair<std::string, bool> {
                    bool is_valid = field_data->is_valid(i);
                    if (is_valid) {
                        return {string_array[i], true};
                    }
                    return {"", false};
                };

                process_geometry_data(
                    reserved_offset, size, vec_base, accessor, "FieldData");
            }
            return;
        }
    }

    // For other scalar fields, not implemented yet
    ThrowInfo(Unsupported,
              "ScalarFieldIndexing::AppendSegmentIndex from FieldDataPtr not "
              "implemented for non-geometry scalar fields. Type: {}",
              get_data_type());
}

template <typename T>
template <typename GeometryDataAccessor>
void
ScalarFieldIndexing<T>::process_geometry_data(int64_t reserved_offset,
                                              int64_t size,
                                              const VectorBase* vec_base,
                                              GeometryDataAccessor&& accessor,
                                              const std::string& log_source) {
    // Special handling for geometry fields (stored as std::string)
    if constexpr (std::is_same_v<T, std::string>) {
        if (get_data_type() == DataType::GEOMETRY) {
            // Cast to R-Tree index for geometry data
            auto* rtree_index =
                dynamic_cast<index::RTreeIndex<std::string>*>(index_.get());
            if (!rtree_index) {
                ThrowInfo(UnexpectedError,
                          "Failed to cast to R-Tree index for geometry field");
            }

            // Initialize R-Tree index on first data arrival (no threshold waiting)
            if (!built_) {
                try {
                    // Initialize R-Tree for building immediately when first data arrives
                    rtree_index->InitForBuildIndex();
                    built_ = true;
                    sync_with_index_ = true;
                    LOG_INFO(
                        "Initialized R-Tree index for immediate incremental "
                        "building from {}",
                        log_source);
                } catch (std::exception& error) {
                    ThrowInfo(UnexpectedError,
                              "R-Tree index initialization error: {}",
                              error.what());
                }
            }

            // Always add geometries incrementally (no bulk build phase)
            int64_t added_count = 0;
            for (int64_t i = 0; i < size; ++i) {
                int64_t global_offset = reserved_offset + i;

                // Use the accessor to get geometry data and validity
                auto [wkb_data, is_valid] = accessor(i);

                if (is_valid) {
                    try {
                        rtree_index->AddGeometry(wkb_data, global_offset);
                        added_count++;
                    } catch (std::exception& error) {
                        ThrowInfo(UnexpectedError,
                                  "Failed to add geometry at offset {}: {}",
                                  global_offset,
                                  error.what());
                    }
                }
            }

            // Update statistics
            index_cur_.fetch_add(added_count);
            sync_with_index_.store(true);

            LOG_INFO("Added {} geometries to R-Tree index immediately from {}",
                     added_count,
                     log_source);
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
            field_meta.get_data_type() == DataType::VECTOR_INT8 ||
            field_meta.get_data_type() == DataType::VECTOR_SPARSE_U32_F32) {
            return std::make_unique<VectorFieldIndexing>(field_meta,
                                                         field_index_meta,
                                                         segment_max_row_count,
                                                         segcore_config,
                                                         field_raw_data);
        } else {
            ThrowInfo(DataTypeInvalid,
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
        case DataType::TIMESTAMPTZ:
            return std::make_unique<ScalarFieldIndexing<int64_t>>(
                field_meta, segcore_config);
        case DataType::VARCHAR:
            return std::make_unique<ScalarFieldIndexing<std::string>>(
                field_meta, segcore_config);
        case DataType::GEOMETRY:
            return std::make_unique<ScalarFieldIndexing<std::string>>(
                field_meta,
                field_index_meta,
                segment_max_row_count,
                segcore_config,
                field_raw_data);
        default:
            ThrowInfo(DataTypeInvalid,
                      fmt::format("unsupported scalar type in index: {}",
                                  field_meta.get_data_type()));
    }
}

// Explicit template instantiation for ScalarFieldIndexing
template class ScalarFieldIndexing<std::string>;

}  // namespace milvus::segcore
