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

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <map>
#include <memory>

#include <tbb/concurrent_vector.h>
#include <index/Index.h>
#include <index/ScalarIndex.h>

#include "InsertRecord.h"
#include "cachinglayer/CacheSlot.h"
#include "common/FieldMeta.h"
#include "common/Schema.h"
#include "common/IndexMeta.h"
#include "IndexConfigGenerator.h"
#include "common/Types.h"
#include "knowhere/config.h"
#include "log/Log.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/InsertRecord.h"
#include "index/VectorIndex.h"

namespace milvus::segcore {

// this should be concurrent
// All concurrent
class FieldIndexing {
 public:
    explicit FieldIndexing(const FieldMeta& field_meta,
                           const SegcoreConfig& segcore_config)
        : data_type_(field_meta.get_data_type()),
          dim_(IsVectorDataType(field_meta.get_data_type()) &&
                       !IsSparseFloatVectorDataType(field_meta.get_data_type())
                   ? field_meta.get_dim()
                   : 1),
          segcore_config_(segcore_config) {
    }
    FieldIndexing(const FieldIndexing&) = delete;
    FieldIndexing&
    operator=(const FieldIndexing&) = delete;
    virtual ~FieldIndexing() = default;

    virtual void
    AppendSegmentIndexDense(int64_t reserved_offset,
                            int64_t size,
                            const VectorBase* vec_base,
                            const void* data_source) = 0;

    // new_data_dim is the dimension of the new data being appended(data_source)
    virtual void
    AppendSegmentIndexSparse(int64_t reserved_offset,
                             int64_t size,
                             int64_t new_data_dim,
                             const VectorBase* vec_base,
                             const void* data_source) = 0;

    // For scalar fields (including geometry), append data incrementally
    virtual void
    AppendSegmentIndex(int64_t reserved_offset,
                       int64_t size,
                       const VectorBase* vec_base,
                       const DataArray* stream_data) = 0;

    // For scalar fields (including geometry), append data incrementally (FieldDataPtr version)
    virtual void
    AppendSegmentIndex(int64_t reserved_offset,
                       int64_t size,
                       const VectorBase* vec_base,
                       const FieldDataPtr& field_data) = 0;

    virtual void
    GetDataFromIndex(const int64_t* seg_offsets,
                     int64_t count,
                     int64_t element_size,
                     void* output) = 0;

    virtual int64_t
    get_build_threshold() const = 0;

    virtual bool
    sync_data_with_index() const = 0;

    virtual bool
    has_raw_data() const {
        return true;
    }

    DataType
    get_data_type() const {
        return data_type_;
    }

    int64_t
    get_dim() const {
        return dim_;
    }

    int64_t
    get_size_per_chunk() const {
        return segcore_config_.get_chunk_rows();
    }

    virtual PinWrapper<index::IndexBase*>
    get_chunk_indexing(int64_t chunk_id) const = 0;

    virtual PinWrapper<index::IndexBase*>
    get_segment_indexing() const = 0;

 protected:
    // additional info
    const DataType data_type_;
    const int64_t dim_;
    const SegcoreConfig& segcore_config_;
};

template <typename T>
class ScalarFieldIndexing : public FieldIndexing {
 public:
    using FieldIndexing::FieldIndexing;

    explicit ScalarFieldIndexing(const FieldMeta& field_meta,
                                 const FieldIndexMeta& field_index_meta,
                                 int64_t segment_max_row_count,
                                 const SegcoreConfig& segcore_config,
                                 const VectorBase* field_raw_data);

    void
    AppendSegmentIndexDense(int64_t reserved_offset,
                            int64_t size,
                            const VectorBase* vec_base,
                            const void* data_source) override {
        ThrowInfo(Unsupported,
                  "scalar index doesn't support append vector segment index");
    }

    void
    AppendSegmentIndexSparse(int64_t reserved_offset,
                             int64_t size,
                             int64_t new_data_dim,
                             const VectorBase* vec_base,
                             const void* data_source) override {
        ThrowInfo(Unsupported,
                  "scalar index doesn't support append vector segment index");
    }

    void
    AppendSegmentIndex(int64_t reserved_offset,
                       int64_t size,
                       const VectorBase* vec_base,
                       const DataArray* stream_data) override;

    void
    AppendSegmentIndex(int64_t reserved_offset,
                       int64_t size,
                       const VectorBase* vec_base,
                       const FieldDataPtr& field_data) override;

    void
    GetDataFromIndex(const int64_t* seg_offsets,
                     int64_t count,
                     int64_t element_size,
                     void* output) override {
        ThrowInfo(Unsupported,
                  "scalar index don't support get data from index");
    }

    bool
    has_raw_data() const override {
        return index_->HasRawData();
    }

    int64_t
    get_build_threshold() const override {
        return 0;
    }

    bool
    sync_data_with_index() const override {
        // For geometry fields, check if index is built and synchronized
        if constexpr (std::is_same_v<T, std::string>) {
            if (data_type_ == DataType::GEOMETRY) {
                bool is_built = built_.load();
                bool is_synced = sync_with_index_.load();
                LOG_DEBUG(
                    "ScalarFieldIndexing::sync_data_with_index for geometry "
                    "field: built={}, synced={}",
                    is_built,
                    is_synced);
                return is_built && is_synced;
            }
        }
        // For other scalar fields, not supported yet
        return false;
    }

    // concurrent
    PinWrapper<index::IndexBase*>
    get_chunk_indexing(int64_t chunk_id) const override {
        Assert(!IsVectorDataType(data_type_));
        return data_.at(chunk_id).get();
    }

    PinWrapper<index::IndexBase*>
    get_segment_indexing() const override {
        // For geometry fields, return the single index
        if constexpr (std::is_same_v<T, std::string>) {
            if (data_type_ == DataType::GEOMETRY) {
                return index_.get();
            }
        }
        // For other scalar fields, not supported yet
        return nullptr;
    }

 private:
    void
    recreate_index(const FieldMeta& field_meta,
                   const VectorBase* field_raw_data);

    // Helper function to process geometry data and add to R-Tree index
    template <typename GeometryDataAccessor>
    void
    process_geometry_data(int64_t reserved_offset,
                          int64_t size,
                          const VectorBase* vec_base,
                          GeometryDataAccessor&& accessor,
                          const std::string& log_source);

    // current number of rows in index.
    std::atomic<idx_t> index_cur_ = 0;
    // whether the growing index has been built.
    std::atomic<bool> built_ = false;
    // whether all inserted data has been added to growing index and can be searched.
    std::atomic<bool> sync_with_index_ = false;

    // Configuration for scalar index building
    std::unique_ptr<FieldIndexMeta> config_;

    // Single scalar index for incremental indexing (new approach)
    std::unique_ptr<index::ScalarIndex<T>> index_;

    // Chunk-based indexes for compatibility (old approach)
    tbb::concurrent_vector<index::ScalarIndexPtr<T>> data_;
};

class VectorFieldIndexing : public FieldIndexing {
 public:
    using FieldIndexing::FieldIndexing;

    explicit VectorFieldIndexing(const FieldMeta& field_meta,
                                 const FieldIndexMeta& field_index_meta,
                                 int64_t segment_max_row_count,
                                 const SegcoreConfig& segcore_config,
                                 const VectorBase* field_raw_data);

    void
    AppendSegmentIndexDense(int64_t reserved_offset,
                            int64_t size,
                            const VectorBase* field_raw_data,
                            const void* data_source) override;

    void
    AppendSegmentIndexSparse(int64_t reserved_offset,
                             int64_t size,
                             int64_t new_data_dim,
                             const VectorBase* field_raw_data,
                             const void* data_source) override;

    void
    AppendSegmentIndex(int64_t reserved_offset,
                       int64_t size,
                       const VectorBase* vec_base,
                       const DataArray* stream_data) override {
        ThrowInfo(Unsupported,
                  "vector index should use AppendSegmentIndexDense/Sparse");
    }

    void
    AppendSegmentIndex(int64_t reserved_offset,
                       int64_t size,
                       const VectorBase* vec_base,
                       const FieldDataPtr& field_data) override {
        ThrowInfo(Unsupported,
                  "vector index should use AppendSegmentIndexDense/Sparse");
    }

    // for sparse float vector:
    //   * element_size is not used
    //   * output_raw pooints at a milvus::schema::proto::SparseFloatArray.
    void
    GetDataFromIndex(const int64_t* seg_offsets,
                     int64_t count,
                     int64_t element_size,
                     void* output) override;

    int64_t
    get_build_threshold() const override {
        return config_->GetBuildThreshold();
    }

    // concurrent
    PinWrapper<index::IndexBase*>
    get_chunk_indexing(int64_t chunk_id) const override {
        Assert(IsVectorDataType(data_type_));
        return PinWrapper<index::IndexBase*>(data_.at(chunk_id).get());
    }

    PinWrapper<index::IndexBase*>
    get_segment_indexing() const override {
        return PinWrapper<index::IndexBase*>(index_.get());
    }

    bool
    sync_data_with_index() const override;

    bool
    has_raw_data() const override;

    knowhere::Json
    get_build_params(DataType data_type) const;

    SearchInfo
    get_search_params(const SearchInfo& searchInfo) const;

 private:
    void
    recreate_index(DataType data_type, const VectorBase* field_raw_data);
    // current number of rows in index.
    std::atomic<idx_t> index_cur_ = 0;
    // whether the growing index has been built.
    std::atomic<bool> built_;
    // whether all insertd data has been added to growing index and can be
    // searched.
    std::atomic<bool> sync_with_index_;
    std::unique_ptr<VecIndexConfig> config_;
    std::unique_ptr<index::VectorIndex> index_;
    tbb::concurrent_vector<std::unique_ptr<index::VectorIndex>> data_;
};

std::unique_ptr<FieldIndexing>
CreateIndex(const FieldMeta& field_meta,
            const FieldIndexMeta& field_index_meta,
            int64_t segment_max_row_count,
            const SegcoreConfig& segcore_config,
            const VectorBase* field_raw_data = nullptr);

class IndexingRecord {
 public:
    explicit IndexingRecord(const Schema& schema,
                            const IndexMetaPtr& indexMetaPtr,
                            const SegcoreConfig& segcore_config,
                            const InsertRecord<false>* insert_record)
        : schema_(schema),
          index_meta_(indexMetaPtr),
          segcore_config_(segcore_config) {
        Initialize(insert_record);
    }

    void
    Initialize(const InsertRecord<false>* insert_record) {
        int offset_id = 0;
        auto enable_growing_mmap = storage::MmapManager::GetInstance()
                                       .GetMmapConfig()
                                       .GetEnableGrowingMmap();
        for (auto& [field_id, field_meta] : schema_.get_fields()) {
            ++offset_id;
            if (field_meta.is_vector() &&
                segcore_config_.get_enable_interim_segment_index() &&
                !enable_growing_mmap) {
                // TODO: skip binary small index now, reenable after config.yaml is ready
                if (field_meta.get_data_type() == DataType::VECTOR_BINARY) {
                    continue;
                }

                if (index_meta_ == nullptr) {
                    LOG_INFO("miss index meta for growing interim index");
                    continue;
                }
                //Small-Index enabled, create index for vector field only
                if (index_meta_->GetIndexMaxRowCount() > 0 &&
                    index_meta_->HasField(field_id)) {
                    auto vec_field_meta =
                        index_meta_->GetFieldIndexMeta(field_id);
                    //Disable growing index for flat and embedding list
                    if (!vec_field_meta.IsFlatIndex() &&
                        field_meta.get_data_type() != DataType::VECTOR_ARRAY) {
                        auto field_raw_data =
                            insert_record->get_data_base(field_id);
                        field_indexings_.try_emplace(
                            field_id,
                            CreateIndex(field_meta,
                                        vec_field_meta,
                                        index_meta_->GetIndexMaxRowCount(),
                                        segcore_config_,
                                        field_raw_data));
                    }
                }
            } else if (field_meta.get_data_type() == DataType::GEOMETRY) {
                if (index_meta_ == nullptr) {
                    LOG_INFO("miss index meta for growing interim index");
                    continue;
                }

                if (index_meta_->GetIndexMaxRowCount() > 0 &&
                    index_meta_->HasField(field_id)) {
                    auto geo_field_meta =
                        index_meta_->GetFieldIndexMeta(field_id);
                    auto field_raw_data =
                        insert_record->get_data_base(field_id);
                    field_indexings_.try_emplace(
                        field_id,
                        CreateIndex(field_meta,
                                    geo_field_meta,
                                    index_meta_->GetIndexMaxRowCount(),
                                    segcore_config_,
                                    field_raw_data));
                }
            }
        }
        assert(offset_id == schema_.size());
    }

    // concurrent, reentrant
    void
    AppendingIndex(int64_t reserved_offset,
                   int64_t size,
                   FieldId fieldId,
                   const DataArray* stream_data,
                   const InsertRecord<false>& record) {
        if (!is_in(fieldId)) {
            return;
        }
        auto& indexing = field_indexings_.at(fieldId);
        auto type = indexing->get_data_type();
        auto field_raw_data = record.get_data_base(fieldId);
        if (type == DataType::VECTOR_FLOAT &&
            reserved_offset + size >= indexing->get_build_threshold()) {
            indexing->AppendSegmentIndexDense(
                reserved_offset,
                size,
                field_raw_data,
                stream_data->vectors().float_vector().data().data());
        } else if (type == DataType::VECTOR_FLOAT16 &&
                   reserved_offset + size >= indexing->get_build_threshold()) {
            indexing->AppendSegmentIndexDense(
                reserved_offset,
                size,
                field_raw_data,
                stream_data->vectors().float16_vector().data());
        } else if (type == DataType::VECTOR_BFLOAT16 &&
                   reserved_offset + size >= indexing->get_build_threshold()) {
            indexing->AppendSegmentIndexDense(
                reserved_offset,
                size,
                field_raw_data,
                stream_data->vectors().bfloat16_vector().data());
        } else if (type == DataType::VECTOR_SPARSE_U32_F32) {
            auto data = SparseBytesToRows(
                stream_data->vectors().sparse_float_vector().contents());
            indexing->AppendSegmentIndexSparse(
                reserved_offset,
                size,
                stream_data->vectors().sparse_float_vector().dim(),
                field_raw_data,
                data.get());
        } else if (type == DataType::GEOMETRY) {
            // For geometry fields, append data incrementally to RTree index
            indexing->AppendSegmentIndex(
                reserved_offset, size, field_raw_data, stream_data);
        }
    }

    // concurrent, reentrant
    void
    AppendingIndex(int64_t reserved_offset,
                   int64_t size,
                   FieldId fieldId,
                   const FieldDataPtr data,
                   const InsertRecord<false>& record) {
        if (!is_in(fieldId)) {
            return;
        }
        auto& indexing = field_indexings_.at(fieldId);
        auto type = indexing->get_data_type();
        const void* p = data->Data();

        if ((type == DataType::VECTOR_FLOAT ||
             type == DataType::VECTOR_FLOAT16 ||
             type == DataType::VECTOR_BFLOAT16) &&
            reserved_offset + size >= indexing->get_build_threshold()) {
            auto vec_base = record.get_data_base(fieldId);
            indexing->AppendSegmentIndexDense(
                reserved_offset, size, vec_base, data->Data());
        } else if (type == DataType::VECTOR_SPARSE_U32_F32) {
            auto vec_base = record.get_data_base(fieldId);
            indexing->AppendSegmentIndexSparse(
                reserved_offset,
                size,
                std::dynamic_pointer_cast<const FieldData<SparseFloatVector>>(
                    data)
                    ->Dim(),
                vec_base,
                p);
        } else if (type == DataType::GEOMETRY) {
            // For geometry fields, append data incrementally to RTree index
            auto vec_base = record.get_data_base(fieldId);
            indexing->AppendSegmentIndex(reserved_offset, size, vec_base, data);
        }
    }

    // for sparse float vector:
    //   * element_size is not used
    //   * output_raw pooints at a milvus::schema::proto::SparseFloatArray.
    void
    GetDataFromIndex(FieldId fieldId,
                     const int64_t* seg_offsets,
                     int64_t count,
                     int64_t element_size,
                     void* output_raw) const {
        if (is_in(fieldId)) {
            auto& indexing = field_indexings_.at(fieldId);
            auto data_type = indexing->get_data_type();
            if (data_type == DataType::VECTOR_FLOAT ||
                data_type == DataType::VECTOR_FLOAT16 ||
                data_type == DataType::VECTOR_BFLOAT16 ||
                data_type == DataType::VECTOR_SPARSE_U32_F32) {
                indexing->GetDataFromIndex(
                    seg_offsets, count, element_size, output_raw);
            }
        }
    }

    // result shows the index has synchronized with all inserted data or not
    bool
    SyncDataWithIndex(FieldId fieldId) const {
        if (is_in(fieldId)) {
            const FieldIndexing& indexing = get_field_indexing(fieldId);
            return indexing.sync_data_with_index();
        }
        return false;
    }

    bool
    HasRawData(FieldId fieldId) const {
        if (is_in(fieldId) && SyncDataWithIndex(fieldId)) {
            const FieldIndexing& indexing = get_field_indexing(fieldId);
            return indexing.has_raw_data();
        }
        // if this field id not in IndexingRecord or not build index, we should find raw data in InsertRecord instead of IndexingRecord.
        return false;
    }

    // concurrent
    int64_t
    get_finished_ack() const {
        return finished_ack_.GetAck();
    }

    const FieldIndexing&
    get_field_indexing(FieldId field_id) const {
        Assert(field_indexings_.count(field_id));
        return *field_indexings_.at(field_id);
    }

    const VectorFieldIndexing&
    get_vec_field_indexing(FieldId field_id) const {
        auto& field_indexing = get_field_indexing(field_id);
        auto ptr = dynamic_cast<const VectorFieldIndexing*>(&field_indexing);
        AssertInfo(ptr, "invalid indexing");
        return *ptr;
    }

    const FieldIndexMeta&
    get_field_index_meta(FieldId fieldId) const {
        return index_meta_->GetFieldIndexMeta(fieldId);
    }

    bool
    is_in(FieldId field_id) const {
        return field_indexings_.count(field_id);
    }

    template <typename T>
    auto
    get_scalar_field_indexing(FieldId field_id) const
        -> const ScalarFieldIndexing<T>& {
        auto& entry = get_field_indexing(field_id);
        auto ptr = dynamic_cast<const ScalarFieldIndexing<T>*>(&entry);
        AssertInfo(ptr, "invalid indexing");
        return *ptr;
    }

 private:
    const Schema& schema_;
    IndexMetaPtr index_meta_;
    const SegcoreConfig& segcore_config_;

    // control info
    std::atomic<int64_t> resource_ack_ = 0;
    //    std::atomic<int64_t> finished_ack_ = 0;
    AckResponder finished_ack_;
    std::mutex mutex_;

    // field_offset => indexing
    std::map<FieldId, std::unique_ptr<FieldIndexing>> field_indexings_;
};

}  // namespace milvus::segcore
