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

#include <optional>
#include <map>
#include <memory>

#include <tbb/concurrent_vector.h>
#include <index/Index.h>
#include <index/ScalarIndex.h>

#include "AckResponder.h"
#include "InsertRecord.h"
#include "common/Schema.h"
#include "common/IndexMeta.h"
#include "IndexConfigGenerator.h"
#include "segcore/SegcoreConfig.h"
#include "index/VectorIndex.h"

namespace milvus::segcore {

// this should be concurrent
// All concurrent
class FieldIndexing {
 public:
    explicit FieldIndexing(const FieldMeta& field_meta,
                           const SegcoreConfig& segcore_config)
        : field_meta_(field_meta), segcore_config_(segcore_config) {
    }
    FieldIndexing(const FieldIndexing&) = delete;
    FieldIndexing&
    operator=(const FieldIndexing&) = delete;
    virtual ~FieldIndexing() = default;

    // Do this in parallel
    virtual void
    BuildIndexRange(int64_t ack_beg,
                    int64_t ack_end,
                    const VectorBase* vec_base) = 0;

    virtual void
    AppendSegmentIndex(int64_t reserved_offset,
                       int64_t size,
                       const VectorBase* vec_base,
                       const void* data_source) = 0;

    virtual void
    GetDataFromIndex(const int64_t* seg_offsets,
                     int64_t count,
                     int64_t element_size,
                     void* output) = 0;

    virtual int64_t
    get_build_threshold() const = 0;

    virtual bool
    sync_data_with_index() const = 0;

    const FieldMeta&
    get_field_meta() {
        return field_meta_;
    }

    virtual idx_t
    get_index_cursor() = 0;

    int64_t
    get_size_per_chunk() const {
        return segcore_config_.get_chunk_rows();
    }

    virtual index::IndexBase*
    get_chunk_indexing(int64_t chunk_id) const = 0;

    virtual index::IndexBase*
    get_segment_indexing() const = 0;

 protected:
    // additional info
    const FieldMeta& field_meta_;
    const SegcoreConfig& segcore_config_;
};

template <typename T>
class ScalarFieldIndexing : public FieldIndexing {
 public:
    using FieldIndexing::FieldIndexing;

    void
    BuildIndexRange(int64_t ack_beg,
                    int64_t ack_end,
                    const VectorBase* vec_base) override;

    void
    AppendSegmentIndex(int64_t reserved_offset,
                       int64_t size,
                       const VectorBase* vec_base,
                       const void* data_source) override {
        PanicInfo("scalar index don't support append segment index");
    }

    void
    GetDataFromIndex(const int64_t* seg_offsets,
                     int64_t count,
                     int64_t element_size,
                     void* output) override {
        PanicInfo("scalar index don't support get data from index");
    }
    idx_t
    get_index_cursor() override {
        return 0;
    }

    int64_t
    get_build_threshold() const override {
        return 0;
    }

    bool
    sync_data_with_index() const override {
        return false;
    }

    // concurrent
    index::ScalarIndex<T>*
    get_chunk_indexing(int64_t chunk_id) const override {
        Assert(!field_meta_.is_vector());
        return data_.at(chunk_id).get();
    }

    index::IndexBase*
    get_segment_indexing() const override {
        return nullptr;
    }

 private:
    tbb::concurrent_vector<index::ScalarIndexPtr<T>> data_;
};

class VectorFieldIndexing : public FieldIndexing {
 public:
    using FieldIndexing::FieldIndexing;

    explicit VectorFieldIndexing(const FieldMeta& field_meta,
                                 const FieldIndexMeta& field_index_meta,
                                 int64_t segment_max_row_count,
                                 const SegcoreConfig& segcore_config);

    void
    BuildIndexRange(int64_t ack_beg,
                    int64_t ack_end,
                    const VectorBase* vec_base) override;

    void
    AppendSegmentIndex(int64_t reserved_offset,
                       int64_t size,
                       const VectorBase* vec_base,
                       const void* data_source) override;

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
    index::IndexBase*
    get_chunk_indexing(int64_t chunk_id) const override {
        Assert(field_meta_.is_vector());
        return data_.at(chunk_id).get();
    }
    index::IndexBase*
    get_segment_indexing() const override {
        return index_.get();
    }

    bool
    sync_data_with_index() const override;

    idx_t
    get_index_cursor() override;

    knowhere::Json
    get_build_params() const;

    SearchInfo
    get_search_params(const SearchInfo& searchInfo) const;

 private:
    std::atomic<idx_t> index_cur_ = 0;
    std::atomic<bool> sync_with_index;
    std::unique_ptr<VecIndexConfig> config_;
    std::unique_ptr<index::VectorIndex> index_;
    tbb::concurrent_vector<std::unique_ptr<index::VectorIndex>> data_;
};

std::unique_ptr<FieldIndexing>
CreateIndex(const FieldMeta& field_meta,
            const FieldIndexMeta& field_index_meta,
            int64_t segment_max_row_count,
            const SegcoreConfig& segcore_config);

class IndexingRecord {
 public:
    explicit IndexingRecord(const Schema& schema,
                            const IndexMetaPtr& indexMetaPtr,
                            const SegcoreConfig& segcore_config)
        : schema_(schema),
          index_meta_(indexMetaPtr),
          segcore_config_(segcore_config) {
        Initialize();
    }

    void
    Initialize() {
        int offset_id = 0;
        for (auto& [field_id, field_meta] : schema_.get_fields()) {
            ++offset_id;
            if (field_meta.is_vector() &&
                segcore_config_.get_enable_growing_segment_index()) {
                // TODO: skip binary small index now, reenable after config.yaml is ready
                if (field_meta.get_data_type() == DataType::VECTOR_BINARY) {
                    continue;
                }
                //Small-Index disabled, create index for vector field only
                if (index_meta_->GetIndexMaxRowCount() > 0 &&
                    index_meta_->HasFiled(field_id)) {
                    field_indexings_.try_emplace(
                        field_id,
                        CreateIndex(field_meta,
                                    index_meta_->GetFieldIndexMeta(field_id),
                                    index_meta_->GetIndexMaxRowCount(),
                                    segcore_config_));
                }
            }
        }
        assert(offset_id == schema_.size());
    }

    // concurrent, reentrant
    template <bool is_sealed>
    void
    AppendingIndex(int64_t reserved_offset,
                   int64_t size,
                   FieldId fieldId,
                   const DataArray* stream_data,
                   const InsertRecord<is_sealed>& record) {
        if (is_in(fieldId)) {
            auto& indexing = field_indexings_.at(fieldId);
            if (indexing->get_field_meta().is_vector() &&
                indexing->get_field_meta().get_data_type() ==
                    DataType::VECTOR_FLOAT &&
                reserved_offset + size >= indexing->get_build_threshold()) {
                auto vec_base = record.get_field_data_base(fieldId);
                indexing->AppendSegmentIndex(
                    reserved_offset,
                    size,
                    vec_base,
                    stream_data->vectors().float_vector().data().data());
            }
        }
    }

    // concurrent, reentrant
    template <bool is_sealed>
    void
    AppendingIndex(int64_t reserved_offset,
                   int64_t size,
                   FieldId fieldId,
                   const storage::FieldDataPtr data,
                   const InsertRecord<is_sealed>& record) {
        if (is_in(fieldId)) {
            auto& indexing = field_indexings_.at(fieldId);
            if (indexing->get_field_meta().is_vector() &&
                indexing->get_field_meta().get_data_type() ==
                    DataType::VECTOR_FLOAT &&
                reserved_offset + size >= indexing->get_build_threshold()) {
                auto vec_base = record.get_field_data_base(fieldId);
                indexing->AppendSegmentIndex(
                    reserved_offset, size, vec_base, data->Data());
            }
        }
    }

    void
    GetDataFromIndex(FieldId fieldId,
                     const int64_t* seg_offsets,
                     int64_t count,
                     int64_t element_size,
                     void* output_raw) const {
        if (is_in(fieldId)) {
            auto& indexing = field_indexings_.at(fieldId);
            if (indexing->get_field_meta().is_vector() &&
                indexing->get_field_meta().get_data_type() ==
                    DataType::VECTOR_FLOAT) {
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

 private:
    // control info
    std::atomic<int64_t> resource_ack_ = 0;
    //    std::atomic<int64_t> finished_ack_ = 0;
    AckResponder finished_ack_;
    std::mutex mutex_;

 private:
    // field_offset => indexing
    std::map<FieldId, std::unique_ptr<FieldIndexing>> field_indexings_;
};

}  // namespace milvus::segcore
