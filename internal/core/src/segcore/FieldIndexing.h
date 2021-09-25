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
#include "AckResponder.h"
#include <tbb/concurrent_vector.h>
#include "common/Schema.h"
#include <optional>
#include <map>
#include <memory>
#include "InsertRecord.h"
#include <knowhere/index/vector_index/IndexIVF.h>
#include <knowhere/index/structured_index_simple/StructuredIndexSort.h>
#include "segcore/SegcoreConfig.h"

namespace milvus::segcore {

// this should be concurrent
// All concurrent
class FieldIndexing {
 public:
    explicit FieldIndexing(const FieldMeta& field_meta, const SegcoreConfig& segcore_config)
        : field_meta_(field_meta), segcore_config_(segcore_config) {
    }
    FieldIndexing(const FieldIndexing&) = delete;
    FieldIndexing&
    operator=(const FieldIndexing&) = delete;

    // Do this in parallel
    virtual void
    BuildIndexRange(int64_t ack_beg, int64_t ack_end, const VectorBase* vec_base) = 0;

    const FieldMeta&
    get_field_meta() {
        return field_meta_;
    }

    int64_t
    get_size_per_chunk() const {
        return segcore_config_.get_chunk_rows();
    }

    virtual knowhere::Index*
    get_chunk_indexing(int64_t chunk_id) const = 0;

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
    BuildIndexRange(int64_t ack_beg, int64_t ack_end, const VectorBase* vec_base) override;

    // concurrent
    knowhere::scalar::StructuredIndex<T>*
    get_chunk_indexing(int64_t chunk_id) const override {
        Assert(!field_meta_.is_vector());
        return data_.at(chunk_id).get();
    }

 private:
    tbb::concurrent_vector<std::unique_ptr<knowhere::scalar::StructuredIndex<T>>> data_;
};

class VectorFieldIndexing : public FieldIndexing {
 public:
    using FieldIndexing::FieldIndexing;

    void
    BuildIndexRange(int64_t ack_beg, int64_t ack_end, const VectorBase* vec_base) override;

    // concurrent
    knowhere::VecIndex*
    get_chunk_indexing(int64_t chunk_id) const override {
        Assert(field_meta_.is_vector());
        return data_.at(chunk_id).get();
    }

    knowhere::Config
    get_build_params() const;

    knowhere::Config
    get_search_params(int top_k) const;

 private:
    tbb::concurrent_vector<std::unique_ptr<knowhere::VecIndex>> data_;
};

std::unique_ptr<FieldIndexing>
CreateIndex(const FieldMeta& field_meta, const SegcoreConfig& segcore_config);

class IndexingRecord {
 public:
    explicit IndexingRecord(const Schema& schema, const SegcoreConfig& segcore_config)
        : schema_(schema), segcore_config_(segcore_config) {
        Initialize();
    }

    void
    Initialize() {
        int offset_id = 0;
        for (const FieldMeta& field : schema_) {
            auto offset = FieldOffset(offset_id);
            ++offset_id;

            if (field.is_vector()) {
                // TODO: skip binary small index now, reenable after config.yaml is ready
                if (field.get_data_type() == DataType::VECTOR_BINARY) {
                    continue;
                }
                // flat should be skipped
                if (!field.get_metric_type().has_value()) {
                    continue;
                }
            }

            field_indexings_.try_emplace(offset, CreateIndex(field, segcore_config_));
        }
        assert(offset_id == schema_.size());
    }

    // concurrent, reentrant
    void
    UpdateResourceAck(int64_t chunk_ack, const InsertRecord& record);

    // concurrent
    int64_t
    get_finished_ack() const {
        return finished_ack_.GetAck();
    }

    const FieldIndexing&
    get_field_indexing(FieldOffset field_offset) const {
        Assert(field_indexings_.count(field_offset));
        return *field_indexings_.at(field_offset);
    }

    const VectorFieldIndexing&
    get_vec_field_indexing(FieldOffset field_offset) const {
        auto& field_indexing = get_field_indexing(field_offset);
        auto ptr = dynamic_cast<const VectorFieldIndexing*>(&field_indexing);
        AssertInfo(ptr, "invalid indexing");
        return *ptr;
    }

    bool
    is_in(FieldOffset field_offset) const {
        return field_indexings_.count(field_offset);
    }

    template <typename T>
    auto
    get_scalar_field_indexing(FieldOffset field_offset) const -> const ScalarFieldIndexing<T>& {
        auto& entry = get_field_indexing(field_offset);
        auto ptr = dynamic_cast<const ScalarFieldIndexing<T>*>(&entry);
        AssertInfo(ptr, "invalid indexing");
        return *ptr;
    }

 private:
    const Schema& schema_;
    const SegcoreConfig& segcore_config_;

 private:
    // control info
    std::atomic<int64_t> resource_ack_ = 0;
    //    std::atomic<int64_t> finished_ack_ = 0;
    AckResponder finished_ack_;
    std::mutex mutex_;

 private:
    // field_offset => indexing
    std::map<FieldOffset, std::unique_ptr<FieldIndexing>> field_indexings_;
};

}  // namespace milvus::segcore
