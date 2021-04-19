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

namespace milvus::segcore {

// this should be concurrent
// All concurrent
class IndexingEntry {
 public:
    explicit IndexingEntry(const FieldMeta& field_meta, int64_t chunk_size)
        : field_meta_(field_meta), chunk_size_(chunk_size) {
    }
    IndexingEntry(const IndexingEntry&) = delete;
    IndexingEntry&
    operator=(const IndexingEntry&) = delete;

    // Do this in parallel
    virtual void
    BuildIndexRange(int64_t ack_beg, int64_t ack_end, const VectorBase* vec_base) = 0;

    const FieldMeta&
    get_field_meta() {
        return field_meta_;
    }

    int64_t
    get_chunk_size() const {
        return chunk_size_;
    }

 protected:
    // additional info
    const FieldMeta& field_meta_;
    const int64_t chunk_size_;
};
template <typename T>
class ScalarIndexingEntry : public IndexingEntry {
 public:
    using IndexingEntry::IndexingEntry;

    void
    BuildIndexRange(int64_t ack_beg, int64_t ack_end, const VectorBase* vec_base) override;

    // concurrent
    knowhere::scalar::StructuredIndex<T>*
    get_indexing(int64_t chunk_id) const {
        Assert(!field_meta_.is_vector());
        return data_.at(chunk_id).get();
    }

 private:
    tbb::concurrent_vector<std::unique_ptr<knowhere::scalar::StructuredIndex<T>>> data_;
};

class VecIndexingEntry : public IndexingEntry {
 public:
    using IndexingEntry::IndexingEntry;

    void
    BuildIndexRange(int64_t ack_beg, int64_t ack_end, const VectorBase* vec_base) override;

    // concurrent
    knowhere::VecIndex*
    get_vec_indexing(int64_t chunk_id) const {
        Assert(field_meta_.is_vector());
        return data_.at(chunk_id).get();
    }

    knowhere::Config
    get_build_conf() const;
    knowhere::Config
    get_search_conf(int top_k) const;

 private:
    tbb::concurrent_vector<std::unique_ptr<knowhere::VecIndex>> data_;
};

std::unique_ptr<IndexingEntry>
CreateIndex(const FieldMeta& field_meta, int64_t chunk_size);

class IndexingRecord {
 public:
    explicit IndexingRecord(const Schema& schema, int64_t chunk_size) : schema_(schema), chunk_size_(chunk_size) {
        Initialize();
    }

    void
    Initialize() {
        int offset = 0;
        for (auto& field : schema_) {
            if (field.get_data_type() != DataType::VECTOR_BINARY) {
                entries_.try_emplace(FieldOffset(offset), CreateIndex(field, chunk_size_));
            }
            ++offset;
        }
        assert(offset == schema_.size());
    }

    // concurrent, reentrant
    void
    UpdateResourceAck(int64_t chunk_ack, const InsertRecord& record);

    // concurrent
    int64_t
    get_finished_ack() const {
        return finished_ack_.GetAck();
    }

    const IndexingEntry&
    get_entry(FieldOffset field_offset) const {
        assert(entries_.count(field_offset));
        return *entries_.at(field_offset);
    }

    const VecIndexingEntry&
    get_vec_entry(FieldOffset field_offset) const {
        auto& entry = get_entry(field_offset);
        auto ptr = dynamic_cast<const VecIndexingEntry*>(&entry);
        AssertInfo(ptr, "invalid indexing");
        return *ptr;
    }
    template <typename T>
    auto
    get_scalar_entry(FieldOffset field_offset) const -> const ScalarIndexingEntry<T>& {
        auto& entry = get_entry(field_offset);
        auto ptr = dynamic_cast<const ScalarIndexingEntry<T>*>(&entry);
        AssertInfo(ptr, "invalid indexing");
        return *ptr;
    }

 private:
    const Schema& schema_;

 private:
    // control info
    std::atomic<int64_t> resource_ack_ = 0;
    //    std::atomic<int64_t> finished_ack_ = 0;
    AckResponder finished_ack_;
    std::mutex mutex_;
    int64_t chunk_size_;

 private:
    // field_offset => indexing
    std::map<FieldOffset, std::unique_ptr<IndexingEntry>> entries_;
};

}  // namespace milvus::segcore
