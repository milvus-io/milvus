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

#include <memory>
#include <vector>
#include <unordered_map>
#include <utility>

#include "common/Schema.h"
#include "segcore/AckResponder.h"
#include "segcore/ConcurrentVector.h"
#include "segcore/Record.h"
#include "TimestampIndex.h"

namespace milvus::segcore {

class OffsetMap {
 public:
    virtual ~OffsetMap() = default;

    virtual std::vector<SegOffset>
    find_with_timestamp(const PkType pk, Timestamp timestamp, const ConcurrentVector<Timestamp>& timestamps) const = 0;

    virtual std::vector<SegOffset>
    find_with_barrier(const PkType pk, int64_t barrier) const = 0;

    virtual void
    insert(const PkType pk, int64_t offset) = 0;

    virtual bool
    empty() const = 0;
};

template <typename T>
class OffsetHashMap : public OffsetMap {
 public:
    std::vector<SegOffset>
    find_with_timestamp(const PkType pk, Timestamp timestamp, const ConcurrentVector<Timestamp>& timestamps) const {
        std::vector<SegOffset> res_offsets;
        auto offset_iter = map_.find(std::get<T>(pk));
        if (offset_iter != map_.end()) {
            for (auto offset : offset_iter->second) {
                if (timestamps[offset] <= timestamp) {
                    res_offsets.push_back(SegOffset(offset));
                }
            }
        }
        return res_offsets;
    }

    std::vector<SegOffset>
    find_with_barrier(const PkType pk, int64_t barrier) const {
        std::vector<SegOffset> res_offsets;
        auto offset_iter = map_.find(std::get<T>(pk));
        if (offset_iter != map_.end()) {
            for (auto offset : offset_iter->second) {
                if (offset <= barrier) {
                    res_offsets.push_back(SegOffset(offset));
                }
            }
        }
        return res_offsets;
    }

    void
    insert(const PkType pk, int64_t offset) {
        map_[std::get<T>(pk)].emplace_back(offset);
    }

    bool
    empty() const {
        return map_.empty();
    }

 private:
    std::unordered_map<T, std::vector<int64_t>> map_;
};

struct InsertRecord {
    ConcurrentVector<Timestamp> timestamps_;
    ConcurrentVector<idx_t> row_ids_;

    // used for preInsert of growing segment
    std::atomic<int64_t> reserved = 0;
    AckResponder ack_responder_;

    // used for timestamps index of sealed segment
    TimestampIndex timestamp_index_;

    // pks to row offset
    std::unique_ptr<OffsetMap> pk2offset_;

    explicit InsertRecord(const Schema& schema, int64_t size_per_chunk);

    std::vector<SegOffset>
    search_pk(const PkType pk, Timestamp timestamp) const {
        std::shared_lock lck(shared_mutex_);
        return pk2offset_->find_with_timestamp(pk, timestamp, timestamps_);
    }

    std::vector<SegOffset>
    search_pk(const PkType pk, int64_t insert_barrier) const {
        std::shared_lock lck(shared_mutex_);
        return pk2offset_->find_with_barrier(pk, insert_barrier);
    }

    void
    insert_pk(const PkType pk, int64_t offset) {
        std::lock_guard lck(shared_mutex_);
        pk2offset_->insert(pk, offset);
    }

    bool
    empty_pks() const {
        std::shared_lock lck(shared_mutex_);
        return pk2offset_->empty();
    }

    // get field data without knowing the type
    VectorBase*
    get_field_data_base(FieldId field_id) const {
        AssertInfo(fields_data_.find(field_id) != fields_data_.end(),
                   "Cannot find field_data with field_id: " + std::to_string(field_id.get()));
        auto ptr = fields_data_.at(field_id).get();
        return ptr;
    }

    // get field data in given type, const version
    template <typename Type>
    const ConcurrentVector<Type>*
    get_field_data(FieldId field_id) const {
        auto base_ptr = get_field_data_base(field_id);
        auto ptr = dynamic_cast<const ConcurrentVector<Type>*>(base_ptr);
        Assert(ptr);
        return ptr;
    }

    // get field data in given type, non-const version
    template <typename Type>
    ConcurrentVector<Type>*
    get_field_data(FieldId field_id) {
        auto base_ptr = get_field_data_base(field_id);
        auto ptr = dynamic_cast<ConcurrentVector<Type>*>(base_ptr);
        Assert(ptr);
        return ptr;
    }

    // append a column of scalar type
    template <typename Type>
    void
    append_field_data(FieldId field_id, int64_t size_per_chunk) {
        static_assert(IsScalar<Type>);
        fields_data_.emplace(field_id, std::make_unique<ConcurrentVector<Type>>(size_per_chunk));
    }

    // append a column of vector type
    template <typename VectorType>
    void
    append_field_data(FieldId field_id, int64_t dim, int64_t size_per_chunk) {
        static_assert(std::is_base_of_v<VectorTrait, VectorType>);
        fields_data_.emplace(field_id, std::make_unique<ConcurrentVector<VectorType>>(dim, size_per_chunk));
    }

    void
    drop_field_data(FieldId field_id) {
        fields_data_.erase(field_id);
    }

 private:
    //    std::vector<std::unique_ptr<VectorBase>> fields_data_;
    std::unordered_map<FieldId, std::unique_ptr<VectorBase>> fields_data_;
    mutable std::shared_mutex shared_mutex_;
};

}  // namespace milvus::segcore
