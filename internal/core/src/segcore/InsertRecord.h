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
#include <string>
#include <algorithm>
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

    virtual std::vector<int64_t>
    find(const PkType pk) const = 0;

    virtual void
    insert(const PkType pk, int64_t offset) = 0;

    virtual void
    seal() = 0;

    virtual bool
    empty() const = 0;
};

template <typename T>
class OffsetHashMap : public OffsetMap {
 public:
    std::vector<int64_t>
    find(const PkType pk) const {
        auto offset_vector = map_.find(std::get<T>(pk));
        return offset_vector != map_.end() ? offset_vector->second : std::vector<int64_t>();
    }

    void
    insert(const PkType pk, int64_t offset) {
        map_[std::get<T>(pk)].emplace_back(offset);
    }

    void
    seal() {
        PanicInfo("OffsetHashMap used for growing segment could not be sealed.");
    }

    bool
    empty() const {
        return map_.empty();
    }

 private:
    std::unordered_map<T, std::vector<int64_t>> map_;
};

template <typename T>
class OffsetOrderedArray : public OffsetMap {
 public:
    std::vector<int64_t>
    find(const PkType pk) const {
        int left = 0, right = array_.size() - 1;
        T target = std::get<T>(pk);
        if (!is_sealed)
            PanicInfo("OffsetOrderedArray could not search before seal");

        while (left < right) {
            int mid = (left + right) >> 1;
            if (array_[mid].first < target)
                left = mid + 1;
            else
                right = mid;
        }

        std::vector<int64_t> offset_vector;
        for (int offset_id = right; offset_id < array_.size(); offset_id++) {
            if (offset_id < 0 || array_[offset_id].first != target)
                break;
            offset_vector.push_back(array_[offset_id].second);
        }

        return offset_vector;
    }

    void
    insert(const PkType pk, int64_t offset) {
        if (is_sealed)
            PanicInfo("OffsetOrderedArray could not insert after seal");
        array_.push_back(std::make_pair(std::get<T>(pk), offset));
    }

    void
    seal() {
        sort(array_.begin(), array_.end());
        is_sealed = true;
    }

    bool
    empty() const {
        return array_.empty();
    }

 private:
    bool is_sealed = false;
    std::vector<std::pair<T, int64_t>> array_;
};

template <bool is_sealed = false>
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

    InsertRecord(const Schema& schema, int64_t size_per_chunk) : row_ids_(size_per_chunk), timestamps_(size_per_chunk) {
        std::optional<FieldId> pk_field_id = schema.get_primary_field_id();

        for (auto& field : schema) {
            auto field_id = field.first;
            auto& field_meta = field.second;
            if (pk2offset_ == nullptr && pk_field_id.has_value() && pk_field_id.value() == field_id) {
                switch (field_meta.get_data_type()) {
                    case DataType::INT64: {
                        if (is_sealed)
                            pk2offset_ = std::make_unique<OffsetOrderedArray<int64_t>>();
                        else
                            pk2offset_ = std::make_unique<OffsetHashMap<int64_t>>();
                        break;
                    }
                    case DataType::VARCHAR: {
                        if (is_sealed)
                            pk2offset_ = std::make_unique<OffsetOrderedArray<std::string>>();
                        else
                            pk2offset_ = std::make_unique<OffsetHashMap<std::string>>();
                        break;
                    }
                    default: {
                        PanicInfo("unsupported pk type");
                    }
                }
            }
            if (field_meta.is_vector()) {
                if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
                    this->append_field_data<FloatVector>(field_id, field_meta.get_dim(), size_per_chunk);
                    continue;
                } else if (field_meta.get_data_type() == DataType::VECTOR_BINARY) {
                    this->append_field_data<BinaryVector>(field_id, field_meta.get_dim(), size_per_chunk);
                    continue;
                } else {
                    PanicInfo("unsupported");
                }
            }
            switch (field_meta.get_data_type()) {
                case DataType::BOOL: {
                    this->append_field_data<bool>(field_id, size_per_chunk);
                    break;
                }
                case DataType::INT8: {
                    this->append_field_data<int8_t>(field_id, size_per_chunk);
                    break;
                }
                case DataType::INT16: {
                    this->append_field_data<int16_t>(field_id, size_per_chunk);
                    break;
                }
                case DataType::INT32: {
                    this->append_field_data<int32_t>(field_id, size_per_chunk);
                    break;
                }
                case DataType::INT64: {
                    this->append_field_data<int64_t>(field_id, size_per_chunk);
                    break;
                }
                case DataType::FLOAT: {
                    this->append_field_data<float>(field_id, size_per_chunk);
                    break;
                }
                case DataType::DOUBLE: {
                    this->append_field_data<double>(field_id, size_per_chunk);
                    break;
                }
                case DataType::VARCHAR: {
                    this->append_field_data<std::string>(field_id, size_per_chunk);
                    break;
                }
                default: {
                    PanicInfo("unsupported");
                }
            }
        }
    }

    std::vector<SegOffset>
    search_pk(const PkType pk, Timestamp timestamp) const {
        std::shared_lock lck(shared_mutex_);
        std::vector<SegOffset> res_offsets;
        auto offset_iter = pk2offset_->find(pk);
        for (auto offset : offset_iter) {
            if (timestamps_[offset] <= timestamp) {
                res_offsets.push_back(SegOffset(offset));
            }
        }
        return res_offsets;
    }

    std::vector<SegOffset>
    search_pk(const PkType pk, int64_t insert_barrier) const {
        std::shared_lock lck(shared_mutex_);
        std::vector<SegOffset> res_offsets;
        auto offset_iter = pk2offset_->find(pk);
        for (auto offset : offset_iter) {
            if (offset <= insert_barrier) {
                res_offsets.push_back(SegOffset(offset));
            }
        }
        return res_offsets;
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

    void
    seal_pks() {
        pk2offset_->seal();
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
