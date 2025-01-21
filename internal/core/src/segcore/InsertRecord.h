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

#include <algorithm>
#include <cstddef>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <queue>

#include "TimestampIndex.h"
#include "common/EasyAssert.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "fmt/format.h"
#include "mmap/ChunkedColumn.h"
#include "mmap/Column.h"
#include "segcore/AckResponder.h"
#include "segcore/ConcurrentVector.h"
#include "segcore/Record.h"
#include "storage/MmapManager.h"

namespace milvus::segcore {

constexpr int64_t Unlimited = -1;
constexpr int64_t NoLimit = 0;
// If `bitset_count * 100` > `total_count * BruteForceSelectivity`, we use pk index.
// Otherwise, we use bruteforce to retrieve all the pks and then sort them.
constexpr int64_t BruteForceSelectivity = 10;

class OffsetMap {
 public:
    virtual ~OffsetMap() = default;

    virtual bool
    contain(const PkType& pk) const = 0;

    virtual std::vector<int64_t>
    find(const PkType& pk) const = 0;

    virtual void
    insert(const PkType& pk, int64_t offset) = 0;

    virtual void
    seal() = 0;

    virtual bool
    empty() const = 0;

    using OffsetType = int64_t;
    // TODO: in fact, we can retrieve the pk here. Not sure which way is more efficient.
    virtual std::pair<std::vector<OffsetMap::OffsetType>, bool>
    find_first(int64_t limit, const BitsetType& bitset) const = 0;

    virtual void
    clear() = 0;
};

template <typename T>
class OffsetOrderedMap : public OffsetMap {
 public:
    bool
    contain(const PkType& pk) const override {
        std::shared_lock<std::shared_mutex> lck(mtx_);

        return map_.find(std::get<T>(pk)) != map_.end();
    }

    std::vector<int64_t>
    find(const PkType& pk) const override {
        std::shared_lock<std::shared_mutex> lck(mtx_);

        auto offset_vector = map_.find(std::get<T>(pk));
        return offset_vector != map_.end() ? offset_vector->second
                                           : std::vector<int64_t>();
    }

    void
    insert(const PkType& pk, int64_t offset) override {
        std::unique_lock<std::shared_mutex> lck(mtx_);

        map_[std::get<T>(pk)].emplace_back(offset);
    }

    void
    seal() override {
        PanicInfo(
            NotImplemented,
            "OffsetOrderedMap used for growing segment could not be sealed.");
    }

    bool
    empty() const override {
        std::shared_lock<std::shared_mutex> lck(mtx_);

        return map_.empty();
    }

    std::pair<std::vector<OffsetMap::OffsetType>, bool>
    find_first(int64_t limit, const BitsetType& bitset) const override {
        std::shared_lock<std::shared_mutex> lck(mtx_);

        if (limit == Unlimited || limit == NoLimit) {
            limit = map_.size();
        }

        // TODO: we can't retrieve pk by offset very conveniently.
        //      Selectivity should be done outside.
        return find_first_by_index(limit, bitset);
    }

    void
    clear() override {
        std::unique_lock<std::shared_mutex> lck(mtx_);
        map_.clear();
    }

 private:
    std::pair<std::vector<OffsetMap::OffsetType>, bool>
    find_first_by_index(int64_t limit, const BitsetType& bitset) const {
        int64_t hit_num = 0;  // avoid counting the number everytime.
        auto size = bitset.size();
        int64_t cnt = size - bitset.count();
        limit = std::min(limit, cnt);
        std::vector<int64_t> seg_offsets;
        seg_offsets.reserve(limit);
        auto it = map_.begin();
        for (; hit_num < limit && it != map_.end(); it++) {
            // Offsets in the growing segment are ordered by timestamp,
            // so traverse from back to front to obtain the latest offset.
            for (int i = it->second.size() - 1; i >= 0; --i) {
                auto seg_offset = it->second[i];
                if (seg_offset >= size) {
                    // Frequently concurrent insert/query will cause this case.
                    continue;
                }

                if (!bitset[seg_offset]) {
                    seg_offsets.push_back(seg_offset);
                    hit_num++;
                    // PK hit, no need to continue traversing offsets with the same PK.
                    break;
                }
            }
        }
        return {seg_offsets, it != map_.end()};
    }

 private:
    using OrderedMap = std::map<T, std::vector<int64_t>, std::less<>>;
    OrderedMap map_;
    mutable std::shared_mutex mtx_;
};

template <typename T>
class OffsetOrderedArray : public OffsetMap {
 public:
    bool
    contain(const PkType& pk) const override {
        const T& target = std::get<T>(pk);
        auto it =
            std::lower_bound(array_.begin(),
                             array_.end(),
                             target,
                             [](const std::pair<T, int64_t>& elem,
                                const T& value) { return elem.first < value; });

        return it != array_.end() && it->first == target;
    }

    std::vector<int64_t>
    find(const PkType& pk) const override {
        check_search();

        const T& target = std::get<T>(pk);
        auto it =
            std::lower_bound(array_.begin(),
                             array_.end(),
                             target,
                             [](const std::pair<T, int64_t>& elem,
                                const T& value) { return elem.first < value; });

        std::vector<int64_t> offset_vector;
        for (; it != array_.end() && it->first == target; ++it) {
            offset_vector.push_back(it->second);
        }

        return offset_vector;
    }

    void
    insert(const PkType& pk, int64_t offset) override {
        if (is_sealed) {
            PanicInfo(Unsupported,
                      "OffsetOrderedArray could not insert after seal");
        }
        array_.push_back(
            std::make_pair(std::get<T>(pk), static_cast<int32_t>(offset)));
    }

    void
    seal() override {
        sort(array_.begin(), array_.end());
        is_sealed = true;
    }

    bool
    empty() const override {
        return array_.empty();
    }

    std::pair<std::vector<OffsetMap::OffsetType>, bool>
    find_first(int64_t limit, const BitsetType& bitset) const override {
        check_search();

        if (limit == Unlimited || limit == NoLimit) {
            limit = array_.size();
        }

        // TODO: we can't retrieve pk by offset very conveniently.
        //      Selectivity should be done outside.
        return find_first_by_index(limit, bitset);
    }

    void
    clear() override {
        array_.clear();
        is_sealed = false;
    }

 private:
    std::pair<std::vector<OffsetMap::OffsetType>, bool>
    find_first_by_index(int64_t limit, const BitsetType& bitset) const {
        int64_t hit_num = 0;  // avoid counting the number everytime.
        auto size = bitset.size();
        int64_t cnt = size - bitset.count();
        auto more_hit_than_limit = cnt > limit;
        limit = std::min(limit, cnt);
        std::vector<int64_t> seg_offsets;
        seg_offsets.reserve(limit);
        auto it = array_.begin();
        for (; hit_num < limit && it != array_.end(); it++) {
            auto seg_offset = it->second;
            if (seg_offset >= size) {
                // In fact, this case won't happen on sealed segments.
                continue;
            }

            if (!bitset[seg_offset]) {
                seg_offsets.push_back(seg_offset);
                hit_num++;
            }
        }
        return {seg_offsets, more_hit_than_limit && it != array_.end()};
    }

    void
    check_search() const {
        AssertInfo(is_sealed,
                   "OffsetOrderedArray could not search before seal");
    }

 private:
    bool is_sealed = false;
    std::vector<std::pair<T, int32_t>> array_;
};

class ThreadSafeValidData {
 public:
    explicit ThreadSafeValidData() = default;
    explicit ThreadSafeValidData(FixedVector<bool> data)
        : data_(std::move(data)) {
    }

    void
    set_data_raw(const std::vector<FieldDataPtr>& datas) {
        std::unique_lock<std::shared_mutex> lck(mutex_);
        auto total = 0;
        for (auto& field_data : datas) {
            total += field_data->get_num_rows();
        }
        if (length_ + total > data_.size()) {
            data_.resize(length_ + total);
        }

        for (auto& field_data : datas) {
            auto num_row = field_data->get_num_rows();
            for (size_t i = 0; i < num_row; i++) {
                data_[length_ + i] = field_data->is_valid(i);
            }
            length_ += num_row;
        }
    }

    void
    set_data_raw(size_t num_rows,
                 const DataArray* data,
                 const FieldMeta& field_meta) {
        std::unique_lock<std::shared_mutex> lck(mutex_);
        if (field_meta.is_nullable()) {
            if (length_ + num_rows > data_.size()) {
                data_.resize(length_ + num_rows);
            }
            auto src = data->valid_data().data();
            std::copy_n(src, num_rows, data_.data() + length_);
            length_ += num_rows;
        }
    }

    bool
    is_valid(size_t offset) {
        std::shared_lock<std::shared_mutex> lck(mutex_);
        Assert(offset < length_);
        return data_[offset];
    }

    bool*
    get_chunk_data(size_t offset) {
        std::shared_lock<std::shared_mutex> lck(mutex_);
        Assert(offset < length_);
        return &data_[offset];
    }

 private:
    mutable std::shared_mutex mutex_{};
    FixedVector<bool> data_;
    // number of actual elements
    size_t length_{0};
};

template <bool is_sealed = false>
struct InsertRecord {
    InsertRecord(
        const Schema& schema,
        const int64_t size_per_chunk,
        const storage::MmapChunkDescriptorPtr mmap_descriptor = nullptr)
        : timestamps_(size_per_chunk), mmap_descriptor_(mmap_descriptor) {
        std::optional<FieldId> pk_field_id = schema.get_primary_field_id();

        for (auto& field : schema) {
            auto field_id = field.first;
            auto& field_meta = field.second;
            if (field_meta.is_nullable()) {
                this->append_valid_data(field_id);
            }
            if (pk2offset_ == nullptr && pk_field_id.has_value() &&
                pk_field_id.value() == field_id) {
                switch (field_meta.get_data_type()) {
                    case DataType::INT64: {
                        if constexpr (is_sealed) {
                            pk2offset_ =
                                std::make_unique<OffsetOrderedArray<int64_t>>();
                        } else {
                            pk2offset_ =
                                std::make_unique<OffsetOrderedMap<int64_t>>();
                        }
                        break;
                    }
                    case DataType::VARCHAR: {
                        if constexpr (is_sealed) {
                            pk2offset_ = std::make_unique<
                                OffsetOrderedArray<std::string>>();
                        } else {
                            pk2offset_ = std::make_unique<
                                OffsetOrderedMap<std::string>>();
                        }
                        break;
                    }
                    default: {
                        PanicInfo(DataTypeInvalid,
                                  fmt::format("unsupported pk type",
                                              field_meta.get_data_type()));
                    }
                }
            }
            if (field_meta.is_vector()) {
                if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
                    this->append_data<FloatVector>(
                        field_id, field_meta.get_dim(), size_per_chunk);
                    continue;
                } else if (field_meta.get_data_type() ==
                           DataType::VECTOR_BINARY) {
                    this->append_data<BinaryVector>(
                        field_id, field_meta.get_dim(), size_per_chunk);
                    continue;
                } else if (field_meta.get_data_type() ==
                           DataType::VECTOR_FLOAT16) {
                    this->append_data<Float16Vector>(
                        field_id, field_meta.get_dim(), size_per_chunk);
                    continue;
                } else if (field_meta.get_data_type() ==
                           DataType::VECTOR_BFLOAT16) {
                    this->append_data<BFloat16Vector>(
                        field_id, field_meta.get_dim(), size_per_chunk);
                    continue;
                } else if (field_meta.get_data_type() ==
                           DataType::VECTOR_SPARSE_FLOAT) {
                    this->append_data<SparseFloatVector>(field_id,
                                                         size_per_chunk);
                    continue;
                } else if (field_meta.get_data_type() ==
                           DataType::VECTOR_INT8) {
                    this->append_data<Int8Vector>(
                        field_id, field_meta.get_dim(), size_per_chunk);
                    continue;
                } else {
                    PanicInfo(DataTypeInvalid,
                              fmt::format("unsupported vector type",
                                          field_meta.get_data_type()));
                }
            }
            switch (field_meta.get_data_type()) {
                case DataType::BOOL: {
                    this->append_data<bool>(field_id, size_per_chunk);
                    break;
                }
                case DataType::INT8: {
                    this->append_data<int8_t>(field_id, size_per_chunk);
                    break;
                }
                case DataType::INT16: {
                    this->append_data<int16_t>(field_id, size_per_chunk);
                    break;
                }
                case DataType::INT32: {
                    this->append_data<int32_t>(field_id, size_per_chunk);
                    break;
                }
                case DataType::INT64: {
                    this->append_data<int64_t>(field_id, size_per_chunk);
                    break;
                }
                case DataType::FLOAT: {
                    this->append_data<float>(field_id, size_per_chunk);
                    break;
                }
                case DataType::DOUBLE: {
                    this->append_data<double>(field_id, size_per_chunk);
                    break;
                }
                case DataType::VARCHAR: {
                    this->append_data<std::string>(field_id, size_per_chunk);
                    break;
                }
                case DataType::JSON: {
                    this->append_data<Json>(field_id, size_per_chunk);
                    break;
                }
                case DataType::ARRAY: {
                    this->append_data<Array>(field_id, size_per_chunk);
                    break;
                }
                default: {
                    PanicInfo(DataTypeInvalid,
                              fmt::format("unsupported scalar type",
                                          field_meta.get_data_type()));
                }
            }
        }
    }

    bool
    contain(const PkType& pk) const {
        return pk2offset_->contain(pk);
    }

    std::vector<SegOffset>
    search_pk(const PkType& pk, Timestamp timestamp) const {
        std::shared_lock lck(shared_mutex_);
        std::vector<SegOffset> res_offsets;
        auto offset_iter = pk2offset_->find(pk);
        for (auto offset : offset_iter) {
            if (timestamps_[offset] <= timestamp) {
                res_offsets.emplace_back(offset);
            }
        }
        return res_offsets;
    }

    void
    insert_pks(milvus::DataType data_type,
               const std::shared_ptr<ChunkedColumnBase>& data) {
        std::lock_guard lck(shared_mutex_);
        int64_t offset = 0;
        switch (data_type) {
            case DataType::INT64: {
                auto column = std::dynamic_pointer_cast<ChunkedColumn>(data);
                auto num_chunk = column->num_chunks();
                for (int i = 0; i < num_chunk; ++i) {
                    auto pks =
                        reinterpret_cast<const int64_t*>(column->Data(i));
                    auto chunk_num_rows = column->chunk_row_nums(i);
                    for (int j = 0; j < chunk_num_rows; ++j) {
                        pk2offset_->insert(pks[j], offset++);
                    }
                }
                break;
            }
            case DataType::VARCHAR: {
                auto column = std::dynamic_pointer_cast<
                    ChunkedVariableColumn<std::string>>(data);

                auto num_chunk = column->num_chunks();
                for (int i = 0; i < num_chunk; ++i) {
                    auto pks = column->StringViews(i).first;
                    for (auto& pk : pks) {
                        pk2offset_->insert(std::string(pk), offset++);
                    }
                }
                break;
            }
            default: {
                PanicInfo(DataTypeInvalid,
                          fmt::format("unsupported primary key data type",
                                      data_type));
            }
        }
    }

    void
    insert_pks(milvus::DataType data_type,
               const std::shared_ptr<SingleChunkColumnBase>& data) {
        std::lock_guard lck(shared_mutex_);
        int64_t offset = 0;
        switch (data_type) {
            case DataType::INT64: {
                auto column =
                    std::dynamic_pointer_cast<SingleChunkColumn>(data);
                auto pks = reinterpret_cast<const int64_t*>(column->Data(0));
                for (int i = 0; i < column->NumRows(); ++i) {
                    pk2offset_->insert(pks[i], offset++);
                }
                break;
            }
            case DataType::VARCHAR: {
                auto column = std::dynamic_pointer_cast<
                    SingleChunkVariableColumn<std::string>>(data);
                auto pks = column->Views();

                for (int i = 0; i < column->NumRows(); ++i) {
                    pk2offset_->insert(std::string(pks[i]), offset++);
                }
                break;
            }
            default: {
                PanicInfo(DataTypeInvalid,
                          fmt::format("unsupported primary key data type",
                                      data_type));
            }
        }
    }

    void
    insert_pks(const std::vector<FieldDataPtr>& field_datas) {
        std::lock_guard lck(shared_mutex_);
        int64_t offset = 0;
        for (auto& data : field_datas) {
            int64_t row_count = data->get_num_rows();
            auto data_type = data->get_data_type();
            switch (data_type) {
                case DataType::INT64: {
                    for (int i = 0; i < row_count; ++i) {
                        pk2offset_->insert(
                            *static_cast<const int64_t*>(data->RawValue(i)),
                            offset++);
                    }
                    break;
                }
                case DataType::VARCHAR: {
                    for (int i = 0; i < row_count; ++i) {
                        pk2offset_->insert(
                            *static_cast<const std::string*>(data->RawValue(i)),
                            offset++);
                    }
                    break;
                }
                default: {
                    PanicInfo(DataTypeInvalid,
                              fmt::format("unsupported primary key data type",
                                          data_type));
                }
            }
        }
    }

    std::vector<SegOffset>
    search_pk(const PkType& pk, int64_t insert_barrier) const {
        std::shared_lock lck(shared_mutex_);
        std::vector<SegOffset> res_offsets;
        auto offset_iter = pk2offset_->find(pk);
        for (auto offset : offset_iter) {
            if (offset < insert_barrier) {
                res_offsets.emplace_back(offset);
            }
        }
        return res_offsets;
    }

    void
    insert_pk(const PkType& pk, int64_t offset) {
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
        std::lock_guard lck(shared_mutex_);
        pk2offset_->seal();
    }

    // get data without knowing the type
    VectorBase*
    get_data_base(FieldId field_id) const {
        AssertInfo(data_.find(field_id) != data_.end(),
                   "Cannot find field_data with field_id: " +
                       std::to_string(field_id.get()));
        AssertInfo(data_.at(field_id) != nullptr,
                   "data_ at i is null" + std::to_string(field_id.get()));
        return data_.at(field_id).get();
    }

    // get field data in given type, const version
    template <typename Type>
    const ConcurrentVector<Type>*
    get_data(FieldId field_id) const {
        auto base_ptr = get_data_base(field_id);
        auto ptr = dynamic_cast<const ConcurrentVector<Type>*>(base_ptr);
        Assert(ptr);
        return ptr;
    }

    // get field data in given type, non-const version
    template <typename Type>
    ConcurrentVector<Type>*
    get_data(FieldId field_id) {
        auto base_ptr = get_data_base(field_id);
        auto ptr = dynamic_cast<ConcurrentVector<Type>*>(base_ptr);
        Assert(ptr);
        return ptr;
    }

    ThreadSafeValidData*
    get_valid_data(FieldId field_id) const {
        AssertInfo(valid_data_.find(field_id) != valid_data_.end(),
                   "Cannot find valid_data with field_id: " +
                       std::to_string(field_id.get()));
        AssertInfo(valid_data_.at(field_id) != nullptr,
                   "valid_data_ at i is null" + std::to_string(field_id.get()));
        return valid_data_.at(field_id).get();
    }

    bool
    is_data_exist(FieldId field_id) const {
        return data_.find(field_id) != data_.end();
    }

    bool
    is_valid_data_exist(FieldId field_id) const {
        return valid_data_.find(field_id) != valid_data_.end();
    }

    SpanBase
    get_span_base(FieldId field_id, int64_t chunk_id) const {
        auto data = get_data_base(field_id);
        if (is_valid_data_exist(field_id)) {
            auto size = data->get_chunk_size(chunk_id);
            auto element_offset = data->get_element_offset(chunk_id);
            return SpanBase(
                data->get_chunk_data(chunk_id),
                get_valid_data(field_id)->get_chunk_data(element_offset),
                size,
                data->get_element_size());
        }
        return data->get_span_base(chunk_id);
    }

    // append a column of scalar or sparse float vector type
    template <typename Type>
    void
    append_data(FieldId field_id, int64_t size_per_chunk) {
        static_assert(IsScalar<Type> || IsSparse<Type>);
        data_.emplace(field_id,
                      std::make_unique<ConcurrentVector<Type>>(
                          size_per_chunk, mmap_descriptor_));
    }

    // append a column of scalar type
    void
    append_valid_data(FieldId field_id) {
        valid_data_.emplace(field_id, std::make_unique<ThreadSafeValidData>());
    }

    // append a column of vector type
    template <typename VectorType>
    void
    append_data(FieldId field_id, int64_t dim, int64_t size_per_chunk) {
        static_assert(std::is_base_of_v<VectorTrait, VectorType>);
        data_.emplace(field_id,
                      std::make_unique<ConcurrentVector<VectorType>>(
                          dim, size_per_chunk, mmap_descriptor_));
    }

    void
    drop_field_data(FieldId field_id) {
        data_.erase(field_id);
        valid_data_.erase(field_id);
    }

    const ConcurrentVector<Timestamp>&
    timestamps() const {
        return timestamps_;
    }

    int64_t
    size() const {
        return ack_responder_.GetAck();
    }

    void
    clear() {
        timestamps_.clear();
        reserved = 0;
        ack_responder_.clear();
        timestamp_index_ = TimestampIndex();
        pk2offset_->clear();
        data_.clear();
    }

    bool
    empty() const {
        return pk2offset_->empty();
    }

 public:
    ConcurrentVector<Timestamp> timestamps_;

    // used for preInsert of growing segment
    std::atomic<int64_t> reserved = 0;
    AckResponder ack_responder_;

    // used for timestamps index of sealed segment
    TimestampIndex timestamp_index_;

    // pks to row offset
    std::unique_ptr<OffsetMap> pk2offset_;

 private:
    std::unordered_map<FieldId, std::unique_ptr<VectorBase>> data_{};
    std::unordered_map<FieldId, std::unique_ptr<ThreadSafeValidData>>
        valid_data_{};
    mutable std::shared_mutex shared_mutex_{};
    storage::MmapChunkDescriptorPtr mmap_descriptor_;
};

}  // namespace milvus::segcore
