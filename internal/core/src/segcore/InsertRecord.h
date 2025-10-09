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

#include "TimestampIndex.h"
#include "common/EasyAssert.h"
#include "common/Schema.h"
#include "common/TrackingStdAllocator.h"
#include "common/Types.h"
#include "mmap/ChunkedColumn.h"
#include "segcore/AckResponder.h"
#include "segcore/ConcurrentVector.h"
#include <type_traits>

namespace milvus::segcore {

constexpr int64_t Unlimited = -1;
constexpr int64_t NoLimit = 0;
// If `bitset_count * 100` > `total_count * BruteForceSelectivity`, we use pk index.
// Otherwise, we use bruteforce to retrieve all the pks and then sort them.
constexpr int64_t BruteForceSelectivity = 10;

using Condition = std::function<bool(int64_t)>;

class OffsetMap {
 public:
    virtual ~OffsetMap() = default;

    virtual bool
    contain(const PkType& pk) const = 0;

    virtual std::vector<int64_t>
    find(const PkType& pk) const = 0;

    virtual void
    find_range(const PkType& pk,
               proto::plan::OpType op,
               BitsetTypeView& bitset,
               Condition condition) const = 0;

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

    virtual size_t
    size() const = 0;
};

template <typename T>
class OffsetOrderedMap : public OffsetMap {
 public:
    using OrderedMap = std::map<
        T,
        std::vector<int64_t>,
        std::less<>,
        TrackingStdAllocator<std::pair<const T, std::vector<int64_t>>>>;

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
    find_range(const PkType& pk,
               proto::plan::OpType op,
               BitsetTypeView& bitset,
               Condition condition) const override {
        std::shared_lock<std::shared_mutex> lck(mtx_);
        const T& target = std::get<T>(pk);

        if (op == proto::plan::OpType::Equal) {
            auto it = map_.find(target);
            if (it != map_.end()) {
                for (auto offset : it->second) {
                    if (condition(offset) && offset < bitset.size()) {
                        bitset[offset] = true;
                    }
                }
            }
        } else if (op == proto::plan::OpType::GreaterEqual) {
            auto it = map_.lower_bound(target);
            for (; it != map_.end(); ++it) {
                for (auto offset : it->second) {
                    if (condition(offset) && offset < bitset.size()) {
                        bitset[offset] = true;
                    }
                }
            }
        } else if (op == proto::plan::OpType::GreaterThan) {
            auto it = map_.upper_bound(target);
            for (; it != map_.end(); ++it) {
                for (auto offset : it->second) {
                    if (condition(offset) && offset < bitset.size()) {
                        bitset[offset] = true;
                    }
                }
            }
        } else if (op == proto::plan::OpType::LessEqual) {
            auto it = map_.upper_bound(target);
            for (auto ptr = map_.begin(); ptr != it; ++ptr) {
                for (auto offset : ptr->second) {
                    if (condition(offset) && offset < bitset.size()) {
                        bitset[offset] = true;
                    }
                }
            }
        } else if (op == proto::plan::OpType::LessThan) {
            auto it = map_.lower_bound(target);
            for (auto ptr = map_.begin(); ptr != it; ++ptr) {
                for (auto offset : ptr->second) {
                    if (condition(offset) && offset < bitset.size()) {
                        bitset[offset] = true;
                    }
                }
            }
        } else {
            ThrowInfo(ErrorCode::Unsupported,
                      fmt::format("unsupported op type {}", op));
        }
    }

    void
    insert(const PkType& pk, int64_t offset) override {
        std::unique_lock<std::shared_mutex> lck(mtx_);

        map_[std::get<T>(pk)].emplace_back(offset);
    }

    void
    seal() override {
        ThrowInfo(
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

    size_t
    size() const override {
        std::shared_lock<std::shared_mutex> lck(mtx_);
        return map_.get_allocator().total_allocated();
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
    find_range(const PkType& pk,
               proto::plan::OpType op,
               BitsetTypeView& bitset,
               Condition condition) const override {
        check_search();
        auto lower_bound_comp = [](const std::pair<T, int64_t>& elem,
                                   const T& value) {
            return elem.first < value;
        };
        auto upper_bound_comp = [](const T& value,
                                   const std::pair<T, int64_t>& elem) {
            return value < elem.first;
        };

        const T& target = std::get<T>(pk);
        if (op == proto::plan::OpType::Equal) {
            auto it = std::lower_bound(
                array_.begin(), array_.end(), target, lower_bound_comp);
            for (; it != array_.end() && it->first == target; ++it) {
                if (condition(it->second)) {
                    bitset[it->second] = true;
                }
            }
        } else if (op == proto::plan::OpType::GreaterEqual) {
            auto it = std::lower_bound(
                array_.begin(), array_.end(), target, lower_bound_comp);
            for (; it < array_.end(); ++it) {
                if (condition(it->second)) {
                    bitset[it->second] = true;
                }
            }
        } else if (op == proto::plan::OpType::GreaterThan) {
            auto it = std::upper_bound(
                array_.begin(), array_.end(), target, upper_bound_comp);
            for (; it < array_.end(); ++it) {
                if (condition(it->second)) {
                    bitset[it->second] = true;
                }
            }
        } else if (op == proto::plan::OpType::LessEqual) {
            auto it = std::upper_bound(
                array_.begin(), array_.end(), target, upper_bound_comp);
            for (auto ptr = array_.begin(); ptr < it; ++ptr) {
                if (condition(ptr->second)) {
                    bitset[ptr->second] = true;
                }
            }
        } else if (op == proto::plan::OpType::LessThan) {
            auto it = std::lower_bound(
                array_.begin(), array_.end(), target, lower_bound_comp);
            for (auto ptr = array_.begin(); ptr < it; ++ptr) {
                if (condition(ptr->second)) {
                    bitset[ptr->second] = true;
                }
            }
        } else {
            ThrowInfo(ErrorCode::Unsupported,
                      fmt::format("unsupported op type {}", op));
        }
    }

    void
    insert(const PkType& pk, int64_t offset) override {
        if (is_sealed) {
            ThrowInfo(Unsupported,
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

    size_t
    size() const override {
        return sizeof(std::pair<T, int32_t>) * array_.capacity();
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

class InsertRecordSealed {
 public:
    InsertRecordSealed(const Schema& schema,
                       const int64_t size_per_chunk,
                       const storage::MmapChunkDescriptorPtr
                       /* mmap_descriptor */
                       = nullptr)
        : timestamps_(size_per_chunk) {
        std::optional<FieldId> pk_field_id = schema.get_primary_field_id();
        // for sealed segment, only pk field is added.
        for (auto& field : schema) {
            auto field_id = field.first;
            if (pk_field_id.has_value() && pk_field_id.value() == field_id) {
                auto& field_meta = field.second;
                AssertInfo(!field_meta.is_nullable(),
                           "Primary key should not be nullable");
                switch (field_meta.get_data_type()) {
                    case DataType::INT64: {
                        pk2offset_ =
                            std::make_unique<OffsetOrderedArray<int64_t>>();
                        break;
                    }
                    case DataType::VARCHAR: {
                        pk2offset_ =
                            std::make_unique<OffsetOrderedArray<std::string>>();
                        break;
                    }
                    default: {
                        ThrowInfo(DataTypeInvalid,
                                  fmt::format("unsupported pk type",
                                              field_meta.get_data_type()));
                    }
                }
            }
        }
    }

    ~InsertRecordSealed() {
        if (estimated_memory_size_ > 0) {
            cachinglayer::Manager::GetInstance().RefundLoadedResource(
                {static_cast<int64_t>(estimated_memory_size_), 0});
            estimated_memory_size_ = 0;
        }
    }

    bool
    contain(const PkType& pk) const {
        return pk2offset_->contain(pk);
    }

    std::vector<SegOffset>
    search_pk(const PkType& pk,
              Timestamp timestamp,
              bool include_same_ts = true) const {
        std::shared_lock lck(shared_mutex_);
        std::vector<SegOffset> res_offsets;
        auto offset_iter = pk2offset_->find(pk);
        auto timestamp_hit =
            include_same_ts ? [](const Timestamp& ts1,
                                 const Timestamp& ts2) { return ts1 <= ts2; }
                            : [](const Timestamp& ts1, const Timestamp& ts2) {
                                  return ts1 < ts2;
                              };
        for (auto offset : offset_iter) {
            if (timestamp_hit(timestamps_[offset], timestamp)) {
                res_offsets.emplace_back(offset);
            }
        }
        return res_offsets;
    }

    void
    search_pk_range(const PkType& pk,
                    proto::plan::OpType op,
                    BitsetTypeView& bitset) const {
        pk2offset_->find_range(
            pk, op, bitset, [](int64_t offset) { return true; });
    }

    void
    search_pk_range(const PkType& pk,
                    Timestamp timestamp,
                    proto::plan::OpType op,
                    BitsetTypeView& bitset) const {
        auto condition = [this, timestamp](int64_t offset) {
            return timestamps_[offset] <= timestamp;
        };
        pk2offset_->find_range(pk, op, bitset, condition);
    }

    void
    insert_pks(milvus::DataType data_type, ChunkedColumnInterface* data) {
        std::lock_guard lck(shared_mutex_);
        int64_t offset = 0;
        switch (data_type) {
            case DataType::INT64: {
                auto num_chunk = data->num_chunks();
                for (int i = 0; i < num_chunk; ++i) {
                    auto pw = data->DataOfChunk(nullptr, i);
                    auto pks = reinterpret_cast<const int64_t*>(pw.get());
                    auto chunk_num_rows = data->chunk_row_nums(i);
                    for (int j = 0; j < chunk_num_rows; ++j) {
                        pk2offset_->insert(pks[j], offset++);
                    }
                }
                break;
            }
            case DataType::VARCHAR: {
                auto num_chunk = data->num_chunks();
                for (int i = 0; i < num_chunk; ++i) {
                    auto column =
                        reinterpret_cast<ChunkedVariableColumn<std::string>*>(
                            data);
                    auto pw = column->StringViews(nullptr, i);
                    auto pks = pw.get().first;
                    for (auto& pk : pks) {
                        pk2offset_->insert(std::string(pk), offset++);
                    }
                }
                break;
            }
            default: {
                ThrowInfo(DataTypeInvalid,
                          fmt::format("unsupported primary key data type",
                                      data_type));
            }
        }
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
        // update estimated memory size to caching layer
        cachinglayer::Manager::GetInstance().ChargeLoadedResource(
            {static_cast<int64_t>(pk2offset_->size()), 0});
        estimated_memory_size_ += pk2offset_->size();
    }

    void
    init_timestamps(const std::vector<Timestamp>& timestamps,
                    const TimestampIndex& timestamp_index) {
        std::lock_guard lck(shared_mutex_);
        timestamps_.set_data_raw(0, timestamps.data(), timestamps.size());
        timestamp_index_ = std::move(timestamp_index);
        AssertInfo(timestamps_.num_chunk() == 1,
                   "num chunk not equal to 1 for sealed segment");
        size_t size =
            timestamps.size() * sizeof(Timestamp) + timestamp_index_.size();
        cachinglayer::Manager::GetInstance().ChargeLoadedResource(
            {static_cast<int64_t>(size), 0});
        estimated_memory_size_ += size;
    }

    const ConcurrentVector<Timestamp>&
    timestamps() const {
        return timestamps_;
    }

    void
    clear() {
        timestamps_.clear();
        timestamp_index_ = TimestampIndex();
        pk2offset_->clear();
        reserved = 0;
        if (estimated_memory_size_ > 0) {
            cachinglayer::Manager::GetInstance().RefundLoadedResource(
                {static_cast<int64_t>(estimated_memory_size_), 0});
            estimated_memory_size_ = 0;
        }
    }

 public:
    ConcurrentVector<Timestamp> timestamps_;
    std::atomic<int64_t> reserved = 0;
    // used for timestamps index of sealed segment
    TimestampIndex timestamp_index_;
    // pks to row offset
    std::unique_ptr<OffsetMap> pk2offset_;
    // estimated memory size of InsertRecord, only used for sealed segment
    int64_t estimated_memory_size_{0};

 protected:
    mutable std::shared_mutex shared_mutex_{};
};

class InsertRecordGrowing {
 public:
    InsertRecordGrowing(
        const Schema& schema,
        const int64_t size_per_chunk,
        const storage::MmapChunkDescriptorPtr mmap_descriptor = nullptr)
        : timestamps_(size_per_chunk) {
        std::optional<FieldId> pk_field_id = schema.get_primary_field_id();
        for (auto& field : schema) {
            auto field_id = field.first;
            auto& field_meta = field.second;
            if (pk_field_id.has_value() && pk_field_id.value() == field_id) {
                AssertInfo(!field_meta.is_nullable(),
                           "Primary key should not be nullable");
                switch (field_meta.get_data_type()) {
                    case DataType::INT64: {
                        pk2offset_ =
                            std::make_unique<OffsetOrderedMap<int64_t>>();
                        break;
                    }
                    case DataType::VARCHAR: {
                        pk2offset_ =
                            std::make_unique<OffsetOrderedMap<std::string>>();
                        break;
                    }
                    default: {
                        ThrowInfo(DataTypeInvalid,
                                  fmt::format("unsupported pk type: {}",
                                              field_meta.get_data_type()));
                    }
                }
            }
            append_field_meta(
                field_id, field_meta, size_per_chunk, mmap_descriptor);
        }
    }

    bool
    contain(const PkType& pk) const {
        return pk2offset_->contain(pk);
    }

    std::vector<SegOffset>
    search_pk(const PkType& pk,
              Timestamp timestamp,
              bool include_same_ts = true) const {
        std::shared_lock<std::shared_mutex> lck(shared_mutex_);
        std::vector<SegOffset> res_offsets;
        auto offset_iter = pk2offset_->find(pk);
        auto timestamp_hit =
            include_same_ts ? [](const Timestamp& ts1,
                                 const Timestamp& ts2) { return ts1 <= ts2; }
                            : [](const Timestamp& ts1, const Timestamp& ts2) {
                                  return ts1 < ts2;
                              };
        for (auto offset : offset_iter) {
            if (timestamp_hit(timestamps_[offset], timestamp)) {
                res_offsets.emplace_back(offset);
            }
        }
        return res_offsets;
    }

    void
    search_pk_range(const PkType& pk,
                    proto::plan::OpType op,
                    BitsetTypeView& bitset) const {
        pk2offset_->find_range(
            pk, op, bitset, [](int64_t offset) { return true; });
    }

    void
    search_pk_range(const PkType& pk,
                    Timestamp timestamp,
                    proto::plan::OpType op,
                    BitsetTypeView& bitset) const {
        auto condition = [this, timestamp](int64_t offset) {
            return timestamps_[offset] <= timestamp;
        };
        pk2offset_->find_range(pk, op, bitset, condition);
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
                    ThrowInfo(DataTypeInvalid,
                              fmt::format("unsupported primary key data type",
                                          data_type));
                }
            }
        }
    }

    bool
    empty_pks() const {
        std::shared_lock lck(shared_mutex_);
        return pk2offset_->empty();
    }

    void
    seal_pks() {
        std::lock_guard lck(shared_mutex_);
        pk2offset_
            ->seal();  // will throw for growing map, consistent with previous behavior
    }

    const ConcurrentVector<Timestamp>&
    timestamps() const {
        return timestamps_;
    }

    void
    clear() {
        timestamps_.clear();
        timestamp_index_ = TimestampIndex();
        pk2offset_->clear();
        reserved = 0;
        data_.clear();
        ack_responder_.clear();
    }

    void
    append_field_meta(
        FieldId field_id,
        const FieldMeta& field_meta,
        int64_t size_per_chunk,
        const storage::MmapChunkDescriptorPtr mmap_descriptor = nullptr) {
        if (field_meta.is_nullable()) {
            this->append_valid_data(field_id);
        }
        const milvus::storage::MmapConfig mmap_config =
            storage::MmapManager::GetInstance().GetMmapConfig();
        // todo: @cqy123456, scalar and vector mmap should be depend on GetScalarFieldEnableMmap()/ GetVectorFieldEnableMmap()
        storage::MmapChunkDescriptorPtr scalar_mmap_descriptor =
            mmap_config.GetEnableGrowingMmap() ? mmap_descriptor : nullptr;
        storage::MmapChunkDescriptorPtr vec_mmap_descriptor =
            mmap_config.GetEnableGrowingMmap() ? mmap_descriptor : nullptr;
        storage::MmapChunkDescriptorPtr dense_vec_mmap_descriptor = nullptr;
        // todo: @cqy123456, remove all condition and select of dense_vec_mmap_descriptor later
        {
            const auto& segcore_config =
                milvus::segcore::SegcoreConfig::default_config();
            auto enable_intermin_index =
                segcore_config.get_enable_interim_segment_index();
            auto dense_vec_intermin_index_type =
                segcore_config.get_dense_vector_intermin_index_type();
            auto enable_mmap_field_mmap =
                mmap_config.GetVectorFieldEnableMmap();
            bool enable_dense_vec_mmap =
                enable_intermin_index && enable_mmap_field_mmap &&
                (dense_vec_intermin_index_type ==
                 knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR);
            dense_vec_mmap_descriptor =
                enable_dense_vec_mmap || mmap_config.GetEnableGrowingMmap()
                    ? mmap_descriptor
                    : nullptr;
        }
        if (field_meta.is_vector()) {
            if (field_meta.get_data_type() == DataType::VECTOR_FLOAT) {
                this->append_data<FloatVector>(field_id,
                                               field_meta.get_dim(),
                                               size_per_chunk,
                                               dense_vec_mmap_descriptor);
                return;
            } else if (field_meta.get_data_type() == DataType::VECTOR_BINARY) {
                this->append_data<BinaryVector>(field_id,
                                                field_meta.get_dim(),
                                                size_per_chunk,
                                                dense_vec_mmap_descriptor);
                return;
            } else if (field_meta.get_data_type() == DataType::VECTOR_FLOAT16) {
                this->append_data<Float16Vector>(field_id,
                                                 field_meta.get_dim(),
                                                 size_per_chunk,
                                                 dense_vec_mmap_descriptor);
                return;
            } else if (field_meta.get_data_type() ==
                       DataType::VECTOR_BFLOAT16) {
                this->append_data<BFloat16Vector>(field_id,
                                                  field_meta.get_dim(),
                                                  size_per_chunk,
                                                  dense_vec_mmap_descriptor);
                return;
            } else if (field_meta.get_data_type() ==
                       DataType::VECTOR_SPARSE_U32_F32) {
                this->append_data<SparseFloatVector>(
                    field_id, size_per_chunk, vec_mmap_descriptor);
                return;
            } else if (field_meta.get_data_type() == DataType::VECTOR_INT8) {
                this->append_data<Int8Vector>(field_id,
                                              field_meta.get_dim(),
                                              size_per_chunk,
                                              dense_vec_mmap_descriptor);
                return;
            } else if (field_meta.get_data_type() == DataType::VECTOR_ARRAY) {
                this->append_data<VectorArray>(field_id,
                                               field_meta.get_dim(),
                                               size_per_chunk,
                                               dense_vec_mmap_descriptor);
                return;
            } else {
                ThrowInfo(DataTypeInvalid,
                          fmt::format("unsupported vector type",
                                      field_meta.get_data_type()));
            }
        }
        switch (field_meta.get_data_type()) {
            case DataType::BOOL: {
                this->append_data<bool>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::INT8: {
                this->append_data<int8_t>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::INT16: {
                this->append_data<int16_t>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::INT32: {
                this->append_data<int32_t>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::INT64: {
                this->append_data<int64_t>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::FLOAT: {
                this->append_data<float>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::DOUBLE: {
                this->append_data<double>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::TIMESTAMPTZ: {
                this->append_data<int64_t>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::VARCHAR:
            case DataType::TEXT: {
                this->append_data<std::string>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::JSON: {
                this->append_data<Json>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::ARRAY: {
                this->append_data<Array>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            case DataType::GEOMETRY: {
                this->append_data<std::string>(
                    field_id, size_per_chunk, scalar_mmap_descriptor);
                return;
            }
            default: {
                ThrowInfo(DataTypeInvalid,
                          fmt::format("unsupported scalar type",
                                      field_meta.get_data_type()));
            }
        }
    }

    void
    insert_pk(const PkType& pk, int64_t offset) {
        std::lock_guard lck(shared_mutex_);
        pk2offset_->insert(pk, offset);
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

    ThreadSafeValidDataPtr
    get_valid_data(FieldId field_id) const {
        AssertInfo(valid_data_.find(field_id) != valid_data_.end(),
                   "Cannot find valid_data with field_id: " +
                       std::to_string(field_id.get()));
        AssertInfo(valid_data_.at(field_id) != nullptr,
                   "valid_data_ at i is null" + std::to_string(field_id.get()));
        return valid_data_.at(field_id);
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

    // append a column of scalar type
    void
    append_valid_data(FieldId field_id) {
        valid_data_.emplace(field_id, std::make_shared<ThreadSafeValidData>());
    }

    // append a column of vector type
    // vector not support nullable, not pass valid data ptr
    template <typename VectorType>
    void
    append_data(FieldId field_id,
                int64_t dim,
                int64_t size_per_chunk,
                const storage::MmapChunkDescriptorPtr mmap_descriptor) {
        static_assert(std::is_base_of_v<VectorTrait, VectorType>);
        data_.emplace(field_id,
                      std::make_unique<ConcurrentVector<VectorType>>(
                          dim, size_per_chunk, mmap_descriptor));
    }

    // append a column of scalar or sparse float vector type
    template <typename Type>
    void
    append_data(FieldId field_id,
                int64_t size_per_chunk,
                const storage::MmapChunkDescriptorPtr mmap_descriptor) {
        static_assert(IsScalar<Type> || IsSparse<Type>);
        data_.emplace(
            field_id,
            std::make_unique<ConcurrentVector<Type>>(
                size_per_chunk,
                mmap_descriptor,
                is_valid_data_exist(field_id) ? get_valid_data(field_id)
                                              : nullptr));
    }

    void
    drop_field_data(FieldId field_id) {
        data_.erase(field_id);
        valid_data_.erase(field_id);
    }

    int64_t
    size() const {
        return ack_responder_.GetAck();
    }

    bool
    empty() const {
        return pk2offset_->empty();
    }

 public:
    ConcurrentVector<Timestamp> timestamps_;
    std::atomic<int64_t> reserved = 0;
    TimestampIndex timestamp_index_;
    std::unique_ptr<OffsetMap> pk2offset_;

    // used for preInsert of growing segment
    AckResponder ack_responder_;

 private:
    std::unordered_map<FieldId, std::unique_ptr<VectorBase>> data_{};
    std::unordered_map<FieldId, ThreadSafeValidDataPtr> valid_data_{};
    mutable std::shared_mutex shared_mutex_{};
};

// Keep the original template API via alias
template <bool is_sealed>
using InsertRecord =
    std::conditional_t<is_sealed, InsertRecordSealed, InsertRecordGrowing>;

}  // namespace milvus::segcore
