// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <optional>
#include "common/QueryInfo.h"
#include "knowhere/index/index_node.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/ConcurrentVector.h"
#include "common/Span.h"
#include "query/Utils.h"
#include "segcore/SegmentSealed.h"

namespace milvus {
namespace exec {

template <typename T>
class DataGetter {
 public:
    virtual std::optional<T>
    Get(int64_t idx) const = 0;
};

template <typename T>
class GrowingDataGetter : public DataGetter<T> {
 public:
    GrowingDataGetter(const segcore::SegmentGrowingImpl& segment,
                      FieldId fieldId) {
        growing_raw_data_ = segment.get_insert_record().get_data<T>(fieldId);
        valid_data_ = segment.get_insert_record().get_valid_data(fieldId);
    }

    GrowingDataGetter(const GrowingDataGetter<T>& other)
        : growing_raw_data_(other.growing_raw_data_) {
    }

    std::optional<T>
    Get(int64_t idx) const {
        if (valid_data_ && !valid_data_->is_valid(idx)) {
            return std::nullopt;
        }
        if constexpr (std::is_same_v<std::string, T>) {
            if (growing_raw_data_->is_mmap()) {
                // when scalar data is mapped, it's needed to get the scalar data view and reconstruct string from the view
                return T(growing_raw_data_->view_element(idx));
            }
        }
        return growing_raw_data_->operator[](idx);
    }

 protected:
    const segcore::ConcurrentVector<T>* growing_raw_data_;
    segcore::ThreadSafeValidDataPtr valid_data_;
};

template <typename T>
class SealedDataGetter : public DataGetter<T> {
 private:
    const segcore::SegmentSealed& segment_;
    const FieldId field_id_;
    bool from_data_;

    mutable std::unordered_map<int64_t, std::vector<std::string_view>>
        str_view_map_;
    // Getting str_view from segment is cpu-costly, this map is to cache this view for performance
 public:
    SealedDataGetter(const segcore::SegmentSealed& segment, FieldId& field_id)
        : segment_(segment), field_id_(field_id) {
        from_data_ = segment_.HasFieldData(field_id_);
        if (!from_data_ && !segment_.HasIndex(field_id_)) {
            PanicInfo(
                UnexpectedError,
                "The segment:{} used to init data getter has no effective "
                "data source, neither"
                "index or data",
                segment_.get_segment_id());
        }
    }

    std::optional<T>
    Get(int64_t idx) const {
        if (from_data_) {
            auto id_offset_pair = segment_.get_chunk_by_offset(field_id_, idx);
            auto chunk_id = id_offset_pair.first;
            auto inner_offset = id_offset_pair.second;
            Span<T> span = segment_.chunk_data<T>(field_id_, chunk_id);
            if (span.valid_data() && !span.valid_data()[inner_offset]) {
                return std::nullopt;
            }
            if constexpr (std::is_same_v<T, std::string>) {
                if (str_view_map_.find(chunk_id) == str_view_map_.end()) {
                    auto [str_chunk_view, _] =
                        segment_.chunk_view<std::string_view>(field_id_,
                                                              chunk_id);
                    str_view_map_[chunk_id] = std::move(str_chunk_view);
                }
                auto& str_chunk_view = str_view_map_[chunk_id];
                std::string_view str_val_view =
                    str_chunk_view.operator[](inner_offset);
                return std::string(str_val_view.data(), str_val_view.length());
            } else {
                auto raw = span.operator[](inner_offset);
                return raw;
            }
        } else {
            // null is not supported for indexed fields
            auto& chunk_index = segment_.chunk_scalar_index<T>(field_id_, 0);
            auto raw = chunk_index.Reverse_Lookup(idx);
            AssertInfo(raw.has_value(), "field data not found");
            return raw.value();
        }
    }
};

template <typename T>
static const std::shared_ptr<DataGetter<T>>
GetDataGetter(const segcore::SegmentInternalInterface& segment,
              FieldId fieldId) {
    if (const auto* growing_segment =
            dynamic_cast<const segcore::SegmentGrowingImpl*>(&segment)) {
        return std::make_shared<GrowingDataGetter<T>>(*growing_segment,
                                                      fieldId);
    } else if (const auto* sealed_segment =
                   dynamic_cast<const segcore::SegmentSealed*>(&segment)) {
        return std::make_shared<SealedDataGetter<T>>(*sealed_segment, fieldId);
    } else {
        PanicInfo(UnexpectedError,
                  "The segment used to init data getter is neither growing or "
                  "sealed, wrong state");
    }
}

void
SearchGroupBy(const std::vector<std::shared_ptr<VectorIterator>>& iterators,
              const SearchInfo& searchInfo,
              std::vector<GroupByValueType>& group_by_values,
              const segcore::SegmentInternalInterface& segment,
              std::vector<int64_t>& seg_offsets,
              std::vector<float>& distances,
              std::vector<size_t>& topk_per_nq_prefix_sum);

template <typename T>
void
GroupIteratorsByType(
    const std::vector<std::shared_ptr<VectorIterator>>& iterators,
    int64_t topK,
    int64_t group_size,
    bool strict_group_size,
    const DataGetter<T>& data_getter,
    std::vector<GroupByValueType>& group_by_values,
    std::vector<int64_t>& seg_offsets,
    std::vector<float>& distances,
    const knowhere::MetricType& metrics_type,
    std::vector<size_t>& topk_per_nq_prefix_sum);

template <typename T>
struct GroupByMap {
 private:
    std::unordered_map<std::optional<T>, int> group_map_{};
    int group_capacity_{0};
    int group_size_{0};
    int enough_group_count_{0};
    bool strict_group_size_{false};

 public:
    GroupByMap(int group_capacity,
               int group_size,
               bool strict_group_size = false)
        : group_capacity_(group_capacity),
          group_size_(group_size),
          strict_group_size_(strict_group_size){};
    bool
    IsGroupResEnough() {
        bool enough = false;
        if (strict_group_size_) {
            enough = group_map_.size() == group_capacity_ &&
                     enough_group_count_ == group_capacity_;
        } else {
            enough = group_map_.size() == group_capacity_;
        }
        return enough;
    }
    bool
    Push(const std::optional<T>& t) {
        if (group_map_.size() >= group_capacity_ &&
            group_map_.find(t) == group_map_.end()) {
            return false;
        }
        if (group_map_[t] >= group_size_) {
            //we ignore following input no matter the distance as knowhere::iterator doesn't guarantee
            //strictly increase/decreasing distance output
            //but this should not be a very serious influence to overall recall rate
            return false;
        }
        group_map_[t] += 1;
        if (group_map_[t] >= group_size_) {
            enough_group_count_ += 1;
        }
        return true;
    }
};

template <typename T>
void
GroupIteratorResult(const std::shared_ptr<VectorIterator>& iterator,
                    int64_t topK,
                    int64_t group_size,
                    bool strict_group_size,
                    const DataGetter<T>& data_getter,
                    std::vector<int64_t>& offsets,
                    std::vector<float>& distances,
                    const knowhere::MetricType& metrics_type);

}  // namespace exec
}  // namespace milvus
