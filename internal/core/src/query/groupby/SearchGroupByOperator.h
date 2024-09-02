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

#include "common/QueryInfo.h"
#include "knowhere/index/index_node.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealedImpl.h"
#include "segcore/ConcurrentVector.h"
#include "common/Span.h"
#include "query/Utils.h"

namespace milvus {
namespace query {

template <typename T>
class DataGetter {
 public:
    virtual T
    Get(int64_t idx) const = 0;
};

template <typename T>
class GrowingDataGetter : public DataGetter<T> {
 public:
    const segcore::ConcurrentVector<T>* growing_raw_data_;
    GrowingDataGetter(const segcore::SegmentGrowingImpl& segment,
                      FieldId fieldId) {
        growing_raw_data_ = segment.get_insert_record().get_data<T>(fieldId);
    }

    GrowingDataGetter(const GrowingDataGetter<T>& other)
        : growing_raw_data_(other.growing_raw_data_) {
    }

    T
    Get(int64_t idx) const {
        return growing_raw_data_->operator[](idx);
    }
};

template <typename T>
class SealedDataGetter : public DataGetter<T> {
 private:
    std::shared_ptr<Span<T>> field_data_;
    std::shared_ptr<std::vector<std::string_view>> str_field_data_;
    const index::ScalarIndex<T>* field_index_;

 public:
    SealedDataGetter(const segcore::SegmentSealedImpl& segment,
                     FieldId& field_id) {
        if (segment.HasFieldData(field_id)) {
            if constexpr (std::is_same_v<T, std::string>) {
                str_field_data_ =
                    std::make_shared<std::vector<std::string_view>>(
                        segment.chunk_view<std::string_view>(field_id, 0)
                            .first);
            } else {
                auto span = segment.chunk_data<T>(field_id, 0);
                field_data_ = std::make_shared<Span<T>>(
                    span.data(), span.valid_data(), span.row_count());
            }
        } else if (segment.HasIndex(field_id)) {
            this->field_index_ = &(segment.chunk_scalar_index<T>(field_id, 0));
        } else {
            PanicInfo(UnexpectedError,
                      "The segment used to init data getter has no effective "
                      "data source, neither"
                      "index or data");
        }
    }

    SealedDataGetter(const SealedDataGetter<T>& other)
        : field_data_(other.field_data_),
          str_field_data_(other.str_field_data_),
          field_index_(other.field_index_) {
    }

    T
    Get(int64_t idx) const {
        if (field_data_ || str_field_data_) {
            if constexpr (std::is_same_v<T, std::string>) {
                std::string_view str_val_view =
                    str_field_data_->operator[](idx);
                return std::string(str_val_view.data(), str_val_view.length());
            }
            return field_data_->operator[](idx);
        } else {
            return (*field_index_).Reverse_Lookup(idx);
        }
    }
};

template <typename T>
static const std::shared_ptr<DataGetter<T>>
GetDataGetter(const segcore::SegmentInternalInterface& segment,
              FieldId fieldId) {
    if (const segcore::SegmentGrowingImpl* growing_segment =
            dynamic_cast<const segcore::SegmentGrowingImpl*>(&segment)) {
        return std::make_shared<GrowingDataGetter<T>>(*growing_segment,
                                                      fieldId);
    } else if (const segcore::SegmentSealedImpl* sealed_segment =
                   dynamic_cast<const segcore::SegmentSealedImpl*>(&segment)) {
        return std::make_shared<SealedDataGetter<T>>(*sealed_segment, fieldId);
    } else {
        PanicInfo(UnexpectedError,
                  "The segment used to init data getter is neither growing or "
                  "sealed, wrong state");
    }
}

static bool
PrepareVectorIteratorsFromIndex(const SearchInfo& search_info,
                                int nq,
                                const DatasetPtr dataset,
                                SearchResult& search_result,
                                const BitsetView& bitset,
                                const index::VectorIndex& index) {
    if (search_info.group_by_field_id_.has_value()) {
        try {
            auto search_conf = index.PrepareSearchParams(search_info);
            knowhere::expected<std::vector<knowhere::IndexNode::IteratorPtr>>
                iterators_val =
                    index.VectorIterators(dataset, search_conf, bitset);
            if (iterators_val.has_value()) {
                search_result.AssembleChunkVectorIterators(
                    nq, 1, -1, iterators_val.value());
            } else {
                LOG_ERROR(
                    "Returned knowhere iterator has non-ready iterators "
                    "inside, terminate group_by operation:{}",
                    knowhere::Status2String(iterators_val.error()));
                PanicInfo(ErrorCode::Unsupported,
                          "Returned knowhere iterator has non-ready iterators "
                          "inside, terminate group_by operation");
            }
            search_result.total_nq_ = dataset->GetRows();
            search_result.unity_topK_ = search_info.topk_;
        } catch (const std::runtime_error& e) {
            LOG_ERROR(
                "Caught error:{} when trying to initialize ann iterators for "
                "group_by: "
                "group_by operation will be terminated",
                e.what());
            PanicInfo(
                ErrorCode::Unsupported,
                "Failed to groupBy, current index:" + index.GetIndexType() +
                    " doesn't support search_group_by");
        }
        return true;
    }
    return false;
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
    bool group_strict_size,
    const DataGetter<T>& data_getter,
    std::vector<GroupByValueType>& group_by_values,
    std::vector<int64_t>& seg_offsets,
    std::vector<float>& distances,
    const knowhere::MetricType& metrics_type,
    std::vector<size_t>& topk_per_nq_prefix_sum);

template <typename T>
struct GroupByMap {
 private:
    std::unordered_map<T, int> group_map_{};
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
    Push(const T& t) {
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
                    bool group_strict_size,
                    const DataGetter<T>& data_getter,
                    std::vector<GroupByValueType>& group_by_values,
                    std::vector<int64_t>& offsets,
                    std::vector<float>& distances,
                    const knowhere::MetricType& metrics_type);

}  // namespace query
}  // namespace milvus
