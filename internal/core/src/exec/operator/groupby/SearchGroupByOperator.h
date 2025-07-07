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
#include <type_traits>

#include "cachinglayer/CacheSlot.h"
#include "common/Json.h"
#include "common/JsonUtils.h"
#include "common/QueryInfo.h"
#include "common/Types.h"
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

 protected:
    std::optional<std::string> json_path_;
};

template <typename OutputType, typename InnerRawType>
class GrowingDataGetter : public DataGetter<OutputType> {
 public:
    GrowingDataGetter(const segcore::SegmentGrowingImpl& segment,
                      FieldId fieldId,
                      std::optional<std::string> json_path) {
        growing_raw_data_ =
            segment.get_insert_record().get_data<InnerRawType>(fieldId);
        valid_data_ = segment.get_insert_record().is_valid_data_exist(fieldId)
                          ? segment.get_insert_record().get_valid_data(fieldId)
                          : nullptr;
        this->json_path_ = json_path;
    }

    GrowingDataGetter(const GrowingDataGetter<OutputType, InnerRawType>& other)
        : growing_raw_data_(other.growing_raw_data_) {
    }

    std::optional<OutputType>
    Get(int64_t idx) const {
        if (valid_data_ && !valid_data_->is_valid(idx)) {
            return std::nullopt;
        }
        if constexpr (std::is_same_v<InnerRawType, std::string>) {
            if (growing_raw_data_->is_mmap()) {
                // when scalar data is mapped, it's needed to get the scalar data view and reconstruct string from the view
                return std::optional<std::string>(
                    growing_raw_data_->view_element(idx));
            }
        } else if constexpr (std::is_same_v<InnerRawType, milvus::Json>) {
            auto parse_json_doc =
                [&](milvus::Json& json_val) -> std::optional<OutputType> {
                if constexpr (std::is_same_v<OutputType, bool>) {
                    auto result = json_val.at<bool>(this->json_path_.value());
                    if (result.error() != simdjson::SUCCESS)
                        return std::nullopt;
                    return result.value();
                } else if constexpr (std::is_same_v<OutputType, int8_t>) {
                    auto result = json_val.at<int64_t>(this->json_path_.value());
                    if (result.error() != simdjson::SUCCESS)
                        return std::nullopt;
                    return static_cast<int8_t>(result.value());
                } else if constexpr (std::is_same_v<OutputType, int16_t>) {
                    auto result = json_val.at<int64_t>(this->json_path_.value());
                    if (result.error() != simdjson::SUCCESS)
                        return std::nullopt;
                    return static_cast<int16_t>(result.value());
                } else if constexpr (std::is_same_v<OutputType, int32_t>) {
                    auto result = json_val.at<int64_t>(this->json_path_.value());
                    if (result.error() != simdjson::SUCCESS)
                        return std::nullopt;
                    return static_cast<int32_t>(result.value());
                } else if constexpr (std::is_same_v<OutputType, int64_t>) {
                    LOG_INFO("hc===GrowingDataGetter=int64_t, json_path: {}", this->json_path_.value());
                    auto result = json_val.at<int64_t>(this->json_path_.value());
                    if (result.error() != simdjson::SUCCESS)
                        return std::nullopt;
                    LOG_INFO("hc===GrowingDataGetter=result.value(): {}", result.value());
                    return result.value();
                } else if constexpr (std::is_same_v<OutputType, std::string>) {
                    LOG_INFO("hc===GrowingDataGetter=std::string, json_path: {}", this->json_path_.value());
                    auto str_result = json_val.at<std::string_view>(this->json_path_.value());
                    if (str_result.error() != simdjson::SUCCESS)
                        return std::nullopt;
                    LOG_INFO("hc===GrowingDataGetter=str_result.value(): {}", str_result.value());
                    return std::string(str_result.value());
                }
                return std::nullopt;
            };
            if (growing_raw_data_->is_mmap()) {
                auto json_val_view = growing_raw_data_->view_element(idx);
                milvus::Json json_val(json_val_view);
                return parse_json_doc(json_val);
            } else {
                auto json_val = growing_raw_data_->operator[](idx);
                return parse_json_doc(json_val);
            }
        } else {
            static_assert(std::is_same_v<OutputType, InnerRawType>,
                          "OutputType and InnerRawType must be the same for "
                          "non-json field group by");
            return std::optional<OutputType>(
                static_cast<OutputType>(growing_raw_data_->operator[](idx)));
        }
    }

 protected:
    const segcore::ConcurrentVector<InnerRawType>* growing_raw_data_;
    segcore::ThreadSafeValidDataPtr valid_data_;
};

template <typename OutputType, typename InnerRawType>
class SealedDataGetter : public DataGetter<OutputType> {
 private:
    const segcore::SegmentSealed& segment_;
    const FieldId field_id_;
    bool from_data_;

    mutable std::unordered_map<
        int64_t,
        PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>>
        pw_map_;
    // Getting str_view from segment is cpu-costly, this map is to cache this view for performance
 public:
    SealedDataGetter(const segcore::SegmentSealed& segment,
                     FieldId field_id,
                     std::optional<std::string> json_path)
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
        this->json_path_ = json_path;
    }

    std::optional<OutputType>
    Get(int64_t idx) const {
        //std::cout << "hc===SealedDataGetter=Get, idx: " << idx << ", from_data: " << from_data_ << ", json_path: " << this->json_path_.value() << std::endl;
        //LOG_INFO("hc===SealedDataGetter=Get, idx: {}, from_data: {}, json_path: {}", idx, from_data_, this->json_path_.value());
        if (from_data_) {
            auto id_offset_pair = segment_.get_chunk_by_offset(field_id_, idx);
            auto chunk_id = id_offset_pair.first;
            auto inner_offset = id_offset_pair.second;
            LOG_INFO("hc===SealedDataGetter=Get, chunk_id: {}, inner_offset: {}", chunk_id, inner_offset);
            if constexpr (std::is_same_v<InnerRawType, std::string>) {
                //LOG_INFO("hc===SealedDataGetter=Get, std::string, json_path: {}", this->json_path_.value());
                if (pw_map_.find(chunk_id) == pw_map_.end()) {
                    // for now, search_group_by does not handle null values
                    auto pw = segment_.chunk_view<std::string_view>(field_id_,
                                                                    chunk_id);
                    pw_map_[chunk_id] = std::move(pw);
                }
                auto& pw = pw_map_[chunk_id];
                auto& [str_chunk_view, valid_data] = pw.get();
                if (!valid_data.empty() && !valid_data[inner_offset]) {
                    return std::nullopt;
                }
                std::string_view str_val_view = str_chunk_view[inner_offset];
                return std::string(str_val_view.data(), str_val_view.length());
            } else if constexpr (std::is_same_v<InnerRawType, milvus::Json>) {
                LOG_INFO("hc===SealedDataGetter=Get, milvus::Json, json_path: {}", this->json_path_.value());
                auto pw =
                    segment_.chunk_view<milvus::Json>(field_id_, chunk_id);
                auto& [json_chunk_view, valid_data] = pw.get();
                if (!valid_data.empty() && !valid_data[inner_offset]) {
                    return std::nullopt;
                }
                auto& json_val = json_chunk_view[inner_offset];
                if constexpr (std::is_same_v<OutputType, bool>) {
                    auto result = json_val.at<bool>(this->json_path_.value());
                    if (result.error() != simdjson::SUCCESS)
                        return std::nullopt;
                    return result.value();
                } else if constexpr (std::is_same_v<OutputType, int8_t>) {
                    auto result = json_val.at<int64_t>(this->json_path_.value());
                    if (result.error() != simdjson::SUCCESS)
                        return std::nullopt;
                    return static_cast<int8_t>(result.value());
                } else if constexpr (std::is_same_v<OutputType, int16_t>) {
                    auto result = json_val.at<int64_t>(this->json_path_.value());
                    if (result.error() != simdjson::SUCCESS)
                        return std::nullopt;
                    return static_cast<int16_t>(result.value());
                } else if constexpr (std::is_same_v<OutputType, int32_t>) {
                    auto result = json_val.at<int64_t>(this->json_path_.value());
                    if (result.error() != simdjson::SUCCESS)
                        return std::nullopt;
                    return static_cast<int32_t>(result.value());
                } else if constexpr (std::is_same_v<OutputType, int64_t>) {
                    auto result = json_val.at<int64_t>(this->json_path_.value());
                    if (result.error() != simdjson::SUCCESS)
                        return std::nullopt;
                    LOG_INFO("hc===SealedDataGetter=result.value(): {}", result.value());
                    return result.value();
                } else if constexpr (std::is_same_v<OutputType, std::string>) {
                    LOG_INFO("hc===SealedDataGetter=std::string, json_path: {}", this->json_path_.value());
                    auto str_result = json_val.at<std::string_view>(this->json_path_.value());
                    if (str_result.error() != simdjson::SUCCESS) {
                        return std::nullopt;
                    }
                    LOG_INFO("hc===SealedDataGetter=str_result.value(): {}", str_result.value());
                    return std::string(str_result.value());
                }
                return std::nullopt;
            } else {
                //LOG_INFO("hc===SealedDataGetter=Get, non-json/string field group by, json_path: {}", this->json_path_.value());
                static_assert(std::is_same_v<OutputType, InnerRawType>,
                              "OutputType and InnerRawType must be the same for "
                              "non-json/string field group by");
                auto pw = segment_.chunk_data<InnerRawType>(field_id_, chunk_id);
                auto& span = pw.get();
                if (span.valid_data() && !span.valid_data()[inner_offset]) {
                    return std::nullopt;
                }
                auto raw = span.operator[](inner_offset);
                return raw;
            }
        } else {
            //LOG_INFO("hc===SealedDataGetter=Get, null is not supported for indexed fields, json_path: {}", this->json_path_.value());
            // null is not supported for indexed fields
            auto pw = segment_.chunk_scalar_index<OutputType>(field_id_, 0);
            auto* chunk_index = pw.get();
            auto raw = chunk_index->Reverse_Lookup(idx);
            AssertInfo(raw.has_value(), "field data not found");
            return raw.value();
        }
    }
};

template <typename OutputType, typename InnerRawType>
static const std::shared_ptr<DataGetter<OutputType>>
GetDataGetter(const segcore::SegmentInternalInterface& segment,
              FieldId fieldId,
              std::optional<std::string> json_path = std::nullopt) {
    if(json_path.has_value()) {
        auto json_path_tokens = milvus::parse_json_pointer(json_path.value());
        json_path = milvus::Json::pointer(json_path_tokens);
        LOG_INFO("hc===Converted DataGetter=json_path: {}", json_path.value());
    }
    if (const auto* growing_segment =
            dynamic_cast<const segcore::SegmentGrowingImpl*>(&segment)) {
        if (json_path.has_value()) {
            LOG_INFO("hc===Converted DataGetter=GrowingDataGetter, json_path: {}", json_path.value());
        }
        return std::make_shared<GrowingDataGetter<OutputType, InnerRawType>>(
            *growing_segment, fieldId, json_path);
    } else if (const auto* sealed_segment =
                   dynamic_cast<const segcore::SegmentSealed*>(&segment)) {
        if (json_path.has_value()) {
            LOG_INFO("hc===Converted DataGetter=SealedDataGetter, json_path: {}", json_path.value());
        }
        return std::make_shared<SealedDataGetter<OutputType, InnerRawType>>(
            *sealed_segment, fieldId, json_path);
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
    const std::shared_ptr<DataGetter<T>>& data_getter,
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
                    const std::shared_ptr<DataGetter<T>>& data_getter,
                    std::vector<int64_t>& offsets,
                    std::vector<float>& distances,
                    const knowhere::MetricType& metrics_type);

}  // namespace exec
}  // namespace milvus
