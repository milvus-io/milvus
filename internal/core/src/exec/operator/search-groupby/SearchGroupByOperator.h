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

#include <simdjson.h>
#include <stdint.h>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cachinglayer/CacheSlot.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/JsonUtils.h"
#include "common/OpContext.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "index/Index.h"
#include "index/ScalarIndex.h"
#include "knowhere/comp/index_param.h"
#include "segcore/ConcurrentVector.h"
#include "segcore/InsertRecord.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentSealed.h"
#include "simdjson/error.h"

namespace milvus {
namespace exec {

#define JSON_TYPE_CASE(OutputType, TargetType, JSON_INNER_TYPE, CastExpr)     \
    if constexpr (std::is_same_v<OutputType, TargetType>) {                   \
        auto result = json_val.at<JSON_INNER_TYPE>(this->json_path_.value()); \
        if (result.error() == simdjson::SUCCESS) {                            \
            return CastExpr;                                                  \
        }                                                                     \
        if (this->strict_cast_) {                                             \
            ThrowInfo(UnexpectedError,                                        \
                      "failed to cast json value to " #TargetType             \
                      ", wrong json data inner type");                        \
        }                                                                     \
        return std::nullopt;                                                  \
    }

#define JSON_STRING_CASE(OutputType)                                          \
    if constexpr (std::is_same_v<OutputType, std::string>) {                  \
        if (this->specific_json_type_) {                                      \
            auto str_result =                                                 \
                json_val.at<std::string_view>(this->json_path_.value());      \
            if (str_result.error() == simdjson::SUCCESS) {                    \
                return std::string(str_result.value());                       \
            }                                                                 \
            if (this->strict_cast_) {                                         \
                ThrowInfo(UnexpectedError,                                    \
                          "failed to cast json string to string, wrong json " \
                          "data inner type");                                 \
            }                                                                 \
            return std::nullopt;                                              \
        } else {                                                              \
            auto str_result =                                                 \
                json_val.at_string_any(this->json_path_.value());             \
            if (str_result.error() == simdjson::SUCCESS) {                    \
                return std::string(str_result.value());                       \
            }                                                                 \
            if (this->strict_cast_) {                                         \
                ThrowInfo(UnexpectedError,                                    \
                          "failed to cast json object node to string, wrong " \
                          "json data inner type");                            \
            }                                                                 \
            return std::nullopt;                                              \
        }                                                                     \
    }

#define JSON_TYPE_CASES(OutputType)                                           \
    JSON_TYPE_CASE(OutputType, bool, bool, static_cast<bool>(result.value())) \
    JSON_TYPE_CASE(                                                           \
        OutputType, int8_t, int64_t, static_cast<int8_t>(result.value()))     \
    JSON_TYPE_CASE(                                                           \
        OutputType, int16_t, int64_t, static_cast<int16_t>(result.value()))   \
    JSON_TYPE_CASE(                                                           \
        OutputType, int32_t, int64_t, static_cast<int32_t>(result.value()))   \
    JSON_TYPE_CASE(                                                           \
        OutputType, int64_t, int64_t, static_cast<int64_t>(result.value()))

template <typename T>
class DataGetter {
 public:
    virtual ~DataGetter() = default;

    virtual std::optional<T>
    Get(int64_t idx) const = 0;

 protected:
    std::optional<std::string> json_path_;
    bool specific_json_type_ = false;
    bool strict_cast_ = false;
};

template <typename OutputType, typename InnerRawType = OutputType>
class GrowingDataGetter : public DataGetter<OutputType> {
 public:
    GrowingDataGetter(milvus::OpContext* op_ctx,
                      const segcore::SegmentGrowingImpl& segment,
                      FieldId fieldId,
                      std::optional<std::string> json_path,
                      std::optional<DataType> json_type,
                      bool strict_cast) {
        growing_raw_data_ =
            segment.get_insert_record().get_data<InnerRawType>(fieldId);
        valid_data_ = segment.get_insert_record().is_valid_data_exist(fieldId)
                          ? segment.get_insert_record().get_valid_data(fieldId)
                          : nullptr;
        this->json_path_ = json_path;
        this->specific_json_type_ = json_type.has_value();
        this->strict_cast_ = strict_cast;
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
            return growing_raw_data_->operator[](idx);
        } else if constexpr (std::is_same_v<InnerRawType, milvus::Json>) {
            auto parse_json_doc =
                [&](milvus::Json& json_val) -> std::optional<OutputType> {
                JSON_TYPE_CASES(OutputType)
                JSON_STRING_CASE(OutputType)
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

template <typename OutputType, typename InnerRawType = OutputType>
class SealedDataGetter : public DataGetter<OutputType> {
 private:
    milvus::OpContext* op_ctx_;
    const segcore::SegmentSealed& segment_;
    const FieldId field_id_;
    bool from_data_;

    // Thread-safety contract: str_pw_map / json_pw_map are `mutable` and
    // accessed without locks inside Get(). This is safe ONLY because each
    // SealedDataGetter instance is scoped to a single SearchGroupBy invocation
    // on a single segment, executed on one Driver thread (Velox-style single-
    // threaded operator pipeline). If groupby is ever parallelized per-nq or
    // shared across queries, this cache must be guarded — data race otherwise.
    mutable std::unordered_map<
        int64_t,
        PinWrapper<std::pair<std::vector<std::string_view>, FixedVector<bool>>>>
        str_pw_map;

    PinWrapper<const index::IndexBase*> index_ptr_;
    // Getting str_view from segment is cpu-costly, this map is to cache this view for performance.
    // Shares the same single-thread contract as str_pw_map above.
    mutable std::unordered_map<
        int64_t,
        PinWrapper<std::pair<std::vector<milvus::Json>, FixedVector<bool>>>>
        json_pw_map;

 public:
    SealedDataGetter(milvus::OpContext* op_ctx,
                     const segcore::SegmentSealed& segment,
                     FieldId field_id,
                     std::optional<std::string> json_path,
                     std::optional<DataType> json_type,
                     bool strict_cast)
        : op_ctx_(op_ctx), segment_(segment), field_id_(field_id) {
        from_data_ = segment_.HasFieldData(field_id_);
        if (!from_data_) {
            auto index = segment_.PinIndex(op_ctx_, field_id_);
            if (index.empty()) {
                ThrowInfo(
                    UnexpectedError,
                    "The segment:{} used to init data getter has no effective "
                    "data source, neither"
                    "index or data",
                    segment_.get_segment_id());
            }
            index_ptr_ = std::move(index[0]);
        }
        this->json_path_ = json_path;
        this->specific_json_type_ = json_type.has_value();
        this->strict_cast_ = strict_cast;
    }

    std::optional<OutputType>
    Get(int64_t idx) const {
        if (from_data_) {
            auto id_offset_pair = segment_.get_chunk_by_offset(field_id_, idx);
            auto chunk_id = id_offset_pair.first;
            auto inner_offset = id_offset_pair.second;
            if constexpr (std::is_same_v<InnerRawType, std::string>) {
                if (str_pw_map.find(chunk_id) == str_pw_map.end()) {
                    // for now, search_group_by does not handle null values
                    auto pw = segment_.chunk_view<std::string_view>(
                        op_ctx_, field_id_, chunk_id);
                    str_pw_map[chunk_id] = std::move(pw);
                }
                auto& pw = str_pw_map[chunk_id];
                auto& [str_chunk_view, valid_data] = pw.get();
                if (!valid_data.empty() && !valid_data[inner_offset]) {
                    return std::nullopt;
                }
                std::string_view str_val_view = str_chunk_view[inner_offset];
                return std::string(str_val_view.data(), str_val_view.length());
            } else if constexpr (std::is_same_v<InnerRawType, milvus::Json>) {
                if (json_pw_map.find(chunk_id) == json_pw_map.end()) {
                    auto pw = segment_.chunk_view<milvus::Json>(
                        op_ctx_, field_id_, chunk_id);
                    json_pw_map[chunk_id] = std::move(pw);
                }
                auto& pw = json_pw_map[chunk_id];
                auto& [json_chunk_view, valid_data] = pw.get();
                if (!valid_data.empty() && !valid_data[inner_offset]) {
                    return std::nullopt;
                }
                auto& json_val = json_chunk_view[inner_offset];
                JSON_TYPE_CASES(OutputType)
                JSON_STRING_CASE(OutputType)
                return std::nullopt;
            } else {
                static_assert(
                    std::is_same_v<OutputType, InnerRawType>,
                    "OutputType and InnerRawType must be the same for "
                    "non-json/string field group by");
                auto pw = segment_.chunk_data<InnerRawType>(
                    op_ctx_, field_id_, chunk_id);
                auto& span = pw.get();
                if (span.valid_data() && !span.valid_data()[inner_offset]) {
                    return std::nullopt;
                }
                auto raw = span.operator[](inner_offset);
                return raw;
            }
        } else {
            // null is not supported for indexed fields
            AssertInfo(index_ptr_.get() != nullptr,
                       "indexed field {} has no valid index pointer",
                       field_id_.get());
            auto chunk_index =
                dynamic_cast<const index::ScalarIndex<OutputType>*>(
                    index_ptr_.get());
            AssertInfo(chunk_index != nullptr,
                       "index type mismatch for field {}: expected "
                       "ScalarIndex<OutputType>",
                       field_id_.get());
            auto raw = chunk_index->Reverse_Lookup(idx);
            AssertInfo(raw.has_value(), "field data not found");
            return raw.value();
        }
    }
};

template <typename OutputType, typename InnerRawType = OutputType>
static const std::shared_ptr<DataGetter<OutputType>>
GetDataGetter(milvus::OpContext* op_ctx,
              const segcore::SegmentInternalInterface& segment,
              FieldId fieldId,
              std::optional<std::string> json_path = std::nullopt,
              std::optional<DataType> json_type = std::nullopt,
              bool strict_cast = false) {
    if (json_path.has_value()) {
        auto json_path_tokens = milvus::parse_json_pointer(json_path.value());
        json_path = milvus::Json::pointer(json_path_tokens);
    }
    if (const auto* growing_segment =
            dynamic_cast<const segcore::SegmentGrowingImpl*>(&segment)) {
        return std::make_shared<GrowingDataGetter<OutputType, InnerRawType>>(
            op_ctx,
            *growing_segment,
            fieldId,
            json_path,
            json_type,
            strict_cast);
    } else if (const auto* sealed_segment =
                   dynamic_cast<const segcore::SegmentSealed*>(&segment)) {
        return std::make_shared<SealedDataGetter<OutputType, InnerRawType>>(
            op_ctx,
            *sealed_segment,
            fieldId,
            json_path,
            json_type,
            strict_cast);
    } else {
        ThrowInfo(UnexpectedError,
                  "The segment used to init data getter is neither growing or "
                  "sealed, wrong state");
    }
}

// GroupByMap for CompositeGroupKey
struct CompositeGroupByMap {
 private:
    std::unordered_map<CompositeGroupKey, int, CompositeGroupKeyHash>
        group_map_{};
    int group_capacity_{0};
    int group_size_{0};
    int enough_group_count_{0};
    bool strict_group_size_{false};

 public:
    CompositeGroupByMap(int group_capacity,
                        int group_size,
                        bool strict_group_size = false)
        : group_capacity_(group_capacity),
          group_size_(group_size),
          strict_group_size_(strict_group_size) {
        if (group_capacity > 0) {
            group_map_.reserve(static_cast<size_t>(group_capacity));
        }
    }

    bool
    IsGroupResEnough() {
        bool enough = false;
        if (strict_group_size_) {
            enough = static_cast<int>(group_map_.size()) == group_capacity_ &&
                     enough_group_count_ == group_capacity_;
        } else {
            enough = static_cast<int>(group_map_.size()) == group_capacity_;
        }
        return enough;
    }

    bool
    Push(const CompositeGroupKey& key) {
        auto [it, inserted] = group_map_.try_emplace(key, 0);
        if (inserted) {
            if (static_cast<int>(group_map_.size()) > group_capacity_) {
                group_map_.erase(it);
                return false;
            }
        }
        if (it->second >= group_size_) {
            return false;
        }
        it->second += 1;
        if (it->second >= group_size_) {
            enough_group_count_ += 1;
        }
        return true;
    }

    int
    GetGroupCount() const {
        return group_map_.size();
    }

    int
    GetEnoughGroupCount() const {
        return enough_group_count_;
    }
};

// Multi-field DataGetter that reads multiple fields and builds CompositeGroupKey
class MultiFieldDataGetter {
 public:
    MultiFieldDataGetter(
        milvus::OpContext* op_ctx,
        const segcore::SegmentInternalInterface& segment,
        const std::vector<FieldId>& field_ids,
        const std::optional<std::string>& json_path = std::nullopt,
        const std::optional<DataType>& json_type = std::nullopt,
        bool strict_cast = false);

    CompositeGroupKey
    Get(int64_t idx) const;

    void
    GetInto(int64_t idx, CompositeGroupKey& out) const;

 private:
    std::vector<std::function<GroupByValueType(int64_t)>> getters_;
    size_t field_count_;
};

// Unified group by interface - always emits CompositeGroupKey
void
SearchGroupBy(milvus::OpContext* op_ctx,
              const std::vector<std::shared_ptr<VectorIterator>>& iterators,
              const SearchInfo& searchInfo,
              std::vector<CompositeGroupKey>& composite_group_by_values,
              const segcore::SegmentInternalInterface& segment,
              std::vector<int64_t>& seg_offsets,
              std::vector<float>& distances,
              std::vector<size_t>& topk_per_nq_prefix_sum);

}  // namespace exec
}  // namespace milvus

#undef JSON_TYPE_CASE
#undef JSON_STRING_CASE
#undef JSON_TYPE_CASES
