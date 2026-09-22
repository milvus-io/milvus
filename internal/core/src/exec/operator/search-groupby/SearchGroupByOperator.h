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
#include "common/Chunk.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/JsonCastType.h"
#include "common/JsonUtils.h"
#include "common/OpContext.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "exec/expression/IndexPathSelection.h"
#include "index/contracts/query/IJsonIndexReader.h"
#include "index/contracts/query/IScalarValueReader.h"
#include "knowhere/comp/index_param.h"
#include "mmap/ChunkedColumnInterface.h"
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
    using IndexValueType = std::conditional_t<
        std::is_same_v<OutputType, std::string>,
        std::string_view,
        OutputType>;

    milvus::OpContext* op_ctx_;
    const segcore::SegmentSealed& segment_;
    const FieldId field_id_;
    bool from_data_;

    // Thread-safety contract: string_chunk_pins_ is mutable and accessed
    // without locks inside Get(). Each getter belongs to one SearchGroupBy
    // invocation on one segment and is used on a single Driver thread.
    // Sharing a getter across threads would require synchronizing this cache.
    mutable std::unordered_map<int64_t, PinWrapper<Chunk*>> string_chunk_pins_;

    // Keep the parent cell pinned longer than a resolved JSON view. The typed
    // value interface is borrowed from the root reader or that view.
    segcore::IndexPin index_pin_;
    index::JsonResolvedReader json_view_;
    const index::IScalarValueReader<IndexValueType>* value_reader_{nullptr};

    // VARCHAR and raw JSON share StringChunk storage. Keep visited chunks
    // pinned for the getter's lifetime and construct only the requested view.
    // The returned view borrows from this cache and must not outlive the getter.
    std::optional<std::string_view>
    GetStringRow(int64_t chunk_id, int64_t inner_offset) const {
        auto it = string_chunk_pins_.find(chunk_id);
        if (it == string_chunk_pins_.end()) {
            auto column = segment_.GetChunkedColumn(field_id_);
            AssertInfo(column != nullptr,
                       "group-by field {} has no raw string column",
                       field_id_.get());
            auto pin = column->GetChunk(op_ctx_, chunk_id);
            it = string_chunk_pins_.emplace(chunk_id, std::move(pin)).first;
        }
        const auto* chunk = static_cast<const StringChunk*>(it->second.get());
        if (!chunk->isValid(inner_offset)) {
            return std::nullopt;
        }
        return (*chunk)[inner_offset];
    }

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
            if (!InitValueReader(json_path, json_type, strict_cast)) {
                ThrowInfo(
                    UnexpectedError,
                    "The segment:{} used to init data getter has no effective "
                    "data source, neither"
                    "index or data",
                    segment_.get_segment_id());
            }
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
                auto row = GetStringRow(chunk_id, inner_offset);
                if (!row.has_value()) {
                    return std::nullopt;
                }
                return std::string(*row);
            } else if constexpr (std::is_same_v<InnerRawType, milvus::Json>) {
                auto row = GetStringRow(chunk_id, inner_offset);
                if (!row.has_value()) {
                    return std::nullopt;
                }
                // JSONChunkWriter provides SIMDJSON_PADDING after the final
                // row. The cached pin keeps both the bytes and padding alive.
                milvus::Json json_val(*row);
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
                if (!span.is_valid(inner_offset)) {
                    return std::nullopt;
                }
                auto raw = span.operator[](inner_offset);
                return raw;
            }
        } else {
            AssertInfo(value_reader_ != nullptr,
                       "indexed field {} has no typed value reader",
                       field_id_.get());
            // Lookup owns string bytes and returns nullopt for NULL rows, so
            // indexed and raw sources produce the same nullable group key.
            return value_reader_->Lookup(idx);
        }
    }

 private:
    bool
    InitValueReader(const std::optional<std::string>& json_path,
                    const std::optional<DataType>& json_type,
                    bool strict_cast) {
        constexpr bool is_json =
            std::is_same_v<InnerRawType, milvus::Json>;
        if constexpr (is_json) {
            // The index cannot reproduce at_string_any, strict cast failures,
            // or the original integer-vs-floating JSON number category from a
            // projected DOUBLE value. Those cases require raw JSON.
            if (!json_path.has_value() || !json_type.has_value() ||
                strict_cast ||
                (json_type.value() != DataType::BOOL &&
                 json_type.value() != DataType::VARCHAR)) {
                return false;
            }
        }

        auto value_type = segment_.GetFieldDataType(field_id_);
        if (value_type == DataType::TIMESTAMPTZ) {
            value_type = DataType::INT64;
        }
        ExprIndexRequirement requirement{
            .field_id = field_id_,
            .reader = RequiredReader::ValueLookup,
            .value_type = value_type,
        };
        if constexpr (is_json) {
            requirement.value_type = json_type.value();
            requirement.is_json_field = true;
            requirement.json_path = json_path.value();
        }

        const auto capabilities = segment_.IndexCapability(field_id_);
        const auto decision = DetermineExecPath(requirement, capabilities);
        if (!decision.key.has_value()) {
            return false;
        }
        const auto* entry = capabilities.Find(*decision.key);
        AssertInfo(entry != nullptr,
                   "selected group-by index for field {} is absent from metadata",
                   field_id_.get());
        index_pin_ = segment_.PinIndex(op_ctx_, *decision.key);
        if (!index_pin_) {
            return false;
        }
        AssertInfo(segcore::SameCaps(entry->caps, index_pin_->Caps()),
                   "group-by index metadata for field {} does not match reader "
                   "capabilities",
                   field_id_.get());

        const index::IIndexReaderBase* reader = index_pin_.get();
        if constexpr (is_json) {
            if (entry->caps.json_paths) {
                auto* json_reader =
                    dynamic_cast<const index::IJsonIndexReader*>(reader);
                AssertInfo(json_reader != nullptr,
                           "selected JSON group-by index has no path reader");
                json_view_ = json_reader->Resolve(requirement.json_path,
                                                  entry->json_cast_type);
                if (!json_view_) {
                    return false;
                }
                reader = json_view_.get();
            }
        }
        value_reader_ =
            dynamic_cast<const index::IScalarValueReader<IndexValueType>*>(reader);
        AssertInfo(value_reader_ != nullptr,
                   "selected group-by index for field {} has no value reader",
                   field_id_.get());
        return true;
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
              std::vector<size_t>& topk_per_nq_prefix_sum,
              std::vector<int32_t>* element_indices = nullptr,
              SearchResult* search_result = nullptr);

bool
TryStrictGroupFilteredSearch(
    milvus::OpContext* op_ctx,
    const std::vector<std::shared_ptr<VectorIterator>>& iterators,
    const SearchInfo& info,
    const segcore::SegmentInternalInterface& segment,
    SearchResult* result,
    std::vector<CompositeGroupKey>& groups,
    std::vector<int64_t>& offsets,
    std::vector<float>& distances,
    std::vector<size_t>& prefix);

}  // namespace exec
}  // namespace milvus

#undef JSON_TYPE_CASE
#undef JSON_STRING_CASE
#undef JSON_TYPE_CASES
