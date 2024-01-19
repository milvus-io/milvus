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
#include "knowhere/index_node.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealedImpl.h"
#include "segcore/ConcurrentVector.h"
#include "common/Span.h"

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
        growing_raw_data_ =
            segment.get_insert_record().get_field_data<T>(fieldId);
    }

    GrowingDataGetter(const GrowingDataGetter<T>& other)
        : growing_raw_data_(other.growing_raw_data_) {
    }

    T
    Get(int64_t idx) const {
        const T& val_ref = growing_raw_data_->operator[](idx);
        T val = val_ref;
        return val;
    }
};

template <typename T>
class SealedDataGetter : public DataGetter<T> {
 private:
    std::shared_ptr<Span<T>> field_data_;
    std::shared_ptr<Span<std::string_view>> str_field_data_;
    const index::ScalarIndex<T>* field_index_;

 public:
    SealedDataGetter(const segcore::SegmentSealedImpl& segment,
                     FieldId& field_id) {
        if (segment.HasFieldData(field_id)) {
            if constexpr (std::is_same_v<T, std::string>) {
                auto span = segment.chunk_data<std::string_view>(field_id, 0);
                str_field_data_ = std::make_shared<Span<std::string_view>>(
                    span.data(), span.row_count());
            } else {
                auto span = segment.chunk_data<T>(field_id, 0);
                field_data_ =
                    std::make_shared<Span<T>>(span.data(), span.row_count());
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

void
GroupBy(const std::vector<std::shared_ptr<knowhere::IndexNode::iterator>>&
            iterators,
        const SearchInfo& searchInfo,
        std::vector<GroupByValueType>& group_by_values,
        const segcore::SegmentInternalInterface& segment,
        std::vector<int64_t>& seg_offsets,
        std::vector<float>& distances);

template <typename T>
void
GroupIteratorsByType(
    const std::vector<std::shared_ptr<knowhere::IndexNode::iterator>>&
        iterators,
    int64_t topK,
    const DataGetter<T>& data_getter,
    std::vector<GroupByValueType>& group_by_values,
    std::vector<int64_t>& seg_offsets,
    std::vector<float>& distances,
    const knowhere::MetricType& metrics_type);

template <typename T>
void
GroupIteratorResult(
    const std::shared_ptr<knowhere::IndexNode::iterator>& iterator,
    int64_t topK,
    const DataGetter<T>& data_getter,
    std::vector<GroupByValueType>& group_by_values,
    std::vector<int64_t>& offsets,
    std::vector<float>& distances,
    const knowhere::MetricType& metrics_type);

}  // namespace query
}  // namespace milvus
