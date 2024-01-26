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
#include "common/Span.h"

namespace milvus {
namespace query {

template <typename T>
struct DataGetter {
    std::shared_ptr<Span<T>> field_data_;
    std::shared_ptr<Span<std::string_view>> str_field_data_;
    const index::ScalarIndex<T>* field_index_;

    DataGetter(const segcore::SegmentInternalInterface& segment,
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

 public:
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
    FieldId field_id,
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
    FieldId field_id,
    int64_t topK,
    const DataGetter<T>& data_getter,
    std::vector<GroupByValueType>& group_by_values,
    std::vector<int64_t>& offsets,
    std::vector<float>& distances,
    const knowhere::MetricType& metrics_type);

}  // namespace query
}  // namespace milvus
