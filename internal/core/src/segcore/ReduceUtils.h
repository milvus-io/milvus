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

#include "pb/schema.pb.h"
#include "common/Types.h"
#include "query/PlanImpl.h"
#include "plan/PlanNode.h"
#include "segcore/SegmentInterface.h"
#include <functional>
#include <unordered_map>

namespace milvus::segcore {

// Helper template to convert typed value to OrderByValueType
template <typename T>
inline OrderByValueType
MakeOrderByValue(const std::optional<T>& val) {
    if (!val.has_value()) {
        return std::nullopt;
    }
    return OrderByValueType(std::make_optional(OrderByVariant(val.value())));
}

// Overload for direct value (not wrapped in optional)
template <typename T>
inline OrderByValueType
MakeOrderByValue(const T& val) {
    return OrderByValueType(std::make_optional(OrderByVariant(val)));
}

// Compare two OrderByValueType values
// Returns: -1 if lhs < rhs, 0 if lhs == rhs, 1 if lhs > rhs
// Handles null values (null < non-null) and NaN for float/double
int
CompareOrderByValue(const OrderByValueType& lhs, const OrderByValueType& rhs);

// Helper function to read a single order_by field value from a segment
// This consolidates the type-switch logic that was duplicated across multiple files
OrderByValueType
ReadOrderByFieldValue(OpContext* op_ctx,
                      const SegmentInternalInterface& segment,
                      const plan::OrderByField& field,
                      int64_t seg_offset);

// Helper function to read all order_by field values for a single row
std::vector<OrderByValueType>
ReadAllOrderByFieldValues(
    OpContext* op_ctx,
    const SegmentInternalInterface& segment,
    const std::vector<plan::OrderByField>& order_by_fields,
    int64_t seg_offset);

// Type alias for a getter function that reads order_by value at a given offset
using OrderByGetterFunc = std::function<OrderByValueType(int64_t)>;

// OrderByFieldReader: Caches DataGetters per segment for efficient repeated reads
// This avoids creating DataGetter objects on every call, which is expensive in hot paths
class OrderByFieldReader {
 public:
    explicit OrderByFieldReader(
        const std::vector<plan::OrderByField>& order_by_fields)
        : order_by_fields_(order_by_fields) {
    }

    // Read all order_by field values for a single row
    // Caches getters per segment for efficiency
    std::vector<OrderByValueType>
    Read(const SegmentInternalInterface& segment, int64_t seg_offset);

    // Check if there are any order_by fields
    bool
    Empty() const {
        return order_by_fields_.empty();
    }

    // Get the number of order_by fields
    size_t
    Size() const {
        return order_by_fields_.size();
    }

 private:
    // Get or create cached getters for a segment
    const std::vector<OrderByGetterFunc>&
    GetOrCreateGetters(const SegmentInternalInterface& segment);

    // Create getters for all order_by fields for a segment
    std::vector<OrderByGetterFunc>
    CreateGetters(const SegmentInternalInterface& segment);

    const std::vector<plan::OrderByField>& order_by_fields_;

    // Cache of getters per segment (keyed by segment pointer)
    std::unordered_map<const void*, std::vector<OrderByGetterFunc>>
        segment_getters_cache_;

    // OpContext for creating getters (reused across calls)
    OpContext op_ctx_;
};

void
AssembleGroupByValues(
    std::unique_ptr<milvus::proto::schema::SearchResultData>& search_result,
    const std::vector<GroupByValueType>& group_by_vals,
    milvus::query::Plan* plan);

void
AssembleOrderByValues(
    std::unique_ptr<milvus::proto::schema::SearchResultData>& search_result,
    const std::vector<std::vector<OrderByValueType>>& order_by_vals_list,
    milvus::query::Plan* plan);

}  // namespace milvus::segcore