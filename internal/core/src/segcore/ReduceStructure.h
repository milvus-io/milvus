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

#include <limits>
#include <utility>
#include <optional>
#include <vector>

#include "common/Consts.h"
#include "common/Types.h"
#include "common/QueryResult.h"
#include "plan/PlanNode.h"

using milvus::SearchResult;

// Forward declarations
int CompareGroupByValue(const milvus::GroupByValueType& lhs,
                       const milvus::GroupByValueType& rhs);
int CompareOrderByValue(const milvus::OrderByValueType& lhs,
                       const milvus::OrderByValueType& rhs);

struct SearchResultPair {
    milvus::PkType primary_key_;
    float distance_;
    milvus::SearchResult* search_result_;
    int64_t segment_index_;
    int64_t offset_;
    int64_t offset_rb_;                                       // right bound
    std::optional<milvus::GroupByValueType> group_by_value_;  //for group_by
    std::optional<std::vector<milvus::OrderByValueType>>
        order_by_values_;  // for order_by (multiple fields)

    SearchResultPair(milvus::PkType primary_key,
                     float distance,
                     SearchResult* result,
                     int64_t index,
                     int64_t lb,
                     int64_t rb)
        : SearchResultPair(
              primary_key, distance, result, index, lb, rb, std::nullopt) {
    }

    SearchResultPair(milvus::PkType primary_key,
                     float distance,
                     SearchResult* result,
                     int64_t index,
                     int64_t lb,
                     int64_t rb,
                     std::optional<milvus::GroupByValueType> group_by_value)
        : primary_key_(std::move(primary_key)),
          distance_(distance),
          search_result_(result),
          segment_index_(index),
          offset_(lb),
          offset_rb_(rb),
          group_by_value_(group_by_value) {
    }

    bool
    operator>(const SearchResultPair& other) const {
        if (std::fabs(distance_ - other.distance_) < EPSILON) {
            return primary_key_ < other.primary_key_;
        }
        return distance_ > other.distance_;
    }

    void
    advance() {
        offset_++;
        if (offset_ < offset_rb_) {
            primary_key_ = search_result_->primary_keys_.at(offset_);
            distance_ = search_result_->distances_.at(offset_);
            if (search_result_->group_by_values_.has_value() &&
                offset_ < search_result_->group_by_values_.value().size()) {
                group_by_value_ =
                    search_result_->group_by_values_.value().at(offset_);
            }
            // Note: order_by_values_ should be updated by the caller
            // (OrderByReduceHelper) when needed
        } else {
            primary_key_ = INVALID_PK;
            distance_ = std::numeric_limits<float>::min();
            order_by_values_ = std::nullopt;
        }
    }
};

struct SearchResultPairComparator {
    std::optional<std::vector<milvus::plan::OrderByField>>
        order_by_fields_;
    bool has_order_by_;

    SearchResultPairComparator()
        : has_order_by_(false) {
    }

    explicit SearchResultPairComparator(
        const std::optional<std::vector<milvus::plan::OrderByField>>&
            order_by_fields)
        : order_by_fields_(order_by_fields),
          has_order_by_(order_by_fields.has_value() &&
                        !order_by_fields.value().empty()) {
    }

    bool
    operator()(const SearchResultPair* lhs, const SearchResultPair* rhs) const {
        if (has_order_by_ && lhs->order_by_values_.has_value() &&
            rhs->order_by_values_.has_value()) {
            // Compare by order_by fields
            const auto& lhs_vals = lhs->order_by_values_.value();
            const auto& rhs_vals = rhs->order_by_values_.value();
            const auto& fields = order_by_fields_.value();

            for (size_t i = 0; i < fields.size() && i < lhs_vals.size() &&
                               i < rhs_vals.size();
                 ++i) {
                const auto& field = fields[i];
                const auto& lhs_val = lhs_vals[i];
                const auto& rhs_val = rhs_vals[i];

                // Handle null values
                if (!lhs_val.has_value() && !rhs_val.has_value()) {
                    continue;  // Both null, compare next field
                }
                if (!lhs_val.has_value()) {
                    return field.ascending_;  // null < non-null
                }
                if (!rhs_val.has_value()) {
                    return !field.ascending_;  // non-null > null
                }

                // Compare values using OrderByValueType comparison
                int cmp = CompareOrderByValue(lhs_val, rhs_val);
                if (cmp < 0) {
                    return field.ascending_;
                }
                if (cmp > 0) {
                    return !field.ascending_;
                }
                // Equal, continue to next field
            }
            // All order_by fields equal, use distance as tie-breaker
            if (std::fabs(lhs->distance_ - rhs->distance_) < EPSILON) {
                return lhs->primary_key_ < rhs->primary_key_;
            }
            return lhs->distance_ > rhs->distance_;
        } else {
            // Original logic: compare by distance
            return *rhs > *lhs;
        }
    }

 private:
    static int
    CompareGroupByValue(const milvus::GroupByValueType& lhs,
                       const milvus::GroupByValueType& rhs);
};
