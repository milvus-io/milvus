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

#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/Types.h"
#include "index/FieldChunkMetricsProvider.h"
#include "index/skipindex_stats/SkipIndexStats.h"

namespace milvus {
class SkipIndex {
 private:
    template <typename T>
    struct IsAllowedType {
        static constexpr bool isAllowedType =
            std::is_integral<T>::value || std::is_floating_point<T>::value ||
            std::is_same<T, std::string>::value ||
            std::is_same<T, std::string_view>::value;
        static constexpr bool isDisabledType =
            std::is_same<T, milvus::Json>::value ||
            std::is_same<T, bool>::value;
        static constexpr bool value = isAllowedType && !isDisabledType;
        static constexpr bool in_value = isAllowedType;
    };

 public:
    SkipIndex() = default;

    std::shared_ptr<SkipIndex>
    Clone() const {
        auto cloned = std::make_shared<SkipIndex>();
        std::shared_lock lck(mutex_);
        cloned->fieldMetricSources_ = fieldMetricSources_;
        return cloned;
    }

    void
    Erase(FieldId field_id) {
        std::unique_lock lck(mutex_);
        fieldMetricSources_.erase(field_id);
    }

    // Bind a field to the column generation that owns its per-chunk metrics.
    // Storage V2 footer metrics live in the same generation as the
    // row-group/cell layout they describe, so keeping the source itself as the
    // provider prevents the two from being replaced independently. Rebinding a
    // field (a replaced column) drops the previous generation's source, and a
    // source with no metrics simply fails open. A Vortex/Parquet/Milvus-native
    // stats source plugs in here by implementing FieldChunkMetricsProvider.
    void
    LoadSkipSource(FieldId field_id,
                   std::shared_ptr<FieldChunkMetricsProvider> source) {
        std::unique_lock lck(mutex_);
        fieldMetricSources_.insert_or_assign(field_id, std::move(source));
    }

    template <typename T>
    std::enable_if_t<SkipIndex::IsAllowedType<T>::value, bool>
    CanSkipUnaryRange(FieldId field_id,
                      int64_t chunk_id,
                      OpType op_type,
                      const T& val) const {
        const auto* metrics = GetFieldChunkMetrics(field_id, chunk_id);
        return Decide(metrics,
                      metrics->CanSkipUnaryRange(op_type, index::Metrics{val}));
    }

    template <typename T>
    std::enable_if_t<!SkipIndex::IsAllowedType<T>::value, bool>
    CanSkipUnaryRange(FieldId field_id,
                      int64_t chunk_id,
                      OpType op_type,
                      const T& val) const {
        return false;
    }

    template <typename T>
    std::enable_if_t<SkipIndex::IsAllowedType<T>::value, bool>
    CanSkipBinaryRange(FieldId field_id,
                       int64_t chunk_id,
                       const T& lower_val,
                       const T& upper_val,
                       bool lower_inclusive,
                       bool upper_inclusive) const {
        const auto* metrics = GetFieldChunkMetrics(field_id, chunk_id);
        return Decide(metrics,
                      metrics->CanSkipBinaryRange(index::Metrics{lower_val},
                                                  index::Metrics{upper_val},
                                                  lower_inclusive,
                                                  upper_inclusive));
    }

    template <typename T>
    std::enable_if_t<!SkipIndex::IsAllowedType<T>::value, bool>
    CanSkipBinaryRange(FieldId field_id,
                       int64_t chunk_id,
                       const T& lower_val,
                       const T& upper_val,
                       bool lower_inclusive,
                       bool upper_inclusive) const {
        return false;
    }

    template <typename T>
    std::enable_if_t<SkipIndex::IsAllowedType<T>::in_value, bool>
    CanSkipInQuery(FieldId field_id,
                   int64_t chunk_id,
                   const std::vector<T>& values) const {
        const auto* metrics = GetFieldChunkMetrics(field_id, chunk_id);
        auto vals = std::vector<index::Metrics>{};
        vals.reserve(values.size());
        for (const auto& v : values) {
            vals.emplace_back(v);
        }
        return Decide(metrics, metrics->CanSkipIn(vals));
    }

    template <typename T>
    std::enable_if_t<!SkipIndex::IsAllowedType<T>::in_value, bool>
    CanSkipInQuery(FieldId field_id,
                   int64_t chunk_id,
                   const std::vector<T>& values) const {
        return false;
    }

 private:
    // A chunk whose rows are all NULL matches no predicate this index serves,
    // so it is skippable even when the bounds themselves say nothing.
    static bool
    Decide(const index::FieldChunkMetrics* metrics, bool can_skip) {
        return can_skip || metrics->GetNullState() ==
                               index::FieldChunkMetrics::NullState::AllNulls;
    }

    const index::FieldChunkMetrics*
    GetFieldChunkMetrics(FieldId field_id, int64_t chunk_id) const;

    std::unordered_map<FieldId, std::shared_ptr<FieldChunkMetricsProvider>>
        fieldMetricSources_;
    mutable std::shared_mutex mutex_;
};
}  // namespace milvus
