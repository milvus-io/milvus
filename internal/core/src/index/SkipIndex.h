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
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/Types.h"
#include "index/FieldChunkMetricsProvider.h"
#include "index/skipindex_stats/SkipIndexStats.h"

namespace milvus {

namespace skipindex_detail {
template <typename T>
struct IsAllowedType {
    static constexpr bool isAllowedType =
        std::is_integral<T>::value || std::is_floating_point<T>::value ||
        std::is_same<T, std::string>::value ||
        std::is_same<T, std::string_view>::value;
    static constexpr bool isDisabledType =
        std::is_same<T, milvus::Json>::value || std::is_same<T, bool>::value;
    static constexpr bool value = isAllowedType && !isDisabledType;
    static constexpr bool in_value = isAllowedType;
};
}  // namespace skipindex_detail

// One field's skip metrics, resolved once per expression.
//
// Sealed segments resolve the column from runtime.fields, then FromProvider
// resolves its metrics list once. Every cell-level CanSkip*() afterwards is a
// bounds check plus an index into the list. The view keeps that column
// generation alive, so its metrics stay valid even if the field is replaced
// or dropped. Callers must bind chunk layout and data to the same generation;
// the production request read lease protects this binding during execution.
//
// A provider that does not expose lists (FieldChunkMetricsProvider's original
// per-chunk contract) is still honoured: the view then calls
// GetSkipMetrics(chunk_id) per cell, exactly as before the view existed. A
// provider that exposes lists but has none for this field never prunes and is
// never called back, so the common no-metrics case costs nothing per cell. A
// default constructed view is unbound and never skips.
class FieldSkipMetricsView {
 public:
    FieldSkipMetricsView() = default;
    FieldSkipMetricsView(std::shared_ptr<FieldChunkMetricsProvider> owner,
                         const SkipMetricsList* list,
                         bool per_chunk_fallback)
        : owner_(std::move(owner)),
          list_(list),
          per_chunk_fallback_(per_chunk_fallback) {
    }

    // Build a view directly from the column generation that owns the metrics.
    // Segment state uses this entry point instead of maintaining a second
    // field -> provider map alongside its field columns.
    static FieldSkipMetricsView
    FromProvider(std::shared_ptr<FieldChunkMetricsProvider> owner) {
        if (owner == nullptr) {
            return {};
        }
        auto list = owner->GetSkipMetricsList();
        if (!list.has_value()) {
            return {std::move(owner),
                    nullptr,
                    /*per_chunk_fallback=*/true};
        }
        return {std::move(owner), *list, /*per_chunk_fallback=*/false};
    }

    // A column may exist without any metrics. Do not bind a filter for a
    // known absent/empty list; per-chunk providers must still be consulted.
    bool
    HasMetrics() const {
        return owner_ != nullptr &&
               (per_chunk_fallback_ || (list_ != nullptr && !list_->empty()));
    }

    template <typename T>
    std::enable_if_t<skipindex_detail::IsAllowedType<T>::value, bool>
    CanSkipUnaryRange(int64_t chunk_id, OpType op_type, const T& val) const {
        const auto* metrics = MetricsAt(chunk_id);
        if (auto decision = Precheck(metrics); decision.has_value()) {
            return *decision;
        }
        return metrics->CanSkipUnaryRange(op_type, index::Metrics{val});
    }

    template <typename T>
    std::enable_if_t<!skipindex_detail::IsAllowedType<T>::value, bool>
    CanSkipUnaryRange(int64_t, OpType, const T&) const {
        return false;
    }

    template <typename T>
    std::enable_if_t<skipindex_detail::IsAllowedType<T>::value, bool>
    CanSkipBinaryRange(int64_t chunk_id,
                       const T& lower_val,
                       const T& upper_val,
                       bool lower_inclusive,
                       bool upper_inclusive) const {
        const auto* metrics = MetricsAt(chunk_id);
        if (auto decision = Precheck(metrics); decision.has_value()) {
            return *decision;
        }
        return metrics->CanSkipBinaryRange(index::Metrics{lower_val},
                                           index::Metrics{upper_val},
                                           lower_inclusive,
                                           upper_inclusive);
    }

    template <typename T>
    std::enable_if_t<!skipindex_detail::IsAllowedType<T>::value, bool>
    CanSkipBinaryRange(int64_t, const T&, const T&, bool, bool) const {
        return false;
    }

    // Hot paths prepare this vector once per expression. String entries may
    // be string_views as long as their owner outlives the calls.
    bool
    CanSkipInQuery(int64_t chunk_id,
                   const std::vector<index::Metrics>& values) const {
        const auto* metrics = MetricsAt(chunk_id);
        if (auto decision = Precheck(metrics); decision.has_value()) {
            return *decision;
        }
        return metrics->CanSkipIn(values);
    }

    template <typename T>
    std::enable_if_t<skipindex_detail::IsAllowedType<T>::in_value, bool>
    CanSkipInQuery(int64_t chunk_id, const std::vector<T>& values) const {
        const auto* metrics = MetricsAt(chunk_id);
        if (auto decision = Precheck(metrics); decision.has_value()) {
            return *decision;
        }
        auto vals = std::vector<index::Metrics>{};
        vals.reserve(values.size());
        for (const auto& v : values) {
            if constexpr (std::is_same_v<T, std::string>) {
                vals.emplace_back(std::string_view(v));
            } else {
                vals.emplace_back(v);
            }
        }
        return metrics->CanSkipIn(vals);
    }

    template <typename T>
    std::enable_if_t<!skipindex_detail::IsAllowedType<T>::in_value, bool>
    CanSkipInQuery(int64_t, const std::vector<T>&) const {
        return false;
    }

 private:
    // Resolve metadata-only decisions before preparing or inspecting query
    // values. AllNulls takes precedence over absent bounds; unknown/missing
    // statistics otherwise fail open.
    static std::optional<bool>
    Precheck(const index::FieldChunkMetrics* metrics) {
        if (metrics->GetNullState() ==
            index::FieldChunkMetrics::NullState::AllNulls) {
            return true;
        }
        if (!metrics->HasUsableStats()) {
            return false;
        }
        return std::nullopt;
    }

    // Never nullptr: an unbound field, an absent list, an out-of-range chunk
    // or a per-chunk provider returning null all resolve to the shared NONE
    // metrics, which never prune.
    const index::FieldChunkMetrics*
    MetricsAt(int64_t chunk_id) const;

    std::shared_ptr<FieldChunkMetricsProvider> owner_;
    const SkipMetricsList* list_{nullptr};
    // Set only for providers that do not expose lists at all.
    bool per_chunk_fallback_{false};
};

// Field-addressed helper retained for standalone tests and the JsonKeyStats
// callback interface. Sealed segment runtime state does not own this map.
class SkipIndex {
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

    // Resolve one of this helper's field bindings into an owning view.
    // Sealed segment expressions use GetFieldSkipMetrics instead.
    FieldSkipMetricsView
    ResolveField(FieldId field_id) const;

    // Field-addressed conveniences for callers that do not hold a view. Each
    // call resolves the field, so they are not for per-cell hot paths.
    template <typename T>
    bool
    CanSkipUnaryRange(FieldId field_id,
                      int64_t chunk_id,
                      OpType op_type,
                      const T& val) const {
        return ResolveField(field_id).CanSkipUnaryRange<T>(
            chunk_id, op_type, val);
    }

    template <typename T>
    bool
    CanSkipBinaryRange(FieldId field_id,
                       int64_t chunk_id,
                       const T& lower_val,
                       const T& upper_val,
                       bool lower_inclusive,
                       bool upper_inclusive) const {
        return ResolveField(field_id).CanSkipBinaryRange<T>(
            chunk_id, lower_val, upper_val, lower_inclusive, upper_inclusive);
    }

    bool
    CanSkipInQuery(FieldId field_id,
                   int64_t chunk_id,
                   const std::vector<index::Metrics>& values) const {
        return ResolveField(field_id).CanSkipInQuery(chunk_id, values);
    }

    template <typename T>
    bool
    CanSkipInQuery(FieldId field_id,
                   int64_t chunk_id,
                   const std::vector<T>& values) const {
        return ResolveField(field_id).CanSkipInQuery<T>(chunk_id, values);
    }

 private:
    std::unordered_map<FieldId, std::shared_ptr<FieldChunkMetricsProvider>>
        fieldMetricSources_;
    mutable std::shared_mutex mutex_;
};
}  // namespace milvus
