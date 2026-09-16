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

#include "SkipIndex.h"

namespace milvus {

static const index::NoneFieldChunkMetrics defaultFieldChunkMetrics{};

const index::FieldChunkMetrics*
FieldSkipMetricsView::MetricsAt(int64_t chunk_id) const {
    if (list_ != nullptr) {
        if (chunk_id >= 0 && static_cast<size_t>(chunk_id) < list_->size()) {
            if (const auto* metrics = (*list_)[chunk_id].get();
                metrics != nullptr) {
                return metrics;
            }
        }
        return &defaultFieldChunkMetrics;
    }
    if (per_chunk_fallback_ && owner_ != nullptr) {
        if (const auto* metrics = owner_->GetSkipMetrics(chunk_id);
            metrics != nullptr) {
            return metrics;
        }
    }
    return &defaultFieldChunkMetrics;
}

FieldSkipMetricsView
SkipIndex::ResolveField(FieldId field_id) const {
    std::shared_lock lck(mutex_);
    // Copy the provider while holding the binding map's read lock.
    // Sealed segment expressions resolve directly from their field columns
    // through GetFieldSkipMetrics and do not use this map.
    auto source = fieldMetricSources_.find(field_id);
    if (source == fieldMetricSources_.end() || source->second == nullptr) {
        return {};
    }
    return FieldSkipMetricsView::FromProvider(source->second);
}

}  // namespace milvus
