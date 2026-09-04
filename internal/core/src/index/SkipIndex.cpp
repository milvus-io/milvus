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
SkipIndex::GetFieldChunkMetrics(milvus::FieldId field_id,
                                int64_t chunk_id) const {
    // skip index structure must be setup before using, thus we do not lock here.
    auto source = fieldMetricSources_.find(field_id);
    if (source != fieldMetricSources_.end() && source->second != nullptr) {
        if (auto metrics = source->second->GetSkipMetrics(chunk_id);
            metrics != nullptr) {
            return metrics;
        }
    }
    return &defaultFieldChunkMetrics;
}

}  // namespace milvus
