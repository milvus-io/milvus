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

#include <memory>

#include "common/Tracer.h"
#include "common/Types.h"
#include "knowhere/config.h"

namespace milvus {

struct SearchIteratorV2Info {
    std::string token = "";
    uint32_t batch_size = 0;
    std::optional<float> last_bound = std::nullopt;
};

struct SearchInfo {
    int64_t topk_{0};
    int64_t group_size_{1};
    bool strict_group_size_{false};
    int64_t round_decimal_{0};
    FieldId field_id_;
    MetricType metric_type_;
    knowhere::Json search_params_;
    std::optional<FieldId> group_by_field_id_;
    tracer::TraceContext trace_ctx_;
    bool materialized_view_involved = false;
    bool iterative_filter_execution = false;
    std::optional<SearchIteratorV2Info> iterator_v2_info_ = std::nullopt;
};

using SearchInfoPtr = std::shared_ptr<SearchInfo>;

}  // namespace milvus
