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

#include "index/vector/RangeSearchParams.h"

#include "common/RangeSearchHelper.h"
#include "index/Meta.h"
#include "index/Utils.h"

namespace milvus::index {

bool
CheckAndUpdateKnowhereRangeSearchParam(const VectorSearchParams& params,
                                       int64_t topk,
                                       const MetricType& metric_type,
                                       knowhere::Json& search_config) {
    const auto radius =
        GetValueFromConfig<float>(params.search_params_, RADIUS);
    if (!radius.has_value()) {
        return false;
    }

    search_config[RADIUS] = radius.value();
    // range_search_k only controls iterator early termination; it does not
    // guarantee the exact number of returned results. A value of -1 retains all
    // results in the requested range.
    search_config[knowhere::meta::RANGE_SEARCH_K] = topk;

    const auto range_filter =
        GetValueFromConfig<float>(params.search_params_, RANGE_FILTER);
    if (range_filter.has_value()) {
        search_config[RANGE_FILTER] = range_filter.value();
        CheckRangeSearchParam(
            search_config[RADIUS], search_config[RANGE_FILTER], metric_type);
    }

    const auto page_retain_order =
        GetValueFromConfig<bool>(params.search_params_, PAGE_RETAIN_ORDER);
    if (page_retain_order.has_value()) {
        search_config[knowhere::meta::RETAIN_ITERATOR_ORDER] =
            page_retain_order.value();
    }
    return true;
}

}  // namespace milvus::index
