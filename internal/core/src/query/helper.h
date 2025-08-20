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
#include "common/Types.h"

namespace milvus::query {
namespace dataset {
struct RawDataset {
    int64_t begin_id = 0;
    int64_t dim;
    int64_t num_raw_data;
    const void* raw_data;
    const size_t* raw_data_lims = nullptr;
};
struct SearchDataset {
    knowhere::MetricType metric_type;
    int64_t num_queries;
    int64_t topk;
    int64_t round_decimal;
    int64_t dim;
    const void* query_data;
    // used for embedding list query
    const size_t* query_lims = nullptr;
};

}  // namespace dataset
}  // namespace milvus::query
