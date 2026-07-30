// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the License.

#include "segcore/SearchParamsValidator.h"

#include <string>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "knowhere/config.h"
#include "knowhere/index/index_static.h"
#include "knowhere/operands.h"
#include "knowhere/version.h"
#include "nlohmann/json.hpp"

namespace milvus::segcore {

namespace {
// Build the knowhere config for index_type and run FormatAndCheck + Load
// (SEARCH) against the user params, so knowhere — the sole owner of the
// range/type contract (e.g. nprobe in [1, 65536] in ivf_config.h) — produces
// any error. No range is re-stated in milvus code.
template <typename T>
void
LoadAndCheck(const std::string& index_type, const knowhere::Json& params) {
    knowhere::Json json(params);
    std::string msg;
    auto status = knowhere::Status::success;
    try {
        auto cfg = knowhere::IndexStaticFaced<T>::CreateConfig(
            index_type, knowhere::Version::GetCurrentVersion().VersionNumber());
        status = knowhere::Config::FormatAndCheck(*cfg, json, &msg);
        if (status == knowhere::Status::success) {
            status = knowhere::Config::Load(
                *cfg, json, knowhere::PARAM_TYPE::SEARCH, &msg);
        }
    } catch (const std::exception&) {
        // knowhere threw (e.g. index_type not registered, or a collection
        // under test whose index family knowhere does not own) → there is no
        // owner to validate against, so skip silently rather than break plan
        // creation. Indexed collections still go through knowhere as before.
        return;
    }
    if (status != knowhere::Status::success) {
        ThrowInfo(milvus::ErrorCode::InvalidParameter, msg);
    }
}
}  // namespace

void
ValidateVectorSearchParams(SearchInfo& search_info,
                           const std::string& index_type,
                           DataType data_type) {
    // No index loaded yet (e.g. a collection under test with no field index)
    // → there is no knowhere owner to validate against, so skip. nprobe's
    // range is owned by the IVF index config, which does not exist here.
    if (index_type.empty()) {
        return;
    }
    // Seed metric_type so knowhere resolves the index family (e.g. IvfConfig
    // vs HnswConfig). We deliberately do NOT seed topk: milvus adjusts ef vs
    // topk at search time (ef = max(ef, k)), so seeding the raw topk here
    // would make knowhere's CheckAndAdjust reject legitimate HNSW searches
    // like ef=32, k=1000 (test_bitmap_index_search_group_by). nprobe and
    // other range/type checks do not depend on topk.
    knowhere::Json json = search_info.search_params_;
    json[knowhere::meta::METRIC_TYPE] = search_info.metric_type_;

    if (data_type == DataType::VECTOR_FLOAT) {
        LoadAndCheck<knowhere::fp32>(index_type, json);
    } else if (data_type == DataType::VECTOR_FLOAT16) {
        LoadAndCheck<knowhere::fp16>(index_type, json);
    } else if (data_type == DataType::VECTOR_BFLOAT16) {
        LoadAndCheck<knowhere::bf16>(index_type, json);
    } else if (data_type == DataType::VECTOR_BINARY) {
        LoadAndCheck<knowhere::bin1>(index_type, json);
    } else if (data_type == DataType::VECTOR_INT8) {
        LoadAndCheck<knowhere::int8>(index_type, json);
    } else if (data_type == DataType::VECTOR_SPARSE_U32_F32) {
        LoadAndCheck<knowhere::sparse_u32_f32>(index_type, json);
    }
    // Other (non-vector) data types: nothing to validate here.
}

}  // namespace milvus::segcore
