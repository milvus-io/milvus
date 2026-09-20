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

#include "index/vector/VectorLoadResource.h"

#include <algorithm>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/Utils.h"
#include "index/LoadResource.h"
#include "index/ResourceUsageUtils.h"
#include "index/Meta.h"
#include "index/vector/VectorIndexValidDataUtils.h"
#include "index/vector/VectorLoadParamUtils.h"
#include "index/vector/VectorTypeUtils.h"
#include "knowhere/comp/knowhere_check.h"
#include "knowhere/emb_list_utils.h"
#include "knowhere/expected.h"
#include "knowhere/index/index_static.h"
#include "knowhere/operands.h"
#include "knowhere/utils.h"
#include "log/Log.h"

namespace milvus::index {

namespace {

using detail::SaturatingAdd;
using detail::SaturatingMul;

uint64_t
IdMapMmapDiskCost(const Config& config, int64_t num_rows) {
    if (num_rows <= 0) {
        return 0;
    }
    IdMapMmapFlags mmap_flags;
    mmap_flags.enable_o2i = vector_load_params::ReadLenientIdMapMmapFlag(
        config, ENABLE_MMAP_O2I_MAP);
    mmap_flags.enable_i2o = vector_load_params::ReadLenientIdMapMmapFlag(
        config, ENABLE_MMAP_I2O_MAP);
    return mmap_flags.Any()
               ? SaturatingMul(static_cast<uint64_t>(num_rows), sizeof(int32_t))
               : 0;
}

template <typename T>
void
EstimateVector(const IndexType& index_type,
               IndexVersion index_version,
               uint64_t index_size,
               int64_t num_rows,
               int64_t dim,
               const Config& config,
               knowhere::expected<knowhere::Resource>& resource,
               bool& has_raw_data) {
    resource = knowhere::IndexStaticFaced<T>::EstimateLoadResource(
        index_type, index_version, index_size, num_rows, dim, config);
    if (resource.has_value()) {
        has_raw_data = knowhere::IndexStaticFaced<T>::HasRawData(
            index_type, index_version, config);
    }
}

// One mapper owns every knowhere status in the tree (#50768 T2): a local copy
// drifts, and this one did -- it routed invalid_metric_type to
// MetricTypeInvalid and knowhere's whole input_error category to ConfigInvalid,
// neither of which is what the audited table says, and its `default:` arm
// suppressed the -Wswitch drift guard that makes a newly added knowhere status
// a compile error rather than a silent fallback.
[[noreturn]] void
ThrowVectorEstimateError(
    const knowhere::expected<knowhere::Resource>& resource) {
    const auto status = resource.error();
    ThrowInfo(KnowhereStatusToErrorCode(status),
              "knowhere load-resource estimate failed: status {} ({}), "
              "detail: {}",
              static_cast<int>(status),
              knowhere::Status2String(status),
              resource.what());
}

}  // namespace

bool
VectorUsesDiskLoad(const IndexType& index_type, IndexVersion index_version) {
    return knowhere::UseDiskLoad(index_type, index_version);
}

LoadResourceRequest
VecIndexLoadResource(DataType field_type,
                     DataType element_type,
                     IndexVersion index_version,
                     uint64_t index_size_in_bytes,
                     const std::map<std::string, std::string>& index_params,
                     bool mmap_enable,
                     int64_t num_rows,
                     int64_t dim) {
    auto config = ParseConfigFromIndexParams(index_params);
    auto type = index_params.find(INDEX_TYPE);
    AssertInfo(type != index_params.end(), "index type is empty");
    const auto& index_type = type->second;

    bool mmaped = false;
    if (mmap_enable &&
        knowhere::KnowhereCheck::SupportMmapIndexTypeCheck(index_type)) {
        config[ENABLE_MMAP] = true;
        mmaped = true;
    }

    knowhere::expected<knowhere::Resource> resource;
    bool has_raw_data = false;
    auto estimate = [&](DataType type) {
        // Sparse implementations must be looked up in knowhere's
        // sparse_u32_f32 static registry (#53106), not the fp32 registry.
        return DispatchPhysicalVectorDataType(
            type,
            [&]<typename T>() {
                EstimateVector<T>(index_type,
                                  index_version,
                                  index_size_in_bytes,
                                  num_rows,
                                  dim,
                                  config,
                                  resource,
                                  has_raw_data);
                return true;
            },
            [] { return false; });
    };

    bool valid_type = false;
    if (field_type == DataType::VECTOR_ARRAY) {
        valid_type = estimate(element_type);
        auto metric = GetMetricTypeFromConfig(config);
        if (!knowhere::get_el_metric_type(metric).has_value()) {
            has_raw_data = false;
        }
    } else {
        valid_type = estimate(field_type);
    }
    if (!valid_type) {
        LOG_ERROR(
            "invalid data type to estimate index load resource: field_type "
            "{}, element_type {}",
            field_type,
            element_type);
        return LoadResourceRequest{0, 0, 0, 0, true};
    }
    if (!resource.has_value()) {
        ThrowVectorEstimateError(resource);
    }

    const auto& estimated = resource.value();
    LoadResourceRequest request{};
    request.has_raw_data = CanUseIndexRawDataForField(field_type, has_raw_data);
    request.final_disk_cost = estimated.diskCost;
    request.final_memory_cost = estimated.memoryCost;
    if (VectorUsesDiskLoad(index_type, index_version) || mmaped) {
        request.max_disk_cost = estimated.diskCost;
        request.max_memory_cost = std::max<uint64_t>(
            estimated.memoryCost, DEFAULT_FIELD_MAX_MEMORY_LIMIT);
    } else {
        request.max_memory_cost = SaturatingMul(2, estimated.memoryCost);
    }
    if (VectorUsesDiskLoad(index_type, index_version)) {
        auto offset_cost = IdMapMmapDiskCost(config, num_rows);
        request.final_disk_cost =
            SaturatingAdd(request.final_disk_cost, offset_cost);
        request.max_disk_cost =
            SaturatingAdd(request.max_disk_cost, offset_cost);
    }
    return request;
}

}  // namespace milvus::index
