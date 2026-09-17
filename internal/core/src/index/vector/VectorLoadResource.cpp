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
#include "index/LoadResource.h"
#include "index/ResourceUsageUtils.h"
#include "index/Meta.h"
#include "index/Utils.h"
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
OffsetMappingMmapDiskCost(const Config& config, int64_t num_rows) {
    if (num_rows <= 0) {
        return 0;
    }
    auto mmap_o2i =
        GetValueFromConfig<bool>(config, ENABLE_MMAP_O2I_MAP).value_or(false);
    auto mmap_i2o =
        GetValueFromConfig<bool>(config, ENABLE_MMAP_I2O_MAP).value_or(false);
    return mmap_o2i || mmap_i2o
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

[[noreturn]] void
ThrowVectorEstimateError(
    const knowhere::expected<knowhere::Resource>& resource) {
    const auto status = resource.error();
    auto error_code = ErrorCode::KnowhereError;
    switch (status) {
        case knowhere::Status::invalid_metric_type:
            error_code = ErrorCode::MetricTypeInvalid;
            break;
        case knowhere::Status::malloc_error:
            error_code = ErrorCode::MemAllocateFailed;
            break;
        case knowhere::Status::disk_file_error:
            error_code = ErrorCode::FileReadFailed;
            break;
        case knowhere::Status::not_implemented:
        case knowhere::Status::invalid_instruction_set:
            error_code = ErrorCode::Unsupported;
            break;
        case knowhere::Status::invalid_serialized_index_type:
            error_code = ErrorCode::DataFormatBroken;
            break;
        default:
            switch (knowhere::StatusCategoryOf(status)) {
                case knowhere::StatusCategory::input_error:
                    error_code = ErrorCode::ConfigInvalid;
                    break;
                case knowhere::StatusCategory::transient_error:
                    error_code = ErrorCode::StorageTransientError;
                    break;
                case knowhere::StatusCategory::success:
                case knowhere::StatusCategory::permanent_error:
                    error_code = ErrorCode::KnowhereError;
                    break;
            }
            break;
    }
    ThrowInfo(error_code,
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
        switch (type) {
            case DataType::VECTOR_BINARY:
                EstimateVector<knowhere::bin1>(index_type,
                                               index_version,
                                               index_size_in_bytes,
                                               num_rows,
                                               dim,
                                               config,
                                               resource,
                                               has_raw_data);
                return true;
            case DataType::VECTOR_FLOAT:
                EstimateVector<knowhere::fp32>(index_type,
                                               index_version,
                                               index_size_in_bytes,
                                               num_rows,
                                               dim,
                                               config,
                                               resource,
                                               has_raw_data);
                return true;
            case DataType::VECTOR_FLOAT16:
                EstimateVector<knowhere::fp16>(index_type,
                                               index_version,
                                               index_size_in_bytes,
                                               num_rows,
                                               dim,
                                               config,
                                               resource,
                                               has_raw_data);
                return true;
            case DataType::VECTOR_BFLOAT16:
                EstimateVector<knowhere::bf16>(index_type,
                                               index_version,
                                               index_size_in_bytes,
                                               num_rows,
                                               dim,
                                               config,
                                               resource,
                                               has_raw_data);
                return true;
            case DataType::VECTOR_SPARSE_U32_F32:
                EstimateVector<knowhere::sparse_u32_f32>(index_type,
                                                         index_version,
                                                         index_size_in_bytes,
                                                         num_rows,
                                                         dim,
                                                         config,
                                                         resource,
                                                         has_raw_data);
                if (resource.has_value()) {
                    // Preserve the existing factory's raw-data query for
                    // sparse indexes; changing it belongs to a separate
                    // behavior change.
                    has_raw_data =
                        knowhere::IndexStaticFaced<knowhere::fp32>::HasRawData(
                            index_type, index_version, config);
                }
                return true;
            case DataType::VECTOR_INT8:
                EstimateVector<knowhere::int8>(index_type,
                                               index_version,
                                               index_size_in_bytes,
                                               num_rows,
                                               dim,
                                               config,
                                               resource,
                                               has_raw_data);
                return true;
            default:
                return false;
        }
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
        auto offset_cost = OffsetMappingMmapDiskCost(config, num_rows);
        request.final_disk_cost =
            SaturatingAdd(request.final_disk_cost, offset_cost);
        request.max_disk_cost =
            SaturatingAdd(request.max_disk_cost, offset_cost);
    }
    return request;
}

}  // namespace milvus::index
