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

#include "index/vector/KnowhereEngine.h"

#include <limits>
#include <string>
#include <utility>

#include "common/EasyAssert.h"
#include "common/FastMem.h"
#include "knowhere/index/index_factory.h"
#include "knowhere/segcore_error_code.h"

namespace milvus::index {

namespace {

void
ValidatePhysicalTypes(DataType physical_type, DataType elem_type) {
    switch (physical_type) {
        case DataType::VECTOR_FLOAT:
        case DataType::VECTOR_BINARY:
        case DataType::VECTOR_FLOAT16:
        case DataType::VECTOR_BFLOAT16:
        case DataType::VECTOR_INT8:
        case DataType::VECTOR_SPARSE_U32_F32:
            break;
        default:
            ThrowInfo(DataTypeInvalid,
                      "invalid physical vector data type {}",
                      physical_type);
    }

    if (elem_type != DataType::NONE && elem_type != physical_type) {
        ThrowInfo(DataTypeInvalid,
                  "embedding-list element type {} disagrees with physical "
                  "vector type {}",
                  elem_type,
                  physical_type);
    }
    if (elem_type == DataType::VECTOR_SPARSE_U32_F32) {
        ThrowInfo(Unsupported,
                  "sparse vectors are not supported as embedding-list "
                  "elements");
    }
}

template <typename T>
void
ValidateMetricType(const MetricType& metric_type) {
    bool supported = false;
    if constexpr (std::is_same_v<T, bin1>) {
        supported = IsBinaryVectorMetricType(metric_type);
    } else if constexpr (std::is_same_v<T, int8>) {
        supported = IsIntVectorMetricType(metric_type);
    } else {
        supported = IsFloatVectorMetricType(metric_type);
    }
    if (!supported) {
        ThrowInfo(MetricTypeInvalid,
                  "physical vector type {} does not support metric {}",
                  PhysicalVectorDataType<T>(),
                  metric_type);
    }
}

bool
IsUnsupportedMemoryCombination(const IndexType& index_type,
                               const MetricType& metric_type) {
    if (index_type == knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT &&
        metric_type == knowhere::metric::L2) {
        return true;
    }
    if (index_type != knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX &&
        index_type != knowhere::IndexEnum::INDEX_SPARSE_WAND) {
        return false;
    }
    return metric_type == knowhere::metric::L2 ||
           metric_type == knowhere::metric::COSINE ||
           metric_type == knowhere::metric::HAMMING ||
           metric_type == knowhere::metric::JACCARD ||
           metric_type == knowhere::metric::SUBSTRUCTURE ||
           metric_type == knowhere::metric::SUPERSTRUCTURE;
}

void
ValidateVersion(IndexVersion version) {
    const auto current = knowhere::Version::GetCurrentVersion().VersionNumber();
    AssertInfo(knowhere::Version::VersionSupport(knowhere::Version(version)),
               "version not support : {} , knowhere current version {}",
               version,
               current);
}

template <typename T>
knowhere::Index<knowhere::IndexNode>
CreateTypedIndex(const IndexType& index_type,
                 const MetricType& metric_type,
                 IndexVersion version,
                 const knowhere::Object* engine_object,
                 bool reject_memory_combination) {
    ValidateMetricType<T>(metric_type);
    if (reject_memory_combination &&
        IsUnsupportedMemoryCombination(index_type, metric_type)) {
        ThrowInfo(MetricTypeInvalid,
                  "{} does not support metric {}",
                  index_type,
                  metric_type);
    }

    auto created =
        engine_object == nullptr
            ? knowhere::IndexFactory::Instance().Create<T>(index_type, version)
            : knowhere::IndexFactory::Instance().Create<T>(
                  index_type, version, *engine_object);
    if (!created.has_value()) {
        const auto status = created.error();
        const auto error_code = status == knowhere::Status::invalid_index_error
                                    ? ErrorCode::Unsupported
                                    : knowhere::ToSegcoreErrorCode(status);
        ThrowInfo(error_code,
                  "failed to create knowhere index {}: status {} ({}), "
                  "detail: {}",
                  index_type,
                  static_cast<int>(status),
                  knowhere::Status2String(status),
                  created.what());
    }
    if (created.value().Node() == nullptr) {
        // knowhere::Index<T>::Create uses new (std::nothrow), and the factory
        // returns that handle as a successful expected value without checking
        // its node. Preserve the allocation category at this source boundary.
        ThrowInfo(ErrorCode::MemAllocateFailed,
                  "failed to create knowhere index {}: allocation returned a "
                  "null node",
                  index_type);
    }
    return std::move(created.value());
}

knowhere::Index<knowhere::IndexNode>
CreateIndex(DataType physical_type,
            DataType elem_type,
            const IndexType& index_type,
            const MetricType& metric_type,
            IndexVersion version,
            const knowhere::Object* engine_object,
            bool reject_memory_combination,
            bool check_compatible) {
    ValidatePhysicalTypes(physical_type, elem_type);
    if (check_compatible) {
        ValidateVersion(version);
    }

    switch (physical_type) {
        case DataType::VECTOR_FLOAT:
            return CreateTypedIndex<float>(index_type,
                                           metric_type,
                                           version,
                                           engine_object,
                                           reject_memory_combination);
        case DataType::VECTOR_BINARY:
            return CreateTypedIndex<bin1>(index_type,
                                          metric_type,
                                          version,
                                          engine_object,
                                          reject_memory_combination);
        case DataType::VECTOR_FLOAT16:
            return CreateTypedIndex<float16>(index_type,
                                             metric_type,
                                             version,
                                             engine_object,
                                             reject_memory_combination);
        case DataType::VECTOR_BFLOAT16:
            return CreateTypedIndex<bfloat16>(index_type,
                                              metric_type,
                                              version,
                                              engine_object,
                                              reject_memory_combination);
        case DataType::VECTOR_INT8:
            return CreateTypedIndex<int8>(index_type,
                                          metric_type,
                                          version,
                                          engine_object,
                                          reject_memory_combination);
        case DataType::VECTOR_SPARSE_U32_F32:
            return CreateTypedIndex<sparse_u32_f32>(index_type,
                                                    metric_type,
                                                    version,
                                                    engine_object,
                                                    reject_memory_combination);
        default:
            ThrowInfo(DataTypeInvalid,
                      "invalid physical vector data type {}",
                      physical_type);
    }
}

knowhere::Index<knowhere::IndexNode>
CreateDataViewIndex(DataType physical_type,
                    DataType elem_type,
                    const IndexType& index_type,
                    const MetricType& metric_type,
                    IndexVersion version,
                    knowhere::ViewDataOp view_data) {
    auto view_data_pack = knowhere::Pack(std::move(view_data));
    return CreateIndex(physical_type,
                       elem_type,
                       index_type,
                       metric_type,
                       version,
                       &view_data_pack,
                       true,
                       false);
}

size_t
CheckedElementCount(int64_t count, const char* label) {
    if (count < 0 ||
        static_cast<uint64_t>(count) >
            static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        ThrowInfo(
            KnowhereError, "knowhere {} count {} is invalid", label, count);
    }
    return static_cast<size_t>(count);
}

template <typename T>
size_t
CheckedVectorRowBytes(int64_t dim) {
    const auto dimension = CheckedElementCount(dim, "vector dimension");
    if constexpr (std::is_same_v<T, bin1>) {
        if (dimension % 8 != 0) {
            ThrowInfo(KnowhereError,
                      "knowhere binary vector dimension {} is not divisible "
                      "by 8",
                      dimension);
        }
        return dimension / 8;
    } else {
        if (dimension > std::numeric_limits<size_t>::max() / sizeof(T)) {
            ThrowInfo(KnowhereError, "knowhere vector row byte size overflows");
        }
        return dimension * sizeof(T);
    }
}

size_t
CheckedPayloadBytes(size_t row_bytes, size_t rows) {
    if (row_bytes != 0 &&
        rows > std::numeric_limits<size_t>::max() / row_bytes) {
        ThrowInfo(KnowhereError, "knowhere vector payload size overflows");
    }
    return row_bytes * rows;
}

}  // namespace

KnowhereEngine::KnowhereEngine(DataType physical_type,
                               DataType elem_type,
                               IndexType index_type,
                               MetricType metric_type,
                               IndexVersion version,
                               bool use_knowhere_build_pool)
    : native_index(CreateIndex(physical_type,
                               elem_type,
                               index_type,
                               metric_type,
                               version,
                               nullptr,
                               true,
                               true)),
      index_type_(std::move(index_type)),
      metric_type_(std::move(metric_type)),
      physical_type_(physical_type),
      embedding_list_(elem_type != DataType::NONE),
      use_knowhere_build_pool_(use_knowhere_build_pool) {
}

KnowhereEngine::KnowhereEngine(DataType physical_type,
                               DataType elem_type,
                               IndexType index_type,
                               MetricType metric_type,
                               IndexVersion version,
                               knowhere::ViewDataOp view_data,
                               bool use_knowhere_build_pool)
    : native_index(CreateDataViewIndex(physical_type,
                                       elem_type,
                                       index_type,
                                       metric_type,
                                       version,
                                       std::move(view_data))),
      index_type_(std::move(index_type)),
      metric_type_(std::move(metric_type)),
      physical_type_(physical_type),
      embedding_list_(elem_type != DataType::NONE),
      use_knowhere_build_pool_(use_knowhere_build_pool) {
}

KnowhereEngine::KnowhereEngine(
    DataType physical_type,
    DataType elem_type,
    IndexType index_type,
    MetricType metric_type,
    IndexVersion version,
    std::shared_ptr<const void> backing_owner_arg,
    const knowhere::Pack<std::shared_ptr<milvus::FileManager>>& engine_object,
    bool use_knowhere_build_pool)
    : backing_owner(std::move(backing_owner_arg)),
      native_index(CreateIndex(physical_type,
                               elem_type,
                               index_type,
                               metric_type,
                               version,
                               &engine_object,
                               false,
                               true)),
      index_type_(std::move(index_type)),
      metric_type_(std::move(metric_type)),
      physical_type_(physical_type),
      embedding_list_(elem_type != DataType::NONE),
      use_knowhere_build_pool_(use_knowhere_build_pool) {
}

KnowhereEngine&
KnowhereEngine::operator=(KnowhereEngine&& other) {
    if (this != &other) {
        native_index = knowhere::Index<knowhere::IndexNode>{};
        backing_owner.reset();
        backing_owner = std::move(other.backing_owner);
        native_index = std::move(other.native_index);
        index_type_ = std::move(other.index_type_);
        metric_type_ = std::move(other.metric_type_);
        dim_ = other.dim_;
        physical_type_ = other.physical_type_;
        embedding_list_ = other.embedding_list_;
        use_knowhere_build_pool_ = other.use_knowhere_build_pool_;
        empty_emb_list_offsets_ = std::move(other.empty_emb_list_offsets_);
    }
    return *this;
}

knowhere::Json
PrepareVectorSearchParams(const VectorSearchParams& params) {
    auto search_config = params.search_params_;
    search_config[knowhere::meta::METRIC_TYPE] = params.metric_type_;
    search_config[knowhere::meta::TOPK] = params.topk_;
    if (params.trace_ctx_.traceID != nullptr &&
        params.trace_ctx_.spanID != nullptr) {
        search_config[knowhere::meta::TRACE_ID] =
            tracer::GetTraceIDAsHexStr(&params.trace_ctx_);
        search_config[knowhere::meta::SPAN_ID] =
            tracer::GetSpanIDAsHexStr(&params.trace_ctx_);
        search_config[knowhere::meta::TRACE_FLAGS] =
            params.trace_ctx_.traceFlags;
    }
    return search_config;
}

bool
KnowhereMmapSupported(const IndexType& index_type) {
    return knowhere::IndexFactory::Instance().FeatureCheck(
        index_type, knowhere::feature::MMAP);
}

template <typename T>
std::vector<uint8_t>
DecodeVectorByIdsResult(const knowhere::DataSetPtr& result) {
    if (result == nullptr) {
        ThrowInfo(KnowhereError, "knowhere returned a null vector dataset");
    }
    const auto rows = CheckedElementCount(result->GetRows(), "vector row");
    const auto row_bytes = CheckedVectorRowBytes<T>(result->GetDim());
    const auto data_size = CheckedPayloadBytes(row_bytes, rows);
    const auto* tensor = result->GetTensor();
    if (data_size > 0 && tensor == nullptr) {
        ThrowInfo(KnowhereError,
                  "knowhere returned a null vector tensor for {} bytes",
                  data_size);
    }

    std::vector<uint8_t> raw_data(data_size);
    if (data_size > 0) {
        milvus::fastmem::FastMemcpy(raw_data.data(), tensor, data_size);
    }
    return raw_data;
}

template <typename T>
std::pair<std::vector<uint8_t>, std::vector<size_t>>
DecodeEmbListByIdsResult(const knowhere::DataSetPtr& result) {
    if (result == nullptr) {
        ThrowInfo(KnowhereError,
                  "knowhere returned a null embedding-list dataset");
    }
    const auto list_count =
        CheckedElementCount(result->GetRows(), "embedding-list row");
    if (list_count == std::numeric_limits<size_t>::max()) {
        ThrowInfo(KnowhereError, "knowhere embedding-list count overflows");
    }
    const auto* offsets_ptr =
        result->Get<const size_t*>(knowhere::meta::EMB_LIST_OFFSET);
    if (offsets_ptr == nullptr) {
        ThrowInfo(KnowhereError,
                  "knowhere embedding-list result has no offsets");
    }

    std::vector<size_t> offsets(offsets_ptr, offsets_ptr + list_count + 1);
    if (offsets.front() != 0) {
        ThrowInfo(KnowhereError,
                  "knowhere embedding-list offsets do not start at zero");
    }
    for (size_t i = 1; i < offsets.size(); ++i) {
        if (offsets[i] < offsets[i - 1]) {
            ThrowInfo(KnowhereError,
                      "knowhere embedding-list offsets are not monotonic");
        }
    }

    const auto row_bytes = CheckedVectorRowBytes<T>(result->GetDim());
    const auto data_size = CheckedPayloadBytes(row_bytes, offsets.back());
    const auto* tensor = result->GetTensor();
    if (data_size > 0 && tensor == nullptr) {
        ThrowInfo(KnowhereError,
                  "knowhere returned a null embedding-list tensor for {} "
                  "bytes",
                  data_size);
    }
    std::vector<uint8_t> raw_data(data_size);
    if (data_size > 0) {
        milvus::fastmem::FastMemcpy(raw_data.data(), tensor, data_size);
    }
    return {std::move(raw_data), std::move(offsets)};
}

template std::vector<uint8_t>
DecodeVectorByIdsResult<float>(const knowhere::DataSetPtr& result);
template std::vector<uint8_t>
DecodeVectorByIdsResult<bin1>(const knowhere::DataSetPtr& result);
template std::vector<uint8_t>
DecodeVectorByIdsResult<float16>(const knowhere::DataSetPtr& result);
template std::vector<uint8_t>
DecodeVectorByIdsResult<bfloat16>(const knowhere::DataSetPtr& result);
template std::vector<uint8_t>
DecodeVectorByIdsResult<int8>(const knowhere::DataSetPtr& result);

template std::pair<std::vector<uint8_t>, std::vector<size_t>>
DecodeEmbListByIdsResult<float>(const knowhere::DataSetPtr& result);
template std::pair<std::vector<uint8_t>, std::vector<size_t>>
DecodeEmbListByIdsResult<bin1>(const knowhere::DataSetPtr& result);
template std::pair<std::vector<uint8_t>, std::vector<size_t>>
DecodeEmbListByIdsResult<float16>(const knowhere::DataSetPtr& result);
template std::pair<std::vector<uint8_t>, std::vector<size_t>>
DecodeEmbListByIdsResult<bfloat16>(const knowhere::DataSetPtr& result);
template std::pair<std::vector<uint8_t>, std::vector<size_t>>
DecodeEmbListByIdsResult<int8>(const knowhere::DataSetPtr& result);

}  // namespace milvus::index
