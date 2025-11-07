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

#include "index/IndexFactory.h"
#include <cstdlib>
#include <memory>
#include <string>
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/JsonCastType.h"
#include "common/Types.h"
#include "index/Index.h"
#include "index/JsonFlatIndex.h"
#include "index/VectorMemIndex.h"
#include "index/Utils.h"
#include "index/Meta.h"
#include "index/JsonInvertedIndex.h"
#include "index/NgramInvertedIndex.h"
#include "knowhere/utils.h"

#include "index/VectorDiskIndex.h"
#include "index/ScalarIndexSort.h"
#include "index/StringIndexSort.h"
#include "index/StringIndexMarisa.h"
#include "index/BoolIndex.h"
#include "index/InvertedIndexTantivy.h"
#include "index/HybridScalarIndex.h"
#include "index/RTreeIndex.h"
#include "knowhere/comp/knowhere_check.h"
#include "log/Log.h"
#include "pb/schema.pb.h"

namespace milvus::index {

template <typename T>
ScalarIndexPtr<T>
IndexFactory::CreatePrimitiveScalarIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context) {
    auto index_type = create_index_info.index_type;
    if (index_type == INVERTED_INDEX_TYPE) {
        assert(create_index_info.tantivy_index_version != 0);
        // scalar_index_engine_version 0 means we should built tantivy index within single segment
        return std::make_unique<InvertedIndexTantivy<T>>(
            create_index_info.tantivy_index_version,
            file_manager_context,
            create_index_info.scalar_index_engine_version == 0);
    }
    if (index_type == BITMAP_INDEX_TYPE) {
        return std::make_unique<BitmapIndex<T>>(file_manager_context);
    }
    if (index_type == HYBRID_INDEX_TYPE) {
        return std::make_unique<HybridScalarIndex<T>>(
            create_index_info.tantivy_index_version, file_manager_context);
    }
    return CreateScalarIndexSort<T>(file_manager_context);
}

template <>
ScalarIndexPtr<std::string>
IndexFactory::CreatePrimitiveScalarIndex<std::string>(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context) {
    auto index_type = create_index_info.index_type;
#if defined(__linux__) || defined(__APPLE__)
    if (index_type == INVERTED_INDEX_TYPE) {
        assert(create_index_info.tantivy_index_version != 0);
        // scalar_index_engine_version 0 means we should built tantivy index within single segment
        return std::make_unique<InvertedIndexTantivy<std::string>>(
            create_index_info.tantivy_index_version,
            file_manager_context,
            create_index_info.scalar_index_engine_version == 0);
    }
    if (index_type == BITMAP_INDEX_TYPE) {
        return std::make_unique<BitmapIndex<std::string>>(file_manager_context);
    } else if (index_type == HYBRID_INDEX_TYPE) {
        return std::make_unique<HybridScalarIndex<std::string>>(
            create_index_info.tantivy_index_version, file_manager_context);
    } else if (index_type == MARISA_TRIE || index_type == MARISA_TRIE_UPPER) {
        return CreateStringIndexMarisa(file_manager_context);
    } else if (index_type == ASCENDING_SORT) {
        return CreateStringIndexSort(file_manager_context);
    } else {
        ThrowInfo(Unsupported, "unsupported index type: {}", index_type);
    }
#else
    ThrowInfo(Unsupported, "unsupported platform");
#endif
}

LoadResourceRequest
IndexFactory::IndexLoadResource(
    DataType field_type,
    DataType element_type,
    IndexVersion index_version,
    uint64_t index_size_in_bytes,
    const std::map<std::string, std::string>& index_params,
    bool mmap_enable,
    int64_t num_rows,
    int64_t dim) {
    if (milvus::IsVectorDataType(field_type)) {
        return VecIndexLoadResource(field_type,
                                    element_type,
                                    index_version,
                                    index_size_in_bytes,
                                    index_params,
                                    mmap_enable,
                                    num_rows,
                                    dim);
    } else {
        return ScalarIndexLoadResource(field_type,
                                       index_version,
                                       index_size_in_bytes,
                                       index_params,
                                       mmap_enable);
    }
}

LoadResourceRequest
IndexFactory::VecIndexLoadResource(
    DataType field_type,
    DataType element_type,
    IndexVersion index_version,
    uint64_t index_size_in_bytes,
    const std::map<std::string, std::string>& index_params,
    bool mmap_enable,
    int64_t num_rows,
    int64_t dim) {
    auto config = milvus::index::ParseConfigFromIndexParams(index_params);

    AssertInfo(index_params.find("index_type") != index_params.end(),
               "index type is empty");
    std::string index_type = index_params.at("index_type");

    bool mmaped = false;
    if (mmap_enable &&
        knowhere::KnowhereCheck::SupportMmapIndexTypeCheck(index_type)) {
        config["enable_mmap"] = true;
        mmaped = true;
    }

    knowhere::expected<knowhere::Resource> resource;
    uint64_t download_buffer_size_in_bytes = DEFAULT_FIELD_MAX_MEMORY_LIMIT;

    bool has_raw_data = false;
    switch (field_type) {
        case milvus::DataType::VECTOR_BINARY:
            resource = knowhere::IndexStaticFaced<
                knowhere::bin1>::EstimateLoadResource(index_type,
                                                      index_version,
                                                      index_size_in_bytes,
                                                      num_rows,
                                                      dim,
                                                      config);
            has_raw_data =
                knowhere::IndexStaticFaced<knowhere::bin1>::HasRawData(
                    index_type, index_version, config);
            break;
        case milvus::DataType::VECTOR_FLOAT:
            resource = knowhere::IndexStaticFaced<
                knowhere::fp32>::EstimateLoadResource(index_type,
                                                      index_version,
                                                      index_size_in_bytes,
                                                      num_rows,
                                                      dim,
                                                      config);
            has_raw_data =
                knowhere::IndexStaticFaced<knowhere::fp32>::HasRawData(
                    index_type, index_version, config);
            break;
        case milvus::DataType::VECTOR_FLOAT16:
            resource = knowhere::IndexStaticFaced<
                knowhere::fp16>::EstimateLoadResource(index_type,
                                                      index_version,
                                                      index_size_in_bytes,
                                                      num_rows,
                                                      dim,
                                                      config);
            has_raw_data =
                knowhere::IndexStaticFaced<knowhere::fp16>::HasRawData(
                    index_type, index_version, config);
            break;
        case milvus::DataType::VECTOR_BFLOAT16:
            resource = knowhere::IndexStaticFaced<
                knowhere::bf16>::EstimateLoadResource(index_type,
                                                      index_version,
                                                      index_size_in_bytes,
                                                      num_rows,
                                                      dim,
                                                      config);
            has_raw_data =
                knowhere::IndexStaticFaced<knowhere::bf16>::HasRawData(
                    index_type, index_version, config);
            break;
        case milvus::DataType::VECTOR_SPARSE_U32_F32:
            resource = knowhere::IndexStaticFaced<knowhere::sparse_u32_f32>::
                EstimateLoadResource(index_type,
                                     index_version,
                                     index_size_in_bytes,
                                     num_rows,
                                     dim,
                                     config);
            has_raw_data =
                knowhere::IndexStaticFaced<knowhere::fp32>::HasRawData(
                    index_type, index_version, config);
            break;
        case milvus::DataType::VECTOR_INT8:
            resource = knowhere::IndexStaticFaced<
                knowhere::int8>::EstimateLoadResource(index_type,
                                                      index_version,
                                                      index_size_in_bytes,
                                                      num_rows,
                                                      dim,
                                                      config);
            has_raw_data =
                knowhere::IndexStaticFaced<knowhere::int8>::HasRawData(
                    index_type, index_version, config);
            break;
        case milvus::DataType::VECTOR_ARRAY: {
            switch (element_type) {
                case milvus::DataType::VECTOR_FLOAT:
                    resource = knowhere::IndexStaticFaced<knowhere::fp32>::
                        EstimateLoadResource(index_type,
                                             index_version,
                                             index_size_in_bytes,
                                             num_rows,
                                             dim,
                                             config);
                    break;
                case milvus::DataType::VECTOR_FLOAT16:
                    resource = knowhere::IndexStaticFaced<knowhere::fp16>::
                        EstimateLoadResource(index_type,
                                             index_version,
                                             index_size_in_bytes,
                                             num_rows,
                                             dim,
                                             config);
                    break;
                case milvus::DataType::VECTOR_BFLOAT16:
                    resource = knowhere::IndexStaticFaced<knowhere::bf16>::
                        EstimateLoadResource(index_type,
                                             index_version,
                                             index_size_in_bytes,
                                             num_rows,
                                             dim,
                                             config);
                    break;
                case milvus::DataType::VECTOR_BINARY:
                    resource = knowhere::IndexStaticFaced<knowhere::bin1>::
                        EstimateLoadResource(index_type,
                                             index_version,
                                             index_size_in_bytes,
                                             num_rows,
                                             dim,
                                             config);
                    break;
                case milvus::DataType::VECTOR_INT8:
                    resource = knowhere::IndexStaticFaced<knowhere::int8>::
                        EstimateLoadResource(index_type,
                                             index_version,
                                             index_size_in_bytes,
                                             num_rows,
                                             dim,
                                             config);
                    break;

                default:
                    LOG_ERROR(
                        "invalid data type to estimate index load resource: "
                        "field_type {}, element_type {}",
                        field_type,
                        element_type);
                    return LoadResourceRequest{0, 0, 0, 0, true};
            }
            // For VectorArray, has_raw_data is always false as get_vector of index does not provide offsets which
            // is required for reconstructing the raw data
            has_raw_data = false;
            break;
        }
        default:
            LOG_ERROR("invalid data type to estimate index load resource: {}",
                      field_type);
            return LoadResourceRequest{0, 0, 0, 0, true};
    }

    LoadResourceRequest request{};

    request.has_raw_data = has_raw_data;
    request.final_disk_cost = resource.value().diskCost;
    request.final_memory_cost = resource.value().memoryCost;
    if (knowhere::UseDiskLoad(index_type, index_version) || mmaped) {
        request.max_disk_cost = resource.value().diskCost;
        request.max_memory_cost = std::max(resource.value().memoryCost,
                                           download_buffer_size_in_bytes);
    } else {
        request.max_disk_cost = 0;
        request.max_memory_cost = 2 * resource.value().memoryCost;
    }
    return request;
}

LoadResourceRequest
IndexFactory::ScalarIndexLoadResource(
    DataType field_type,
    IndexVersion index_version,
    uint64_t index_size_in_bytes,
    const std::map<std::string, std::string>& index_params,
    bool mmap_enable) {
    auto config = milvus::index::ParseConfigFromIndexParams(index_params);

    AssertInfo(index_params.find("index_type") != index_params.end(),
               "index type is empty");
    std::string index_type = index_params.at("index_type");

    knowhere::expected<knowhere::Resource> resource;

    LoadResourceRequest request{};
    request.has_raw_data = false;

    if (index_type == milvus::index::ASCENDING_SORT) {
        request.final_memory_cost = index_size_in_bytes;
        request.final_disk_cost = 0;
        request.max_memory_cost = 2 * index_size_in_bytes;
        request.max_disk_cost = 0;
        request.has_raw_data = true;
    } else if (index_type == milvus::index::MARISA_TRIE ||
               index_type == milvus::index::MARISA_TRIE_UPPER) {
        if (mmap_enable) {
            request.final_memory_cost = 0;
            request.final_disk_cost = index_size_in_bytes;
            request.max_memory_cost = index_size_in_bytes;
            request.max_disk_cost = index_size_in_bytes;
        } else {
            request.final_memory_cost = index_size_in_bytes;
            request.final_disk_cost = 0;
            request.max_memory_cost = 2 * index_size_in_bytes;
            request.max_disk_cost = index_size_in_bytes;
        }
        request.has_raw_data = true;
    } else if (index_type == milvus::index::INVERTED_INDEX_TYPE ||
               index_type == milvus::index::NGRAM_INDEX_TYPE ||
               index_type == milvus::index::RTREE_INDEX_TYPE) {
        request.final_memory_cost = 0;
        request.final_disk_cost = index_size_in_bytes;
        request.max_memory_cost = index_size_in_bytes;
        request.max_disk_cost = index_size_in_bytes;

        request.has_raw_data = false;
    } else if (index_type == milvus::index::BITMAP_INDEX_TYPE) {
        if (mmap_enable) {
            request.final_memory_cost = 0;
            request.final_disk_cost = index_size_in_bytes;
            request.max_memory_cost = index_size_in_bytes;
            request.max_disk_cost = index_size_in_bytes;
        } else {
            request.final_memory_cost = index_size_in_bytes;
            request.final_disk_cost = 0;
            request.max_memory_cost = 2 * index_size_in_bytes;
            request.max_disk_cost = 0;
        }

        request.has_raw_data = false;
    } else if (index_type == milvus::index::HYBRID_INDEX_TYPE) {
        request.final_memory_cost = index_size_in_bytes;
        request.final_disk_cost = index_size_in_bytes;
        request.max_memory_cost = 2 * index_size_in_bytes;
        request.max_disk_cost = index_size_in_bytes;
        request.has_raw_data = false;
    } else {
        LOG_ERROR(
            "invalid index type to estimate scalar index load resource: {}",
            index_type);
        return LoadResourceRequest{0, 0, 0, 0, false};
    }
    return request;
}

IndexBasePtr
IndexFactory::CreateIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context,
    bool use_build_pool) {
    if (IsVectorDataType(create_index_info.field_type)) {
        return CreateVectorIndex(
            create_index_info, file_manager_context, use_build_pool);
    }

    return CreateScalarIndex(create_index_info, file_manager_context);
}

IndexBasePtr
IndexFactory::CreatePrimitiveScalarIndex(
    DataType data_type,
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context) {
    switch (data_type) {
        // create scalar index
        case DataType::BOOL:
            return CreatePrimitiveScalarIndex<bool>(create_index_info,
                                                    file_manager_context);
        case DataType::INT8:
            return CreatePrimitiveScalarIndex<int8_t>(create_index_info,
                                                      file_manager_context);
        case DataType::INT16:
            return CreatePrimitiveScalarIndex<int16_t>(create_index_info,
                                                       file_manager_context);
        case DataType::INT32:
            return CreatePrimitiveScalarIndex<int32_t>(create_index_info,
                                                       file_manager_context);
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return CreatePrimitiveScalarIndex<int64_t>(create_index_info,
                                                       file_manager_context);
        case DataType::FLOAT:
            return CreatePrimitiveScalarIndex<float>(create_index_info,
                                                     file_manager_context);
        case DataType::DOUBLE:
            return CreatePrimitiveScalarIndex<double>(create_index_info,
                                                      file_manager_context);

            // create string index
        case DataType::STRING:
        case DataType::VARCHAR: {
            auto& ngram_params = create_index_info.ngram_params;
            if (ngram_params.has_value()) {
                return std::make_unique<NgramInvertedIndex>(
                    file_manager_context, ngram_params.value());
            }
            return CreatePrimitiveScalarIndex<std::string>(
                create_index_info, file_manager_context);
        }
        default:
            ThrowInfo(
                DataTypeInvalid,
                fmt::format("invalid data type to build index: {}", data_type));
    }
}

IndexBasePtr
IndexFactory::CreateCompositeScalarIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context) {
    auto index_type = create_index_info.index_type;
    if (index_type == HYBRID_INDEX_TYPE || index_type == BITMAP_INDEX_TYPE ||
        index_type == INVERTED_INDEX_TYPE) {
        auto element_type = static_cast<DataType>(
            file_manager_context.fieldDataMeta.field_schema.element_type());
        return CreatePrimitiveScalarIndex(
            element_type, create_index_info, file_manager_context);
    } else {
        ThrowInfo(
            Unsupported,
            fmt::format("index type: {} for composite scalar not supported now",
                        index_type));
    }
}

IndexBasePtr
IndexFactory::CreateComplexScalarIndex(
    IndexType index_type,
    const storage::FileManagerContext& file_manager_context) {
    ThrowInfo(Unsupported, "Complex index not supported now");
}

IndexBasePtr
IndexFactory::CreateJsonIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context) {
    AssertInfo(create_index_info.index_type == INVERTED_INDEX_TYPE ||
                   create_index_info.index_type == NGRAM_INDEX_TYPE,
               "Invalid index type for json index");

    const auto& cast_dtype = create_index_info.json_cast_type;
    const auto& nested_path = create_index_info.json_path;
    const auto& json_cast_function = create_index_info.json_cast_function;
    switch (cast_dtype.element_type()) {
        case JsonCastType::DataType::BOOL:
            return std::make_unique<index::JsonInvertedIndex<bool>>(
                cast_dtype,
                nested_path,
                file_manager_context,
                create_index_info.tantivy_index_version,
                JsonCastFunction::FromString(json_cast_function));
        case JsonCastType::DataType::DOUBLE:
            return std::make_unique<index::JsonInvertedIndex<double>>(
                cast_dtype,
                nested_path,
                file_manager_context,
                create_index_info.tantivy_index_version,
                JsonCastFunction::FromString(json_cast_function));
        case JsonCastType::DataType::VARCHAR: {
            auto& ngram_params = create_index_info.ngram_params;
            if (ngram_params.has_value()) {
                return std::make_unique<NgramInvertedIndex>(
                    file_manager_context, ngram_params.value(), nested_path);
            }
            return std::make_unique<index::JsonInvertedIndex<std::string>>(
                cast_dtype,
                nested_path,
                file_manager_context,
                create_index_info.tantivy_index_version,
                JsonCastFunction::FromString(json_cast_function));
        }
        case JsonCastType::DataType::JSON:
            return std::make_unique<JsonFlatIndex>(
                file_manager_context,
                nested_path,
                create_index_info.tantivy_index_version);
        default:
            ThrowInfo(DataTypeInvalid, "Invalid data type:{}", cast_dtype);
    }
}

IndexBasePtr
IndexFactory::CreateGeometryIndex(
    IndexType index_type,
    const storage::FileManagerContext& file_manager_context) {
    AssertInfo(index_type == RTREE_INDEX_TYPE,
               "Invalid index type for geometry index");
    return std::make_unique<RTreeIndex<std::string>>(file_manager_context);
}

IndexBasePtr
IndexFactory::CreateScalarIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context) {
    auto data_type = create_index_info.field_type;
    switch (data_type) {
        case DataType::BOOL:
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
        case DataType::INT64:
        case DataType::FLOAT:
        case DataType::DOUBLE:
        case DataType::VARCHAR:
        case DataType::STRING:
        case DataType::TIMESTAMPTZ:
            return CreatePrimitiveScalarIndex(
                data_type, create_index_info, file_manager_context);
        case DataType::ARRAY: {
            return CreateCompositeScalarIndex(create_index_info,
                                              file_manager_context);
        }
        case DataType::JSON: {
            return CreateJsonIndex(create_index_info, file_manager_context);
        }
        case DataType::GEOMETRY: {
            return CreateGeometryIndex(create_index_info.index_type,
                                       file_manager_context);
        }
        default:
            ThrowInfo(DataTypeInvalid, "Invalid data type:{}", data_type);
    }
}

IndexBasePtr
IndexFactory::CreateVectorIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context,
    bool use_knowhere_build_pool) {
    auto index_type = create_index_info.index_type;
    auto metric_type = create_index_info.metric_type;
    auto version = create_index_info.index_engine_version;
    // create disk index
    auto data_type = create_index_info.field_type;
    if (knowhere::UseDiskLoad(index_type, version)) {
        switch (data_type) {
            case DataType::VECTOR_FLOAT: {
                return std::make_unique<VectorDiskAnnIndex<float>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    file_manager_context);
            }
            case DataType::VECTOR_FLOAT16: {
                return std::make_unique<VectorDiskAnnIndex<float16>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    file_manager_context);
            }
            case DataType::VECTOR_BFLOAT16: {
                return std::make_unique<VectorDiskAnnIndex<bfloat16>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    file_manager_context);
            }
            case DataType::VECTOR_BINARY: {
                return std::make_unique<VectorDiskAnnIndex<bin1>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    file_manager_context);
            }
            case DataType::VECTOR_SPARSE_U32_F32: {
                return std::make_unique<VectorDiskAnnIndex<sparse_u32_f32>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    file_manager_context);
            }
            case DataType::VECTOR_ARRAY: {
                auto element_type =
                    static_cast<DataType>(file_manager_context.fieldDataMeta
                                              .field_schema.element_type());
                switch (element_type) {
                    case DataType::VECTOR_FLOAT:
                        return std::make_unique<VectorDiskAnnIndex<float>>(
                            element_type,
                            index_type,
                            metric_type,
                            version,
                            file_manager_context);
                    case DataType::VECTOR_FLOAT16:
                        return std::make_unique<VectorDiskAnnIndex<float16>>(
                            element_type,
                            index_type,
                            metric_type,
                            version,
                            file_manager_context);
                    case DataType::VECTOR_BFLOAT16:
                        return std::make_unique<VectorDiskAnnIndex<bfloat16>>(
                            element_type,
                            index_type,
                            metric_type,
                            version,
                            file_manager_context);
                    case DataType::VECTOR_BINARY:
                        return std::make_unique<VectorDiskAnnIndex<bin1>>(
                            element_type,
                            index_type,
                            metric_type,
                            version,
                            file_manager_context);
                    case DataType::VECTOR_INT8:
                        return std::make_unique<VectorDiskAnnIndex<int8>>(
                            element_type,
                            index_type,
                            metric_type,
                            version,
                            file_manager_context);
                    default:
                        ThrowInfo(NotImplemented,
                                  fmt::format("not implemented data type to "
                                              "build disk index: {}",
                                              element_type));
                }
            }
            case DataType::VECTOR_INT8: {
                return std::make_unique<VectorDiskAnnIndex<int8>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    file_manager_context);
            }
            default:
                ThrowInfo(
                    DataTypeInvalid,
                    fmt::format("invalid data type to build disk index: {}",
                                data_type));
        }
    } else {  // create mem index
        switch (data_type) {
            case DataType::VECTOR_FLOAT: {
                return std::make_unique<VectorMemIndex<float>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    use_knowhere_build_pool,
                    file_manager_context);
            }
            case DataType::VECTOR_SPARSE_U32_F32: {
                return std::make_unique<VectorMemIndex<sparse_u32_f32>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    use_knowhere_build_pool,
                    file_manager_context);
            }
            case DataType::VECTOR_BINARY: {
                return std::make_unique<VectorMemIndex<bin1>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    use_knowhere_build_pool,
                    file_manager_context);
            }
            case DataType::VECTOR_FLOAT16: {
                return std::make_unique<VectorMemIndex<float16>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    use_knowhere_build_pool,
                    file_manager_context);
            }
            case DataType::VECTOR_BFLOAT16: {
                return std::make_unique<VectorMemIndex<bfloat16>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    use_knowhere_build_pool,
                    file_manager_context);
            }
            case DataType::VECTOR_INT8: {
                return std::make_unique<VectorMemIndex<int8>>(
                    DataType::NONE,
                    index_type,
                    metric_type,
                    version,
                    use_knowhere_build_pool,
                    file_manager_context);
            }
            case DataType::VECTOR_ARRAY: {
                auto element_type =
                    static_cast<DataType>(file_manager_context.fieldDataMeta
                                              .field_schema.element_type());
                switch (element_type) {
                    case DataType::VECTOR_FLOAT:
                        return std::make_unique<VectorMemIndex<float>>(
                            element_type,
                            index_type,
                            metric_type,
                            version,
                            use_knowhere_build_pool,
                            file_manager_context);
                    case DataType::VECTOR_FLOAT16: {
                        return std::make_unique<VectorMemIndex<float16>>(
                            element_type,
                            index_type,
                            metric_type,
                            version,
                            use_knowhere_build_pool,
                            file_manager_context);
                    }
                    case DataType::VECTOR_BFLOAT16: {
                        return std::make_unique<VectorMemIndex<bfloat16>>(
                            element_type,
                            index_type,
                            metric_type,
                            version,
                            use_knowhere_build_pool,
                            file_manager_context);
                    }
                    case DataType::VECTOR_BINARY: {
                        return std::make_unique<VectorMemIndex<bin1>>(
                            element_type,
                            index_type,
                            metric_type,
                            version,
                            use_knowhere_build_pool,
                            file_manager_context);
                    }
                    case DataType::VECTOR_INT8: {
                        return std::make_unique<VectorMemIndex<int8>>(
                            element_type,
                            index_type,
                            metric_type,
                            version,
                            use_knowhere_build_pool,
                            file_manager_context);
                    }
                    default:
                        ThrowInfo(NotImplemented,
                                  fmt::format("not implemented data type to "
                                              "build mem index: {}",
                                              element_type));
                }
            }
            default:
                ThrowInfo(
                    DataTypeInvalid,
                    fmt::format("invalid data type to build mem index: {}",
                                data_type));
        }
    }
}
}  // namespace milvus::index
