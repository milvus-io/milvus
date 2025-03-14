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
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/Types.h"
#include "index/VectorMemIndex.h"
#include "index/Utils.h"
#include "index/Meta.h"
#include "index/JsonInvertedIndex.h"
#include "knowhere/utils.h"

#include "index/VectorDiskIndex.h"
#include "index/ScalarIndexSort.h"
#include "index/StringIndexMarisa.h"
#include "index/BoolIndex.h"
#include "index/InvertedIndexTantivy.h"
#include "index/HybridScalarIndex.h"
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
        // scalar_index_engine_version 0 means we should built tantivy index within single segment
        return std::make_unique<InvertedIndexTantivy<T>>(
            file_manager_context,
            create_index_info.scalar_index_engine_version == 0);
    }
    if (index_type == BITMAP_INDEX_TYPE) {
        return std::make_unique<BitmapIndex<T>>(file_manager_context);
    }
    if (index_type == HYBRID_INDEX_TYPE) {
        return std::make_unique<HybridScalarIndex<T>>(file_manager_context);
    }
    return CreateScalarIndexSort<T>(file_manager_context);
}

// template <>
// inline ScalarIndexPtr<bool>
// IndexFactory::CreateScalarIndex(const IndexType& index_type) {
//    return CreateBoolIndex();
//}
//

template <>
ScalarIndexPtr<std::string>
IndexFactory::CreatePrimitiveScalarIndex<std::string>(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context) {
    auto index_type = create_index_info.index_type;
#if defined(__linux__) || defined(__APPLE__)
    if (index_type == INVERTED_INDEX_TYPE) {
        // scalar_index_engine_version 0 means we should built tantivy index within single segment
        return std::make_unique<InvertedIndexTantivy<std::string>>(
            file_manager_context,
            create_index_info.scalar_index_engine_version == 0);
    }
    if (index_type == BITMAP_INDEX_TYPE) {
        return std::make_unique<BitmapIndex<std::string>>(file_manager_context);
    }
    if (index_type == HYBRID_INDEX_TYPE) {
        return std::make_unique<HybridScalarIndex<std::string>>(
            file_manager_context);
    }
    return CreateStringIndexMarisa(file_manager_context);
#else
    PanicInfo(Unsupported, "unsupported platform");
#endif
}

LoadResourceRequest
IndexFactory::IndexLoadResource(
    DataType field_type,
    IndexVersion index_version,
    float index_size,
    std::map<std::string, std::string>& index_params,
    bool mmap_enable) {
    if (milvus::IsVectorDataType(field_type)) {
        return VecIndexLoadResource(
            field_type, index_version, index_size, index_params, mmap_enable);
    } else {
        return ScalarIndexLoadResource(
            field_type, index_version, index_size, index_params, mmap_enable);
    }
}

LoadResourceRequest
IndexFactory::VecIndexLoadResource(
    DataType field_type,
    IndexVersion index_version,
    float index_size,
    std::map<std::string, std::string>& index_params,
    bool mmap_enable) {
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
    float index_size_gb = index_size * 1.0 / 1024.0 / 1024.0 / 1024.0;
    float download_buffer_size_gb =
        DEFAULT_FIELD_MAX_MEMORY_LIMIT * 1.0 / 1024.0 / 1024.0 / 1024.0;

    bool has_raw_data = false;
    switch (field_type) {
        case milvus::DataType::VECTOR_BINARY:
            resource = knowhere::IndexStaticFaced<
                knowhere::bin1>::EstimateLoadResource(index_type,
                                                      index_version,
                                                      index_size_gb,
                                                      config);
            has_raw_data =
                knowhere::IndexStaticFaced<knowhere::bin1>::HasRawData(
                    index_type, index_version, config);
            break;
        case milvus::DataType::VECTOR_FLOAT:
            resource = knowhere::IndexStaticFaced<
                knowhere::fp32>::EstimateLoadResource(index_type,
                                                      index_version,
                                                      index_size_gb,
                                                      config);
            has_raw_data =
                knowhere::IndexStaticFaced<knowhere::fp32>::HasRawData(
                    index_type, index_version, config);
            break;
        case milvus::DataType::VECTOR_FLOAT16:
            resource = knowhere::IndexStaticFaced<
                knowhere::fp16>::EstimateLoadResource(index_type,
                                                      index_version,
                                                      index_size_gb,
                                                      config);
            has_raw_data =
                knowhere::IndexStaticFaced<knowhere::fp16>::HasRawData(
                    index_type, index_version, config);
            break;
        case milvus::DataType::VECTOR_BFLOAT16:
            resource = knowhere::IndexStaticFaced<
                knowhere::bf16>::EstimateLoadResource(index_type,
                                                      index_version,
                                                      index_size_gb,
                                                      config);
            has_raw_data =
                knowhere::IndexStaticFaced<knowhere::bf16>::HasRawData(
                    index_type, index_version, config);
            break;
        case milvus::DataType::VECTOR_SPARSE_FLOAT:
            resource = knowhere::IndexStaticFaced<
                knowhere::fp32>::EstimateLoadResource(index_type,
                                                      index_version,
                                                      index_size_gb,
                                                      config);
            has_raw_data =
                knowhere::IndexStaticFaced<knowhere::fp32>::HasRawData(
                    index_type, index_version, config);
            break;
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
        request.max_memory_cost =
            std::max(resource.value().memoryCost, download_buffer_size_gb);
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
    float index_size,
    std::map<std::string, std::string>& index_params,
    bool mmap_enable) {
    auto config = milvus::index::ParseConfigFromIndexParams(index_params);

    AssertInfo(index_params.find("index_type") != index_params.end(),
               "index type is empty");
    std::string index_type = index_params.at("index_type");

    knowhere::expected<knowhere::Resource> resource;
    float index_size_gb = index_size * 1.0 / 1024.0 / 1024.0 / 1024.0;

    LoadResourceRequest request{};
    request.has_raw_data = false;

    if (index_type == milvus::index::ASCENDING_SORT) {
        request.final_memory_cost = index_size_gb;
        request.final_disk_cost = 0;
        request.max_memory_cost = 2 * index_size_gb;
        request.max_disk_cost = 0;
        request.has_raw_data = true;
    } else if (index_type == milvus::index::MARISA_TRIE ||
               index_type == milvus::index::MARISA_TRIE_UPPER) {
        if (mmap_enable) {
            request.final_memory_cost = 0;
            request.final_disk_cost = index_size_gb;
            request.max_memory_cost = index_size_gb;
            request.max_disk_cost = index_size_gb;
        } else {
            request.final_memory_cost = index_size_gb;
            request.final_disk_cost = 0;
            request.max_memory_cost = 2 * index_size_gb;
            request.max_disk_cost = 0;
        }
        request.has_raw_data = true;
    } else if (index_type == milvus::index::INVERTED_INDEX_TYPE) {
        if (mmap_enable) {
            request.final_memory_cost = 0;
            request.final_disk_cost = index_size_gb;
            request.max_memory_cost = index_size_gb;
            request.max_disk_cost = index_size_gb;
        } else {
            request.final_memory_cost = index_size_gb;
            request.final_disk_cost = 0;
            request.max_memory_cost = 2 * index_size_gb;
            request.max_disk_cost = 0;
        }

        request.has_raw_data = false;
    } else if (index_type == milvus::index::BITMAP_INDEX_TYPE) {
        if (mmap_enable) {
            request.final_memory_cost = 0;
            request.final_disk_cost = index_size_gb;
            request.max_memory_cost = index_size_gb;
            request.max_disk_cost = index_size_gb;
        } else {
            request.final_memory_cost = index_size_gb;
            request.final_disk_cost = 0;
            request.max_memory_cost = 2 * index_size_gb;
            request.max_disk_cost = 0;
        }

        if (field_type == milvus::DataType::ARRAY) {
            request.has_raw_data = false;
        } else {
            request.has_raw_data = true;
        }
    } else if (index_type == milvus::index::HYBRID_INDEX_TYPE) {
        request.final_memory_cost = index_size_gb;
        request.final_disk_cost = index_size_gb;
        request.max_memory_cost = 2 * index_size_gb;
        request.max_disk_cost = index_size_gb;
        request.has_raw_data = false;
    } else {
        LOG_ERROR(
            "invalid index type to estimate scalar index load resource: {}",
            index_type);
        return LoadResourceRequest{0, 0, 0, 0, true};
    }
    return request;
}

IndexBasePtr
IndexFactory::CreateIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context) {
    if (IsVectorDataType(create_index_info.field_type)) {
        return CreateVectorIndex(create_index_info, file_manager_context);
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
        case DataType::VARCHAR:
            return CreatePrimitiveScalarIndex<std::string>(
                create_index_info, file_manager_context);
        default:
            PanicInfo(
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
        PanicInfo(
            Unsupported,
            fmt::format("index type: {} for composite scalar not supported now",
                        index_type));
    }
}

IndexBasePtr
IndexFactory::CreateComplexScalarIndex(
    IndexType index_type,
    const storage::FileManagerContext& file_manager_context) {
    PanicInfo(Unsupported, "Complex index not supported now");
}

IndexBasePtr
IndexFactory::CreateJsonIndex(
    IndexType index_type,
    DataType cast_dtype,
    const std::string& nested_path,
    const storage::FileManagerContext& file_manager_context) {
    AssertInfo(index_type == INVERTED_INDEX_TYPE,
               "Invalid index type for json index");
    switch (cast_dtype) {
        case DataType::BOOL:
            return std::make_unique<index::JsonInvertedIndex<bool>>(
                proto::schema::DataType::Bool,
                nested_path,
                file_manager_context);
        case DataType::INT8:
        case DataType::INT16:
        case DataType::INT32:
        case DataType::INT64:
        case DataType::FLOAT:
        case DataType::DOUBLE:
            return std::make_unique<index::JsonInvertedIndex<double>>(
                proto::schema::DataType::Double,
                nested_path,
                file_manager_context);
        case DataType::STRING:
        case DataType::VARCHAR:
            return std::make_unique<index::JsonInvertedIndex<std::string>>(
                proto::schema::DataType::String,
                nested_path,
                file_manager_context);
        default:
            PanicInfo(DataTypeInvalid, "Invalid data type:{}", cast_dtype);
    }
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
            return CreatePrimitiveScalarIndex(
                data_type, create_index_info, file_manager_context);
        case DataType::ARRAY: {
            return CreateCompositeScalarIndex(create_index_info,
                                              file_manager_context);
        }
        case DataType::JSON: {
            return CreateJsonIndex(create_index_info.index_type,
                                   create_index_info.json_cast_type,
                                   create_index_info.json_path,
                                   file_manager_context);
        }
        default:
            PanicInfo(DataTypeInvalid, "Invalid data type:{}", data_type);
    }
}

IndexBasePtr
IndexFactory::CreateVectorIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context) {
    auto index_type = create_index_info.index_type;
    auto metric_type = create_index_info.metric_type;
    auto version = create_index_info.index_engine_version;
    // create disk index
    auto data_type = create_index_info.field_type;
    if (knowhere::UseDiskLoad(index_type, version)) {
        switch (data_type) {
            case DataType::VECTOR_FLOAT: {
                return std::make_unique<VectorDiskAnnIndex<float>>(
                    index_type, metric_type, version, file_manager_context);
            }
            case DataType::VECTOR_FLOAT16: {
                return std::make_unique<VectorDiskAnnIndex<float16>>(
                    index_type, metric_type, version, file_manager_context);
            }
            case DataType::VECTOR_BFLOAT16: {
                return std::make_unique<VectorDiskAnnIndex<bfloat16>>(
                    index_type, metric_type, version, file_manager_context);
            }
            case DataType::VECTOR_BINARY: {
                return std::make_unique<VectorDiskAnnIndex<bin1>>(
                    index_type, metric_type, version, file_manager_context);
            }
            case DataType::VECTOR_SPARSE_FLOAT: {
                return std::make_unique<VectorDiskAnnIndex<float>>(
                    index_type, metric_type, version, file_manager_context);
            }
            default:
                PanicInfo(
                    DataTypeInvalid,
                    fmt::format("invalid data type to build disk index: {}",
                                data_type));
        }
    } else {  // create mem index
        switch (data_type) {
            case DataType::VECTOR_FLOAT:
            case DataType::VECTOR_SPARSE_FLOAT: {
                return std::make_unique<VectorMemIndex<float>>(
                    index_type, metric_type, version, file_manager_context);
            }
            case DataType::VECTOR_BINARY: {
                return std::make_unique<VectorMemIndex<bin1>>(
                    index_type, metric_type, version, file_manager_context);
            }
            case DataType::VECTOR_FLOAT16: {
                return std::make_unique<VectorMemIndex<float16>>(
                    index_type, metric_type, version, file_manager_context);
            }
            case DataType::VECTOR_BFLOAT16: {
                return std::make_unique<VectorMemIndex<bfloat16>>(
                    index_type, metric_type, version, file_manager_context);
            }
            default:
                PanicInfo(
                    DataTypeInvalid,
                    fmt::format("invalid data type to build mem index: {}",
                                data_type));
        }
    }
}
}  // namespace milvus::index
