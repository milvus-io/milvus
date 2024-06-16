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
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "index/VectorMemIndex.h"
#include "index/Utils.h"
#include "index/Meta.h"
#include "knowhere/utils.h"

#include "index/VectorDiskIndex.h"
#include "index/ScalarIndexSort.h"
#include "index/StringIndexMarisa.h"
#include "index/BoolIndex.h"
#include "index/InvertedIndexTantivy.h"
#include "index/HybridScalarIndex.h"

namespace milvus::index {

template <typename T>
ScalarIndexPtr<T>
IndexFactory::CreatePrimitiveScalarIndex(
    const IndexType& index_type,
    const storage::FileManagerContext& file_manager_context) {
    if (index_type == INVERTED_INDEX_TYPE) {
        return std::make_unique<InvertedIndexTantivy<T>>(file_manager_context);
    }
    if (index_type == BITMAP_INDEX_TYPE) {
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
    const IndexType& index_type,
    const storage::FileManagerContext& file_manager_context) {
#if defined(__linux__) || defined(__APPLE__)
    if (index_type == INVERTED_INDEX_TYPE) {
        return std::make_unique<InvertedIndexTantivy<std::string>>(
            file_manager_context);
    }
    if (index_type == BITMAP_INDEX_TYPE) {
        return std::make_unique<HybridScalarIndex<std::string>>(
            file_manager_context);
    }
    return CreateStringIndexMarisa(file_manager_context);
#else
    throw SegcoreError(Unsupported, "unsupported platform");
#endif
}

template <typename T>
ScalarIndexPtr<T>
IndexFactory::CreatePrimitiveScalarIndex(
    const IndexType& index_type,
    const storage::FileManagerContext& file_manager_context,
    std::shared_ptr<milvus_storage::Space> space) {
    if (index_type == INVERTED_INDEX_TYPE) {
        return std::make_unique<InvertedIndexTantivy<T>>(file_manager_context,
                                                         space);
    }
    if (index_type == BITMAP_INDEX_TYPE) {
        return std::make_unique<HybridScalarIndex<T>>(file_manager_context,
                                                      space);
    }
    return CreateScalarIndexSort<T>(file_manager_context, space);
}

template <>
ScalarIndexPtr<std::string>
IndexFactory::CreatePrimitiveScalarIndex<std::string>(
    const IndexType& index_type,
    const storage::FileManagerContext& file_manager_context,
    std::shared_ptr<milvus_storage::Space> space) {
#if defined(__linux__) || defined(__APPLE__)
    if (index_type == INVERTED_INDEX_TYPE) {
        return std::make_unique<InvertedIndexTantivy<std::string>>(
            file_manager_context, space);
    }
    if (index_type == BITMAP_INDEX_TYPE) {
        return std::make_unique<HybridScalarIndex<std::string>>(
            file_manager_context, space);
    }
    return CreateStringIndexMarisa(file_manager_context, space);
#else
    throw SegcoreError(Unsupported, "unsupported platform");
#endif
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
IndexFactory::CreateIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context,
    std::shared_ptr<milvus_storage::Space> space) {
    if (IsVectorDataType(create_index_info.field_type)) {
        return CreateVectorIndex(
            create_index_info, file_manager_context, space);
    }

    return CreateScalarIndex(create_index_info, file_manager_context, space);
}

IndexBasePtr
IndexFactory::CreatePrimitiveScalarIndex(
    DataType data_type,
    IndexType index_type,
    const storage::FileManagerContext& file_manager_context) {
    switch (data_type) {
        // create scalar index
        case DataType::BOOL:
            return CreatePrimitiveScalarIndex<bool>(index_type,
                                                    file_manager_context);
        case DataType::INT8:
            return CreatePrimitiveScalarIndex<int8_t>(index_type,
                                                      file_manager_context);
        case DataType::INT16:
            return CreatePrimitiveScalarIndex<int16_t>(index_type,
                                                       file_manager_context);
        case DataType::INT32:
            return CreatePrimitiveScalarIndex<int32_t>(index_type,
                                                       file_manager_context);
        case DataType::INT64:
            return CreatePrimitiveScalarIndex<int64_t>(index_type,
                                                       file_manager_context);
        case DataType::FLOAT:
            return CreatePrimitiveScalarIndex<float>(index_type,
                                                     file_manager_context);
        case DataType::DOUBLE:
            return CreatePrimitiveScalarIndex<double>(index_type,
                                                      file_manager_context);

            // create string index
        case DataType::STRING:
        case DataType::VARCHAR:
            return CreatePrimitiveScalarIndex<std::string>(
                index_type, file_manager_context);
        default:
            throw SegcoreError(
                DataTypeInvalid,
                fmt::format("invalid data type to build index: {}", data_type));
    }
}

IndexBasePtr
IndexFactory::CreateCompositeScalarIndex(
    IndexType index_type,
    const storage::FileManagerContext& file_manager_context) {
    if (index_type == BITMAP_INDEX_TYPE) {
        auto element_type = static_cast<DataType>(
            file_manager_context.fieldDataMeta.field_schema.element_type());
        return CreatePrimitiveScalarIndex(
            element_type, index_type, file_manager_context);
    } else if (index_type == INVERTED_INDEX_TYPE) {
        auto element_type = static_cast<DataType>(
            file_manager_context.fieldDataMeta.field_schema.element_type());
        return CreatePrimitiveScalarIndex(
            element_type, index_type, file_manager_context);
    }
}

IndexBasePtr
IndexFactory::CreateComplexScalarIndex(
    IndexType index_type,
    const storage::FileManagerContext& file_manager_context) {
    PanicInfo(Unsupported, "Complex index not supported now");
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
                data_type, create_index_info.index_type, file_manager_context);
        case DataType::ARRAY: {
            return CreateCompositeScalarIndex(create_index_info.index_type,
                                              file_manager_context);
        }
        case DataType::JSON: {
            return CreateComplexScalarIndex(create_index_info.index_type,
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
                throw SegcoreError(
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
                return std::make_unique<VectorMemIndex<uint8_t>>(
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
                throw SegcoreError(
                    DataTypeInvalid,
                    fmt::format("invalid data type to build mem index: {}",
                                data_type));
        }
    }
}

IndexBasePtr
IndexFactory::CreateVectorIndex(
    const CreateIndexInfo& create_index_info,
    const storage::FileManagerContext& file_manager_context,
    std::shared_ptr<milvus_storage::Space> space) {
    auto data_type = create_index_info.field_type;
    auto index_type = create_index_info.index_type;
    auto metric_type = create_index_info.metric_type;
    auto version = create_index_info.index_engine_version;

    if (knowhere::UseDiskLoad(index_type, version)) {
        switch (data_type) {
            case DataType::VECTOR_FLOAT: {
                return std::make_unique<VectorDiskAnnIndex<float>>(
                    index_type,
                    metric_type,
                    version,
                    space,
                    file_manager_context);
            }
            case DataType::VECTOR_FLOAT16: {
                return std::make_unique<VectorDiskAnnIndex<float16>>(
                    index_type,
                    metric_type,
                    version,
                    space,
                    file_manager_context);
            }
            case DataType::VECTOR_BFLOAT16: {
                return std::make_unique<VectorDiskAnnIndex<bfloat16>>(
                    index_type,
                    metric_type,
                    version,
                    space,
                    file_manager_context);
            }
            case DataType::VECTOR_BINARY: {
                return std::make_unique<VectorDiskAnnIndex<bin1>>(
                    index_type,
                    metric_type,
                    version,
                    space,
                    file_manager_context);
            }
            case DataType::VECTOR_SPARSE_FLOAT: {
                return std::make_unique<VectorDiskAnnIndex<float>>(
                    index_type,
                    metric_type,
                    version,
                    space,
                    file_manager_context);
            }
            default:
                throw SegcoreError(
                    DataTypeInvalid,
                    fmt::format("invalid data type to build disk index: {}",
                                data_type));
        }
    } else {  // create mem index
        switch (data_type) {
            case DataType::VECTOR_FLOAT:
            case DataType::VECTOR_SPARSE_FLOAT: {
                return std::make_unique<VectorMemIndex<float>>(
                    create_index_info, file_manager_context, space);
            }
            case DataType::VECTOR_BINARY: {
                return std::make_unique<VectorMemIndex<uint8_t>>(
                    create_index_info, file_manager_context, space);
            }
            case DataType::VECTOR_FLOAT16: {
                return std::make_unique<VectorMemIndex<float16>>(
                    create_index_info, file_manager_context, space);
            }
            case DataType::VECTOR_BFLOAT16: {
                return std::make_unique<VectorMemIndex<bfloat16>>(
                    create_index_info, file_manager_context, space);
            }
            default:
                throw SegcoreError(
                    DataTypeInvalid,
                    fmt::format("invalid data type to build mem index: {}",
                                data_type));
        }
    }
}
}  // namespace milvus::index
