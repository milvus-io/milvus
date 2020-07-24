// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "server/ValidationUtil.h"
#include "db/Utils.h"
#include "knowhere/index/vector_index/ConfAdapter.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "utils/Log.h"
#include "utils/StringHelpFunctions.h"

#include <fiu-local.h>
#include <limits>
#include <string>

namespace milvus {
namespace server {

namespace {

constexpr size_t NAME_SIZE_LIMIT = 255;
constexpr int64_t COLLECTION_DIMENSION_LIMIT = 32768;
constexpr int32_t INDEX_FILE_SIZE_LIMIT = 4096;  // index trigger size max = 4096 MB
constexpr int64_t M_BYTE = 1024 * 1024;
constexpr int64_t MAX_INSERT_DATA_SIZE = 256 * M_BYTE;

Status
CheckParameterRange(const milvus::json& json_params, const std::string& param_name, int64_t min, int64_t max,
                    bool min_close = true, bool max_closed = true) {
    if (json_params.find(param_name) == json_params.end()) {
        std::string msg = "Parameter list must contain: ";
        msg += param_name;
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }

    try {
        int64_t value = json_params[param_name];
        bool min_err = min_close ? value < min : value <= min;
        bool max_err = max_closed ? value > max : value >= max;
        if (min_err || max_err) {
            std::string msg = "Invalid " + param_name + " value: " + std::to_string(value) + ". Valid range is " +
                              (min_close ? "[" : "(") + std::to_string(min) + ", " + std::to_string(max) +
                              (max_closed ? "]" : ")");
            LOG_SERVER_ERROR_ << msg;
            return Status(SERVER_INVALID_ARGUMENT, msg);
        }
    } catch (std::exception& e) {
        std::string msg = "Invalid " + param_name + ": ";
        msg += e.what();
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }

    return Status::OK();
}

Status
CheckParameterExistence(const milvus::json& json_params, const std::string& param_name) {
    if (json_params.find(param_name) == json_params.end()) {
        std::string msg = "Parameter list must contain: ";
        msg += param_name;
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }

    try {
        int64_t value = json_params[param_name];
        if (value < 0) {
            std::string msg = "Invalid " + param_name + " value: " + std::to_string(value);
            LOG_SERVER_ERROR_ << msg;
            return Status(SERVER_INVALID_ARGUMENT, msg);
        }
    } catch (std::exception& e) {
        std::string msg = "Invalid " + param_name + ": ";
        msg += e.what();
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }

    return Status::OK();
}

}  // namespace

Status
ValidateCollectionName(const std::string& collection_name) {
    // Collection name shouldn't be empty.
    if (collection_name.empty()) {
        std::string msg = "Collection name should not be empty.";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_COLLECTION_NAME, msg);
    }

    std::string invalid_msg = "Invalid collection name: " + collection_name + ". ";
    // Collection name size shouldn't exceed 255.
    if (collection_name.size() > NAME_SIZE_LIMIT) {
        std::string msg = invalid_msg + "The length of a collection name must be less than 255 characters.";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_COLLECTION_NAME, msg);
    }

    // Collection name first character should be underscore or character.
    char first_char = collection_name[0];
    if (first_char != '_' && std::isalpha(first_char) == 0) {
        std::string msg = invalid_msg + "The first character of a collection name must be an underscore or letter.";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_COLLECTION_NAME, msg);
    }

    int64_t table_name_size = collection_name.size();
    for (int64_t i = 1; i < table_name_size; ++i) {
        char name_char = collection_name[i];
        if (name_char != '_' && std::isalnum(name_char) == 0) {
            std::string msg = invalid_msg + "Collection name can only contain numbers, letters, and underscores.";
            LOG_SERVER_ERROR_ << msg;
            return Status(SERVER_INVALID_COLLECTION_NAME, msg);
        }
    }

    return Status::OK();
}

Status
ValidateFieldName(const std::string& field_name) {
    // Field name shouldn't be empty.
    if (field_name.empty()) {
        std::string msg = "Field name should not be empty.";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_FIELD_NAME, msg);
    }

    std::string invalid_msg = "Invalid field name: " + field_name + ". ";
    // Field name size shouldn't exceed 255.
    if (field_name.size() > NAME_SIZE_LIMIT) {
        std::string msg = invalid_msg + "The length of a field name must be less than 255 characters.";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_FIELD_NAME, msg);
    }

    // Field name first character should be underscore or character.
    char first_char = field_name[0];
    if (first_char != '_' && std::isalpha(first_char) == 0) {
        std::string msg = invalid_msg + "The first character of a field name must be an underscore or letter.";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_FIELD_NAME, msg);
    }

    int64_t field_name_size = field_name.size();
    for (int64_t i = 1; i < field_name_size; ++i) {
        char name_char = field_name[i];
        if (name_char != '_' && std::isalnum(name_char) == 0) {
            std::string msg = invalid_msg + "Field name cannot only contain numbers, letters, and underscores.";
            LOG_SERVER_ERROR_ << msg;
            return Status(SERVER_INVALID_FIELD_NAME, msg);
        }
    }

    return Status::OK();
}

Status
ValidateIndexName(const std::string& index_name) {
    // Index name shouldn't be empty.
    if (index_name.empty()) {
        std::string msg = "Index name should not be empty.";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_FIELD_NAME, msg);
    }

    std::string invalid_msg = "Invalid index name: " + index_name + ". ";
    // Index name size shouldn't exceed 255.
    if (index_name.size() > NAME_SIZE_LIMIT) {
        std::string msg = invalid_msg + "The length of a field name must be less than 255 characters.";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_FIELD_NAME, msg);
    }

    // Field name first character should be underscore or character.
    char first_char = index_name[0];
    if (first_char != '_' && std::isalpha(first_char) == 0) {
        std::string msg = invalid_msg + "The first character of a field name must be an underscore or letter.";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_FIELD_NAME, msg);
    }

    int64_t field_name_size = index_name.size();
    for (int64_t i = 1; i < field_name_size; ++i) {
        char name_char = index_name[i];
        if (name_char != '_' && std::isalnum(name_char) == 0) {
            std::string msg = invalid_msg + "Field name cannot only contain numbers, letters, and underscores.";
            LOG_SERVER_ERROR_ << msg;
            return Status(SERVER_INVALID_FIELD_NAME, msg);
        }
    }

    return Status::OK();
}

Status
ValidateTableDimension(int64_t dimension, int64_t metric_type) {
    if (dimension <= 0 || dimension > COLLECTION_DIMENSION_LIMIT) {
        std::string msg = "Invalid collection dimension: " + std::to_string(dimension) + ". " +
                          "The collection dimension must be within the range of 1 ~ " +
                          std::to_string(COLLECTION_DIMENSION_LIMIT) + ".";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_VECTOR_DIMENSION, msg);
    }

    if (milvus::engine::utils::IsBinaryMetricType(metric_type)) {
        if ((dimension % 8) != 0) {
            std::string msg = "Invalid collection dimension: " + std::to_string(dimension) + ". " +
                              "The collection dimension must be a multiple of 8";
            LOG_SERVER_ERROR_ << msg;
            return Status(SERVER_INVALID_VECTOR_DIMENSION, msg);
        }
    }

    return Status::OK();
}

Status
ValidateCollectionIndexType(int32_t index_type) {
    int engine_type = static_cast<int>(engine::EngineType(index_type));
    if (engine_type <= 0 || engine_type > static_cast<int>(engine::EngineType::MAX_VALUE)) {
        std::string index_type_str;
        for (auto it = engine::s_map_engine_type.begin(); it != engine::s_map_engine_type.end(); it++) {
            if (it->second == (engine::EngineType)index_type) {
                index_type_str = it->first;
            }
        }
        std::string msg =
            "Invalid index type: " + index_type_str + ". " + "Make sure the index type is in IndexType list.";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_INDEX_TYPE, msg);
    }

#ifndef MILVUS_GPU_VERSION
    // special case, hybird index only available in customize faiss library
    if (engine_type == static_cast<int>(engine::EngineType::FAISS_IVFSQ8H)) {
        std::string msg = "Unsupported index type: " + std::to_string(index_type);
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_INDEX_TYPE, msg);
    }
#endif

    return Status::OK();
}

Status
ValidateIndexParams(const milvus::json& index_params, int64_t dimension, int32_t index_type) {
    switch (index_type) {
        case (int32_t)engine::EngineType::FAISS_IDMAP:
        case (int32_t)engine::EngineType::FAISS_BIN_IDMAP: {
            break;
        }
        case (int32_t)engine::EngineType::FAISS_IVFFLAT:
        case (int32_t)engine::EngineType::FAISS_IVFSQ8:
        case (int32_t)engine::EngineType::FAISS_IVFSQ8NR:
        case (int32_t)engine::EngineType::FAISS_IVFSQ8H:
        case (int32_t)engine::EngineType::FAISS_BIN_IVFFLAT: {
            auto status = CheckParameterRange(index_params, knowhere::IndexParams::nlist, 1, 999999);
            if (!status.ok()) {
                return status;
            }
            break;
        }
        case (int32_t)engine::EngineType::FAISS_PQ: {
            auto status = CheckParameterRange(index_params, knowhere::IndexParams::nlist, 1, 999999);
            if (!status.ok()) {
                return status;
            }

            status = CheckParameterExistence(index_params, knowhere::IndexParams::m);
            if (!status.ok()) {
                return status;
            }

            // special check for 'm' parameter
            std::vector<int64_t> resset;
            milvus::knowhere::IVFPQConfAdapter::GetValidMList(dimension, resset);
            int64_t m_value = index_params[knowhere::IndexParams::m];
            if (resset.empty()) {
                std::string msg = "Invalid collection dimension, unable to get reasonable values for 'm'";
                LOG_SERVER_ERROR_ << msg;
                return Status(SERVER_INVALID_COLLECTION_DIMENSION, msg);
            }

            auto iter = std::find(std::begin(resset), std::end(resset), m_value);
            if (iter == std::end(resset)) {
                std::string msg =
                    "Invalid " + std::string(knowhere::IndexParams::m) + ", must be one of the following values: ";
                for (size_t i = 0; i < resset.size(); i++) {
                    if (i != 0) {
                        msg += ",";
                    }
                    msg += std::to_string(resset[i]);
                }

                LOG_SERVER_ERROR_ << msg;
                return Status(SERVER_INVALID_ARGUMENT, msg);
            }

            break;
        }
        case (int32_t)engine::EngineType::NSG_MIX: {
            auto status = CheckParameterRange(index_params, knowhere::IndexParams::search_length, 10, 300);
            if (!status.ok()) {
                return status;
            }
            status = CheckParameterRange(index_params, knowhere::IndexParams::out_degree, 5, 300);
            if (!status.ok()) {
                return status;
            }
            status = CheckParameterRange(index_params, knowhere::IndexParams::candidate, 50, 1000);
            if (!status.ok()) {
                return status;
            }
            status = CheckParameterRange(index_params, knowhere::IndexParams::knng, 5, 300);
            if (!status.ok()) {
                return status;
            }
            break;
        }
        case (int32_t)engine::EngineType::HNSW_SQ8NM:
        case (int32_t)engine::EngineType::HNSW: {
            auto status = CheckParameterRange(index_params, knowhere::IndexParams::M, 4, 64);
            if (!status.ok()) {
                return status;
            }
            status = CheckParameterRange(index_params, knowhere::IndexParams::efConstruction, 8, 512);
            if (!status.ok()) {
                return status;
            }
            break;
        }
        case (int32_t)engine::EngineType::ANNOY: {
            auto status = CheckParameterRange(index_params, knowhere::IndexParams::n_trees, 1, 1024);
            if (!status.ok()) {
                return status;
            }
            break;
        }
    }
    return Status::OK();
}

Status
ValidateSearchParams(const milvus::json& search_params, const engine::meta::CollectionSchema& collection_schema,
                     int64_t topk) {
    switch (collection_schema.engine_type_) {
        case (int32_t)engine::EngineType::FAISS_IDMAP:
        case (int32_t)engine::EngineType::FAISS_BIN_IDMAP: {
            break;
        }
        case (int32_t)engine::EngineType::FAISS_IVFFLAT:
        case (int32_t)engine::EngineType::FAISS_IVFSQ8:
        case (int32_t)engine::EngineType::FAISS_IVFSQ8NR:
        case (int32_t)engine::EngineType::FAISS_IVFSQ8H:
        case (int32_t)engine::EngineType::FAISS_BIN_IVFFLAT:
        case (int32_t)engine::EngineType::FAISS_PQ: {
            auto status = CheckParameterRange(search_params, knowhere::IndexParams::nprobe, 1, 999999);
            if (!status.ok()) {
                return status;
            }
            break;
        }
        case (int32_t)engine::EngineType::NSG_MIX: {
            auto status = CheckParameterRange(search_params, knowhere::IndexParams::search_length, 10, 300);
            if (!status.ok()) {
                return status;
            }
            break;
        }
        case (int32_t)engine::EngineType::HNSW_SQ8NM:
        case (int32_t)engine::EngineType::HNSW: {
            auto status = CheckParameterRange(search_params, knowhere::IndexParams::ef, topk, 4096);
            if (!status.ok()) {
                return status;
            }
            break;
        }
        case (int32_t)engine::EngineType::ANNOY: {
            auto status = CheckParameterRange(search_params, knowhere::IndexParams::search_k,
                                              std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
            if (!status.ok()) {
                return status;
            }
            break;
        }
    }
    return Status::OK();
}

Status
ValidateVectorData(const engine::VectorsData& vectors, const engine::meta::CollectionSchema& collection_schema) {
    uint64_t vector_count = vectors.vector_count_;
    if ((vectors.float_data_.empty() && vectors.binary_data_.empty()) || vector_count == 0) {
        return Status(SERVER_INVALID_ROWRECORD_ARRAY,
                      "The vector array is empty. Make sure you have entered vector records.");
    }

    if (engine::utils::IsBinaryMetricType(collection_schema.metric_type_)) {
        // check prepared binary data
        if (vectors.binary_data_.size() % vector_count != 0) {
            return Status(SERVER_INVALID_ROWRECORD_ARRAY,
                          "The vector dimension must be equal to the collection dimension.");
        }

        if (vectors.binary_data_.size() * 8 / vector_count != collection_schema.dimension_) {
            return Status(SERVER_INVALID_VECTOR_DIMENSION,
                          "The vector dimension must be equal to the collection dimension.");
        }
    } else {
        // check prepared float data
        fiu_do_on("SearchRequest.OnExecute.invalod_rowrecord_array", vector_count = vectors.float_data_.size() + 1);
        if (vectors.float_data_.size() % vector_count != 0) {
            return Status(SERVER_INVALID_ROWRECORD_ARRAY,
                          "The vector dimension must be equal to the collection dimension.");
        }
        if (vectors.float_data_.size() / vector_count != collection_schema.dimension_) {
            return Status(SERVER_INVALID_VECTOR_DIMENSION,
                          "The vector dimension must be equal to the collection dimension.");
        }
    }

    return Status::OK();
}

Status
ValidateVectorDataSize(const engine::VectorsData& vectors, const engine::meta::CollectionSchema& collection_schema) {
    std::string msg =
        "The amount of data inserted each time cannot exceed " + std::to_string(MAX_INSERT_DATA_SIZE / M_BYTE) + " MB";
    if (engine::utils::IsBinaryMetricType(collection_schema.metric_type_)) {
        if (vectors.binary_data_.size() > MAX_INSERT_DATA_SIZE) {
            return Status(SERVER_INVALID_ROWRECORD_ARRAY, msg);
        }

    } else {
        if (vectors.float_data_.size() * sizeof(float) > MAX_INSERT_DATA_SIZE) {
            return Status(SERVER_INVALID_ROWRECORD_ARRAY, msg);
        }
    }

    return Status::OK();
}

Status
ValidateCollectionIndexFileSize(int64_t index_file_size) {
    if (index_file_size <= 0 || index_file_size > INDEX_FILE_SIZE_LIMIT) {
        std::string msg = "Invalid index file size: " + std::to_string(index_file_size) + ". " +
                          "The index file size must be within the range of 1 ~ " +
                          std::to_string(INDEX_FILE_SIZE_LIMIT) + ".";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_INDEX_FILE_SIZE, msg);
    }

    return Status::OK();
}

Status
ValidateCollectionIndexMetricType(int32_t metric_type) {
    if (metric_type <= 0 || metric_type > static_cast<int32_t>(engine::MetricType::MAX_VALUE)) {
        std::string msg = "Invalid index metric type: " + std::to_string(metric_type) + ". " +
                          "Make sure the metric type is in MetricType list.";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_INDEX_METRIC_TYPE, msg);
    }
    return Status::OK();
}

Status
ValidateSearchTopk(int64_t top_k) {
    if (top_k <= 0 || top_k > QUERY_MAX_TOPK) {
        std::string msg =
            "Invalid topk: " + std::to_string(top_k) + ". " + "The topk must be within the range of 1 ~ 2048.";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_TOPK, msg);
    }

    return Status::OK();
}

Status
ValidatePartitionName(const std::string& partition_name) {
    if (partition_name.empty()) {
        std::string msg = "Partition name should not be empty.";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_COLLECTION_NAME, msg);
    }

    std::string invalid_msg = "Invalid partition name: " + partition_name + ". ";
    // Collection name size shouldn't exceed 255.
    if (partition_name.size() > NAME_SIZE_LIMIT) {
        std::string msg = invalid_msg + "The length of a partition name must be less than 255 characters.";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_COLLECTION_NAME, msg);
    }

    // Collection name first character should be underscore or character.
    char first_char = partition_name[0];
    if (first_char != '_' && std::isalpha(first_char) == 0) {
        std::string msg = invalid_msg + "The first character of a partition name must be an underscore or letter.";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_COLLECTION_NAME, msg);
    }

    int64_t table_name_size = partition_name.size();
    for (int64_t i = 1; i < table_name_size; ++i) {
        char name_char = partition_name[i];
        if (name_char != '_' && std::isalnum(name_char) == 0) {
            std::string msg = invalid_msg + "Partition name can only contain numbers, letters, and underscores.";
            LOG_SERVER_ERROR_ << msg;
            return Status(SERVER_INVALID_COLLECTION_NAME, msg);
        }
    }

    return Status::OK();
}

Status
ValidatePartitionTags(const std::vector<std::string>& partition_tags) {
    for (const std::string& tag : partition_tags) {
        // trim side-blank of tag, only compare valid characters
        // for example: " ab cd " is treated as "ab cd"
        std::string valid_tag = tag;
        StringHelpFunctions::TrimStringBlank(valid_tag);
        if (valid_tag.empty()) {
            std::string msg = "Invalid partition tag: " + valid_tag + ". " + "Partition tag should not be empty.";
            LOG_SERVER_ERROR_ << msg;
            return Status(SERVER_INVALID_PARTITION_TAG, msg);
        }

        // max length of partition tag
        if (valid_tag.length() > 255) {
            std::string msg = "Invalid partition tag: " + valid_tag + ". " + "Partition tag exceed max length(255).";
            LOG_SERVER_ERROR_ << msg;
            return Status(SERVER_INVALID_PARTITION_TAG, msg);
        }
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
