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

#include "utils/ValidationUtil.h"
#include "Log.h"
#include "db/Types.h"
#include "db/Utils.h"
#include "db/engine/ExecutionEngine.h"
#include "knowhere/index/vector_index/ConfAdapter.h"
#include "knowhere/index/vector_index/helpers/IndexParameter.h"
#include "utils/StringHelpFunctions.h"

#include <arpa/inet.h>

#ifdef MILVUS_GPU_VERSION

#include <cuda_runtime.h>

#endif

#include <fiu-local.h>
#include <algorithm>
#include <cmath>
#include <limits>
#include <regex>
#include <string>

namespace milvus {
namespace server {

namespace {
constexpr size_t COLLECTION_NAME_SIZE_LIMIT = 255;
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
ValidationUtil::ValidateCollectionName(const std::string& collection_name) {
    // Collection name shouldn't be empty.
    if (collection_name.empty()) {
        std::string msg = "Collection name should not be empty.";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_COLLECTION_NAME, msg);
    }

    std::string invalid_msg = "Invalid collection name: " + collection_name + ". ";
    // Collection name size shouldn't exceed 16384.
    if (collection_name.size() > COLLECTION_NAME_SIZE_LIMIT) {
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
ValidationUtil::ValidateTableDimension(int64_t dimension, int64_t metric_type) {
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
ValidationUtil::ValidateCollectionIndexType(int32_t index_type) {
    int engine_type = static_cast<int>(engine::EngineType(index_type));
    if (engine_type <= 0 || engine_type > static_cast<int>(engine::EngineType::MAX_VALUE)) {
        std::string msg = "Invalid index type: " + std::to_string(index_type) + ". " +
                          "Make sure the index type is in IndexType list.";
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
ValidationUtil::ValidateIndexParams(const milvus::json& index_params,
                                    const engine::meta::CollectionSchema& collection_schema, int32_t index_type) {
    switch (index_type) {
        case (int32_t)engine::EngineType::FAISS_IDMAP:
        case (int32_t)engine::EngineType::FAISS_BIN_IDMAP: {
            break;
        }
        case (int32_t)engine::EngineType::FAISS_IVFFLAT:
        case (int32_t)engine::EngineType::FAISS_IVFSQ8:
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
            milvus::knowhere::IVFPQConfAdapter::GetValidMList(collection_schema.dimension_, resset);
            int64_t m_value = index_params[index_params, knowhere::IndexParams::m];
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
        case (int32_t)engine::EngineType::HNSW: {
            auto status = CheckParameterRange(index_params, knowhere::IndexParams::M, 5, 48);
            if (!status.ok()) {
                return status;
            }
            status = CheckParameterRange(index_params, knowhere::IndexParams::efConstruction, 100, 500);
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
ValidationUtil::ValidateSearchParams(const milvus::json& search_params,
                                     const engine::meta::CollectionSchema& collection_schema, int64_t topk) {
    switch (collection_schema.engine_type_) {
        case (int32_t)engine::EngineType::FAISS_IDMAP:
        case (int32_t)engine::EngineType::FAISS_BIN_IDMAP: {
            break;
        }
        case (int32_t)engine::EngineType::FAISS_IVFFLAT:
        case (int32_t)engine::EngineType::FAISS_IVFSQ8:
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
        case (int32_t)engine::EngineType::HNSW: {
            auto status = CheckParameterRange(search_params, knowhere::IndexParams::ef, topk, 4096);
            if (!status.ok()) {
                return status;
            }
            break;
        }
        case (int32_t)engine::EngineType::ANNOY: {
            auto status = CheckParameterRange(search_params, knowhere::IndexParams::search_k, topk,
                                              std::numeric_limits<int64_t>::max());
            if (!status.ok()) {
                return status;
            }
            break;
        }
    }
    return Status::OK();
}

Status
ValidationUtil::ValidateVectorData(const engine::VectorsData& vectors,
                                   const engine::meta::CollectionSchema& collection_schema) {
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
ValidationUtil::ValidateVectorDataSize(const engine::VectorsData& vectors,
                                       const engine::meta::CollectionSchema& collection_schema) {
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
ValidationUtil::ValidateCollectionIndexFileSize(int64_t index_file_size) {
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
ValidationUtil::ValidateCollectionIndexMetricType(int32_t metric_type) {
    if (metric_type <= 0 || metric_type > static_cast<int32_t>(engine::MetricType::MAX_VALUE)) {
        std::string msg = "Invalid index metric type: " + std::to_string(metric_type) + ". " +
                          "Make sure the metric type is in MetricType list.";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_INDEX_METRIC_TYPE, msg);
    }
    return Status::OK();
}

Status
ValidationUtil::ValidateSearchTopk(int64_t top_k) {
    if (top_k <= 0 || top_k > QUERY_MAX_TOPK) {
        std::string msg =
            "Invalid topk: " + std::to_string(top_k) + ". " + "The topk must be within the range of 1 ~ 2048.";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_TOPK, msg);
    }

    return Status::OK();
}

Status
ValidationUtil::ValidatePartitionName(const std::string& partition_name) {
    if (partition_name.empty()) {
        std::string msg = "Partition name should not be empty.";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_COLLECTION_NAME, msg);
    }

    std::string invalid_msg = "Invalid partition name: " + partition_name + ". ";
    // Collection name size shouldn't exceed 16384.
    if (partition_name.size() > COLLECTION_NAME_SIZE_LIMIT) {
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
ValidationUtil::ValidatePartitionTags(const std::vector<std::string>& partition_tags) {
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

Status
ValidationUtil::ValidateGpuIndex(int32_t gpu_index) {
#ifdef MILVUS_GPU_VERSION
    int num_devices = 0;
    auto cuda_err = cudaGetDeviceCount(&num_devices);
    fiu_do_on("ValidationUtil.ValidateGpuIndex.get_device_count_fail", cuda_err = cudaError::cudaErrorUnknown);

    if (cuda_err != cudaSuccess) {
        std::string msg = "Failed to get gpu card number, cuda error:" + std::to_string(cuda_err);
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_UNEXPECTED_ERROR, msg);
    }

    if (gpu_index >= num_devices) {
        std::string msg = "Invalid gpu index: " + std::to_string(gpu_index);
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }
#endif

    return Status::OK();
}

#ifdef MILVUS_GPU_VERSION

Status
ValidationUtil::GetGpuMemory(int32_t gpu_index, size_t& memory) {
    fiu_return_on("ValidationUtil.GetGpuMemory.return_error", Status(SERVER_UNEXPECTED_ERROR, ""));

    cudaDeviceProp deviceProp;
    auto cuda_err = cudaGetDeviceProperties(&deviceProp, gpu_index);
    if (cuda_err) {
        std::string msg = "Failed to get gpu properties for gpu" + std::to_string(gpu_index) +
                          " , cuda error:" + std::to_string(cuda_err);
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_UNEXPECTED_ERROR, msg);
    }

    memory = deviceProp.totalGlobalMem;
    return Status::OK();
}

#endif

Status
ValidationUtil::ValidateIpAddress(const std::string& ip_address) {
    struct in_addr address;

    int result = inet_pton(AF_INET, ip_address.c_str(), &address);
    fiu_do_on("ValidationUtil.ValidateIpAddress.error_ip_result", result = 2);

    switch (result) {
        case 1:
            return Status::OK();
        case 0: {
            std::string msg = "Invalid IP address: " + ip_address;
            LOG_SERVER_ERROR_ << msg;
            return Status(SERVER_INVALID_ARGUMENT, msg);
        }
        default: {
            std::string msg = "IP address conversion error: " + ip_address;
            LOG_SERVER_ERROR_ << msg;
            return Status(SERVER_UNEXPECTED_ERROR, msg);
        }
    }
}

Status
ValidationUtil::ValidateStringIsNumber(const std::string& str) {
    if (str.empty() || !std::all_of(str.begin(), str.end(), ::isdigit)) {
        return Status(SERVER_INVALID_ARGUMENT, "Invalid number");
    }
    try {
        int64_t value = std::stol(str);
        fiu_do_on("ValidationUtil.ValidateStringIsNumber.throw_exception", throw std::exception());
        if (value < 0) {
            return Status(SERVER_INVALID_ARGUMENT, "Negative number");
        }
    } catch (...) {
        return Status(SERVER_INVALID_ARGUMENT, "Invalid number");
    }
    return Status::OK();
}

Status
ValidationUtil::ValidateStringIsBool(const std::string& str) {
    fiu_return_on("ValidateStringNotBool", Status(SERVER_INVALID_ARGUMENT, "Invalid boolean: " + str));
    std::string s = str;
    std::transform(s.begin(), s.end(), s.begin(), ::tolower);
    if (s == "true" || s == "on" || s == "yes" || s == "1" || s == "false" || s == "off" || s == "no" || s == "0" ||
        s.empty()) {
        return Status::OK();
    }
    return Status(SERVER_INVALID_ARGUMENT, "Invalid boolean: " + str);
}

Status
ValidationUtil::ValidateStringIsFloat(const std::string& str) {
    try {
        float val = std::stof(str);
        if (val < 0.0) {
            return Status(SERVER_INVALID_ARGUMENT, "Negative float: " + str);
        }
    } catch (...) {
        return Status(SERVER_INVALID_ARGUMENT, "Invalid float: " + str);
    }
    return Status::OK();
}

Status
ValidationUtil::ValidateDbURI(const std::string& uri) {
    std::string dialectRegex = "(.*)";
    std::string usernameRegex = "(.*)";
    std::string passwordRegex = "(.*)";
    std::string hostRegex = "(.*)";
    std::string portRegex = "(.*)";
    std::string dbNameRegex = "(.*)";
    std::string uriRegexStr = dialectRegex + "\\:\\/\\/" + usernameRegex + "\\:" + passwordRegex + "\\@" + hostRegex +
                              "\\:" + portRegex + "\\/" + dbNameRegex;
    std::regex uriRegex(uriRegexStr);
    std::smatch pieces_match;

    bool okay = true;

    if (std::regex_match(uri, pieces_match, uriRegex)) {
        std::string dialect = pieces_match[1].str();
        std::transform(dialect.begin(), dialect.end(), dialect.begin(), ::tolower);
        if (dialect.find("mysql") == std::string::npos && dialect.find("sqlite") == std::string::npos) {
            LOG_SERVER_ERROR_ << "Invalid dialect in URI: dialect = " << dialect;
            okay = false;
        }

        /*
         *      Could be DNS, skip checking
         *
                std::string host = pieces_match[4].str();
                if (!host.empty() && host != "localhost") {
                    if (ValidateIpAddress(host) != SERVER_SUCCESS) {
                        LOG_SERVER_ERROR_ << "Invalid host ip address in uri = " << host;
                        okay = false;
                    }
                }
        */

        std::string port = pieces_match[5].str();
        if (!port.empty()) {
            auto status = ValidateStringIsNumber(port);
            if (!status.ok()) {
                LOG_SERVER_ERROR_ << "Invalid port in uri = " << port;
                okay = false;
            }
        }
    } else {
        LOG_SERVER_ERROR_ << "Wrong URI format: URI = " << uri;
        okay = false;
    }

    return (okay ? Status::OK() : Status(SERVER_INVALID_ARGUMENT, "Invalid db backend uri"));
}

Status
ValidationUtil::ValidateStoragePath(const std::string& path) {
    // Validate storage path if is valid, only correct absolute path will be validated pass
    // Invalid path only contain character[a-zA-Z], number[0-9], '-', and '_',
    // and path must start with '/'.
    // examples below are invalid
    // '/a//a', '/a--/a', '/-a/a', '/a@#/a', 'aaa/sfs'
    std::string path_pattern = "^\\/(\\w+-?\\/?)+$";
    std::regex regex(path_pattern);

    return std::regex_match(path, regex) ? Status::OK() : Status(SERVER_INVALID_ARGUMENT, "Invalid file path");
}

bool
ValidationUtil::IsNumber(const std::string& s) {
    return !s.empty() && std::all_of(s.begin(), s.end(), ::isdigit);
}

}  // namespace server
}  // namespace milvus
