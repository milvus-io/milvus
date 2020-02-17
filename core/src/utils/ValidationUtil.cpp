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
#include "db/engine/ExecutionEngine.h"
#include "utils/StringHelpFunctions.h"

#include <arpa/inet.h>

#ifdef MILVUS_GPU_VERSION

#include <cuda_runtime.h>

#endif

#include <fiu-local.h>
#include <algorithm>
#include <cmath>
#include <regex>
#include <string>

namespace milvus {
namespace server {

constexpr size_t TABLE_NAME_SIZE_LIMIT = 255;
constexpr int64_t TABLE_DIMENSION_LIMIT = 32768;
constexpr int32_t INDEX_FILE_SIZE_LIMIT = 4096;  // index trigger size max = 4096 MB

Status
ValidationUtil::ValidateTableName(const std::string& table_name) {
    // Table name shouldn't be empty.
    if (table_name.empty()) {
        std::string msg = "Table name should not be empty.";
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_TABLE_NAME, msg);
    }

    std::string invalid_msg = "Invalid table name: " + table_name + ". ";
    // Table name size shouldn't exceed 16384.
    if (table_name.size() > TABLE_NAME_SIZE_LIMIT) {
        std::string msg = invalid_msg + "The length of a table name must be less than 255 characters.";
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_TABLE_NAME, msg);
    }

    // Table name first character should be underscore or character.
    char first_char = table_name[0];
    if (first_char != '_' && std::isalpha(first_char) == 0) {
        std::string msg = invalid_msg + "The first character of a table name must be an underscore or letter.";
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_TABLE_NAME, msg);
    }

    int64_t table_name_size = table_name.size();
    for (int64_t i = 1; i < table_name_size; ++i) {
        char name_char = table_name[i];
        if (name_char != '_' && std::isalnum(name_char) == 0) {
            std::string msg = invalid_msg + "Table name can only contain numbers, letters, and underscores.";
            SERVER_LOG_ERROR << msg;
            return Status(SERVER_INVALID_TABLE_NAME, msg);
        }
    }

    return Status::OK();
}

Status
ValidationUtil::ValidateTableDimension(int64_t dimension) {
    if (dimension <= 0 || dimension > TABLE_DIMENSION_LIMIT) {
        std::string msg = "Invalid table dimension: " + std::to_string(dimension) + ". " +
                          "The table dimension must be within the range of 1 ~ " +
                          std::to_string(TABLE_DIMENSION_LIMIT) + ".";
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_VECTOR_DIMENSION, msg);
    } else {
        return Status::OK();
    }
}

Status
ValidationUtil::ValidateTableIndexType(int32_t index_type) {
    int engine_type = static_cast<int>(engine::EngineType(index_type));
    if (engine_type <= 0 || engine_type > static_cast<int>(engine::EngineType::MAX_VALUE)) {
        std::string msg = "Invalid index type: " + std::to_string(index_type) + ". " +
                          "Make sure the index type is in IndexType list.";
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_INDEX_TYPE, msg);
    }

#ifndef CUSTOMIZATION
    // special case, hybird index only available in customize faiss library
    if (engine_type == static_cast<int>(engine::EngineType::FAISS_IVFSQ8H)) {
        std::string msg = "Unsupported index type: " + std::to_string(index_type);
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_INDEX_TYPE, msg);
    }
#endif

    return Status::OK();
}

bool
ValidationUtil::IsBinaryIndexType(int32_t index_type) {
    return (index_type == static_cast<int32_t>(engine::EngineType::FAISS_BIN_IDMAP)) ||
           (index_type == static_cast<int32_t>(engine::EngineType::FAISS_BIN_IVFFLAT));
}

Status
ValidationUtil::ValidateTableIndexNlist(int32_t nlist) {
    if (nlist <= 0) {
        std::string msg =
            "Invalid index nlist: " + std::to_string(nlist) + ". " + "The index nlist must be greater than 0.";
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_INDEX_NLIST, msg);
    }

    return Status::OK();
}

Status
ValidationUtil::ValidateTableIndexFileSize(int64_t index_file_size) {
    if (index_file_size <= 0 || index_file_size > INDEX_FILE_SIZE_LIMIT) {
        std::string msg = "Invalid index file size: " + std::to_string(index_file_size) + ". " +
                          "The index file size must be within the range of 1 ~ " +
                          std::to_string(INDEX_FILE_SIZE_LIMIT) + ".";
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_INDEX_FILE_SIZE, msg);
    }

    return Status::OK();
}

Status
ValidationUtil::ValidateTableIndexMetricType(int32_t metric_type) {
    if (metric_type <= 0 || metric_type > static_cast<int32_t>(engine::MetricType::MAX_VALUE)) {
        std::string msg = "Invalid index metric type: " + std::to_string(metric_type) + ". " +
                          "Make sure the metric type is in MetricType list.";
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_INDEX_METRIC_TYPE, msg);
    }
    return Status::OK();
}

bool
ValidationUtil::IsBinaryMetricType(int32_t metric_type) {
    return (metric_type == static_cast<int32_t>(engine::MetricType::HAMMING)) ||
           (metric_type == static_cast<int32_t>(engine::MetricType::JACCARD)) ||
           (metric_type == static_cast<int32_t>(engine::MetricType::TANIMOTO));
}

Status
ValidationUtil::ValidateSearchTopk(int64_t top_k, const engine::meta::TableSchema& table_schema) {
    if (top_k <= 0 || top_k > 2048) {
        std::string msg =
            "Invalid topk: " + std::to_string(top_k) + ". " + "The topk must be within the range of 1 ~ 2048.";
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_TOPK, msg);
    }

    return Status::OK();
}

Status
ValidationUtil::ValidateSearchNprobe(int64_t nprobe, const engine::meta::TableSchema& table_schema) {
    if (nprobe <= 0 || nprobe > table_schema.nlist_) {
        std::string msg = "Invalid nprobe: " + std::to_string(nprobe) + ". " +
                          "The nprobe must be within the range of 1 ~ index nlist.";
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_NPROBE, msg);
    }

    return Status::OK();
}

Status
ValidationUtil::ValidatePartitionName(const std::string& partition_name) {
    if (partition_name.empty()) {
        std::string msg = "Partition name should not be empty.";
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_TABLE_NAME, msg);
    }

    std::string invalid_msg = "Invalid partition name: " + partition_name + ". ";
    // Table name size shouldn't exceed 16384.
    if (partition_name.size() > TABLE_NAME_SIZE_LIMIT) {
        std::string msg = invalid_msg + "The length of a partition name must be less than 255 characters.";
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_TABLE_NAME, msg);
    }

    // Table name first character should be underscore or character.
    char first_char = partition_name[0];
    if (first_char != '_' && std::isalpha(first_char) == 0) {
        std::string msg = invalid_msg + "The first character of a partition name must be an underscore or letter.";
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_TABLE_NAME, msg);
    }

    int64_t table_name_size = partition_name.size();
    for (int64_t i = 1; i < table_name_size; ++i) {
        char name_char = partition_name[i];
        if (name_char != '_' && std::isalnum(name_char) == 0) {
            std::string msg = invalid_msg + "Partition name can only contain numbers, letters, and underscores.";
            SERVER_LOG_ERROR << msg;
            return Status(SERVER_INVALID_TABLE_NAME, msg);
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
            SERVER_LOG_ERROR << msg;
            return Status(SERVER_INVALID_TABLE_NAME, msg);
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
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_UNEXPECTED_ERROR, msg);
    }

    if (gpu_index >= num_devices) {
        std::string msg = "Invalid gpu index: " + std::to_string(gpu_index);
        SERVER_LOG_ERROR << msg;
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
        SERVER_LOG_ERROR << msg;
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
            SERVER_LOG_ERROR << msg;
            return Status(SERVER_INVALID_ARGUMENT, msg);
        }
        default: {
            std::string msg = "IP address conversion error: " + ip_address;
            SERVER_LOG_ERROR << msg;
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
            SERVER_LOG_ERROR << "Invalid dialect in URI: dialect = " << dialect;
            okay = false;
        }

        /*
         *      Could be DNS, skip checking
         *
                std::string host = pieces_match[4].str();
                if (!host.empty() && host != "localhost") {
                    if (ValidateIpAddress(host) != SERVER_SUCCESS) {
                        SERVER_LOG_ERROR << "Invalid host ip address in uri = " << host;
                        okay = false;
                    }
                }
        */

        std::string port = pieces_match[5].str();
        if (!port.empty()) {
            auto status = ValidateStringIsNumber(port);
            if (!status.ok()) {
                SERVER_LOG_ERROR << "Invalid port in uri = " << port;
                okay = false;
            }
        }
    } else {
        SERVER_LOG_ERROR << "Wrong URI format: URI = " << uri;
        okay = false;
    }

    return (okay ? Status::OK() : Status(SERVER_INVALID_ARGUMENT, "Invalid db backend uri"));
}

}  // namespace server
}  // namespace milvus
