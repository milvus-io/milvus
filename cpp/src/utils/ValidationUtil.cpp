// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#include "db/engine/ExecutionEngine.h"
#include "ValidationUtil.h"
#include "Log.h"

#include <cuda_runtime.h>
#include <arpa/inet.h>
#include <regex>
#include <algorithm>


namespace zilliz {
namespace milvus {
namespace server {

constexpr size_t TABLE_NAME_SIZE_LIMIT = 255;
constexpr int64_t TABLE_DIMENSION_LIMIT = 16384;
constexpr int32_t INDEX_FILE_SIZE_LIMIT = 4096; //index trigger size max = 4096 MB

Status
ValidationUtil::ValidateTableName(const std::string &table_name) {

    // Table name shouldn't be empty.
    if (table_name.empty()) {
        std::string msg = "Empty table name";
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_TABLE_NAME, msg);
    }

    // Table name size shouldn't exceed 16384.
    if (table_name.size() > TABLE_NAME_SIZE_LIMIT) {
        std::string msg = "Table name size exceed the limitation";
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_TABLE_NAME, msg);
    }

    // Table name first character should be underscore or character.
    char first_char = table_name[0];
    if (first_char != '_' && std::isalpha(first_char) == 0) {
        std::string msg = "Table name first character isn't underscore or character";
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_TABLE_NAME, msg);
    }

    int64_t table_name_size = table_name.size();
    for (int64_t i = 1; i < table_name_size; ++i) {
        char name_char = table_name[i];
        if (name_char != '_' && std::isalnum(name_char) == 0) {
            std::string msg = "Table name character isn't underscore or alphanumber";
            SERVER_LOG_ERROR << msg;
            return Status(SERVER_INVALID_TABLE_NAME, msg);
        }
    }

    return Status::OK();
}

Status
ValidationUtil::ValidateTableDimension(int64_t dimension) {
    if (dimension <= 0 || dimension > TABLE_DIMENSION_LIMIT) {
        std::string msg = "Table dimension excceed the limitation: " + std::to_string(TABLE_DIMENSION_LIMIT);
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_VECTOR_DIMENSION, msg);
    }
    else {
        return Status::OK();
    }
}

Status
ValidationUtil::ValidateTableIndexType(int32_t index_type) {
    int engine_type = (int) engine::EngineType(index_type);
    if (engine_type <= 0 || engine_type > (int) engine::EngineType::MAX_VALUE) {
        std::string msg = "Invalid index type: " + std::to_string(index_type);
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_INDEX_TYPE, msg);
    }

    return Status::OK();
}

Status
ValidationUtil::ValidateTableIndexNlist(int32_t nlist) {
    if (nlist <= 0) {
        std::string msg = "Invalid nlist value: " + std::to_string(nlist);
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_INDEX_NLIST, msg);
    }

    return Status::OK();
}

Status
ValidationUtil::ValidateTableIndexFileSize(int64_t index_file_size) {
    if (index_file_size <= 0 || index_file_size > INDEX_FILE_SIZE_LIMIT) {
        std::string msg = "Invalid index file size: " + std::to_string(index_file_size);
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_INDEX_FILE_SIZE, msg);
    }

    return Status::OK();
}

Status
ValidationUtil::ValidateTableIndexMetricType(int32_t metric_type) {
    if (metric_type != (int32_t) engine::MetricType::L2 && metric_type != (int32_t) engine::MetricType::IP) {
        std::string msg = "Invalid metric type: " + std::to_string(metric_type);
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_INDEX_METRIC_TYPE, msg);
    }
    return Status::OK();
}

Status
ValidationUtil::ValidateSearchTopk(int64_t top_k, const engine::meta::TableSchema &table_schema) {
    if (top_k <= 0 || top_k > 2048) {
        std::string msg = "Invalid top k value: " + std::to_string(top_k);
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_TOPK, msg);
    }

    return Status::OK();
}

Status
ValidationUtil::ValidateSearchNprobe(int64_t nprobe, const engine::meta::TableSchema &table_schema) {
    if (nprobe <= 0 || nprobe > table_schema.nlist_) {
        std::string msg = "Invalid nprobe value: " + std::to_string(nprobe);
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_NPROBE, msg);
    }

    return Status::OK();
}

Status
ValidationUtil::ValidateGpuIndex(uint32_t gpu_index) {
    int num_devices = 0;
    auto cuda_err = cudaGetDeviceCount(&num_devices);
    if (cuda_err) {
        std::string msg = "Failed to get gpu card number, cuda error:" + std::to_string(cuda_err);
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_UNEXPECTED_ERROR, msg);
    }

    if (gpu_index >= num_devices) {
        std::string msg = "Invalid gpu index: " + std::to_string(gpu_index);
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_INVALID_ARGUMENT, msg);
    }

    return Status::OK();
}

Status
ValidationUtil::GetGpuMemory(uint32_t gpu_index, size_t &memory) {
    cudaDeviceProp deviceProp;
    auto cuda_err = cudaGetDeviceProperties(&deviceProp, gpu_index);
    if (cuda_err) {
        std::string msg = "Failed to get gpu properties, cuda error:" + std::to_string(cuda_err);
        SERVER_LOG_ERROR << msg;
        return Status(SERVER_UNEXPECTED_ERROR, msg);
    }

    memory = deviceProp.totalGlobalMem;
    return Status::OK();
}

Status
ValidationUtil::ValidateIpAddress(const std::string &ip_address) {

    struct in_addr address;

    int result = inet_pton(AF_INET, ip_address.c_str(), &address);

    switch (result) {
        case 1:return Status::OK();
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
ValidationUtil::ValidateStringIsNumber(const std::string &string) {
    if (!string.empty() && std::all_of(string.begin(), string.end(), ::isdigit)) {
        return Status::OK();
    }
    else {
        return Status(SERVER_INVALID_ARGUMENT, "Not a number");
    }
}

Status
ValidationUtil::ValidateStringIsBool(std::string &str) {
    std::transform(str.begin(), str.end(), str.begin(), ::tolower);
    if (str == "true" || str == "on" || str == "yes" || str == "1" ||
        str == "false" || str == "off" || str == "no" || str == "0" ||
        str.empty()) {
        return Status::OK();
    }
    else {
        return Status(SERVER_INVALID_ARGUMENT, "Not a boolean: " + str);
    }
}

Status
ValidationUtil::ValidateStringIsDouble(const std::string &str, double &val) {
    char *end = nullptr;
    val = std::strtod(str.c_str(), &end);
    if (end != str.c_str() && *end == '\0' && val != HUGE_VAL) {
        return Status::OK();
    }
    else {
        return Status(SERVER_INVALID_ARGUMENT, "Not a double value: " + str);
    }
}

Status
ValidationUtil::ValidateDbURI(const std::string &uri) {
    std::string dialectRegex = "(.*)";
    std::string usernameRegex = "(.*)";
    std::string passwordRegex = "(.*)";
    std::string hostRegex = "(.*)";
    std::string portRegex = "(.*)";
    std::string dbNameRegex = "(.*)";
    std::string uriRegexStr = dialectRegex + "\\:\\/\\/" +
        usernameRegex + "\\:" +
        passwordRegex + "\\@" +
        hostRegex + "\\:" +
        portRegex + "\\/" +
        dbNameRegex;
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
    }
    else {
        SERVER_LOG_ERROR << "Wrong URI format: URI = " << uri;
        okay = false;
    }

    return (okay ? Status::OK() : Status(SERVER_INVALID_ARGUMENT, "Invalid db backend uri"));
}

}
}
}