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
#include <set>
#include <string>

namespace milvus {
namespace server {

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
ValidationUtil::GetGpuMemory(int32_t gpu_index, int64_t& memory) {
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

Status
ValidationUtil::ValidateLogLevel(const std::string& level) {
    std::set<std::string> supported_level{"debug", "info", "warning", "error", "fatal"};

    return supported_level.find(level) != supported_level.end()
               ? Status::OK()
               : Status(SERVER_INVALID_ARGUMENT, "Log level must be one of debug, info, warning, error and fatal.");
}

bool
ValidationUtil::IsNumber(const std::string& s) {
    return !s.empty() && std::all_of(s.begin(), s.end(), ::isdigit);
}

}  // namespace server
}  // namespace milvus
