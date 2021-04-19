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

#include "server/init/Directory.h"

#include <fcntl.h>
#include <unistd.h>
#include <boost/filesystem.hpp>
#include <string>

#include "config/ServerConfig.h"

namespace milvus::server {

Status
Directory::Initialize(const std::string& path) {
    try {
        init(path);
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }
    return Status::OK();
}


Status
Directory::Access(const std::string& path) {
    try {
        access_check(path);
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }
    return Status::OK();
}

void
Directory::init(const std::string& path) {
    if (path.empty()) {
        return;
    }
    try {
        // Returns True if a new directory was created, otherwise false.
        boost::filesystem::create_directories(path);
    } catch (std::exception& ex) {
        std::string msg = "Cannot create directory: " + path + ", reason: " + ex.what();
        throw std::runtime_error(msg);
    } catch (...) {
        std::string msg = "Cannot create directory: " + path;
        throw std::runtime_error(msg);
    }
}

void
Directory::access_check(const std::string& path) {
    if (path.empty()) {
        return;
    }
    int ret = access(path.c_str(), F_OK | R_OK | W_OK);
    if (0 != ret) {
        std::string msg = "Cannot access path: " + path + ", error(" + std::to_string(errno) +
                          "): " + std::string(strerror(errno)) + ".";
        throw std::runtime_error(msg);
    }
}

}  // namespace milvus::server
