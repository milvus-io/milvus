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

#include "storage/disk/DiskOperation.h"

#include <boost/filesystem.hpp>

#include "utils/Exception.h"
#include "utils/Log.h"

namespace milvus {
namespace storage {

DiskOperation::DiskOperation(const std::string& dir_path) : Operation(dir_path) {
}

void
DiskOperation::CreateDirectory() {
    if (!boost::filesystem::is_directory(GetDirectory())) {
        auto ret = boost::filesystem::create_directory(GetDirectory());
        if (!ret) {
            std::string err_msg = "Failed to create directory: " + GetDirectory();
            ENGINE_LOG_ERROR << err_msg;
            throw Exception(SERVER_CANNOT_CREATE_FOLDER, err_msg);
        }
    }
}

void
DiskOperation::ListDirectory(std::vector<std::string>& file_paths) const {
    boost::filesystem::path target_path(GetDirectory());
    typedef boost::filesystem::directory_iterator d_it;
    d_it it_end;
    d_it it(target_path);
    if (boost::filesystem::is_directory(GetDirectory())) {
        for (; it != it_end; ++it) {
            file_paths.emplace_back(it->path().string());
        }
    }
}

bool
DiskOperation::DeleteFile(const std::string& file_path) {
    return boost::filesystem::remove(file_path);
}

void
DiskOperation::CopyFile(const std::string& from_name, const std::string& to_name) {
    auto from_path = boost::filesystem::path(from_name);
    auto to_path = boost::filesystem::path(to_name);
    boost::filesystem::copy_file(from_path, to_path);
}

void
DiskOperation::RenameFile(const std::string& old_name, const std::string& new_name) {
    auto old_path = boost::filesystem::path(old_name);
    auto new_path = boost::filesystem::path(new_name);
    boost::filesystem::rename(old_path, new_path);
}

}  // namespace storage
}  // namespace milvus
