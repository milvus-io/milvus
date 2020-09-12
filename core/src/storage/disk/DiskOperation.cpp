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

#include <experimental/filesystem>

<<<<<<< HEAD
#include <fiu/fiu-local.h>

#include "log/Log.h"
=======
#include <fiu-local.h>

>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
#include "storage/disk/DiskOperation.h"
#include "utils/Exception.h"

namespace milvus {
namespace storage {

DiskOperation::DiskOperation(const std::string& dir_path) : dir_path_(dir_path) {
}

void
DiskOperation::CreateDirectory() {
<<<<<<< HEAD
    bool is_dir = std::experimental::filesystem::is_directory(dir_path_);
    fiu_do_on("DiskOperation.CreateDirectory.is_directory", is_dir = false);
    if (!is_dir) {
        /* create directories recursively */
        auto ret = std::experimental::filesystem::create_directories(dir_path_);
=======
    bool is_dir = boost::filesystem::is_directory(dir_path_);
    fiu_do_on("DiskOperation.CreateDirectory.is_directory", is_dir = false);
    if (!is_dir) {
        auto ret = boost::filesystem::create_directory(dir_path_);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
        fiu_do_on("DiskOperation.CreateDirectory.create_directory", ret = false);
        if (!ret) {
            THROW_ERROR(SERVER_CANNOT_CREATE_FOLDER, "Failed to create directory: " + dir_path_);
        }
    }
}

const std::string&
DiskOperation::GetDirectory() const {
    return dir_path_;
}

void
DiskOperation::ListDirectory(std::vector<std::string>& file_paths) {
    std::experimental::filesystem::path target_path(dir_path_);
    using d_it = std::experimental::filesystem::directory_iterator;
    d_it it_end;
    d_it it(target_path);
    if (std::experimental::filesystem::is_directory(dir_path_)) {
        for (; it != it_end; ++it) {
            file_paths.emplace_back(it->path().c_str());
        }
    }
}

bool
DiskOperation::DeleteFile(const std::string& file_path) {
    return std::experimental::filesystem::remove(file_path);
}

}  // namespace storage
}  // namespace milvus
