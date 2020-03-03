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

#include "codecs/default/DefaultDeletedDocsFormat.h"

#include <fcntl.h>
#include <unistd.h>

#include <boost/filesystem.hpp>
#include <memory>
#include <string>
#include <vector>

#include "segment/Types.h"
#include "utils/Exception.h"
#include "utils/Log.h"

namespace milvus {
namespace codec {

void
DefaultDeletedDocsFormat::read(const store::DirectoryPtr& directory_ptr, segment::DeletedDocsPtr& deleted_docs) {
    const std::lock_guard<std::mutex> lock(mutex_);

    std::string dir_path = directory_ptr->GetDirPath();
    const std::string del_file_path = dir_path + "/" + deleted_docs_filename_;

    int del_fd = open(del_file_path.c_str(), O_RDONLY, 00664);
    if (del_fd == -1) {
        std::string err_msg = "Failed to open file: " + del_file_path;
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
    }

    auto file_size = boost::filesystem::file_size(boost::filesystem::path(del_file_path));
    auto deleted_docs_size = file_size / sizeof(segment::offset_t);
    std::vector<segment::offset_t> deleted_docs_list;
    deleted_docs_list.resize(deleted_docs_size);

    if (::read(del_fd, deleted_docs_list.data(), file_size) == -1) {
        std::string err_msg = "Failed to read from file: " + del_file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }

    deleted_docs = std::make_shared<segment::DeletedDocs>(deleted_docs_list);

    if (::close(del_fd) == -1) {
        std::string err_msg = "Failed to close file: " + del_file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
}

void
DefaultDeletedDocsFormat::write(const store::DirectoryPtr& directory_ptr, const segment::DeletedDocsPtr& deleted_docs) {
    const std::lock_guard<std::mutex> lock(mutex_);

    std::string dir_path = directory_ptr->GetDirPath();
    const std::string del_file_path = dir_path + "/" + deleted_docs_filename_;

    // TODO(zhiru): append mode
    int del_fd = open(del_file_path.c_str(), O_WRONLY | O_APPEND | O_CREAT, 00664);
    if (del_fd == -1) {
        std::string err_msg = "Failed to open file: " + del_file_path;
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
    }

    auto deleted_docs_list = deleted_docs->GetDeletedDocs();

    if (::write(del_fd, deleted_docs_list.data(), sizeof(segment::offset_t) * deleted_docs->GetSize()) == -1) {
        std::string err_msg = "Failed to write to file" + del_file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }

    if (::close(del_fd) == -1) {
        std::string err_msg = "Failed to close file: " + del_file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
}

}  // namespace codec
}  // namespace milvus
