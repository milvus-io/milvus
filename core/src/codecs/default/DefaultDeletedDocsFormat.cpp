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
    FILE* del_file = fopen(del_file_path.c_str(), "rb");
    if (del_file == nullptr) {
        std::string err_msg = "Failed to open file: " + del_file_path;
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
    }

    auto file_size = boost::filesystem::file_size(boost::filesystem::path(del_file_path));
    auto deleted_docs_size = file_size / sizeof(segment::offset_t);
    std::vector<segment::offset_t> deleted_docs_list;
    deleted_docs_list.resize(deleted_docs_size);
    fread((void*)(deleted_docs_list.data()), sizeof(segment::offset_t), deleted_docs_size, del_file);
    deleted_docs = std::make_shared<segment::DeletedDocs>(deleted_docs_list);
    fclose(del_file);
}

void
DefaultDeletedDocsFormat::write(const store::DirectoryPtr& directory_ptr, const segment::DeletedDocsPtr& deleted_docs) {
    const std::lock_guard<std::mutex> lock(mutex_);

    std::string dir_path = directory_ptr->GetDirPath();
    const std::string del_file_path = dir_path + "/" + deleted_docs_filename_;
    FILE* del_file = fopen(del_file_path.c_str(), "ab");  // TODO(zhiru): append mode
    if (del_file == nullptr) {
        std::string err_msg = "Failed to open file: " + del_file_path;
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
    }

    auto deleted_docs_list = deleted_docs->GetDeletedDocs();
    fwrite((void*)(deleted_docs_list.data()), sizeof(segment::offset_t), deleted_docs->GetSize(), del_file);
    fclose(del_file);
}

}  // namespace codec
}  // namespace milvus
