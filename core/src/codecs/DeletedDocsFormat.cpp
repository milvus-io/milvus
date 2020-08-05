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

#include "codecs/DeletedDocsFormat.h"

#include <fcntl.h>
#include <unistd.h>

#define BOOST_NO_CXX11_SCOPED_ENUMS

#include <boost/filesystem.hpp>

#undef BOOST_NO_CXX11_SCOPED_ENUMS

#include <memory>
#include <string>
#include <vector>

#include "utils/Exception.h"
#include "utils/Log.h"

namespace milvus {
namespace codec {

const char* DELETED_DOCS_POSTFIX = ".del";

std::string
DeletedDocsFormat::FilePostfix() {
    std::string str = DELETED_DOCS_POSTFIX;
    return str;
}

void
DeletedDocsFormat::Read(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
                        segment::DeletedDocsPtr& deleted_docs) {
    const std::string full_file_path = file_path + DELETED_DOCS_POSTFIX;

    if (!fs_ptr->reader_ptr_->open(full_file_path)) {
        std::string err_msg = "Failed to open file: " + full_file_path;  // + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
    }

    size_t num_bytes;
    fs_ptr->reader_ptr_->read(&num_bytes, sizeof(size_t));

    auto deleted_docs_size = num_bytes / sizeof(segment::offset_t);
    std::vector<segment::offset_t> deleted_docs_list;
    deleted_docs_list.resize(deleted_docs_size);

    fs_ptr->reader_ptr_->read(deleted_docs_list.data(), num_bytes);
    fs_ptr->reader_ptr_->close();

    deleted_docs = std::make_shared<segment::DeletedDocs>(deleted_docs_list);
}

void
DeletedDocsFormat::Write(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
                         const segment::DeletedDocsPtr& deleted_docs) {
    const std::string full_file_path = file_path + DELETED_DOCS_POSTFIX;

    // Create a temporary file from the existing file
    const std::string temp_path = file_path + ".temp_del";
    bool exists = boost::filesystem::exists(full_file_path);
    if (exists) {
        boost::filesystem::copy_file(full_file_path, temp_path, boost::filesystem::copy_option::fail_if_exists);
    }

    // Write to the temp file, in order to avoid possible race condition with search (concurrent read and write)
    size_t old_num_bytes;
    std::vector<segment::offset_t> delete_ids;
    if (exists) {
        if (!fs_ptr->reader_ptr_->open(temp_path)) {
            std::string err_msg = "Failed to read from file: " + temp_path;  // + ", error: " + std::strerror(errno);
            LOG_ENGINE_ERROR_ << err_msg;
            throw Exception(SERVER_WRITE_ERROR, err_msg);
        }
        fs_ptr->reader_ptr_->read(&old_num_bytes, sizeof(size_t));
        delete_ids.resize(old_num_bytes / sizeof(size_t));
        fs_ptr->reader_ptr_->read(delete_ids.data(), old_num_bytes);
        fs_ptr->reader_ptr_->close();
    } else {
        old_num_bytes = 0;
    }

    auto deleted_docs_list = deleted_docs->GetDeletedDocs();
    size_t new_num_bytes = old_num_bytes + sizeof(segment::offset_t) * deleted_docs->GetCount();
    if (!deleted_docs_list.empty()) {
        delete_ids.insert(delete_ids.end(), deleted_docs_list.begin(), deleted_docs_list.end());
    }

    if (!fs_ptr->writer_ptr_->open(temp_path)) {
        std::string err_msg = "Failed to write from file: " + temp_path;  // + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
    }

    fs_ptr->writer_ptr_->write(&new_num_bytes, sizeof(size_t));
    fs_ptr->writer_ptr_->write(delete_ids.data(), new_num_bytes);
    fs_ptr->writer_ptr_->close();

    // Move temp file to delete file
    boost::filesystem::rename(temp_path, full_file_path);
}

void
DeletedDocsFormat::ReadSize(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, size_t& size) {
    const std::string full_file_path = file_path + DELETED_DOCS_POSTFIX;
    if (!fs_ptr->writer_ptr_->open(full_file_path)) {
        std::string err_msg = "Failed to open file: " + full_file_path;  // + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
    }

    size_t num_bytes;
    fs_ptr->reader_ptr_->read(&num_bytes, sizeof(size_t));

    size = num_bytes / sizeof(segment::offset_t);
    fs_ptr->reader_ptr_->close();
}

}  // namespace codec
}  // namespace milvus
