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

#include <unistd.h>

#include <experimental/filesystem>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/Utils.h"
#include "storage/ExtraFileInfo.h"
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

Status
DeletedDocsFormat::Read(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
                        segment::DeletedDocsPtr& deleted_docs) {
    const std::string full_file_path = file_path + DELETED_DOCS_POSTFIX;

    CHECK_MAGIC_VALID(fs_ptr, full_file_path);
    CHECK_SUM_VALID(fs_ptr, full_file_path);
    if (!fs_ptr->reader_ptr_->Open(full_file_path)) {
        return Status(SERVER_CANNOT_OPEN_FILE, "Fail to open deleted docs file: " + full_file_path);
    }

    fs_ptr->reader_ptr_->Seekg(MAGIC_SIZE + HEADER_SIZE);
    size_t num_bytes;
    fs_ptr->reader_ptr_->Read(&num_bytes, sizeof(size_t));

    auto deleted_docs_size = num_bytes / sizeof(engine::offset_t);
    std::vector<engine::offset_t> deleted_docs_list;
    deleted_docs_list.resize(deleted_docs_size);

    fs_ptr->reader_ptr_->Read(deleted_docs_list.data(), num_bytes);
    fs_ptr->reader_ptr_->Close();

    deleted_docs = std::make_shared<segment::DeletedDocs>(deleted_docs_list);

    return Status::OK();
}

Status
DeletedDocsFormat::Write(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
                         const segment::DeletedDocsPtr& deleted_docs) {
    const std::string full_file_path = file_path + DELETED_DOCS_POSTFIX;

    // Create a temporary file from the existing file
    const std::string temp_path = file_path + ".temp_del";
    bool exists = std::experimental::filesystem::exists(full_file_path);
    if (exists) {
        std::experimental::filesystem::copy_file(full_file_path, temp_path,
                                                 std::experimental::filesystem::copy_options::none);
    }

    // Write to the temp file, in order to avoid possible race condition with search (concurrent read and write)
    size_t old_num_bytes;
    std::vector<engine::offset_t> delete_ids;
    if (exists) {
        CHECK_MAGIC_VALID(fs_ptr, full_file_path);
        CHECK_SUM_VALID(fs_ptr, full_file_path);
        if (!fs_ptr->reader_ptr_->Open(temp_path)) {
            return Status(SERVER_CANNOT_OPEN_FILE, "Fail to open tmp deleted docs file: " + temp_path);
        }
        fs_ptr->reader_ptr_->Seekg(MAGIC_SIZE+HEADER_SIZE);
        fs_ptr->reader_ptr_->Read(&old_num_bytes, sizeof(size_t));
        delete_ids.resize(old_num_bytes / sizeof(engine::offset_t));
        fs_ptr->reader_ptr_->Read(delete_ids.data(), old_num_bytes);
        fs_ptr->reader_ptr_->Close();
    } else {
        old_num_bytes = 0;
    }

    auto deleted_docs_list = deleted_docs->GetDeletedDocs();
    size_t new_num_bytes = old_num_bytes + sizeof(engine::offset_t) * deleted_docs->GetCount();
    if (!deleted_docs_list.empty()) {
        delete_ids.insert(delete_ids.end(), deleted_docs_list.begin(), deleted_docs_list.end());
    }
    // TODO: add extra info
    std::unordered_map<std::string, std::string> maps;
    WRITE_MAGIC(fs_ptr, temp_path)
    WRITE_HEADER(fs_ptr, temp_path, maps);

    if (!fs_ptr->writer_ptr_->InOpen(temp_path)) {
        return Status(SERVER_CANNOT_CREATE_FILE, "Fail to write file: " + temp_path);
    }

    try {
        fs_ptr->writer_ptr_->Seekp(MAGIC_SIZE + HEADER_SIZE);
        fs_ptr->writer_ptr_->Write(&new_num_bytes, sizeof(size_t));
        fs_ptr->writer_ptr_->Write(delete_ids.data(), new_num_bytes);
        fs_ptr->writer_ptr_->Close();
        WRITE_SUM(fs_ptr, temp_path);
    } catch (std::exception& ex) {
        std::string err_msg = "Failed to write delete doc: " + std::string(ex.what());
        LOG_ENGINE_ERROR_ << err_msg;

        engine::utils::SendExitSignal();
        return Status(SERVER_WRITE_ERROR, err_msg);
    }

    // Move temp file to delete file
    try {
        std::experimental::filesystem::rename(temp_path, full_file_path);
    } catch (std::exception& ex) {
        std::string msg = "Failed to rename file [" + temp_path + "] to [" + full_file_path + "]";
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_UNEXPECTED_ERROR, msg);
    }

    return Status::OK();
}

Status
DeletedDocsFormat::ReadSize(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, size_t& size) {
    const std::string full_file_path = file_path + DELETED_DOCS_POSTFIX;
    CHECK_MAGIC_VALID(fs_ptr, full_file_path);
    CHECK_SUM_VALID(fs_ptr, full_file_path);
    if (!fs_ptr->writer_ptr_->Open(full_file_path)) {
        return Status(SERVER_CANNOT_CREATE_FILE, "Fail to open deleted docs file: " + full_file_path);
    }

    fs_ptr->reader_ptr_->Seekg(MAGIC_SIZE + HEADER_SIZE);
    size_t num_bytes;
    fs_ptr->reader_ptr_->Read(&num_bytes, sizeof(size_t));

    size = num_bytes / sizeof(engine::offset_t);
    fs_ptr->reader_ptr_->Close();

    return Status::OK();
}

}  // namespace codec
}  // namespace milvus
