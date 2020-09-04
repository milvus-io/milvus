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
//#include "storage/ExtraFileInfo.h"
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

    //CHECK_MAGIC_VALID(fs_ptr, full_file_path);
    //CHECK_SUM_VALID(fs_ptr, full_file_path);
    if (!fs_ptr->reader_ptr_->Open(full_file_path)) {
        return Status(SERVER_CANNOT_OPEN_FILE, "Fail to open deleted docs file: " + full_file_path);
    }

    //fs_ptr->reader_ptr_->Seekg(MAGIC_SIZE + HEADER_SIZE);
    fs_ptr->reader_ptr_->Seekg(0);
    size_t num_bytes;
    fs_ptr->reader_ptr_->Read(&num_bytes, sizeof(size_t));
    //fs_ptr->reader_ptr_->Seekg(MAGIC_SIZE + HEADER_SIZE + sizeof(num_bytes));
    fs_ptr->reader_ptr_->Seekg(sizeof(num_bytes));

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

    // Write to the temp file, in order to avoid possible race condition with search (concurrent read and write)
    bool old_del_file_exist = false;
    size_t old_num_bytes;
    std::vector<engine::offset_t> old_deleted_docs_list;

    if (fs_ptr->reader_ptr_->Open(full_file_path)) {
        old_del_file_exist = true;
        fs_ptr->reader_ptr_->Seekg(0);

        fs_ptr->reader_ptr_->Read(&old_num_bytes, sizeof(old_num_bytes));
        fs_ptr->reader_ptr_->Seekg(sizeof(old_num_bytes));

        auto old_deleted_docs_size = old_num_bytes / sizeof(engine::offset_t);
        old_deleted_docs_list.resize(old_deleted_docs_size);
        fs_ptr->reader_ptr_->Read(old_deleted_docs_list.data(), old_num_bytes);

        fs_ptr->reader_ptr_->Close();
    }

    //std::unordered_map<std::string, std::string> maps;
    //WRITE_MAGIC(fs_ptr, temp_path)
    //WRITE_HEADER(fs_ptr, temp_path, maps);
    //fs_ptr->writer_ptr_->Seekp(MAGIC_SIZE + HEADER_SIZE);

    if (!fs_ptr->writer_ptr_->Open(temp_path)) {
        std::string err_msg = "Fail to open file: " + temp_path + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }

    auto deleted_docs_list = deleted_docs->GetDeletedDocs();
    size_t new_num_bytes;

    if (old_del_file_exist) {
        new_num_bytes = old_num_bytes + sizeof(engine::offset_t) * deleted_docs->GetCount();
        fs_ptr->writer_ptr_->Write(&new_num_bytes, sizeof(size_t));
        fs_ptr->writer_ptr_->Write(old_deleted_docs_list.data(), old_num_bytes);
    } else {
        new_num_bytes = sizeof(engine::offset_t) * deleted_docs->GetCount();
        fs_ptr->writer_ptr_->Write(&new_num_bytes, sizeof(size_t));
    }

    fs_ptr->writer_ptr_->Write(deleted_docs_list.data(), sizeof(engine::offset_t) * deleted_docs->GetCount());

    fs_ptr->writer_ptr_->Close();

    //try {
    //    WRITE_SUM(fs_ptr, temp_path);
    //} catch (std::exception& ex) {
    //    std::string err_msg = "Failed to write delete doc: " + std::string(ex.what());
    //    LOG_ENGINE_ERROR_ << err_msg;

    //    engine::utils::SendExitSignal();
    //    return Status(SERVER_WRITE_ERROR, err_msg);
    //}

    // Move temp file to delete file
    fs_ptr->operation_ptr_->Move(full_file_path, temp_path);

    return Status::OK();
}

Status
DeletedDocsFormat::ReadSize(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, size_t& size) {
    const std::string full_file_path = file_path + DELETED_DOCS_POSTFIX;
    //CHECK_MAGIC_VALID(fs_ptr, full_file_path);
    //CHECK_SUM_VALID(fs_ptr, full_file_path);
    if (!fs_ptr->writer_ptr_->Open(full_file_path)) {
        return Status(SERVER_CANNOT_CREATE_FILE, "Fail to open deleted docs file: " + full_file_path);
    }

    //fs_ptr->reader_ptr_->Seekg(MAGIC_SIZE + HEADER_SIZE);
    fs_ptr->reader_ptr_->Seekg(0);
    size_t num_bytes;
    fs_ptr->reader_ptr_->Read(&num_bytes, sizeof(size_t));

    size = num_bytes / sizeof(engine::offset_t);
    fs_ptr->reader_ptr_->Close();

    return Status::OK();
}

}  // namespace codec
}  // namespace milvus
