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
#include <utility>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "codecs/ExtraFileInfo.h"
#include "db/Utils.h"
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

    if (!fs_ptr->reader_ptr_->Open(full_file_path)) {
        return Status(SERVER_CANNOT_OPEN_FILE, "Fail to open deleted docs file: " + full_file_path);
    }
    CHECK_MAGIC_VALID(fs_ptr);
    CHECK_SUM_VALID(fs_ptr);

    HeaderMap map = ReadHeaderValues(fs_ptr);
    size_t num_bytes = stol(map.at("size"));

    auto deleted_docs_size = num_bytes / sizeof(engine::offset_t);
    std::vector<engine::offset_t> deleted_docs_list;
    deleted_docs_list.resize(deleted_docs_size);

    fs_ptr->reader_ptr_->Seekg(MAGIC_SIZE + HEADER_SIZE);
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
        CHECK_MAGIC_VALID(fs_ptr);
        CHECK_SUM_VALID(fs_ptr);

        HeaderMap map = ReadHeaderValues(fs_ptr);
        size_t old_num_bytes = stol(map.at("size"));

        fs_ptr->reader_ptr_->Seekg(sizeof(old_num_bytes));

        auto old_deleted_docs_size = old_num_bytes / sizeof(engine::offset_t);
        old_deleted_docs_list.resize(old_deleted_docs_size);
        fs_ptr->reader_ptr_->Seekg(MAGIC_SIZE + HEADER_SIZE);
        fs_ptr->reader_ptr_->Read(old_deleted_docs_list.data(), old_num_bytes);

        fs_ptr->reader_ptr_->Close();
    }

    if (!fs_ptr->writer_ptr_->Open(temp_path)) {
        return Status(SERVER_CANNOT_CREATE_FILE, "Fail to write file: " + temp_path);
    }

    auto deleted_docs_list = deleted_docs->GetDeletedDocs();
    size_t new_num_bytes;

    if (old_del_file_exist) {
        new_num_bytes = old_num_bytes + sizeof(engine::offset_t) * deleted_docs->GetCount();
        std::vector<engine::offset_t> new_deleted_docs_list;
        new_deleted_docs_list.reserve(old_deleted_docs_list.size() + deleted_docs_list.size());
        new_deleted_docs_list.insert(new_deleted_docs_list.end(), old_deleted_docs_list.begin(),
                                     old_deleted_docs_list.end());
        new_deleted_docs_list.insert(new_deleted_docs_list.end(), deleted_docs_list.begin(),
                                     deleted_docs_list.end());
        try {
            // TODO: add extra info
            WRITE_MAGIC(fs_ptr);
            HeaderMap maps;
            maps.insert(std::make_pair("size", std::to_string(new_num_bytes)));
            std::string header = HeaderWrapper(maps);
            WRITE_HEADER(fs_ptr, header);

            fs_ptr->writer_ptr_->Write(new_deleted_docs_list.data(), new_num_bytes);

            WRITE_SUM(fs_ptr, header, reinterpret_cast<char*>(new_deleted_docs_list.data()), new_num_bytes);

            fs_ptr->writer_ptr_->Close();
            //        WRITE_SUM(fs_ptr, full_file_path);
        } catch (std::exception& ex) {
            std::string err_msg = "Failed to write delete doc: " + std::string(ex.what());
            LOG_ENGINE_ERROR_ << err_msg;

            engine::utils::SendExitSignal();
            return Status(SERVER_WRITE_ERROR, err_msg);
        }
    } else {
        new_num_bytes = sizeof(engine::offset_t) * deleted_docs->GetCount();
        try {
            // TODO: add extra info
            WRITE_MAGIC(fs_ptr);
            HeaderMap maps;
            maps.insert(std::make_pair("size", std::to_string(new_num_bytes)));
            std::string header = HeaderWrapper(maps);
            WRITE_HEADER(fs_ptr, header);

            fs_ptr->writer_ptr_->Write(deleted_docs_list.data(), new_num_bytes);

            WRITE_SUM(fs_ptr, header, reinterpret_cast<char*>(deleted_docs_list.data()), new_num_bytes);

            fs_ptr->writer_ptr_->Close();
            //        WRITE_SUM(fs_ptr, full_file_path);
        } catch (std::exception& ex) {
            std::string err_msg = "Failed to write delete doc: " + std::string(ex.what());
            LOG_ENGINE_ERROR_ << err_msg;

            engine::utils::SendExitSignal();
            return Status(SERVER_WRITE_ERROR, err_msg);
        }
    }

    fs_ptr->operation_ptr_->Move(full_file_path, temp_path);

    return Status::OK();
}

Status
DeletedDocsFormat::ReadSize(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, size_t& size) {
    const std::string full_file_path = file_path + DELETED_DOCS_POSTFIX;
    if (!fs_ptr->writer_ptr_->Open(full_file_path)) {
        return Status(SERVER_CANNOT_CREATE_FILE, "Fail to open deleted docs file: " + full_file_path);
    }
    CHECK_MAGIC_VALID(fs_ptr);
    CHECK_SUM_VALID(fs_ptr);

    HeaderMap map = ReadHeaderValues(fs_ptr);
    size_t num_bytes = stol(map.at("size"));

    size = num_bytes / sizeof(engine::offset_t);
    fs_ptr->reader_ptr_->Close();

    return Status::OK();
}

}  // namespace codec
}  // namespace milvus
