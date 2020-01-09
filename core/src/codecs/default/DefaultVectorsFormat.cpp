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

#include "DefaultVectorsFormat.h"

#include <fcntl.h>
#include <unistd.h>

#include <boost/filesystem.hpp>

#include "segment/Vector.h"
#include "utils/Exception.h"
#include "utils/Log.h"

namespace milvus {
namespace codec {

void
DefaultVectorsFormat::read(const store::DirectoryPtr& directory_ptr, segment::VectorsPtr& vectors_read) {
    std::string dir_path = directory_ptr->GetDirPath();
    if (!boost::filesystem::is_directory(dir_path)) {
        std::string err_msg = "Directory: " + dir_path + "does not exist";
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_INVALID_ARGUMENT, err_msg);
    }

    for (auto& it : boost::filesystem::directory_iterator(dir_path)) {
        const auto& path = it.path();
        if (path.extension().string() == raw_vector_extension_) {
            int rv_fd = open(path.c_str(), O_RDWR | O_APPEND | O_CREAT, 00664);
            if (rv_fd == -1) {
                std::string err_msg = "Failed to open file: " + path.string();
                ENGINE_LOG_ERROR << err_msg;
                throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
            }
            size_t num_bytes = boost::filesystem::file_size(path);
            std::vector<uint8_t> vector_list(num_bytes);
            ::read(rv_fd, vector_list.data(), num_bytes);

            auto found = vectors_read->vectors_map.find(path.stem().string());
            if (found == vectors_read->vectors_map.end()) {
                vectors_read->vectors_map[path.stem().string()] = std::make_shared<segment::Vector>();
            }

            vectors_read->vectors_map[path.stem().string()]->AddData(vector_list);
        }
        if (path.extension().string() == user_id_extension_) {
            int uid_fd = open(path.c_str(), O_RDWR | O_APPEND | O_CREAT, 00664);
            if (uid_fd == -1) {
                std::string err_msg = "Failed to open file: " + path.string();
                ENGINE_LOG_ERROR << err_msg;
                throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
            }
            auto file_size = boost::filesystem::file_size(path);
            auto count = file_size / sizeof(int64_t);
            std::vector<segment::doc_id_t> uids(count);
            ::read(uid_fd, uids.data(), file_size);

            vectors_read->vectors_map[path.stem().string()]->AddUids(uids);
        }
    }
}

void
DefaultVectorsFormat::write(const store::DirectoryPtr& directory_ptr, const segment::VectorsPtr& vectors) {
    std::string dir_path = directory_ptr->GetDirPath();
    for (auto& it : vectors->vectors_map) {
        const std::string rv_file_path = dir_path + "/" + it.first + "." + raw_vector_extension_;
        const std::string uid_file_path = dir_path + "/" + it.first + "." + user_id_extension_;

        /*
        FILE* rv_file = fopen(rv_file_path.c_str(), "wb");
        if (rv_file == nullptr) {
            std::string err_msg = "Failed to open file: " + rv_file_path;
            ENGINE_LOG_ERROR << err_msg;
            throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
        }

        fwrite((void*)(it.second->GetData()), sizeof(char), it.second->GetNumBytes(), rv_file);
        fclose(rv_file);


        FILE* uid_file = fopen(uid_file_path.c_str(), "wb");
        if (uid_file == nullptr) {
            std::string err_msg = "Failed to open file: " + uid_file_path;
            ENGINE_LOG_ERROR << err_msg;
            throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
        }

        fwrite((void*)(it.second->GetUids()), sizeof it.second->GetUids()[0], it.second->GetCount(), uid_file);
        fclose(rv_file);
        */

        int rv_fd = open(rv_file_path.c_str(), O_RDWR | O_APPEND | O_CREAT, 00664);
        if (rv_fd == -1) {
            std::string err_msg = "Failed to open file: " + rv_file_path;
            ENGINE_LOG_ERROR << err_msg;
            throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
        }
        int uid_fd = open(uid_file_path.c_str(), O_RDWR | O_APPEND | O_CREAT, 00664);
        if (uid_fd == -1) {
            std::string err_msg = "Failed to open file: " + uid_file_path;
            ENGINE_LOG_ERROR << err_msg;
            throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
        }
        if (::write(rv_fd, it.second->GetData().data(), it.second->GetData().size()) == -1) {
            std::string err_msg = "Failed to write to file" + rv_file_path;
            ENGINE_LOG_ERROR << err_msg;
            throw Exception(SERVER_WRITE_ERROR, err_msg);
        }
        if (::write(uid_fd, it.second->GetUids().data(), sizeof(segment::doc_id_t) * it.second->GetCount() == -1)) {
            std::string err_msg = "Failed to write to file" + uid_file_path;
            ENGINE_LOG_ERROR << err_msg;
            throw Exception(SERVER_WRITE_ERROR, err_msg);
        }
    }
}
void
DefaultVectorsFormat::readUids(const store::DirectoryPtr& directory_ptr, std::vector<segment::doc_id_t>& uids) {
    std::string dir_path = directory_ptr->GetDirPath();
    if (!boost::filesystem::is_directory(dir_path)) {
        std::string err_msg = "Directory: " + dir_path + "does not exist";
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_INVALID_ARGUMENT, err_msg);
    }

    for (auto& it : boost::filesystem::directory_iterator(dir_path)) {
        const auto& path = it.path();
        if (path.extension().string() == user_id_extension_) {
            int uid_fd = open(path.c_str(), O_RDWR | O_APPEND | O_CREAT, 00664);
            if (uid_fd == -1) {
                std::string err_msg = "Failed to open file: " + path.string();
                ENGINE_LOG_ERROR << err_msg;
                throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
            }
            auto file_size = boost::filesystem::file_size(path);
            auto count = file_size / sizeof(int64_t);
            ::read(uid_fd, uids.data(), file_size);
        }
    }
}

}  // namespace codec
}  // namespace milvus
