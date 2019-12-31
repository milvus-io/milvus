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

segment::Vectors
DefaultVectorsFormat::read(store::DirectoryPtr directory_ptr) {
    std::string dir_path;
    directory_ptr->GetDirPath(dir_path);
    if (!boost::filesystem::is_directory(dir_path)) {
        std::string err_msg = "Directory: " + dir_path + "does not exist";
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_INVALID_ARGUMENT, err_msg);
    }
    std::unordered_map<std::string, segment::VectorPtr> vectors;
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
            void* vector = malloc(num_bytes);
            ::read(rv_fd, vector, num_bytes);

            auto& vector_ptr = vectors[path.stem().string()];
            if (vector_ptr == nullptr) {
                vector_ptr = std::make_shared<segment::Vector>();
            }
            vector_ptr->SetNbytes(num_bytes);
            vector_ptr->SetData(vector);
        }
        if (path.extension().string() == user_id_extension_) {
            int uid_fd = open(path.c_str(), O_RDWR | O_APPEND | O_CREAT, 00664);
            if (uid_fd == -1) {
                std::string err_msg = "Failed to open file: " + path.string();
                ENGINE_LOG_ERROR << err_msg;
                throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
            }
            size_t file_size = boost::filesystem::file_size(path);
            int64_t* uids = (int64_t*)(malloc(file_size / sizeof(int64_t)));
            ::read(uid_fd, uids, file_size);
            vectors[path.stem().string()]->SetCount(file_size / sizeof(int64_t));
            vectors[path.stem().string()]->SetUids(uids);
        }
    }
}

void
DefaultVectorsFormat::write(store::DirectoryPtr directory_ptr, segment::Vectors vectors) {
    std::string dir_path;
    directory_ptr->GetDirPath(dir_path);
    for (auto& it : vectors.vectors) {
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
        if (::write(rv_fd, it.second->GetData(), it.second->GetNumBytes()) == -1) {
            std::string err_msg = "Failed to write to file" + rv_file_path;
            ENGINE_LOG_ERROR << err_msg;
            throw Exception(SERVER_WRITE_ERROR, err_msg);
        }
        if (::write(uid_fd, it.second->GetUids(), sizeof(int64_t) * it.second->GetCount() == -1)) {
            std::string err_msg = "Failed to write to file" + uid_file_path;
            ENGINE_LOG_ERROR << err_msg;
            throw Exception(SERVER_WRITE_ERROR, err_msg);
        }
    }
}

}  // namespace codec
}  // namespace milvus
