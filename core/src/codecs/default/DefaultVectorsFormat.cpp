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

#include "codecs/default/DefaultVectorsFormat.h"

#include <fcntl.h>
#include <unistd.h>

#include <boost/filesystem.hpp>

#include "utils/Exception.h"
#include "utils/Log.h"

namespace milvus {
namespace codec {

void
DefaultVectorsFormat::read(const store::DirectoryPtr& directory_ptr, segment::VectorsPtr& vectors_read) {
    const std::lock_guard<std::mutex> lock(mutex_);

    std::string dir_path = directory_ptr->GetDirPath();
    if (!boost::filesystem::is_directory(dir_path)) {
        std::string err_msg = "Directory: " + dir_path + "does not exist";
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_INVALID_ARGUMENT, err_msg);
    }

    boost::filesystem::path target_path(dir_path);
    typedef boost::filesystem::directory_iterator d_it;
    d_it it_end;
    d_it it(target_path);
    //    for (auto& it : boost::filesystem::directory_iterator(dir_path)) {
    for (; it != it_end; ++it) {
        const auto& path = it->path();
        if (path.extension().string() == raw_vector_extension_) {
            int rv_fd = open(path.c_str(), O_RDWR | O_APPEND | O_CREAT, 00664);
            if (rv_fd == -1) {
                std::string err_msg = "Failed to open file: " + path.string() + ", error: " + std::strerror(errno);
                ENGINE_LOG_ERROR << err_msg;
                throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
            }
            size_t num_bytes = boost::filesystem::file_size(path);
            std::vector<uint8_t> vector_list;
            vector_list.resize(num_bytes);
            if (::read(rv_fd, vector_list.data(), num_bytes) == -1) {
                std::string err_msg = "Failed to read from file: " + path.string() + ", error: " + std::strerror(errno);
                ENGINE_LOG_ERROR << err_msg;
                throw Exception(SERVER_WRITE_ERROR, err_msg);
            }

            vectors_read->AddData(vector_list);
            vectors_read->SetName(path.stem().string());

            if (::close(rv_fd) == -1) {
                std::string err_msg = "Failed to close file: " + path.string() + ", error: " + std::strerror(errno);
                ENGINE_LOG_ERROR << err_msg;
                throw Exception(SERVER_WRITE_ERROR, err_msg);
            }
        }
        if (path.extension().string() == user_id_extension_) {
            int uid_fd = open(path.c_str(), O_RDWR | O_APPEND | O_CREAT, 00664);
            if (uid_fd == -1) {
                std::string err_msg = "Failed to open file: " + path.string() + ", error: " + std::strerror(errno);
                ENGINE_LOG_ERROR << err_msg;
                throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
            }
            auto file_size = boost::filesystem::file_size(path);
            auto count = file_size / sizeof(segment::doc_id_t);
            std::vector<segment::doc_id_t> uids;
            uids.resize(count);
            if (::read(uid_fd, uids.data(), file_size) == -1) {
                std::string err_msg = "Failed to read from file: " + path.string() + ", error: " + std::strerror(errno);
                ENGINE_LOG_ERROR << err_msg;
                throw Exception(SERVER_WRITE_ERROR, err_msg);
            }

            vectors_read->AddUids(uids);

            if (::close(uid_fd) == -1) {
                std::string err_msg = "Failed to close file: " + path.string() + ", error: " + std::strerror(errno);
                ENGINE_LOG_ERROR << err_msg;
                throw Exception(SERVER_WRITE_ERROR, err_msg);
            }
        }
    }
}

void
DefaultVectorsFormat::write(const store::DirectoryPtr& directory_ptr, const segment::VectorsPtr& vectors) {
    const std::lock_guard<std::mutex> lock(mutex_);

    std::string dir_path = directory_ptr->GetDirPath();

    const std::string rv_file_path = dir_path + "/" + vectors->GetName() + raw_vector_extension_;
    const std::string uid_file_path = dir_path + "/" + vectors->GetName() + user_id_extension_;

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
        std::string err_msg = "Failed to open file: " + rv_file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
    }
    int uid_fd = open(uid_file_path.c_str(), O_RDWR | O_APPEND | O_CREAT, 00664);
    if (uid_fd == -1) {
        std::string err_msg = "Failed to open file: " + uid_file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
    }

    if (::write(rv_fd, vectors->GetData().data(), vectors->GetData().size()) == -1) {
        std::string err_msg = "Failed to write to file" + rv_file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
    if (::close(rv_fd) == -1) {
        std::string err_msg = "Failed to close file: " + rv_file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }

    if (::write(uid_fd, vectors->GetUids().data(), sizeof(segment::doc_id_t) * vectors->GetCount()) == -1) {
        std::string err_msg = "Failed to write to file" + uid_file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
    if (::close(uid_fd) == -1) {
        std::string err_msg = "Failed to close file: " + uid_file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
}

void
DefaultVectorsFormat::read_uids(const store::DirectoryPtr& directory_ptr, std::vector<segment::doc_id_t>& uids) {
    const std::lock_guard<std::mutex> lock(mutex_);

    std::string dir_path = directory_ptr->GetDirPath();
    if (!boost::filesystem::is_directory(dir_path)) {
        std::string err_msg = "Directory: " + dir_path + "does not exist";
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_INVALID_ARGUMENT, err_msg);
    }

    boost::filesystem::path target_path(dir_path);
    typedef boost::filesystem::directory_iterator d_it;
    d_it it_end;
    d_it it(target_path);
    //    for (auto& it : boost::filesystem::directory_iterator(dir_path)) {
    for (; it != it_end; ++it) {
        const auto& path = it->path();
        if (path.extension().string() == user_id_extension_) {
            int uid_fd = open(path.c_str(), O_RDWR | O_APPEND | O_CREAT, 00664);
            if (uid_fd == -1) {
                std::string err_msg = "Failed to open file: " + path.string() + ", error: " + std::strerror(errno);
                ENGINE_LOG_ERROR << err_msg;
                throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
            }
            auto file_size = boost::filesystem::file_size(path);
            auto count = file_size / sizeof(segment::doc_id_t);
            uids.resize(count);
            if (::read(uid_fd, uids.data(), file_size) == -1) {
                std::string err_msg = "Failed to read from file: " + path.string() + ", error: " + std::strerror(errno);
                ENGINE_LOG_ERROR << err_msg;
                throw Exception(SERVER_WRITE_ERROR, err_msg);
            }
            if (::close(uid_fd) == -1) {
                std::string err_msg = "Failed to close file: " + path.string() + ", error: " + std::strerror(errno);
                ENGINE_LOG_ERROR << err_msg;
                throw Exception(SERVER_WRITE_ERROR, err_msg);
            }
        }
    }
}

void
DefaultVectorsFormat::read_vectors(const store::DirectoryPtr& directory_ptr, off_t offset, size_t num_bytes,
                                   std::vector<uint8_t>& raw_vectors) {
    const std::lock_guard<std::mutex> lock(mutex_);

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
                std::string err_msg = "Failed to open file: " + path.string() + ", error: " + std::strerror(errno);
                ENGINE_LOG_ERROR << err_msg;
                throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
            }
            int off = lseek(rv_fd, offset, SEEK_SET);
            if (off == -1) {
                std::string err_msg = "Failed to seek file: " + path.string() + ", error: " + std::strerror(errno);
                ENGINE_LOG_ERROR << err_msg;
                throw Exception(SERVER_WRITE_ERROR, err_msg);
            }

            raw_vectors.resize(num_bytes);

            if (::read(rv_fd, raw_vectors.data(), num_bytes) == -1) {
                std::string err_msg = "Failed to read from file: " + path.string() + ", error: " + std::strerror(errno);
                ENGINE_LOG_ERROR << err_msg;
                throw Exception(SERVER_WRITE_ERROR, err_msg);
            }

            if (::close(rv_fd) == -1) {
                std::string err_msg = "Failed to close file: " + path.string() + ", error: " + std::strerror(errno);
                ENGINE_LOG_ERROR << err_msg;
                throw Exception(SERVER_WRITE_ERROR, err_msg);
            }
        }
    }
}

}  // namespace codec
}  // namespace milvus
