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
#include <algorithm>

#include <boost/filesystem.hpp>

#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace codec {

void
DefaultVectorsFormat::read_vectors_internal(const std::string& file_path, off_t offset, size_t num,
                                            std::vector<uint8_t>& raw_vectors) {
    int rv_fd = open(file_path.c_str(), O_RDONLY, 00664);
    if (rv_fd == -1) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
    }

    size_t num_bytes;
    if (::read(rv_fd, &num_bytes, sizeof(size_t)) == -1) {
        std::string err_msg = "Failed to read from file: " + file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }

    num = std::min(num, num_bytes - offset);

    offset += sizeof(size_t);  // Beginning of file is num_bytes
    int off = lseek(rv_fd, offset, SEEK_SET);
    if (off == -1) {
        std::string err_msg = "Failed to seek file: " + file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }

    raw_vectors.resize(num / sizeof(uint8_t));
    if (::read(rv_fd, raw_vectors.data(), num) == -1) {
        std::string err_msg = "Failed to read from file: " + file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }

    if (::close(rv_fd) == -1) {
        std::string err_msg = "Failed to close file: " + file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
}

void
DefaultVectorsFormat::read_uids_internal(const std::string& file_path, std::vector<segment::doc_id_t>& uids) {
    int uid_fd = open(file_path.c_str(), O_RDONLY, 00664);
    if (uid_fd == -1) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
    }

    size_t num_bytes;
    if (::read(uid_fd, &num_bytes, sizeof(size_t)) == -1) {
        std::string err_msg = "Failed to read from file: " + file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }

    uids.resize(num_bytes / sizeof(segment::doc_id_t));
    if (::read(uid_fd, uids.data(), num_bytes) == -1) {
        std::string err_msg = "Failed to read from file: " + file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }

    if (::close(uid_fd) == -1) {
        std::string err_msg = "Failed to close file: " + file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
}

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
            std::vector<uint8_t> vector_list;
            read_vectors_internal(path.string(), 0, INT64_MAX, vector_list);
            vectors_read->AddData(vector_list);
            vectors_read->SetName(path.stem().string());
        }
        if (path.extension().string() == user_id_extension_) {
            std::vector<segment::doc_id_t> uids;
            read_uids_internal(path.string(), uids);
            vectors_read->AddUids(uids);
        }
    }
}

void
DefaultVectorsFormat::write(const store::DirectoryPtr& directory_ptr, const segment::VectorsPtr& vectors) {
    const std::lock_guard<std::mutex> lock(mutex_);

    std::string dir_path = directory_ptr->GetDirPath();

    const std::string rv_file_path = dir_path + "/" + vectors->GetName() + raw_vector_extension_;
    const std::string uid_file_path = dir_path + "/" + vectors->GetName() + user_id_extension_;

    TimeRecorder rc("write vectors");

    int rv_fd = open(rv_file_path.c_str(), O_WRONLY | O_TRUNC | O_CREAT, 00664);
    if (rv_fd == -1) {
        std::string err_msg = "Failed to open file: " + rv_file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
    }

    size_t rv_num_bytes = vectors->GetData().size() * sizeof(uint8_t);
    if (::write(rv_fd, &rv_num_bytes, sizeof(size_t)) == -1) {
        std::string err_msg = "Failed to write to file: " + rv_file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
    if (::write(rv_fd, vectors->GetData().data(), rv_num_bytes) == -1) {
        std::string err_msg = "Failed to write to file: " + rv_file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
    if (::close(rv_fd) == -1) {
        std::string err_msg = "Failed to close file: " + rv_file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }

    rc.RecordSection("write rv done");

    int uid_fd = open(uid_file_path.c_str(), O_WRONLY | O_TRUNC | O_CREAT, 00664);
    if (uid_fd == -1) {
        std::string err_msg = "Failed to open file: " + uid_file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
    }
    size_t uid_num_bytes = vectors->GetUids().size() * sizeof(segment::doc_id_t);
    if (::write(uid_fd, &uid_num_bytes, sizeof(size_t)) == -1) {
        std::string err_msg = "Failed to write to file" + rv_file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
    if (::write(uid_fd, vectors->GetUids().data(), uid_num_bytes) == -1) {
        std::string err_msg = "Failed to write to file" + uid_file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
    if (::close(uid_fd) == -1) {
        std::string err_msg = "Failed to close file: " + uid_file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }

    rc.RecordSection("write uids done");
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
            read_uids_internal(path.string(), uids);
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

    boost::filesystem::path target_path(dir_path);
    typedef boost::filesystem::directory_iterator d_it;
    d_it it_end;
    d_it it(target_path);
    //    for (auto& it : boost::filesystem::directory_iterator(dir_path)) {
    for (; it != it_end; ++it) {
        const auto& path = it->path();
        if (path.extension().string() == raw_vector_extension_) {
            read_vectors_internal(path.string(), offset, num_bytes, raw_vectors);
        }
    }
}

}  // namespace codec
}  // namespace milvus
