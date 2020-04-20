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
DefaultVectorsFormat::read_vectors_internal(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
                                            off_t offset, size_t num, std::vector<uint8_t>& raw_vectors) {
    if (!fs_ptr->reader_ptr_->open(file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_CANNOT_OPEN_FILE, err_msg);
    }

    size_t num_bytes;
    fs_ptr->reader_ptr_->read(&num_bytes, sizeof(size_t));

    num = std::min(num, num_bytes - offset);

    offset += sizeof(size_t);  // Beginning of file is num_bytes
    fs_ptr->reader_ptr_->seekg(offset);

    raw_vectors.resize(num / sizeof(uint8_t));
    fs_ptr->reader_ptr_->read(raw_vectors.data(), num);

    fs_ptr->reader_ptr_->close();
}

void
DefaultVectorsFormat::read_uids_internal(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
                                         std::vector<segment::doc_id_t>& uids) {
    if (!fs_ptr->reader_ptr_->open(file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_CANNOT_OPEN_FILE, err_msg);
    }

    size_t num_bytes;
    fs_ptr->reader_ptr_->read(&num_bytes, sizeof(size_t));

    uids.resize(num_bytes / sizeof(segment::doc_id_t));
    fs_ptr->reader_ptr_->read(uids.data(), num_bytes);

    fs_ptr->reader_ptr_->close();
}

void
DefaultVectorsFormat::read(const storage::FSHandlerPtr& fs_ptr, segment::VectorsPtr& vectors_read) {
    const std::lock_guard<std::mutex> lock(mutex_);

    std::string dir_path = fs_ptr->operation_ptr_->GetDirectory();
    if (!boost::filesystem::is_directory(dir_path)) {
        std::string err_msg = "Directory: " + dir_path + "does not exist";
        LOG_ENGINE_ERROR_ << err_msg;
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
            auto& vector_list = vectors_read->GetMutableData();
            read_vectors_internal(fs_ptr, path.string(), 0, INT64_MAX, vector_list);
            vectors_read->SetName(path.stem().string());
        } else if (path.extension().string() == user_id_extension_) {
            auto& uids = vectors_read->GetMutableUids();
            read_uids_internal(fs_ptr, path.string(), uids);
        }
    }
}

void
DefaultVectorsFormat::write(const storage::FSHandlerPtr& fs_ptr, const segment::VectorsPtr& vectors) {
    const std::lock_guard<std::mutex> lock(mutex_);

    std::string dir_path = fs_ptr->operation_ptr_->GetDirectory();

    const std::string rv_file_path = dir_path + "/" + vectors->GetName() + raw_vector_extension_;
    const std::string uid_file_path = dir_path + "/" + vectors->GetName() + user_id_extension_;

    TimeRecorder rc("write vectors");

    if (!fs_ptr->writer_ptr_->open(rv_file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + rv_file_path + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
    }

    size_t rv_num_bytes = vectors->GetData().size() * sizeof(uint8_t);
    fs_ptr->writer_ptr_->write(&rv_num_bytes, sizeof(size_t));
    fs_ptr->writer_ptr_->write((void*)vectors->GetData().data(), rv_num_bytes);
    fs_ptr->writer_ptr_->close();

    rc.RecordSection("write rv done");

    if (!fs_ptr->writer_ptr_->open(uid_file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + uid_file_path + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
    }
    size_t uid_num_bytes = vectors->GetUids().size() * sizeof(segment::doc_id_t);
    fs_ptr->writer_ptr_->write(&uid_num_bytes, sizeof(size_t));
    fs_ptr->writer_ptr_->write((void*)vectors->GetUids().data(), uid_num_bytes);
    fs_ptr->writer_ptr_->close();

    rc.RecordSection("write uids done");
}

void
DefaultVectorsFormat::read_uids(const storage::FSHandlerPtr& fs_ptr, std::vector<segment::doc_id_t>& uids) {
    const std::lock_guard<std::mutex> lock(mutex_);

    std::string dir_path = fs_ptr->operation_ptr_->GetDirectory();
    if (!boost::filesystem::is_directory(dir_path)) {
        std::string err_msg = "Directory: " + dir_path + "does not exist";
        LOG_ENGINE_ERROR_ << err_msg;
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
            read_uids_internal(fs_ptr, path.string(), uids);
        }
    }
}

void
DefaultVectorsFormat::read_vectors(const storage::FSHandlerPtr& fs_ptr, off_t offset, size_t num_bytes,
                                   std::vector<uint8_t>& raw_vectors) {
    const std::lock_guard<std::mutex> lock(mutex_);

    std::string dir_path = fs_ptr->operation_ptr_->GetDirectory();
    if (!boost::filesystem::is_directory(dir_path)) {
        std::string err_msg = "Directory: " + dir_path + "does not exist";
        LOG_ENGINE_ERROR_ << err_msg;
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
            read_vectors_internal(fs_ptr, path.string(), offset, num_bytes, raw_vectors);
        }
    }
}

}  // namespace codec
}  // namespace milvus
