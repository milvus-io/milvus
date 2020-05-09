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

#include "codecs/default/DefaultAttrsFormat.h"

#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <memory>

#include <boost/filesystem.hpp>

#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace codec {

void
DefaultAttrsFormat::read_attrs_internal(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, off_t offset,
                                        size_t num, std::vector<uint8_t>& raw_attrs, size_t& nbytes) {
    if (!fs_ptr->reader_ptr_->open(file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
    }

    fs_ptr->reader_ptr_->read(&nbytes, sizeof(size_t));

    num = std::min(num, nbytes - offset);

    offset += sizeof(size_t);
    fs_ptr->reader_ptr_->seekg(offset);

    raw_attrs.resize(num / sizeof(uint8_t));
    fs_ptr->reader_ptr_->read(raw_attrs.data(), num);

    fs_ptr->reader_ptr_->close();
}

void
DefaultAttrsFormat::read_uids_internal(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
                                       std::vector<int64_t>& uids) {
    if (!fs_ptr->reader_ptr_->open(file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
    }

    size_t num_bytes;
    fs_ptr->reader_ptr_->read(&num_bytes, sizeof(size_t));

    uids.resize(num_bytes / sizeof(int64_t));
    fs_ptr->reader_ptr_->read(uids.data(), num_bytes);

    fs_ptr->reader_ptr_->read(uids.data(), num_bytes);
}

void
DefaultAttrsFormat::read(const milvus::storage::FSHandlerPtr& fs_ptr, milvus::segment::AttrsPtr& attrs_read) {
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
    d_it uid_it(target_path);
    std::vector<int64_t> uids;
    for (; uid_it != it_end; ++uid_it) {
        const auto& path = uid_it->path();
        if (path.extension().string() == user_id_extension_) {
            read_uids_internal(fs_ptr, path.string(), uids);
            break;
        }
    }

    d_it it(target_path);
    for (; it != it_end; ++it) {
        const auto& path = it->path();
        if (path.extension().string() == raw_attr_extension_) {
            auto file_name = path.filename().string();
            auto field_name = file_name.substr(0, file_name.size() - 3);
            std::vector<uint8_t> attr_list;
            size_t nbytes;
            read_attrs_internal(fs_ptr, path.string(), 0, INT64_MAX, attr_list, nbytes);
            milvus::segment::AttrPtr attr =
                std::make_shared<milvus::segment::Attr>(attr_list, nbytes, uids, field_name);
            attrs_read->attrs.insert(std::pair(field_name, attr));
        }
    }
}

void
DefaultAttrsFormat::write(const milvus::storage::FSHandlerPtr& fs_ptr, const milvus::segment::AttrsPtr& attrs_ptr) {
    const std::lock_guard<std::mutex> lock(mutex_);

    TimeRecorder rc("write attributes");

    std::string dir_path = fs_ptr->operation_ptr_->GetDirectory();

    auto it = attrs_ptr->attrs.begin();
    if (it == attrs_ptr->attrs.end()) {
        // std::string err_msg = "Attributes is null";
        // LOG_ENGINE_ERROR_ << err_msg;
        return;
    }

#if 0
    const std::string uid_file_path = dir_path + "/" + it->second->GetCollectionId() + user_id_extension_;

    int uid_fd = open(uid_file_path.c_str(), O_WRONLY | O_TRUNC | O_CREAT, 00664);
    if (uid_fd == -1) {
        std::string err_msg = "Failed to open file: " + uid_file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
    }
    size_t uid_num_bytes = it->second->GetUids().size() * sizeof(int64_t);
    if (::write(uid_fd, &uid_num_bytes, sizeof(size_t)) == -1) {
        std::string err_msg = "Failed to write to file" + uid_file_path + ", error: " + std::strerror(errno);
        ENGINE_LOG_ERROR << err_msg;
        throw Exception(SERVER_WRITE_ERROR, err_msg);
    }
    if (::write(uid_fd, it->second->GetUids().data(), uid_num_bytes) == -1) {
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
#endif

    for (; it != attrs_ptr->attrs.end(); it++) {
        const std::string ra_file_path = dir_path + "/" + it->second->GetName() + raw_attr_extension_;

        int ra_fd = open(ra_file_path.c_str(), O_WRONLY | O_TRUNC | O_CREAT, 00664);
        if (ra_fd == -1) {
            std::string err_msg = "Failed to open file: " + ra_file_path + ", error: " + std::strerror(errno);
            LOG_ENGINE_ERROR_ << err_msg;
            throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
        }

        size_t ra_num_bytes = it->second->GetNbytes();
        if (::write(ra_fd, &ra_num_bytes, sizeof(size_t)) == -1) {
            std::string err_msg = "Failed to write to file: " + ra_file_path + ", error: " + std::strerror(errno);
            LOG_ENGINE_ERROR_ << err_msg;
            throw Exception(SERVER_WRITE_ERROR, err_msg);
        }
        if (::write(ra_fd, it->second->GetData().data(), ra_num_bytes) == -1) {
            std::string err_msg = "Failed to write to file: " + ra_file_path + ", error: " + std::strerror(errno);
            LOG_ENGINE_ERROR_ << err_msg;
            throw Exception(SERVER_WRITE_ERROR, err_msg);
        }
        if (::close(ra_fd) == -1) {
            std::string err_msg = "Failed to close file: " + ra_file_path + ", error: " + std::strerror(errno);
            LOG_ENGINE_ERROR_ << err_msg;
            throw Exception(SERVER_WRITE_ERROR, err_msg);
        }

        rc.RecordSection("write rv done");
    }
}

void
DefaultAttrsFormat::read_uids(const milvus::storage::FSHandlerPtr& fs_ptr, std::vector<int64_t>& uids) {
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

}  // namespace codec
}  // namespace milvus
