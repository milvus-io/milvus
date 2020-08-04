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

#include "codecs/BlockFormat.h"

#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <boost/filesystem.hpp>
#include <memory>

#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace codec {

void
BlockFormat::Read(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, engine::BinaryDataPtr& raw) {
    if (!fs_ptr->reader_ptr_->open(file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_CANNOT_OPEN_FILE, err_msg);
    }

    size_t num_bytes;
    fs_ptr->reader_ptr_->read(&num_bytes, sizeof(size_t));

    raw = std::make_shared<engine::BinaryData>();
    raw->data_.resize(num_bytes);
    fs_ptr->reader_ptr_->read(raw->data_.data(), num_bytes);

    fs_ptr->reader_ptr_->close();
}

void
BlockFormat::Read(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, int64_t offset, int64_t num_bytes,
                  engine::BinaryDataPtr& raw) {
    if (offset < 0 || num_bytes <= 0) {
        std::string err_msg = "Invalid input to read: " + file_path;
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_INVALID_ARGUMENT, err_msg);
    }

    if (!fs_ptr->reader_ptr_->open(file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_CANNOT_OPEN_FILE, err_msg);
    }

    size_t total_num_bytes;
    fs_ptr->reader_ptr_->read(&total_num_bytes, sizeof(size_t));

    offset += sizeof(size_t);  // Beginning of file is num_bytes
    if (offset + num_bytes > total_num_bytes) {
        std::string err_msg = "Invalid input to read: " + file_path;
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_INVALID_ARGUMENT, err_msg);
    }

    raw = std::make_shared<engine::BinaryData>();
    raw->data_.resize(num_bytes);
    fs_ptr->reader_ptr_->seekg(offset);
    fs_ptr->reader_ptr_->read(raw->data_.data(), num_bytes);
    fs_ptr->reader_ptr_->close();
}

void
BlockFormat::Read(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, const ReadRanges& read_ranges,
                  engine::BinaryDataPtr& raw) {
    if (read_ranges.empty()) {
        return;
    }

    if (!fs_ptr->reader_ptr_->open(file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_CANNOT_OPEN_FILE, err_msg);
    }

    size_t total_num_bytes;
    fs_ptr->reader_ptr_->read(&total_num_bytes, sizeof(size_t));

    int64_t total_bytes = 0;
    for (auto& range : read_ranges) {
        if (range.offset_ > total_num_bytes) {
            std::string err_msg = "Invalid input to read: " + file_path;
            LOG_ENGINE_ERROR_ << err_msg;
            throw Exception(SERVER_INVALID_ARGUMENT, err_msg);
        }

        total_bytes += range.num_bytes_;
    }

    raw = std::make_shared<engine::BinaryData>();
    raw->data_.resize(total_bytes);
    int64_t poz = 0;
    for (auto& range : read_ranges) {
        int64_t offset = range.offset_ + sizeof(size_t);
        fs_ptr->reader_ptr_->seekg(offset);
        fs_ptr->reader_ptr_->read(raw->data_.data() + poz, range.num_bytes_);
        poz += range.num_bytes_;
    }

    fs_ptr->reader_ptr_->close();
}

void
BlockFormat::Write(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
                   const engine::BinaryDataPtr& raw) {
    if (raw == nullptr) {
        return;
    }

    if (!fs_ptr->writer_ptr_->open(file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
    }

    size_t num_bytes = raw->data_.size();
    fs_ptr->writer_ptr_->write(&num_bytes, sizeof(size_t));
    fs_ptr->writer_ptr_->write((void*)(raw->data_.data()), num_bytes);
    fs_ptr->writer_ptr_->close();
}

}  // namespace codec
}  // namespace milvus
