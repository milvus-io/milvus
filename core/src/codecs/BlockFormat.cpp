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

#include <unistd.h>
#include <algorithm>
#include <boost/filesystem.hpp>
#include <memory>
#include <unordered_map>

#include "storage/ExtraFileInfo.h"
#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace codec {

void
BlockFormat::Read(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, engine::BinaryDataPtr& raw) {
    CHECK_MAGIC_VALID(fs_ptr, file_path);
    CHECK_SUM_VALID(fs_ptr, file_path);
    if (!fs_ptr->reader_ptr_->Open(file_path)) {
        THROW_ERROR(SERVER_CANNOT_OPEN_FILE, "Fail to open file: " + file_path);
    }

    fs_ptr->reader_ptr_->Seekg(MAGIC_SIZE + HEADER_SIZE);
    size_t num_bytes;
    fs_ptr->reader_ptr_->Read(&num_bytes, sizeof(size_t));

    raw = std::make_shared<engine::BinaryData>();
    raw->data_.resize(num_bytes);
    fs_ptr->reader_ptr_->Read(raw->data_.data(), num_bytes);
    fs_ptr->reader_ptr_->Close();
}

void
BlockFormat::Read(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, int64_t offset, int64_t num_bytes,
                  engine::BinaryDataPtr& raw) {
    CHECK_MAGIC_VALID(fs_ptr, file_path);
    CHECK_SUM_VALID(fs_ptr, file_path);
    if (offset < 0 || num_bytes <= 0) {
        THROW_ERROR(SERVER_INVALID_ARGUMENT, "Invalid input to read: " + file_path);
    }

    if (!fs_ptr->reader_ptr_->Open(file_path)) {
        THROW_ERROR(SERVER_CANNOT_OPEN_FILE, "Fail to open file: " + file_path);
    }

    fs_ptr->reader_ptr_->Seekg(MAGIC_SIZE + HEADER_SIZE);

    size_t total_num_bytes;
    fs_ptr->reader_ptr_->Read(&total_num_bytes, sizeof(size_t));

    offset += MAGIC_SIZE + HEADER_SIZE + sizeof(size_t);  // Beginning of file is num_bytes
    if (offset + num_bytes > total_num_bytes) {
        THROW_ERROR(SERVER_INVALID_ARGUMENT, "Invalid argument to read: " + file_path);
    }

    raw = std::make_shared<engine::BinaryData>();
    raw->data_.resize(num_bytes);
    fs_ptr->reader_ptr_->Seekg(offset);
    fs_ptr->reader_ptr_->Read(raw->data_.data(), num_bytes);
    fs_ptr->reader_ptr_->Close();
}

void
BlockFormat::Read(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, const ReadRanges& read_ranges,
                  engine::BinaryDataPtr& raw) {
    CHECK_MAGIC_VALID(fs_ptr, file_path);
    CHECK_SUM_VALID(fs_ptr, file_path);
    if (read_ranges.empty()) {
        return;
    }

    if (!fs_ptr->reader_ptr_->Open(file_path)) {
        THROW_ERROR(SERVER_CANNOT_OPEN_FILE, "Fail to open file: " + file_path);
    }

    fs_ptr->reader_ptr_->Seekg(MAGIC_SIZE + HEADER_SIZE);
    size_t total_num_bytes;
    fs_ptr->reader_ptr_->Read(&total_num_bytes, sizeof(size_t));

    int64_t total_bytes = 0;
    for (auto& range : read_ranges) {
        if (range.offset_ > total_num_bytes) {
            THROW_ERROR(SERVER_INVALID_ARGUMENT, "Invalid argument to read: " + file_path);
        }
        total_bytes += range.num_bytes_;
    }

    raw = std::make_shared<engine::BinaryData>();
    raw->data_.resize(total_bytes);
    int64_t poz = 0;
    for (auto& range : read_ranges) {
        int64_t offset = MAGIC_SIZE + HEADER_SIZE + sizeof(size_t) + range.offset_;
        fs_ptr->reader_ptr_->Seekg(offset);
        fs_ptr->reader_ptr_->Read(raw->data_.data() + poz, range.num_bytes_);
        poz += range.num_bytes_;
    }
    fs_ptr->reader_ptr_->Close();
}

void
BlockFormat::Write(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
                   const engine::BinaryDataPtr& raw) {
    if (raw == nullptr) {
        return;
    }
    // TODO: add extra info
    std::unordered_map<std::string, std::string> maps;
    WRITE_MAGIC(fs_ptr, file_path)
    WRITE_HEADER(fs_ptr, file_path, maps);

    if (!fs_ptr->writer_ptr_->InOpen(file_path)) {
        THROW_ERROR(SERVER_CANNOT_CREATE_FILE, "Fail to open file: " + file_path);
    }

    fs_ptr->writer_ptr_->Seekp(MAGIC_SIZE + HEADER_SIZE);

    size_t num_bytes = raw->data_.size();
    fs_ptr->writer_ptr_->Write(&num_bytes, sizeof(size_t));
    fs_ptr->writer_ptr_->Write((void*)(raw->data_.data()), num_bytes);
    fs_ptr->writer_ptr_->Close();

    WRITE_SUM(fs_ptr, file_path);
}

}  // namespace codec
}  // namespace milvus
