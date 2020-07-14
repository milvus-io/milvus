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

#include "codecs/snapshot/SSBlockFormat.h"

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
SSBlockFormat::read(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, std::vector<uint8_t>& raw) {
    if (!fs_ptr->reader_ptr_->open(file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_CANNOT_OPEN_FILE, err_msg);
    }

    size_t num_bytes;
    fs_ptr->reader_ptr_->read(&num_bytes, sizeof(size_t));

    raw.resize(num_bytes);
    fs_ptr->reader_ptr_->read(raw.data(), num_bytes);

    fs_ptr->reader_ptr_->close();
}

void
SSBlockFormat::write(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
                     const std::vector<uint8_t>& raw) {
    if (!fs_ptr->writer_ptr_->open(file_path.c_str())) {
        std::string err_msg = "Failed to open file: " + file_path + ", error: " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_CANNOT_CREATE_FILE, err_msg);
    }

    size_t num_bytes = raw.size();
    fs_ptr->writer_ptr_->write(&num_bytes, sizeof(size_t));
    fs_ptr->writer_ptr_->write((void*)raw.data(), num_bytes);
    fs_ptr->writer_ptr_->close();
}

}  // namespace codec
}  // namespace milvus
