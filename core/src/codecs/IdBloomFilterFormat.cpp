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

#include "codecs/IdBloomFilterFormat.h"

#include <fiu/fiu-local.h>
#include <memory>
#include <string>

#include "utils/Exception.h"
#include "utils/Log.h"

namespace milvus {
namespace codec {

const char* BLOOM_FILTER_POSTFIX = ".blf";

std::string
IdBloomFilterFormat::FilePostfix() {
    std::string str = BLOOM_FILTER_POSTFIX;
    return str;
}

Status
IdBloomFilterFormat::Read(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
                          segment::IdBloomFilterPtr& id_bloom_filter_ptr) {
    try {
        const std::string full_file_path = file_path + BLOOM_FILTER_POSTFIX;
        if (!fs_ptr->reader_ptr_->Open(full_file_path)) {
            return Status(SERVER_CANNOT_OPEN_FILE, "Fail to open bloom filter file: " + full_file_path);
        }

        id_bloom_filter_ptr = std::make_shared<segment::IdBloomFilter>();
        auto status = id_bloom_filter_ptr->Read(fs_ptr);
        if (!status.ok()) {
            fs_ptr->reader_ptr_->Close();
            return status;
        }

        fs_ptr->reader_ptr_->Close();
    } catch (std::exception& ex) {
        std::string msg = "Failed to read bloom filter file, reason: " + std::string(ex.what());
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_UNEXPECTED_ERROR, msg);
    }

    return Status::OK();
}

Status
IdBloomFilterFormat::Write(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
                           const segment::IdBloomFilterPtr& id_bloom_filter_ptr) {
    try {
        const std::string full_file_path = file_path + BLOOM_FILTER_POSTFIX;
        if (!fs_ptr->writer_ptr_->Open(full_file_path)) {
            return Status(SERVER_CANNOT_OPEN_FILE, "Fail to open bloom filter file: " + full_file_path);
        }

        auto status = id_bloom_filter_ptr->Write(fs_ptr);
        if (!status.ok()) {
            fs_ptr->writer_ptr_->Close();
            return status;
        }

        fs_ptr->writer_ptr_->Close();
    } catch (std::exception& ex) {
        std::string msg = "Failed to write bloom filter file, reason: " + std::string(ex.what());
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_UNEXPECTED_ERROR, msg);
    }

    return Status::OK();
}

}  // namespace codec
}  // namespace milvus
