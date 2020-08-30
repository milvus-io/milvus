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

constexpr unsigned int BLOOM_FILTER_CAPACITY = 500000;
constexpr double BLOOM_FILTER_ERROR_RATE = 0.01;

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
        scaling_bloom_t* bloom_filter =
            new_scaling_bloom_from_file(BLOOM_FILTER_CAPACITY, BLOOM_FILTER_ERROR_RATE, full_file_path.c_str());
        fiu_do_on("bloom_filter_nullptr", bloom_filter = nullptr);
        if (bloom_filter == nullptr) {
            return Status(SERVER_UNEXPECTED_ERROR, "Fail to read bloom filter from file: " + full_file_path);
        }
        id_bloom_filter_ptr = std::make_shared<segment::IdBloomFilter>(bloom_filter);
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
        if (scaling_bloom_flush(id_bloom_filter_ptr->GetBloomFilter()) == -1) {
            return Status(SERVER_UNEXPECTED_ERROR, "Fail to write bloom filter to file: " + full_file_path);
        }
    } catch (std::exception& ex) {
        std::string msg = "Failed to write bloom filter file, reason: " + std::string(ex.what());
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_UNEXPECTED_ERROR, msg);
    }

    return Status::OK();
}

Status
IdBloomFilterFormat::Create(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
                            segment::IdBloomFilterPtr& id_bloom_filter_ptr) {
    try {
        const std::string full_file_path = file_path + BLOOM_FILTER_POSTFIX;
        scaling_bloom_t* bloom_filter =
            new_scaling_bloom(BLOOM_FILTER_CAPACITY, BLOOM_FILTER_ERROR_RATE, full_file_path.c_str());
        if (bloom_filter == nullptr) {
            return Status(SERVER_UNEXPECTED_ERROR, "Failed to read bloom filter from file: " + full_file_path);
        }
        id_bloom_filter_ptr = std::make_shared<segment::IdBloomFilter>(bloom_filter);
    } catch (std::exception& ex) {
        std::string msg = "Failed to create bloom filter file, reason: " + std::string(ex.what());
        LOG_SERVER_ERROR_ << msg;
        return Status(SERVER_UNEXPECTED_ERROR, msg);
    }

    return Status::OK();
}

}  // namespace codec
}  // namespace milvus
