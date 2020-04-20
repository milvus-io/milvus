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

#include "codecs/default/DefaultIdBloomFilterFormat.h"

#include <memory>
#include <string>

#include "utils/Exception.h"
#include "utils/Log.h"

namespace milvus {
namespace codec {

constexpr unsigned int bloom_filter_capacity = 500000;
constexpr double bloom_filter_error_rate = 0.01;

void
DefaultIdBloomFilterFormat::read(const storage::FSHandlerPtr& fs_ptr, segment::IdBloomFilterPtr& id_bloom_filter_ptr) {
    const std::lock_guard<std::mutex> lock(mutex_);

    std::string dir_path = fs_ptr->operation_ptr_->GetDirectory();
    const std::string bloom_filter_file_path = dir_path + "/" + bloom_filter_filename_;
    scaling_bloom_t* bloom_filter =
        new_scaling_bloom_from_file(bloom_filter_capacity, bloom_filter_error_rate, bloom_filter_file_path.c_str());
    if (bloom_filter == nullptr) {
        std::string err_msg =
            "Failed to read bloom filter from file: " + bloom_filter_file_path + ". " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_UNEXPECTED_ERROR, err_msg);
    }
    id_bloom_filter_ptr = std::make_shared<segment::IdBloomFilter>(bloom_filter);
}

void
DefaultIdBloomFilterFormat::write(const storage::FSHandlerPtr& fs_ptr,
                                  const segment::IdBloomFilterPtr& id_bloom_filter_ptr) {
    const std::lock_guard<std::mutex> lock(mutex_);

    std::string dir_path = fs_ptr->operation_ptr_->GetDirectory();
    const std::string bloom_filter_file_path = dir_path + "/" + bloom_filter_filename_;
    if (scaling_bloom_flush(id_bloom_filter_ptr->GetBloomFilter()) == -1) {
        std::string err_msg =
            "Failed to write bloom filter to file: " + bloom_filter_file_path + ". " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_UNEXPECTED_ERROR, err_msg);
    }
}

void
DefaultIdBloomFilterFormat::create(const storage::FSHandlerPtr& fs_ptr,
                                   segment::IdBloomFilterPtr& id_bloom_filter_ptr) {
    std::string dir_path = fs_ptr->operation_ptr_->GetDirectory();
    const std::string bloom_filter_file_path = dir_path + "/" + bloom_filter_filename_;
    scaling_bloom_t* bloom_filter =
        new_scaling_bloom(bloom_filter_capacity, bloom_filter_error_rate, bloom_filter_file_path.c_str());
    if (bloom_filter == nullptr) {
        std::string err_msg =
            "Failed to read bloom filter from file: " + bloom_filter_file_path + ". " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_UNEXPECTED_ERROR, err_msg);
    }
    id_bloom_filter_ptr = std::make_shared<segment::IdBloomFilter>(bloom_filter);
}

}  // namespace codec
}  // namespace milvus
