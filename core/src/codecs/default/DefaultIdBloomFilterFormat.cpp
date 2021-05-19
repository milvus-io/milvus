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

#include <fiu-local.h>

#include <memory>
#include <string>

#include "utils/Exception.h"
#include "utils/Log.h"

namespace milvus {
namespace codec {

// for compatibility with version 1.0.0
constexpr unsigned int BLOOM_FILTER_CAPACITY = 500000;
constexpr double BLOOM_FILTER_ERROR_RATE = 0.01;

// the magic num is converted from string "bloom_0"
constexpr int64_t BLOOM_FILTER_MAGIC_NUM = 0x305F6D6F6F6C62;

void
DefaultIdBloomFilterFormat::read(const storage::FSHandlerPtr& fs_ptr, segment::IdBloomFilterPtr& id_bloom_filter_ptr) {
    const std::lock_guard<std::mutex> lock(mutex_);

    std::string dir_path = fs_ptr->operation_ptr_->GetDirectory();
    const std::string bloom_filter_file_path = dir_path + "/" + bloom_filter_filename_;
    scaling_bloom_t* bloom_filter{nullptr};
    do {
        if (!fs_ptr->operation_ptr_->CacheGet(bloom_filter_file_path)) {
            LOG_ENGINE_ERROR_ << "Fail to cache get bloom filter: " << bloom_filter_file_path;
            break;
        }
        if (!fs_ptr->reader_ptr_->open(bloom_filter_file_path)) {
            LOG_ENGINE_ERROR_ << "Fail to open bloom filter: " << bloom_filter_file_path;
            break;
        }

        unsigned int capacity = 0;
        double error_rate = 0;
        size_t bitmap_bytes = 0;

        int64_t magic_num = 0;
        fs_ptr->reader_ptr_->read(&magic_num, sizeof(magic_num));
        if (magic_num != BLOOM_FILTER_MAGIC_NUM) {
            capacity = BLOOM_FILTER_CAPACITY;
            error_rate = BLOOM_FILTER_ERROR_RATE;
            bitmap_bytes = static_cast<size_t>(fs_ptr->reader_ptr_->length());
            fs_ptr->reader_ptr_->seekg(0);
        } else {
            fs_ptr->reader_ptr_->read(&capacity, sizeof(capacity));
            fs_ptr->reader_ptr_->read(&error_rate, sizeof(error_rate));
            fs_ptr->reader_ptr_->read(&bitmap_bytes, sizeof(bitmap_bytes));
        }

        bitmap_t* bitmap = new_bitmap(bitmap_bytes);
        if (bitmap != nullptr) {
            fs_ptr->reader_ptr_->read(bitmap->array, bitmap_bytes);
            bloom_filter = new_scaling_bloom_from_bitmap(capacity, error_rate, bitmap);
            if (bloom_filter == nullptr) {
                free_bitmap(bitmap);
            }
        }

        fs_ptr->reader_ptr_->close();
    } while (0);

    fiu_do_on("bloom_filter_nullptr", (free_scaling_bloom(bloom_filter) || (bloom_filter = nullptr)));
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
    if (!fs_ptr->writer_ptr_->open(bloom_filter_file_path)) {
        std::string err_msg =
            "Failed to write bloom filter to file: " + bloom_filter_file_path + ". " + std::strerror(errno);
        LOG_ENGINE_ERROR_ << err_msg;
        throw Exception(SERVER_UNEXPECTED_ERROR, err_msg);
    }

    auto bloom_filter = id_bloom_filter_ptr->GetBloomFilter();

    int64_t magic_num = BLOOM_FILTER_MAGIC_NUM;
    fs_ptr->writer_ptr_->write(&magic_num, sizeof(magic_num));
    fs_ptr->writer_ptr_->write(&bloom_filter->capacity, sizeof(bloom_filter->capacity));
    fs_ptr->writer_ptr_->write(&bloom_filter->error_rate, sizeof(bloom_filter->error_rate));
    fs_ptr->writer_ptr_->write(&bloom_filter->bitmap->bytes, sizeof(bloom_filter->bitmap->bytes));
    fs_ptr->writer_ptr_->write(bloom_filter->bitmap->array, bloom_filter->bitmap->bytes);
    fs_ptr->writer_ptr_->close();
}

void
DefaultIdBloomFilterFormat::create(int64_t capacity, segment::IdBloomFilterPtr& id_bloom_filter_ptr) {
    scaling_bloom_t* bloom_filter = new_scaling_bloom(capacity, BLOOM_FILTER_ERROR_RATE);
    id_bloom_filter_ptr = std::make_shared<segment::IdBloomFilter>(bloom_filter);
}

}  // namespace codec
}  // namespace milvus
