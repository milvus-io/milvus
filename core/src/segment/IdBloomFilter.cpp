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

#include "segment/IdBloomFilter.h"
#include "db/Utils.h"
#include "utils/Exception.h"
#include "utils/Log.h"
#include "utils/Status.h"

#include <string>

namespace milvus {
namespace segment {

constexpr double BLOOM_FILTER_ERROR_RATE = 0.01;
constexpr int64_t CAPACITY_EXPAND = 1024;

// the magic num is converted from string "bloom_0"
constexpr int64_t BLOOM_FILE_MAGIC_NUM = 0x305F6D6F6F6C62;

IdBloomFilter::IdBloomFilter(int64_t capacity) : capacity_(capacity + CAPACITY_EXPAND) {
}

IdBloomFilter::~IdBloomFilter() {
    FreeBloomFilter();
}

scaling_bloom_t*
IdBloomFilter::GetBloomFilter() {
    if (bloom_filter_ == nullptr && capacity_ > 0) {
        bloom_filter_ = new_scaling_bloom(capacity_, BLOOM_FILTER_ERROR_RATE);
    }

    return bloom_filter_;
}

void
IdBloomFilter::FreeBloomFilter() {
    if (bloom_filter_) {
        free_scaling_bloom(bloom_filter_);
        bloom_filter_ = nullptr;
    }
}

bool
IdBloomFilter::Check(engine::idx_t uid) {
    std::string s = std::to_string(uid);
    scaling_bloom_t* bloom_filter = GetBloomFilter();
    if (bloom_filter == nullptr) {
        return true;  // bloom filter doesn't work, always return true
    }

    return scaling_bloom_check(bloom_filter, s.c_str(), s.size());
}

Status
IdBloomFilter::Add(engine::idx_t uid) {
    std::string s = std::to_string(uid);
    scaling_bloom_t* bloom_filter = GetBloomFilter();
    if (bloom_filter == nullptr) {
        return Status(DB_ERROR, "bloom filter is null pointer");  // bloom filter doesn't work
    }

    if (scaling_bloom_add(bloom_filter, s.c_str(), s.size(), uid) == -1) {
        // Counter overflow does not affect bloom filter's normal functionality
        LOG_ENGINE_WARNING_ << "Warning adding id=" << s << " to bloom filter: 4 bit counter Overflow";
        // return Status(DB_BLOOM_FILTER_ERROR, "Bloom filter error: 4 bit counter Overflow");
    }

    return Status::OK();
}

Status
IdBloomFilter::Remove(engine::idx_t uid) {
    std::string s = std::to_string(uid);
    scaling_bloom_t* bloom_filter = GetBloomFilter();
    if (bloom_filter == nullptr) {
        return Status(DB_ERROR, "bloom filter is null pointer");  // bloom filter doesn't work
    }

    if (scaling_bloom_remove(bloom_filter, s.c_str(), s.size(), uid) == -1) {
        // Should never go in here, but just to be safe
        LOG_ENGINE_WARNING_ << "Warning removing id=" << s << " in bloom filter: Decrementing zero in counter";
        // return Status(DB_BLOOM_FILTER_ERROR, "Error removing in bloom filter: Decrementing zero in counter");
    }
    return Status::OK();
}

// const std::string&
// IdBloomFilter::GetName() const {
//    return name_;
//}

int64_t
IdBloomFilter::Size() {
    return bloom_filter_ ? bloom_filter_->num_bytes : 0;
}

double
IdBloomFilter::ErrorRate() const {
    return BLOOM_FILTER_ERROR_RATE;
}

Status
IdBloomFilter::Write(const storage::FSHandlerPtr& fs_ptr) {
    scaling_bloom_t* bloom_filter = GetBloomFilter();

    try {
        fs_ptr->writer_ptr_->Write(&(BLOOM_FILE_MAGIC_NUM), sizeof(BLOOM_FILE_MAGIC_NUM));
        fs_ptr->writer_ptr_->Write(&(bloom_filter->capacity), sizeof(bloom_filter->capacity));
        fs_ptr->writer_ptr_->Write(&(bloom_filter->error_rate), sizeof(bloom_filter->error_rate));
        fs_ptr->writer_ptr_->Write(&(bloom_filter->bitmap->bytes), sizeof(bloom_filter->bitmap->bytes));
        fs_ptr->writer_ptr_->Write(bloom_filter->bitmap->array, bloom_filter->bitmap->bytes);
    } catch (std::exception& ex) {
        std::string err_msg = "Failed to write bloom filter: " + std::string(ex.what());
        LOG_ENGINE_ERROR_ << err_msg;

        engine::utils::SendExitSignal();
        return Status(SERVER_WRITE_ERROR, err_msg);
    }

    return Status::OK();
}

Status
IdBloomFilter::Read(const storage::FSHandlerPtr& fs_ptr) {
    FreeBloomFilter();

    try {
        int64_t magic_num = 0;
        fs_ptr->reader_ptr_->Read(&magic_num, sizeof(magic_num));
        if (magic_num != BLOOM_FILE_MAGIC_NUM) {
            LOG_ENGINE_ERROR_ << "legacy bloom filter file, could not read bloom filter data";
            return Status(DB_ERROR, "");
        }

        unsigned int capacity = 0;
        fs_ptr->reader_ptr_->Read(&capacity, sizeof(capacity));
        capacity_ = capacity;

        double error_rate = 0.0;
        fs_ptr->reader_ptr_->Read(&error_rate, sizeof(error_rate));

        size_t bitmap_bytes = 0;
        fs_ptr->reader_ptr_->Read(&bitmap_bytes, sizeof(bitmap_bytes));

        bloom_filter_ = new_scaling_bloom(capacity, error_rate);
        if (bitmap_bytes != bloom_filter_->bitmap->bytes) {
            FreeBloomFilter();
            return Status(DB_ERROR, "Invalid bloom filter file");
        }

        fs_ptr->reader_ptr_->Read(bloom_filter_->bitmap->array, bitmap_bytes);
    } catch (std::exception& ex) {
        std::string err_msg = "Failed to read bloom filter: " + std::string(ex.what());
        LOG_ENGINE_ERROR_ << err_msg;

        FreeBloomFilter();
        return Status(SERVER_UNEXPECTED_ERROR, err_msg);
    }

    return Status::OK();
}

Status
IdBloomFilter::Clone(IdBloomFilterPtr& target) {
    scaling_bloom_t* this_bloom = GetBloomFilter();
    if (this_bloom == nullptr) {
        return Status(DB_ERROR, "Source bloom filter is null");
    }

    target = std::make_shared<IdBloomFilter>(this_bloom->capacity - CAPACITY_EXPAND);
    auto target_bloom = target->GetBloomFilter();
    if (target_bloom->bitmap->bytes != this_bloom->bitmap->bytes) {
        free(target_bloom->bitmap->array);
        target_bloom->bitmap->bytes = this_bloom->bitmap->bytes;
        target_bloom->bitmap->array = new char[this_bloom->bitmap->bytes];
    }

    memcpy(target_bloom->bitmap->array, this_bloom->bitmap->array, this_bloom->bitmap->bytes);

    return Status::OK();
}

}  // namespace segment
}  // namespace milvus
