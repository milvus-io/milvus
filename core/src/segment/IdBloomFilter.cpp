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
#include "utils/Log.h"
#include "utils/Status.h"

#include <string>

namespace milvus {
namespace segment {

IdBloomFilter::IdBloomFilter(scaling_bloom_t* bloom_filter) : bloom_filter_(bloom_filter) {
}

IdBloomFilter::~IdBloomFilter() {
    const std::lock_guard<std::mutex> lock(mutex_);
    if (bloom_filter_) {
        free_scaling_bloom(bloom_filter_);
    }
}

scaling_bloom_t*
IdBloomFilter::GetBloomFilter() {
    const std::lock_guard<std::mutex> lock(mutex_);
    return bloom_filter_;
}

bool
IdBloomFilter::Check(engine::idx_t uid) {
    std::string s = std::to_string(uid);
    const std::lock_guard<std::mutex> lock(mutex_);
    return scaling_bloom_check(bloom_filter_, s.c_str(), s.size());
}

Status
IdBloomFilter::Add(engine::idx_t uid) {
    std::string s = std::to_string(uid);
    const std::lock_guard<std::mutex> lock(mutex_);
    if (scaling_bloom_add(bloom_filter_, s.c_str(), s.size(), uid) == -1) {
        // Counter overflow does not affect bloom filter's normal functionality
        LOG_ENGINE_WARNING_ << "Warning adding id=" << s << " to bloom filter: 4 bit counter Overflow";
        // return Status(DB_BLOOM_FILTER_ERROR, "Bloom filter error: 4 bit counter Overflow");
    }
    return Status::OK();
}

Status
IdBloomFilter::Remove(engine::idx_t uid) {
    std::string s = std::to_string(uid);
    const std::lock_guard<std::mutex> lock(mutex_);
    if (scaling_bloom_remove(bloom_filter_, s.c_str(), s.size(), uid) == -1) {
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

Status
IdBloomFilter::Write(const std::string& path) {
    FILE* file = fopen(path.c_str(), "wb");
    if (file == nullptr) {
        return Status(DB_ERROR, "Failed to create bloom filter file");
    }

    if (fwrite(&(bloom_filter_->capacity), 1, sizeof(bloom_filter_->capacity), file) == 0) {
        fclose(file);
        return Status(DB_ERROR, "Failed to write bloom filter file");
    }

    if (fwrite(&(bloom_filter_->error_rate), 1, sizeof(bloom_filter_->error_rate), file) == 0) {
        fclose(file);
        return Status(DB_ERROR, "Failed to write bloom filter file");
    }

    if (fwrite(&(bloom_filter_->bitmap->bytes), 1, sizeof(bloom_filter_->bitmap->bytes), file) == 0) {
        fclose(file);
        return Status(DB_ERROR, "Failed to write bloom filter file");
    }

    if (fwrite(bloom_filter_->bitmap->array, 1, bloom_filter_->bitmap->bytes, file) == 0) {
        fclose(file);
        return Status(DB_ERROR, "Failed to write bloom filter file");
    }

    fclose(file);

    return Status::OK();
}

Status
IdBloomFilter::Read(const std::string& path) {
    if (bloom_filter_) {
        free_scaling_bloom(bloom_filter_);
    }

    FILE* file = fopen(path.c_str(), "rb");
    if (file == nullptr) {
        return Status(DB_ERROR, "Failed to open bloom filter file");
    }

    unsigned int capacity = 0;
    if (fread(&capacity, 1, sizeof(capacity), file) == 0) {
        fclose(file);
        return Status(DB_ERROR, "Failed to read bloom filter file");
    }

    double error_rate = 0.0;
    if (fread(&error_rate, 1, sizeof(error_rate), file) == 0) {
        fclose(file);
        return Status(DB_ERROR, "Failed to read bloom filter file");
    }

    size_t bitmap_bytes = 0;
    if (fread(&bitmap_bytes, 1, sizeof(bitmap_bytes), file) == 0) {
        fclose(file);
        return Status(DB_ERROR, "Failed to read bloom filter file");
    }

    bloom_filter_ = new_scaling_bloom(capacity, error_rate);
    if (bitmap_bytes != bloom_filter_->bitmap->bytes) {
        fclose(file);
        free_scaling_bloom(bloom_filter_);
        return Status(DB_ERROR, "Invalid bloom filter file");
    }

    if (fread(bloom_filter_->bitmap->array, 1, sizeof(bitmap_bytes), file) == 0) {
        fclose(file);
        free_scaling_bloom(bloom_filter_);
        return Status(DB_ERROR, "Failed to read bloom filter file");
    }

    return Status::OK();
}

}  // namespace segment
}  // namespace milvus
