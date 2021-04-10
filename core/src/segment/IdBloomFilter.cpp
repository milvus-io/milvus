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
    if (bloom_filter_) {
        free_scaling_bloom(bloom_filter_);
    }
}

scaling_bloom_t*
IdBloomFilter::GetBloomFilter() {
    return bloom_filter_;
}

bool
IdBloomFilter::Check(doc_id_t uid) {
    if (bloom_filter_ == nullptr) {
        return true;
    }

    std::string s = std::to_string(uid);
    const std::lock_guard<std::mutex> lock(mutex_);
    return static_cast<bool>(scaling_bloom_check(bloom_filter_, s.c_str(), s.size()));
}

Status
IdBloomFilter::Add(const std::vector<doc_id_t>& uids) {
    if (bloom_filter_ == nullptr) {
        return Status(DB_ERROR, "bloom filter is null pointer");  // bloom filter doesn't work
    }

    const std::lock_guard<std::mutex> lock(mutex_);
    for (auto uid : uids) {
        std::string s = std::to_string(uid);
        if (scaling_bloom_add(bloom_filter_, s.c_str(), s.size(), uid) == -1) {
            // Counter overflow does not affect bloom filter's normal functionality
            LOG_ENGINE_WARNING_ << "Warning adding id=" << s << " to bloom filter: 4 bit counter Overflow";
        }
    }
    return Status::OK();
}

Status
IdBloomFilter::Remove(doc_id_t uid) {
    if (bloom_filter_ == nullptr) {
        return Status(DB_ERROR, "bloom filter is null pointer");  // bloom filter doesn't work
    }

    std::string s = std::to_string(uid);
    const std::lock_guard<std::mutex> lock(mutex_);
    if (scaling_bloom_remove(bloom_filter_, s.c_str(), s.size(), uid) == -1) {
        // Should never go in here, but just to be safe
        LOG_ENGINE_WARNING_ << "Warning removing id=" << s << " in bloom filter: Decrementing zero in counter";
    }
    return Status::OK();
}

int64_t
IdBloomFilter::Size() {
    const std::lock_guard<std::mutex> lock(mutex_);
    return bloom_size(bloom_filter_);
}

}  // namespace segment
}  // namespace milvus
