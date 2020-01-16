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

#include <string>

namespace milvus {
namespace segment {

IdBloomFilter::IdBloomFilter(scaling_bloom_t* bloom_filter) : bloom_filter_(bloom_filter) {
}

IdBloomFilter::~IdBloomFilter() {
    const std::lock_guard<std::mutex> lock(mutex_);
    free_scaling_bloom(bloom_filter_);
}

scaling_bloom_t*
IdBloomFilter::GetBloomFilter() {
    const std::lock_guard<std::mutex> lock(mutex_);
    return bloom_filter_;
}

bool
IdBloomFilter::Check(doc_id_t uid) {
    std::string s = std::to_string(uid);
    const std::lock_guard<std::mutex> lock(mutex_);
    return scaling_bloom_check(bloom_filter_, s.c_str(), s.size());
}

void
IdBloomFilter::Add(doc_id_t uid) {
    std::string s = std::to_string(uid);
    const std::lock_guard<std::mutex> lock(mutex_);
    scaling_bloom_add(bloom_filter_, s.c_str(), s.size(), uid);
}

void
IdBloomFilter::Remove(doc_id_t uid) {
    std::string s = std::to_string(uid);
    const std::lock_guard<std::mutex> lock(mutex_);
    scaling_bloom_remove(bloom_filter_, s.c_str(), s.size(), uid);
}

// const std::string&
// IdBloomFilter::GetName() const {
//    return name_;
//}

size_t
IdBloomFilter::Size() {
    return bloom_filter_->num_bytes;
}

}  // namespace segment
}  // namespace milvus
