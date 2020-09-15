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

#pragma once

#include <memory>
#include <mutex>
#include <string>

#include "cache/DataObj.h"
#include "dablooms/dablooms.h"
#include "db/Types.h"
#include "storage/FSHandler.h"
#include "utils/Status.h"

namespace milvus {
namespace segment {

constexpr int64_t DEFAULT_BLOOM_FILTER_CAPACITY = 500000;

class IdBloomFilter;
using IdBloomFilterPtr = std::shared_ptr<IdBloomFilter>;

class IdBloomFilter : public cache::DataObj {
 public:
    explicit IdBloomFilter(int64_t capacity = DEFAULT_BLOOM_FILTER_CAPACITY);

    ~IdBloomFilter();

    bool
    Check(engine::idx_t uid);

    Status
    Add(engine::idx_t uid);

    Status
    Remove(engine::idx_t uid);

    int64_t
    Size() override;

    double
    ErrorRate() const;

    Status
    Write(const storage::FSHandlerPtr& fs_ptr);

    Status
    Read(const storage::FSHandlerPtr& fs_ptr);

    Status
    Clone(IdBloomFilterPtr& target);

    // No copy and move
    IdBloomFilter(const IdBloomFilter&) = delete;
    IdBloomFilter(IdBloomFilter&&) = delete;

    IdBloomFilter&
    operator=(const IdBloomFilter&) = delete;
    IdBloomFilter&
    operator=(IdBloomFilter&&) = delete;

 private:
    scaling_bloom_t*
    GetBloomFilter();

    void
    FreeBloomFilter();

 private:
    scaling_bloom_t* bloom_filter_ = nullptr;
    int64_t capacity_ = 0;
};

}  // namespace segment
}  // namespace milvus
