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

#include "cache/DataObj.h"
#include "dablooms/dablooms.h"
#include "utils/Status.h"

namespace milvus {
namespace segment {

using doc_id_t = int64_t;

class IdBloomFilter : public cache::DataObj {
 public:
    explicit IdBloomFilter(scaling_bloom_t* bloom_filter);

    ~IdBloomFilter();

    scaling_bloom_t*
    GetBloomFilter();

    bool
    Check(doc_id_t uid);

    Status
    Add(doc_id_t uid);

    Status
    Remove(doc_id_t uid);

    int64_t
    Size() override;

    //    const std::string&
    //    GetName() const;

    // No copy and move
    IdBloomFilter(const IdBloomFilter&) = delete;
    IdBloomFilter(IdBloomFilter&&) = delete;

    IdBloomFilter&
    operator=(const IdBloomFilter&) = delete;
    IdBloomFilter&
    operator=(IdBloomFilter&&) = delete;

 private:
    scaling_bloom_t* bloom_filter_;
    //    const std::string name_ = "bloom_filter";
    std::mutex mutex_;
};

using IdBloomFilterPtr = std::shared_ptr<IdBloomFilter>;

}  // namespace segment
}  // namespace milvus
