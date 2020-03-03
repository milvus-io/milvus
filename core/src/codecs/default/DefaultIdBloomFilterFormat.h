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

#include <mutex>
#include <string>

#include "codecs/IdBloomFilterFormat.h"
#include "segment/IdBloomFilter.h"
#include "store/Directory.h"

namespace milvus {
namespace codec {

class DefaultIdBloomFilterFormat : public IdBloomFilterFormat {
 public:
    DefaultIdBloomFilterFormat() = default;

    void
    read(const store::DirectoryPtr& directory_ptr, segment::IdBloomFilterPtr& id_bloom_filter_ptr) override;

    void
    write(const store::DirectoryPtr& directory_ptr, const segment::IdBloomFilterPtr& id_bloom_filter_ptr) override;

    void
    create(const store::DirectoryPtr& directory_ptr, segment::IdBloomFilterPtr& id_bloom_filter_ptr) override;

    // No copy and move
    DefaultIdBloomFilterFormat(const DefaultIdBloomFilterFormat&) = delete;
    DefaultIdBloomFilterFormat(DefaultIdBloomFilterFormat&&) = delete;

    DefaultIdBloomFilterFormat&
    operator=(const DefaultIdBloomFilterFormat&) = delete;
    DefaultIdBloomFilterFormat&
    operator=(DefaultIdBloomFilterFormat&&) = delete;

 private:
    std::mutex mutex_;

    const std::string bloom_filter_filename_ = "bloom_filter";
};

}  // namespace codec
}  // namespace milvus
