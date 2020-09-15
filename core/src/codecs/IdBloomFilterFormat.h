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
#include <string>

#include "segment/IdBloomFilter.h"
#include "storage/FSHandler.h"
#include "utils/Status.h"

namespace milvus {
namespace codec {

class IdBloomFilterFormat {
 public:
    IdBloomFilterFormat() = default;

    static std::string
    FilePostfix();

    Status
    Read(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
         segment::IdBloomFilterPtr& id_bloom_filter_ptr);

    Status
    Write(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
          const segment::IdBloomFilterPtr& id_bloom_filter_ptr);

    // No copy and move
    IdBloomFilterFormat(const IdBloomFilterFormat&) = delete;
    IdBloomFilterFormat(IdBloomFilterFormat&&) = delete;

    IdBloomFilterFormat&
    operator=(const IdBloomFilterFormat&) = delete;
    IdBloomFilterFormat&
    operator=(IdBloomFilterFormat&&) = delete;
};

using IdBloomFilterFormatPtr = std::shared_ptr<IdBloomFilterFormat>;

}  // namespace codec
}  // namespace milvus
