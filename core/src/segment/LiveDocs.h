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

#include "index/thirdparty/faiss/utils/ConcurrentBitset.h"

namespace milvus {
namespace segment {

class LiveDocs {
 public:
    LiveDocs(faiss::ConcurrentBitset bitset);

    void
    GetBitset(faiss::ConcurrentBitset& bitset);

    // No copy and move
    LiveDocs(const LiveDocs&) = delete;
    LiveDocs(LiveDocs&&) = delete;

    LiveDocs&
    operator=(const LiveDocs&) = delete;
    LiveDocs&
    operator=(LiveDocs&&) = delete;

 private:
    faiss::ConcurrentBitset bitset_;
};

using LiveDocsPtr = std::shared_ptr<LiveDocs>;

}  // namespace segment
}  // namespace milvus
