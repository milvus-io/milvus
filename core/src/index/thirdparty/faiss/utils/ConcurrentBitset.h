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

#include <atomic>
#include <memory>
#include <deque>

namespace faiss {

class ConcurrentBitset {
 public:
    using id_type_t = int64_t;

    explicit ConcurrentBitset(id_type_t size);

    //    ConcurrentBitset(const ConcurrentBitset&) = delete;
    //    ConcurrentBitset&
    //    operator=(const ConcurrentBitset&) = delete;

    bool
    test(id_type_t id);

    void
    set(id_type_t id);

    void
    clear(id_type_t id);

 private:
    std::deque<std::atomic<unsigned char>> bitset_;
    id_type_t size_;
};

using ConcurrentBitsetPtr = std::shared_ptr<ConcurrentBitset>;

}  // namespace faiss
