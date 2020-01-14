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

#include "ConcurrentBitset.h"

namespace faiss {

ConcurrentBitset::ConcurrentBitset(id_type_t size) : size_(size) {
    id_type_t bytes_count = (size >> 3) + 1;
    // bitset_.resize(bytes_count, 0);
    for (auto i = 0; i < bytes_count; ++i) {
        bitset_.emplace_back(0);
    }
}

bool
ConcurrentBitset::test(id_type_t id) {
    return bitset_[id >> 3].load() & (0x1 << (id & 0x7));
}

void
ConcurrentBitset::set(id_type_t id) {
    bitset_[id >> 3].fetch_or(0x1 << (id & 0x7));
}

void
ConcurrentBitset::clear(id_type_t id) {
    bitset_[id >> 3].fetch_and(~(0x1 << (id & 0x7)));
}

}  // namespace faiss
