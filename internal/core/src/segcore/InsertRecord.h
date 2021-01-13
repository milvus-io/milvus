// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once
#include "common/Schema.h"
#include "segcore/ConcurrentVector.h"
#include "segcore/AckResponder.h"
#include "segcore/Record.h"
#include <memory>
#include <vector>

namespace milvus::segcore {
struct InsertRecord {
    std::atomic<int64_t> reserved = 0;
    AckResponder ack_responder_;
    ConcurrentVector<Timestamp> timestamps_;
    ConcurrentVector<idx_t> uids_;
    std::vector<std::shared_ptr<VectorBase>> entity_vec_;

    explicit InsertRecord(const Schema& schema, int64_t chunk_size);
    template <typename Type>
    auto
    get_entity(int offset) const {
        auto ptr = std::dynamic_pointer_cast<const ConcurrentVector<Type>>(entity_vec_[offset]);
        Assert(ptr);
        return ptr;
    }

    template <typename Type>
    auto
    get_entity(int offset) {
        auto ptr = std::dynamic_pointer_cast<ConcurrentVector<Type>>(entity_vec_[offset]);
        Assert(ptr);
        return ptr;
    }
};
}  // namespace milvus::segcore
