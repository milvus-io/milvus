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

    explicit InsertRecord(const Schema& schema, int64_t chunk_size);

    auto
    get_base_entity(FieldOffset field_offset) const {
        auto ptr = entity_vec_[field_offset.get()];
        return ptr;
    }

    template <typename Type>
    auto
    get_entity(FieldOffset field_offset) const {
        auto base_ptr = get_base_entity(field_offset);
        auto ptr = std::dynamic_pointer_cast<const ConcurrentVector<Type>>(base_ptr);
        Assert(ptr);
        return ptr;
    }

    template <typename Type>
    auto
    get_entity(FieldOffset field_offset) {
        auto base_ptr = get_base_entity(field_offset);
        auto ptr = std::dynamic_pointer_cast<ConcurrentVector<Type>>(base_ptr);
        Assert(ptr);
        return ptr;
    }

    template <typename Type>
    void
    insert_entity(int64_t chunk_size) {
        static_assert(std::is_fundamental_v<Type>);
        entity_vec_.emplace_back(std::make_shared<ConcurrentVector<Type>>(chunk_size));
    }

    template <typename VectorType>
    void
    insert_entity(int64_t dim, int64_t chunk_size) {
        static_assert(std::is_base_of_v<VectorTrait, VectorType>);
        entity_vec_.emplace_back(std::make_shared<ConcurrentVector<VectorType>>(dim, chunk_size));
    }

 private:
    std::vector<std::shared_ptr<VectorBase>> entity_vec_;
};
}  // namespace milvus::segcore
