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

    explicit InsertRecord(const Schema& schema, int64_t size_per_chunk);

    // get field data without knowing the type
    VectorBase*
    get_field_data_base(FieldOffset field_offset) const {
        auto ptr = fields_data_[field_offset.get()].get();
        return ptr;
    }

    // get field data in given type, const version
    template <typename Type>
    const ConcurrentVector<Type>*
    get_field_data(FieldOffset field_offset) const {
        auto base_ptr = get_field_data_base(field_offset);
        auto ptr = dynamic_cast<const ConcurrentVector<Type>*>(base_ptr);
        Assert(ptr);
        return ptr;
    }

    // get field data in given type, non-const version
    template <typename Type>
    ConcurrentVector<Type>*
    get_field_data(FieldOffset field_offset) {
        auto base_ptr = get_field_data_base(field_offset);
        auto ptr = dynamic_cast<ConcurrentVector<Type>*>(base_ptr);
        Assert(ptr);
        return ptr;
    }

    // append a column of scalar type
    template <typename Type>
    void
    append_field_data(int64_t size_per_chunk) {
        static_assert(std::is_fundamental_v<Type>);
        fields_data_.emplace_back(std::make_unique<ConcurrentVector<Type>>(size_per_chunk));
    }

    // append a column of vector type
    template <typename VectorType>
    void
    append_field_data(int64_t dim, int64_t size_per_chunk) {
        static_assert(std::is_base_of_v<VectorTrait, VectorType>);
        fields_data_.emplace_back(std::make_unique<ConcurrentVector<VectorType>>(dim, size_per_chunk));
    }

 private:
    std::vector<std::unique_ptr<VectorBase>> fields_data_;
};
}  // namespace milvus::segcore
