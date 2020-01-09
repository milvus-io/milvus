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

#include "Vector.h"

#include <utility>

#include "Vectors.h"

namespace milvus {
namespace segment {

Vector::Vector(std::vector<uint8_t> data, std::vector<doc_id_t> uids) : data_(std::move(data)), uids_(std::move(uids)) {
}

void
Vector::AddData(const std::vector<uint8_t>& data) {
    data_.reserve(data_.size() + data.size());
    data_.insert(data_.end(), std::make_move_iterator(data.begin()), std::make_move_iterator(data.end()));
}

void
Vector::AddUids(const std::vector<doc_id_t>& uids) {
    data_.reserve(data_.size() + uids.size());
    data_.insert(data_.end(), std::make_move_iterator(uids.begin()), std::make_move_iterator(uids.end()));
}

void
Vector::Erase(size_t offset, int vector_type_size) {
    auto step = offset * GetDimension() * vector_type_size;
    data_.erase(data_.begin() + step, data_.begin() + step * 2);
    uids_.erase(uids_.begin() + offset, uids_.begin() + offset + 1);
}

const std::vector<uint8_t>&
Vector::GetData() const {
    return data_;
}

const std::vector<doc_id_t>&
Vector::GetUids() const {
    return uids_;
}

size_t
Vector::GetCount() {
    return uids_.size();
}

size_t
Vector::GetDimension() {
    return data_.size() / GetCount();
}

size_t
Vector::Size() {
    return data_.size() + uids_.size() * sizeof(doc_id_t);
}

}  // namespace segment
}  // namespace milvus