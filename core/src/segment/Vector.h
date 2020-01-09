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
#include <vector>

#include "Types.h"

namespace milvus {
namespace segment {

class Vector {
 public:
    Vector(std::vector<uint8_t> data, std::vector<doc_id_t> uids);

    Vector() = default;

    void
    AddData(const std::vector<uint8_t>& data);

    void
    AddUids(const std::vector<doc_id_t>& uids);

    const std::vector<uint8_t>&
    GetData() const;

    const std::vector<doc_id_t>&
    GetUids() const;

    size_t
    GetCount();

    size_t
    GetDimension();

    void
    Erase(size_t offset, int vector_type_size);

    size_t
    Size();

    // No copy and move
    Vector(const Vector&) = delete;
    Vector(Vector&&) = delete;

    Vector&
    operator=(const Vector&) = delete;
    Vector&
    operator=(Vector&&) = delete;

 private:
    std::vector<uint8_t> data_;
    // TODO: since all vector fields should correspond to the same set of uids, save them in Vectors instead?
    std::vector<doc_id_t> uids_;
};

using VectorPtr = std::shared_ptr<Vector>;

}  // namespace segment
}  // namespace milvus
