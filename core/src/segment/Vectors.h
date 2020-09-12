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
#include <vector>

namespace milvus {
namespace segment {

using doc_id_t = int64_t;

class Vectors {
 public:
    Vectors() = default;

    void
    AddData(const std::vector<uint8_t>& data);

    void
    AddData(const uint8_t* data, uint64_t size);

    void
    AddUids(const std::vector<doc_id_t>& uids);

    void
    SetName(const std::string& name);

    std::vector<uint8_t>&
    GetMutableData();

    std::vector<doc_id_t>&
    GetMutableUids();

    const std::vector<uint8_t>&
    GetData() const;

    const std::vector<doc_id_t>&
    GetUids() const;

    const std::string&
    GetName() const;

    size_t
    GetCount() const;

    size_t
    GetCodeLength() const;

    void
    Erase(int32_t offset);

    void
    Erase(std::vector<int32_t>& offsets);

    size_t
    VectorsSize();

    size_t
    UidsSize();

    void
    Clear();

    // No copy and move
    Vectors(const Vectors&) = delete;
    Vectors(Vectors&&) = delete;

    Vectors&
    operator=(const Vectors&) = delete;
    Vectors&
    operator=(Vectors&&) = delete;

 private:
    std::vector<uint8_t> data_;
    std::vector<doc_id_t> uids_;
    std::string name_;
};

using VectorsPtr = std::shared_ptr<Vectors>;

}  // namespace segment
}  // namespace milvus
