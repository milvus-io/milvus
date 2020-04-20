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

class Attr {
 public:
    Attr(const std::vector<uint8_t>& data, size_t nbytes, const std::vector<int64_t> uids, const std::string& name);

    Attr();

    void
    AddAttr(const std::vector<uint8_t>& data, size_t nbytes);

    void
    AddUids(const std::vector<int64_t>& uids);

    void
    SetName(const std::string& name);

    const std::vector<uint8_t>&
    GetData() const;

    const std::string&
    GetName() const;

    const std::string&
    GetCollectionId();

    const size_t&
    GetNbytes() const;

    const std::vector<int64_t>&
    GetUids() const;

    size_t
    GetCount() const;

    size_t
    GetCodeLength() const;

    void
    Erase(int32_t offset);

    void
    Erase(std::vector<int32_t>& offsets);

    // No copy and move
    Attr(const Attr&) = delete;
    Attr(Attr&&) = delete;

    Attr&
    operator=(const Attr&) = delete;
    Attr&
    operator=(Attr&&) = delete;

 private:
    std::vector<uint8_t> data_;
    size_t nbytes_;
    std::vector<int64_t> uids_;
    std::string name_;
    std::string collection_id_;
};

using AttrPtr = std::shared_ptr<Attr>;

}  // namespace segment
}  // namespace milvus
