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

#include "cache/DataObj.h"

namespace milvus {
namespace segment {

using offset_t = int32_t;

class DeletedDocs : public cache::DataObj {
 public:
    explicit DeletedDocs(const std::vector<offset_t>& deleted_doc_offsets);

    DeletedDocs() = default;

    void
    AddDeletedDoc(offset_t offset);

    const std::vector<offset_t>&
    GetDeletedDocs() const;

    //    // TODO
    //    const std::string&
    //    GetName() const;

    size_t
    GetCount() const;

    int64_t
    Size() override;

    //    void
    //    GetBitset(faiss::ConcurrentBitsetPtr& bitset);

    // No copy and move
    DeletedDocs(const DeletedDocs&) = delete;
    DeletedDocs(DeletedDocs&&) = delete;

    DeletedDocs&
    operator=(const DeletedDocs&) = delete;
    DeletedDocs&
    operator=(DeletedDocs&&) = delete;

 private:
    std::vector<offset_t> deleted_doc_offsets_;
    //    faiss::ConcurrentBitsetPtr bitset_;
    //    const std::string name_ = "deleted_docs";
};

using DeletedDocsPtr = std::shared_ptr<DeletedDocs>;

}  // namespace segment
}  // namespace milvus
