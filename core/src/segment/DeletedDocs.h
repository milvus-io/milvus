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

// using DeletedDocList = std::vector<id_type_t>;

class DeletedDocs {
 public:
    explicit DeletedDocs(const std::vector<doc_id_t>& deleted_doc_ids);

    DeletedDocs() = default;

    void AddDeletedDoc(doc_id_t doc_id);

    const std::vector<doc_id_t>&
    GetDeletedDocs() const;

    // TODO
    const std::string&
    GetName() const;

    size_t
    GetSize() const;

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
    std::vector<doc_id_t> deleted_doc_ids_;
    //    faiss::ConcurrentBitsetPtr bitset_;
    const std::string name_ = "deleted_docs";
};

using DeletedDocsPtr = std::shared_ptr<DeletedDocs>;

}  // namespace segment
}  // namespace milvus
