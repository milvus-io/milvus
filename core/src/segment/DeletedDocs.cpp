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

#include "segment/DeletedDocs.h"

namespace milvus {
namespace segment {

DeletedDocs::DeletedDocs(const std::vector<offset_t>& deleted_doc_offsets) : deleted_doc_offsets_(deleted_doc_offsets) {
}

void
DeletedDocs::AddDeletedDoc(offset_t offset) {
    deleted_doc_offsets_.emplace_back(offset);
}

const std::vector<offset_t>&
DeletedDocs::GetDeletedDocs() const {
    return deleted_doc_offsets_;
}

// const std::string&
// DeletedDocs::GetName() const {
//    return name_;
//}

size_t
DeletedDocs::GetCount() const {
    return deleted_doc_offsets_.size();
}

int64_t
DeletedDocs::Size() {
    return deleted_doc_offsets_.size() * sizeof(offset_t);
}

}  // namespace segment
}  // namespace milvus
