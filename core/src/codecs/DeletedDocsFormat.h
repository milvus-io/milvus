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

#include "segment/DeletedDocs.h"
#include "storage/FSHandler.h"
#include "utils/Status.h"

namespace milvus {
namespace codec {

class DeletedDocsFormat {
 public:
    DeletedDocsFormat() = default;

    static std::string
    FilePostfix();

    Status
    Read(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, segment::DeletedDocsPtr& deleted_docs);

    Status
    Write(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path,
          const segment::DeletedDocsPtr& deleted_docs);

    Status
    ReadSize(const storage::FSHandlerPtr& fs_ptr, const std::string& file_path, size_t& size);

    // No copy and move
    DeletedDocsFormat(const DeletedDocsFormat&) = delete;
    DeletedDocsFormat(DeletedDocsFormat&&) = delete;

    DeletedDocsFormat&
    operator=(const DeletedDocsFormat&) = delete;
    DeletedDocsFormat&
    operator=(DeletedDocsFormat&&) = delete;
};

using DeletedDocsFormatPtr = std::shared_ptr<DeletedDocsFormat>;

}  // namespace codec
}  // namespace milvus
