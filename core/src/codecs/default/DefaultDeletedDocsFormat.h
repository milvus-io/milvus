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

#include <mutex>
#include <string>

#include "codecs/DeletedDocsFormat.h"

namespace milvus {
namespace codec {

class DefaultDeletedDocsFormat : public DeletedDocsFormat {
 public:
    DefaultDeletedDocsFormat() = default;

    void
    read(const storage::FSHandlerPtr& fs_ptr, segment::DeletedDocsPtr& deleted_docs) override;

    void
    write(const storage::FSHandlerPtr& fs_ptr, const segment::DeletedDocsPtr& deleted_docs) override;

    void
    readSize(const storage::FSHandlerPtr& fs_ptr, size_t& size) override;

    // No copy and move
    DefaultDeletedDocsFormat(const DefaultDeletedDocsFormat&) = delete;
    DefaultDeletedDocsFormat(DefaultDeletedDocsFormat&&) = delete;

    DefaultDeletedDocsFormat&
    operator=(const DefaultDeletedDocsFormat&) = delete;
    DefaultDeletedDocsFormat&
    operator=(DefaultDeletedDocsFormat&&) = delete;

 private:
    std::mutex mutex_;

    const std::string deleted_docs_filename_ = "deleted_docs";
};

}  // namespace codec
}  // namespace milvus
