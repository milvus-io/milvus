// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>
#include <utility>

#include "index/contracts/query/IIndexReaderBase.h"

namespace milvus::segcore {

class IndexInventory;

// Retains a request reader. Keep this handle alive while using reader-owned views.
class IndexReaderHandle {
 public:
    IndexReaderHandle() = default;
    IndexReaderHandle(const IndexReaderHandle&) = delete;
    IndexReaderHandle&
    operator=(const IndexReaderHandle&) = delete;
    IndexReaderHandle(IndexReaderHandle&&) noexcept = default;
    IndexReaderHandle&
    operator=(IndexReaderHandle&&) noexcept = default;

    // Missing entries allow a column fallback; load failures must propagate.
    bool
    empty() const {
        return reader_ == nullptr;
    }
    explicit operator bool() const {
        return reader_ != nullptr;
    }
    const index::IIndexReaderBase*
    get() const {
        return reader_.get();
    }
    const index::IIndexReaderBase*
    operator->() const {
        return reader_.get();
    }

    // Deferred operations retain this request reader and its index version.
    std::shared_ptr<void>
    IntoSharedLifetime() && {
        return std::move(reader_);
    }

 private:
    friend class IndexInventory;
    explicit IndexReaderHandle(std::shared_ptr<index::IIndexReaderBase> reader)
        : reader_(std::move(reader)) {
    }
    std::shared_ptr<index::IIndexReaderBase> reader_;
};

}  // namespace milvus::segcore
