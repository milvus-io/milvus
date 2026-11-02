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

#include "cachinglayer/CacheSlot.h"
#include "index/contracts/query/IIndexReaderBase.h"

namespace milvus::segcore {

class IndexInventory;

// Retains a request reader, or a legacy cache accessor during migration. Keep the pin alive while using reader-owned views.
class IndexPin {
 public:
    IndexPin() = default;

    IndexPin(const IndexPin&) = delete;
    IndexPin&
    operator=(const IndexPin&) = delete;

    IndexPin(IndexPin&& other) noexcept
        : lifetime_(std::move(other.lifetime_)),
          reader_(std::exchange(other.reader_, nullptr)) {
        other.lifetime_.reset();
    }

    IndexPin&
    operator=(IndexPin&& other) noexcept {
        if (this != &other) {
            reader_ = nullptr;
            lifetime_.reset();
            lifetime_ = std::move(other.lifetime_);
            reader_ = std::exchange(other.reader_, nullptr);
            other.lifetime_.reset();
        }
        return *this;
    }

    // Empty means the entry or interface is absent, allowing a column
    // fallback. Cache loading failures are not absence and must propagate.
    bool
    empty() const {
        return reader_ == nullptr;
    }

    explicit operator bool() const {
        return reader_ != nullptr;
    }

    const index::IIndexReaderBase*
    get() const {
        return reader_;
    }

    const index::IIndexReaderBase*
    operator->() const {
        return reader_;
    }

    // Deferred operations retain the request reader or legacy cache accessor.
    std::shared_ptr<void>
    IntoSharedLifetime() && {
        reader_ = nullptr;
        auto accessor = std::move(lifetime_);
        lifetime_.reset();
        return std::shared_ptr<void>(std::move(accessor));
    }

 private:
    friend class IndexInventory;

    IndexPin(std::shared_ptr<void> accessor,
             const index::IIndexReaderBase* reader)
        : lifetime_(std::move(accessor)), reader_(reader) {
    }

    std::shared_ptr<void> lifetime_;
    const index::IIndexReaderBase* reader_{nullptr};
};

}  // namespace milvus::segcore
