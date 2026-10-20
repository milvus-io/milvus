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

#include "common/EasyAssert.h"
#include "common/Types.h"

namespace milvus::index {

// Immutable validity in the reader's coordinate domain. A null owner is an
// explicit all-valid sentinel; size() still records the complete row/element
// count. Owners keep bitmap storage alive independently of reader eviction.
class ScalarIndexValidity {
 public:
    ScalarIndexValidity(int64_t count,
                        std::shared_ptr<const TargetBitmap> bitmap = nullptr)
        : count_(count), bitmap_(std::move(bitmap)) {
        AssertInfo(count >= 0 && (!bitmap_ || bitmap_->size() == count),
                   "invalid immutable validity size {}",
                   count);
    }

    int64_t
    size() const {
        return count_;
    }

    bool
    all() const {
        return !bitmap_ || bitmap_->all();
    }

    bool
    operator[](int64_t offset) const {
        AssertInfo(offset >= 0 && offset < count_,
                   "validity offset {} outside {} coordinates",
                   offset,
                   count_);
        return !bitmap_ || (*bitmap_)[offset];
    }

    void
    AppendTo(TargetBitmap& result, int64_t offset, int64_t count) const {
        AssertInfo(offset >= 0 && count >= 0 && offset <= count_ &&
                       count <= count_ - offset,
                   "validity range [{}, +{}) outside {} coordinates",
                   offset,
                   count,
                   count_);
        if (bitmap_) {
            result.append(*bitmap_, offset, count);
        } else {
            result.resize(result.size() + count, true);
        }
    }

    const std::shared_ptr<const TargetBitmap>&
    bitmap() const {
        return bitmap_;
    }

 private:
    int64_t count_;
    std::shared_ptr<const TargetBitmap> bitmap_;
};

}  // namespace milvus::index
