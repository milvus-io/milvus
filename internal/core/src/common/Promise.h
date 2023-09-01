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

#include <folly/Unit.h>
#include <folly/futures/Future.h>

#include "log/Log.h"

namespace milvus {

template <class T>
class MilvusPromise : public folly::Promise<T> {
 public:
    MilvusPromise() : folly::Promise<T>() {
    }

    explicit MilvusPromise(const std::string& context)
        : folly::Promise<T>(), context_(context) {
    }

    MilvusPromise(folly::futures::detail::EmptyConstruct,
                  const std::string& context) noexcept
        : folly::Promise<T>(folly::Promise<T>::makeEmpty()), context_(context) {
    }

    ~MilvusPromise() {
        if (!this->isFulfilled()) {
            LOG_SEGCORE_WARNING_
                << "PROMISE: Unfulfilled promise is being deleted. Context: "
                << context_;
        }
    }

    explicit MilvusPromise(MilvusPromise<T>&& other)
        : folly::Promise<T>(std::move(other)),
          context_(std::move(other.context_)) {
    }

    MilvusPromise&
    operator=(MilvusPromise<T>&& other) noexcept {
        folly::Promise<T>::operator=(std::move(other));
        context_ = std::move(other.context_);
        return *this;
    }

    static MilvusPromise
    MakeEmpty(const std::string& context = "") noexcept {
        return MilvusPromise<T>(folly::futures::detail::EmptyConstruct{},
                                context);
    }

 private:
    /// Optional parameter to understand where this promise was created.
    std::string context_;
};

using ContinuePromise = MilvusPromise<folly::Unit>;
using ContinueFuture = folly::SemiFuture<folly::Unit>;

}  // namespace milvus