// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License
#pragma once

#include <oneapi/tbb/concurrent_queue.h>

#include <exception>
#include <optional>

namespace milvus {
template <typename T>
class Channel {
 public:
    // unbounded channel
    Channel() = default;

    Channel(Channel<T>&& other) noexcept : inner_(std::move(other.inner_)) {
    }

    // bounded channel
    explicit Channel(size_t capacity) {
        inner_.set_capacity(capacity);
    }

    void
    set_capacity(size_t capacity) {
        inner_.set_capacity(capacity);
    }

    void
    push(const T& value) {
        inner_.push(value);
    }

    bool
    pop(T& value) {
        std::optional<T> result;
        inner_.pop(result);
        if (!result.has_value()) {
            if (ex_.has_value()) {
                std::rethrow_exception(ex_.value());
            }
            return false;
        }
        value = std::move(result.value());
        return true;
    }

    void
    close(std::optional<std::exception_ptr> ex = std::nullopt) {
        if (ex.has_value()) {
            ex_ = std::move(ex);
        }
        inner_.push(std::nullopt);
    }

 private:
    oneapi::tbb::concurrent_bounded_queue<std::optional<T>> inner_{};
    std::optional<std::exception_ptr> ex_{};
};
}  // namespace milvus
