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

#include <mutex>
#include <optional>
#include <functional>
#include <vector>

namespace milvus::futures {

/// @brief Ready is a class that holds a value of type T.
/// value of Ready can be only set into ready by once,
/// and allows to register callbacks to be called when the value is ready.
template <class T>
class Ready {
 public:
    Ready() : is_ready_(false){};

    Ready(const Ready<T>&) = delete;

    Ready(Ready<T>&&) noexcept = default;

    Ready&
    operator=(const Ready<T>&) = delete;

    Ready&
    operator=(Ready<T>&&) noexcept = default;

    /// @brief set the value into Ready.
    void
    setValue(T&& value) {
        mutex_.lock();
        value_ = std::move(value);
        is_ready_ = true;
        std::vector<std::function<void()>> callbacks(std::move(callbacks_));
        mutex_.unlock();

        // perform all callbacks which is registered before value is ready.
        for (auto& callback : callbacks) {
            callback();
        }
    }

    /// @brief  get the value from Ready.
    /// @return ready value.
    T
    getValue() && {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!is_ready_) {
            throw std::runtime_error("Value is not ready");
        }
        auto v(std::move(value_.value()));
        value_.reset();
        return std::move(v);
    }

    /// @brief check if the value is ready.
    bool
    isReady() const {
        const std::lock_guard<std::mutex> lock(mutex_);
        return is_ready_;
    }

    /// @brief register a callback into Ready if value is not ready, otherwise call it directly.
    template <typename Fn, typename = std::enable_if<std::is_invocable_v<Fn>>>
    void
    callOrRegisterCallback(Fn&& fn) {
        mutex_.lock();
        // call if value is ready,
        // otherwise register as a callback to be called when value is ready.
        if (is_ready_) {
            mutex_.unlock();
            fn();
            return;
        }
        callbacks_.push_back(std::forward<Fn>(fn));
        mutex_.unlock();
    }

 private:
    std::optional<T> value_;
    mutable std::mutex mutex_;
    std::vector<std::function<void()>> callbacks_;
    bool is_ready_;
};

};  // namespace milvus::futures