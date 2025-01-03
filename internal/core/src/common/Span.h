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

#include <cassert>
#include <string>
#include <string_view>
#include <type_traits>

#include "Types.h"
#include "VectorTrait.h"

namespace milvus {
// type erasure to work around virtual restriction
class SpanBase {
 public:
    explicit SpanBase(const void* data,
                      int64_t row_count,
                      int64_t element_sizeof)
        : data_(data), row_count_(row_count), element_sizeof_(element_sizeof) {
    }
    explicit SpanBase(const void* data,
                      const bool* valid_data,
                      int64_t row_count,
                      int64_t element_sizeof)
        : data_(data),
          valid_data_(valid_data),
          row_count_(row_count),
          element_sizeof_(element_sizeof) {
    }

    int64_t
    row_count() const {
        return row_count_;
    }

    int64_t
    element_sizeof() const {
        return element_sizeof_;
    }

    const void*
    data() const {
        return data_;
    }

    const bool*
    valid_data() const {
        return valid_data_;
    }

 private:
    const void* data_;
    const bool* valid_data_{nullptr};
    int64_t row_count_;
    int64_t element_sizeof_;
};

template <typename T, typename Enable = void>
class Span;

// TODO: refine Span to support T=FloatVector
template <typename T>
class Span<T,
           typename std::enable_if_t<IsSparse<T> || IsScalar<T> ||
                                     std::is_same_v<T, PkType>>> {
 public:
    using embedded_type = T;
    explicit Span(const T* data, const bool* valid_data, int64_t row_count)
        : data_(data), valid_data_(valid_data), row_count_(row_count) {
    }

    explicit Span(std::string_view data, bool* valid_data) {
        Span(data.data(), valid_data, data.size());
    }

    operator SpanBase() const {
        return SpanBase(data_, valid_data_, row_count_, sizeof(T));
    }

    explicit Span(const SpanBase& base)
        : Span(reinterpret_cast<const T*>(base.data()),
               base.valid_data(),
               base.row_count()) {
        assert(base.element_sizeof() == sizeof(T));
    }

    int64_t
    element_sizeof() const {
        return sizeof(T);
    }

    const T*
    data() const {
        return data_;
    }

    const bool*
    valid_data() const {
        return valid_data_;
    }

    const T&
    operator[](int64_t offset) const {
        return data_[offset];
    }

    int64_t
    row_count() const {
        return row_count_;
    }

 private:
    const T* data_;
    const bool* valid_data_;
    const int64_t row_count_;
};

template <typename VectorType>
class Span<
    VectorType,
    typename std::enable_if_t<std::is_base_of_v<VectorTrait, VectorType>>> {
 public:
    using embedded_type = typename VectorType::embedded_type;

    Span(const embedded_type* data, int64_t row_count, int64_t element_sizeof)
        : data_(data), row_count_(row_count), element_sizeof_(element_sizeof) {
    }

    explicit Span(const SpanBase& base)
        : data_(reinterpret_cast<const embedded_type*>(base.data())),
          row_count_(base.row_count()),
          element_sizeof_(base.element_sizeof()) {
    }

    operator SpanBase() const {
        return SpanBase(data_, row_count_, element_sizeof_);
    }

    int64_t
    element_sizeof() const {
        return element_sizeof_;
    }

    const embedded_type*
    data() const {
        return data_;
    }

    int64_t
    row_count() const {
        return row_count_;
    }

 private:
    const embedded_type* data_;
    const int64_t row_count_;
    const int64_t element_sizeof_;
};
}  // namespace milvus
