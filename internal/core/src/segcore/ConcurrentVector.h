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

#include <fmt/core.h>
#include <tbb/concurrent_vector.h>

#include <atomic>
#include <cassert>
#include <deque>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/FieldMeta.h"
#include "common/Json.h"
#include "common/Span.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "exceptions/EasyAssert.h"
#include "storage/FieldData.h"

namespace milvus::segcore {

template <typename Type>
class ThreadSafeVector {
 public:
    template <typename... Args>
    void
    emplace_to_at_least(int64_t size, Args... args) {
        if (size <= size_) {
            return;
        }
        std::lock_guard lck(mutex_);
        while (vec_.size() < size) {
            vec_.emplace_back(std::forward<Args...>(args...));
            ++size_;
        }
    }
    const Type&
    operator[](int64_t index) const {
        AssertInfo(index < size_,
                   fmt::format(
                       "index out of range, index={}, size_={}", index, size_));
        std::shared_lock lck(mutex_);
        return vec_[index];
    }

    Type&
    operator[](int64_t index) {
        AssertInfo(index < size_,
                   fmt::format(
                       "index out of range, index={}, size_={}", index, size_));
        std::shared_lock lck(mutex_);
        return vec_[index];
    }

    int64_t
    size() const {
        return size_;
    }

    void
    clear() {
        std::lock_guard lck(mutex_);
        size_ = 0;
        vec_.clear();
    }

 private:
    std::atomic<int64_t> size_ = 0;
    std::deque<Type> vec_;
    mutable std::shared_mutex mutex_;
};

class VectorBase {
 public:
    explicit VectorBase(int64_t size_per_chunk)
        : size_per_chunk_(size_per_chunk) {
    }
    virtual ~VectorBase() = default;

    virtual void
    grow_to_at_least(int64_t element_count) = 0;

    virtual void
    set_data_raw(ssize_t element_offset,
                 const void* source,
                 ssize_t element_count) = 0;

    virtual void
    set_data_raw(ssize_t element_offset,
                 const std::vector<storage::FieldDataPtr>& data) = 0;

    void
    set_data_raw(ssize_t element_offset,
                 ssize_t element_count,
                 const DataArray* data,
                 const FieldMeta& field_meta);

    virtual void
    fill_chunk_data(const std::vector<storage::FieldDataPtr>& data) = 0;

    virtual SpanBase
    get_span_base(int64_t chunk_id) const = 0;

    int64_t
    get_size_per_chunk() const {
        return size_per_chunk_;
    }

    virtual const void*
    get_chunk_data(ssize_t chunk_index) const = 0;

    virtual ssize_t
    num_chunk() const = 0;

    virtual bool
    empty() = 0;

 protected:
    const int64_t size_per_chunk_;
};

template <typename Type, bool is_scalar = false>
class ConcurrentVectorImpl : public VectorBase {
 public:
    // constants
    using Chunk = FixedVector<Type>;
    ConcurrentVectorImpl(ConcurrentVectorImpl&&) = delete;
    ConcurrentVectorImpl(const ConcurrentVectorImpl&) = delete;

    ConcurrentVectorImpl&
    operator=(ConcurrentVectorImpl&&) = delete;
    ConcurrentVectorImpl&
    operator=(const ConcurrentVectorImpl&) = delete;

    using TraitType =
        std::conditional_t<is_scalar,
                           Type,
                           std::conditional_t<std::is_same_v<Type, float>,
                                              FloatVector,
                                              BinaryVector>>;

 public:
    explicit ConcurrentVectorImpl(ssize_t dim, int64_t size_per_chunk)
        : VectorBase(size_per_chunk), Dim(is_scalar ? 1 : dim) {
        // Assert(is_scalar ? dim == 1 : dim != 1);
    }

    void
    grow_to_at_least(int64_t element_count) override {
        auto chunk_count = upper_div(element_count, size_per_chunk_);
        chunks_.emplace_to_at_least(chunk_count, Dim * size_per_chunk_);
    }

    void
    grow_on_demand(int64_t element_count) {
        auto chunk_count = upper_div(element_count, size_per_chunk_);
        chunks_.emplace_to_at_least(chunk_count, Dim * element_count);
    }

    Span<TraitType>
    get_span(int64_t chunk_id) const {
        auto& chunk = get_chunk(chunk_id);
        if constexpr (is_scalar) {
            return Span<TraitType>(chunk.data(), chunk.size());
        } else if constexpr (std::is_same_v<Type, int64_t> ||  // NOLINT
                             std::is_same_v<Type, int>) {
            // TODO: where should the braces be placed?
            // only for testing
            PanicInfo("unimplemented");
        } else {
            static_assert(
                std::is_same_v<typename TraitType::embedded_type, Type>);
            return Span<TraitType>(chunk.data(), chunk.size(), Dim);
        }
    }

    SpanBase
    get_span_base(int64_t chunk_id) const override {
        return get_span(chunk_id);
    }

    void
    fill_chunk_data(const std::vector<storage::FieldDataPtr>& datas)
        override {  // used only for sealed segment
        AssertInfo(chunks_.size() == 0, "no empty concurrent vector");

        int64_t element_count = 0;
        for (auto& field_data : datas) {
            element_count += field_data->get_num_rows();
        }
        chunks_.emplace_to_at_least(1, Dim * element_count);
        int64_t offset = 0;
        for (auto& field_data : datas) {
            auto num_rows = field_data->get_num_rows();
            set_data(
                offset, static_cast<const Type*>(field_data->Data()), num_rows);
            offset += num_rows;
        }
    }

    void
    set_data_raw(ssize_t element_offset,
                 const std::vector<storage::FieldDataPtr>& datas) override {
        for (auto& field_data : datas) {
            auto num_rows = field_data->get_num_rows();
            set_data_raw(element_offset, field_data->Data(), num_rows);
            element_offset += num_rows;
        }
    }

    void
    set_data_raw(ssize_t element_offset,
                 const void* source,
                 ssize_t element_count) override {
        if (element_count == 0) {
            return;
        }
        this->grow_to_at_least(element_offset + element_count);
        set_data(
            element_offset, static_cast<const Type*>(source), element_count);
    }

    void
    set_data(ssize_t element_offset,
             const Type* source,
             ssize_t element_count) {
        auto chunk_id = element_offset / size_per_chunk_;
        auto chunk_offset = element_offset % size_per_chunk_;
        ssize_t source_offset = 0;
        // first partition:
        if (chunk_offset + element_count <= size_per_chunk_) {
            // only first
            fill_chunk(
                chunk_id, chunk_offset, element_count, source, source_offset);
            return;
        }

        auto first_size = size_per_chunk_ - chunk_offset;
        fill_chunk(chunk_id, chunk_offset, first_size, source, source_offset);

        source_offset += size_per_chunk_ - chunk_offset;
        element_count -= first_size;
        ++chunk_id;

        // the middle
        while (element_count >= size_per_chunk_) {
            fill_chunk(chunk_id, 0, size_per_chunk_, source, source_offset);
            source_offset += size_per_chunk_;
            element_count -= size_per_chunk_;
            ++chunk_id;
        }

        // the final
        if (element_count > 0) {
            fill_chunk(chunk_id, 0, element_count, source, source_offset);
        }
    }

    const Chunk&
    get_chunk(ssize_t chunk_index) const {
        return chunks_[chunk_index];
    }

    Chunk&
    get_chunk(ssize_t index) {
        return chunks_[index];
    }

    const void*
    get_chunk_data(ssize_t chunk_index) const override {
        return chunks_[chunk_index].data();
    }

    // just for fun, don't use it directly
    const Type*
    get_element(ssize_t element_index) const {
        auto chunk_id = element_index / size_per_chunk_;
        auto chunk_offset = element_index % size_per_chunk_;
        return get_chunk(chunk_id).data() + chunk_offset * Dim;
    }

    const Type&
    operator[](ssize_t element_index) const {
        AssertInfo(Dim == 1,
                   fmt::format("The value of Dim is not 1, Dim={}", Dim));
        auto chunk_id = element_index / size_per_chunk_;
        auto chunk_offset = element_index % size_per_chunk_;
        return get_chunk(chunk_id)[chunk_offset];
    }

    ssize_t
    num_chunk() const override {
        return chunks_.size();
    }

    bool
    empty() override {
        for (size_t i = 0; i < chunks_.size(); i++) {
            if (get_chunk(i).size() > 0) {
                return false;
            }
        }

        return true;
    }

    void
    clear() {
        chunks_.clear();
    }

 private:
    void
    fill_chunk(ssize_t chunk_id,
               ssize_t chunk_offset,
               ssize_t element_count,
               const Type* source,
               ssize_t source_offset) {
        if (element_count <= 0) {
            return;
        }
        auto chunk_num = chunks_.size();
        AssertInfo(
            chunk_id < chunk_num,
            fmt::format("chunk_id out of chunk num, chunk_id={}, chunk_num={}",
                        chunk_id,
                        chunk_num));
        Chunk& chunk = chunks_[chunk_id];
        auto ptr = chunk.data();

        std::copy_n(source + source_offset * Dim,
                    element_count * Dim,
                    ptr + chunk_offset * Dim);
    }

    const ssize_t Dim;

 private:
    ThreadSafeVector<Chunk> chunks_;
};

template <typename Type>
class ConcurrentVector : public ConcurrentVectorImpl<Type, true> {
 public:
    static_assert(IsScalar<Type> || std::is_same_v<Type, PkType>);
    explicit ConcurrentVector(int64_t size_per_chunk)
        : ConcurrentVectorImpl<Type, true>::ConcurrentVectorImpl(
              1, size_per_chunk) {
    }
};

template <>
class ConcurrentVector<FloatVector>
    : public ConcurrentVectorImpl<float, false> {
 public:
    ConcurrentVector(int64_t dim, int64_t size_per_chunk)
        : ConcurrentVectorImpl<float, false>::ConcurrentVectorImpl(
              dim, size_per_chunk) {
    }
};

template <>
class ConcurrentVector<BinaryVector>
    : public ConcurrentVectorImpl<uint8_t, false> {
 public:
    explicit ConcurrentVector(int64_t dim, int64_t size_per_chunk)
        : binary_dim_(dim), ConcurrentVectorImpl(dim / 8, size_per_chunk) {
        AssertInfo(dim % 8 == 0,
                   fmt::format("dim is not a multiple of 8, dim={}", dim));
    }

 private:
    int64_t binary_dim_;
};

}  // namespace milvus::segcore
