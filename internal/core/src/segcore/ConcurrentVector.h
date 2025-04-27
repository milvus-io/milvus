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

#include "common/EasyAssert.h"
#include "common/FieldMeta.h"
#include "common/FieldData.h"
#include "common/Json.h"
#include "common/Span.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "mmap/ChunkVector.h"

namespace milvus::segcore {

class ThreadSafeValidData {
 public:
    explicit ThreadSafeValidData() = default;
    explicit ThreadSafeValidData(FixedVector<bool> data)
        : data_(std::move(data)) {
    }

    void
    set_data_raw(const std::vector<FieldDataPtr>& datas) {
        std::unique_lock<std::shared_mutex> lck(mutex_);
        auto total = 0;
        for (auto& field_data : datas) {
            total += field_data->get_num_rows();
        }
        if (length_ + total > data_.size()) {
            data_.resize(length_ + total);
        }

        for (auto& field_data : datas) {
            auto num_row = field_data->get_num_rows();
            for (size_t i = 0; i < num_row; i++) {
                data_[length_ + i] = field_data->is_valid(i);
            }
            length_ += num_row;
        }
    }

    void
    set_data_raw(size_t num_rows,
                 const DataArray* data,
                 const FieldMeta& field_meta) {
        std::unique_lock<std::shared_mutex> lck(mutex_);
        if (field_meta.is_nullable()) {
            if (length_ + num_rows > data_.size()) {
                data_.resize(length_ + num_rows);
            }
            auto src = data->valid_data().data();
            std::copy_n(src, num_rows, data_.data() + length_);
            length_ += num_rows;
        }
    }

    bool
    is_valid(size_t offset) {
        std::shared_lock<std::shared_mutex> lck(mutex_);
        Assert(offset < length_);
        return data_[offset];
    }

    bool*
    get_chunk_data(size_t offset) {
        std::shared_lock<std::shared_mutex> lck(mutex_);
        Assert(offset < length_);
        return &data_[offset];
    }

 private:
    mutable std::shared_mutex mutex_{};
    FixedVector<bool> data_;
    // number of actual elements
    size_t length_{0};
};
using ThreadSafeValidDataPtr = std::shared_ptr<ThreadSafeValidData>;

class VectorBase {
 public:
    explicit VectorBase(int64_t size_per_chunk)
        : size_per_chunk_(size_per_chunk) {
    }
    virtual ~VectorBase() = default;

    virtual void
    set_data_raw(ssize_t element_offset,
                 const void* source,
                 ssize_t element_count) = 0;

    virtual void
    set_data_raw(ssize_t element_offset,
                 const std::vector<FieldDataPtr>& data) = 0;

    virtual void
    set_data_raw(ssize_t element_offset,
                 ssize_t element_count,
                 const DataArray* data,
                 const FieldMeta& field_meta);

    // used only by sealed segment to load system field
    virtual void
    fill_chunk_data(const std::vector<FieldDataPtr>& data) = 0;

    virtual SpanBase
    get_span_base(int64_t chunk_id) const = 0;

    int64_t
    get_size_per_chunk() const {
        return size_per_chunk_;
    }

    virtual const void*
    get_chunk_data(ssize_t chunk_index) const = 0;

    virtual int64_t
    get_chunk_size(ssize_t chunk_index) const = 0;

    virtual int64_t
    get_element_size() const = 0;

    virtual int64_t
    get_element_offset(ssize_t chunk_index) const = 0;

    virtual ssize_t
    num_chunk() const = 0;

    virtual bool
    empty() = 0;

    virtual void
    clear() = 0;

 protected:
    const int64_t size_per_chunk_;
};

template <typename Type, bool is_type_entire_row = false>
class ConcurrentVectorImpl : public VectorBase {
 public:
    ConcurrentVectorImpl(ConcurrentVectorImpl&&) = delete;
    ConcurrentVectorImpl(const ConcurrentVectorImpl&) = delete;

    ConcurrentVectorImpl&
    operator=(ConcurrentVectorImpl&&) = delete;
    ConcurrentVectorImpl&
    operator=(const ConcurrentVectorImpl&) = delete;

    using TraitType = std::conditional_t<
        is_type_entire_row,
        Type,
        std::conditional_t<
            std::is_same_v<Type, float>,
            FloatVector,
            std::conditional_t<
                std::is_same_v<Type, float16>,
                Float16Vector,
                std::conditional_t<
                    std::is_same_v<Type, bfloat16>,
                    BFloat16Vector,
                    std::conditional_t<std::is_same_v<Type, int8>,
                                       Int8Vector,
                                       BinaryVector>>>>>;

 public:
    explicit ConcurrentVectorImpl(
        ssize_t elements_per_row,
        int64_t size_per_chunk,
        storage::MmapChunkDescriptorPtr mmap_descriptor = nullptr,
        ThreadSafeValidDataPtr valid_data_ptr = nullptr)
        : VectorBase(size_per_chunk),
          elements_per_row_(is_type_entire_row ? 1 : elements_per_row),
          valid_data_ptr_(valid_data_ptr) {
        chunks_ptr_ = SelectChunkVectorPtr<Type>(mmap_descriptor);
    }

    SpanBase
    get_span_base(int64_t chunk_id) const override {
        if constexpr (is_type_entire_row) {
            return chunks_ptr_->get_span(chunk_id);
        } else if constexpr (std::is_same_v<Type, int64_t> ||  // NOLINT
                             std::is_same_v<Type, int>) {
            // only for testing
            PanicInfo(NotImplemented, "unimplemented");
        } else {
            auto chunk_data = chunks_ptr_->get_chunk_data(chunk_id);
            auto chunk_size = chunks_ptr_->get_chunk_size(chunk_id);
            static_assert(
                std::is_same_v<typename TraitType::embedded_type, Type>);
            return Span<TraitType>(
                static_cast<Type*>(chunk_data), chunk_size, elements_per_row_);
        }
    }

    void
    fill_chunk_data(const std::vector<FieldDataPtr>& datas) override {
        AssertInfo(chunks_ptr_->size() == 0, "non empty concurrent vector");

        int64_t element_count = 0;
        for (auto& field_data : datas) {
            element_count += field_data->get_num_rows();
        }
        chunks_ptr_->emplace_to_at_least(1, elements_per_row_ * element_count);
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
                 const std::vector<FieldDataPtr>& datas) override {
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
        auto size =
            size_per_chunk_ == MAX_ROW_COUNT ? element_count : size_per_chunk_;
        chunks_ptr_->emplace_to_at_least(
            upper_div(element_offset + element_count, size),
            elements_per_row_ * size);
        set_data(
            element_offset, static_cast<const Type*>(source), element_count);
    }

    const void*
    get_chunk_data(ssize_t chunk_index) const override {
        return (const void*)chunks_ptr_->get_chunk_data(chunk_index);
    }

    int64_t
    get_chunk_size(ssize_t chunk_index) const override {
        return chunks_ptr_->get_chunk_size(chunk_index);
    }

    int64_t
    get_element_size() const override {
        if constexpr (is_type_entire_row) {
            return chunks_ptr_->get_element_size();
        } else if constexpr (std::is_same_v<Type, int64_t> ||  // NOLINT
                             std::is_same_v<Type, int>) {
            // only for testing
            PanicInfo(NotImplemented, "unimplemented");
        } else {
            static_assert(
                std::is_same_v<typename TraitType::embedded_type, Type>);
            return elements_per_row_;
        }
    }

    int64_t
    get_element_offset(ssize_t chunk_index) const override {
        return chunks_ptr_->get_element_offset(chunk_index);
    }

    // just for fun, don't use it directly
    const Type*
    get_element(ssize_t element_index) const {
        auto chunk_id = element_index / size_per_chunk_;
        auto chunk_offset = element_index % size_per_chunk_;
        auto data =
            static_cast<const Type*>(chunks_ptr_->get_chunk_data(chunk_id));
        return data + chunk_offset * elements_per_row_;
    }

    const Type&
    operator[](ssize_t element_index) const {
        AssertInfo(
            elements_per_row_ == 1,
            fmt::format(
                "The value of elements_per_row_ is not 1, elements_per_row_={}",
                elements_per_row_));
        auto chunk_id = element_index / size_per_chunk_;
        auto chunk_offset = element_index % size_per_chunk_;
        auto data =
            static_cast<const Type*>(chunks_ptr_->get_chunk_data(chunk_id));
        return data[chunk_offset];
    }

    ssize_t
    num_chunk() const override {
        return chunks_ptr_->size();
    }

    bool
    empty() override {
        for (size_t i = 0; i < chunks_ptr_->size(); i++) {
            if (chunks_ptr_->get_chunk_size(i) > 0) {
                return false;
            }
        }

        return true;
    }

    void
    clear() override {
        chunks_ptr_->clear();
    }

    bool
    is_mmap() const {
        return chunks_ptr_->is_mmap();
    }

 private:
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
    void
    fill_chunk(ssize_t chunk_id,
               ssize_t chunk_offset,
               ssize_t element_count,
               const Type* source,
               ssize_t source_offset) {
        if (element_count <= 0) {
            return;
        }
        auto chunk_num = chunks_ptr_->size();
        AssertInfo(
            chunk_id < chunk_num,
            fmt::format("chunk_id out of chunk num, chunk_id={}, chunk_num={}",
                        chunk_id,
                        chunk_num));
        size_t chunk_id_offset = chunk_id * size_per_chunk_ * elements_per_row_;
        std::optional<CheckDataValid> check_data_valid = std::nullopt;
        if (valid_data_ptr_ != nullptr) {
            check_data_valid = [valid_data_ptr = valid_data_ptr_,
                                beg_id = chunk_id_offset](size_t offset) {
                return valid_data_ptr->is_valid(beg_id + offset);
            };
        }
        chunks_ptr_->copy_to_chunk(chunk_id,
                                   chunk_offset * elements_per_row_,
                                   source + source_offset * elements_per_row_,
                                   element_count * elements_per_row_,
                                   check_data_valid);
    }

 protected:
    const ssize_t elements_per_row_;
    ChunkVectorPtr<Type> chunks_ptr_ = nullptr;
    ThreadSafeValidDataPtr valid_data_ptr_ = nullptr;
};

template <typename Type>
class ConcurrentVector : public ConcurrentVectorImpl<Type, true> {
 public:
    static_assert(IsScalar<Type> || std::is_same_v<Type, PkType>);
    explicit ConcurrentVector(
        int64_t size_per_chunk,
        storage::MmapChunkDescriptorPtr mmap_descriptor = nullptr,
        ThreadSafeValidDataPtr valid_data_ptr = nullptr)
        : ConcurrentVectorImpl<Type, true>::ConcurrentVectorImpl(
              1, size_per_chunk, mmap_descriptor, valid_data_ptr) {
    }
};

template <>
class ConcurrentVector<std::string>
    : public ConcurrentVectorImpl<std::string, true> {
 public:
    explicit ConcurrentVector(
        int64_t size_per_chunk,
        storage::MmapChunkDescriptorPtr mmap_descriptor = nullptr,
        ThreadSafeValidDataPtr valid_data_ptr = nullptr)
        : ConcurrentVectorImpl<std::string, true>::ConcurrentVectorImpl(
              1, size_per_chunk, std::move(mmap_descriptor), valid_data_ptr) {
    }

    std::string_view
    view_element(ssize_t element_index) const {
        auto chunk_id = element_index / size_per_chunk_;
        auto chunk_offset = element_index % size_per_chunk_;
        return chunks_ptr_->view_element(chunk_id, chunk_offset);
    }
};

template <>
class ConcurrentVector<Json> : public ConcurrentVectorImpl<Json, true> {
 public:
    explicit ConcurrentVector(
        int64_t size_per_chunk,
        storage::MmapChunkDescriptorPtr mmap_descriptor = nullptr,
        ThreadSafeValidDataPtr valid_data_ptr = nullptr)
        : ConcurrentVectorImpl<Json, true>::ConcurrentVectorImpl(
              1, size_per_chunk, std::move(mmap_descriptor), valid_data_ptr) {
    }

    std::string_view
    view_element(ssize_t element_index) const {
        auto chunk_id = element_index / size_per_chunk_;
        auto chunk_offset = element_index % size_per_chunk_;
        return std::string_view(
            chunks_ptr_->view_element(chunk_id, chunk_offset).data());
    }
};

template <>
class ConcurrentVector<Array> : public ConcurrentVectorImpl<Array, true> {
 public:
    explicit ConcurrentVector(
        int64_t size_per_chunk,
        storage::MmapChunkDescriptorPtr mmap_descriptor = nullptr,
        ThreadSafeValidDataPtr valid_data_ptr = nullptr)
        : ConcurrentVectorImpl<Array, true>::ConcurrentVectorImpl(
              1, size_per_chunk, std::move(mmap_descriptor), valid_data_ptr) {
    }

    ArrayView
    view_element(ssize_t element_index) const {
        auto chunk_id = element_index / size_per_chunk_;
        auto chunk_offset = element_index % size_per_chunk_;
        return chunks_ptr_->view_element(chunk_id, chunk_offset);
    }
};

template <>
class ConcurrentVector<SparseFloatVector>
    : public ConcurrentVectorImpl<knowhere::sparse::SparseRow<float>, true> {
 public:
    explicit ConcurrentVector(
        int64_t size_per_chunk,
        storage::MmapChunkDescriptorPtr mmap_descriptor = nullptr,
        ThreadSafeValidDataPtr valid_data_ptr = nullptr)
        : ConcurrentVectorImpl<knowhere::sparse::SparseRow<float>,
                               true>::ConcurrentVectorImpl(1,
                                                           size_per_chunk,
                                                           std::move(
                                                               mmap_descriptor),
                                                           valid_data_ptr),
          dim_(0) {
    }

    void
    set_data_raw(ssize_t element_offset,
                 const void* source,
                 ssize_t element_count) override {
        auto* src =
            static_cast<const knowhere::sparse::SparseRow<float>*>(source);
        for (int i = 0; i < element_count; ++i) {
            dim_ = std::max(dim_, src[i].dim());
        }
        ConcurrentVectorImpl<knowhere::sparse::SparseRow<float>,
                             true>::set_data_raw(element_offset,
                                                 source,
                                                 element_count);
    }

    int64_t
    Dim() const {
        return dim_;
    }

 private:
    int64_t dim_;
};

template <>
class ConcurrentVector<FloatVector>
    : public ConcurrentVectorImpl<float, false> {
 public:
    ConcurrentVector(int64_t dim,
                     int64_t size_per_chunk,
                     storage::MmapChunkDescriptorPtr mmap_descriptor = nullptr,
                     ThreadSafeValidDataPtr valid_data_ptr = nullptr)
        : ConcurrentVectorImpl<float, false>::ConcurrentVectorImpl(
              dim, size_per_chunk, std::move(mmap_descriptor), valid_data_ptr) {
    }
};

template <>
class ConcurrentVector<BinaryVector>
    : public ConcurrentVectorImpl<uint8_t, false> {
 public:
    explicit ConcurrentVector(
        int64_t dim,
        int64_t size_per_chunk,
        storage::MmapChunkDescriptorPtr mmap_descriptor = nullptr,
        ThreadSafeValidDataPtr valid_data_ptr = nullptr)
        : ConcurrentVectorImpl(dim / 8,
                               size_per_chunk,
                               std::move(mmap_descriptor),
                               valid_data_ptr) {
        AssertInfo(dim % 8 == 0,
                   fmt::format("dim is not a multiple of 8, dim={}", dim));
    }
};

template <>
class ConcurrentVector<Float16Vector>
    : public ConcurrentVectorImpl<float16, false> {
 public:
    ConcurrentVector(int64_t dim,
                     int64_t size_per_chunk,
                     storage::MmapChunkDescriptorPtr mmap_descriptor = nullptr,
                     ThreadSafeValidDataPtr valid_data_ptr = nullptr)
        : ConcurrentVectorImpl<float16, false>::ConcurrentVectorImpl(
              dim, size_per_chunk, std::move(mmap_descriptor), valid_data_ptr) {
    }
};

template <>
class ConcurrentVector<BFloat16Vector>
    : public ConcurrentVectorImpl<bfloat16, false> {
 public:
    ConcurrentVector(int64_t dim,
                     int64_t size_per_chunk,
                     storage::MmapChunkDescriptorPtr mmap_descriptor = nullptr,
                     ThreadSafeValidDataPtr valid_data_ptr = nullptr)
        : ConcurrentVectorImpl<bfloat16, false>::ConcurrentVectorImpl(
              dim, size_per_chunk, std::move(mmap_descriptor), valid_data_ptr) {
    }
};

template <>
class ConcurrentVector<Int8Vector> : public ConcurrentVectorImpl<int8, false> {
 public:
    ConcurrentVector(int64_t dim,
                     int64_t size_per_chunk,
                     storage::MmapChunkDescriptorPtr mmap_descriptor = nullptr,
                     ThreadSafeValidDataPtr valid_data_ptr = nullptr)
        : ConcurrentVectorImpl<int8, false>::ConcurrentVectorImpl(
              dim, size_per_chunk, std::move(mmap_descriptor)) {
    }
};

}  // namespace milvus::segcore
