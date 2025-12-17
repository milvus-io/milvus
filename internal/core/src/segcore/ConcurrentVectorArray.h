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

#include <shared_mutex>
#include <vector>

#include "common/Types.h"
#include "common/VectorArray.h"
#include "segcore/ConcurrentVector.h"
#include "storage/MmapManager.h"

namespace milvus::segcore {

// Accessor that holds shared_ptr to ensure data lifetime
class SharedPtrChunkDataAccessor : public VectorBase::ChunkDataAccessor {
 public:
    explicit SharedPtrChunkDataAccessor(std::shared_ptr<std::vector<char>> data)
        : data_(std::move(data)) {
    }
    const void*
    data() const override {
        return data_->data();
    }

 private:
    std::shared_ptr<std::vector<char>> data_;
};

class ConcurrentVectorArray : public VectorBase {
 public:
    ConcurrentVectorArray(
        int64_t dim,
        DataType element_type,
        int64_t size_per_chunk,
        storage::MmapChunkDescriptorPtr mmap_descriptor = nullptr);

    ~ConcurrentVectorArray() override = default;

    // Disable copy
    ConcurrentVectorArray(const ConcurrentVectorArray&) = delete;
    ConcurrentVectorArray&
    operator=(const ConcurrentVectorArray&) = delete;

    void
    set_data_raw(ssize_t element_offset,
                 ssize_t element_count,
                 const DataArray* data,
                 const FieldMeta& field_meta) override;

    void
    set_data_raw(ssize_t element_offset,
                 const std::vector<FieldDataPtr>& data) override;

    void
    set_data_raw(ssize_t element_offset,
                 const void* source,
                 ssize_t element_count) override;

    SpanBase
    get_span_base(int64_t chunk_id) const override;

    std::unique_ptr<ChunkDataAccessor>
    get_chunk_data(ssize_t chunk_index) const override;

    int64_t
    get_chunk_size(ssize_t chunk_index) const override;

    int64_t
    get_element_size() const override;

    int64_t
    get_element_offset(ssize_t chunk_index) const override;

    int64_t
    get_chunk_vector_offset(ssize_t chunk_index) const;

    VectorArrayView
    view_vector_array(ssize_t element_offset) const;

    const size_t*
    get_chunk_offsets(ssize_t chunk_index) const;

    ssize_t
    num_chunk() const override;

    bool
    empty() override;

    void
    clear() override;

 private:
    void
    emplace_to_at_least(int64_t chunk_num) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        while (chunks_.size() < chunk_num) {
            chunks_.emplace_back(size_per_chunk_);
        }
    }

    void
    set_data(ssize_t element_offset,
             const VectorArray* source,
             ssize_t element_count);

    void
    fill_chunk(ssize_t chunk_id,
               ssize_t chunk_offset,
               ssize_t element_count,
               const VectorArray* source,
               ssize_t source_offset);

 private:
    class Chunk {
     public:
        explicit Chunk(ssize_t size_per_chunk)
            : data_(std::make_shared<std::vector<char>>()),
              size_per_chunk_(size_per_chunk) {
            offsets_.push_back(0);
        }

        std::shared_ptr<std::vector<char>>
        get_data_ptr() const {
            return data_;
        }

        void
        copy_to_chunk(const VectorArray* source,
                      ssize_t chunk_offset,
                      ssize_t element_count,
                      int64_t expected_dim,
                      DataType expected_element_type) {
            // Assert sequential write
            AssertInfo(
                chunk_offset == static_cast<ssize_t>(offsets_.size() - 1),
                fmt::format(
                    "non-sequential write: chunk_offset={}, expected={}",
                    chunk_offset,
                    offsets_.size() - 1));

            // Validate first element
            AssertInfo(source[0].dim() == expected_dim,
                       fmt::format("dim mismatch: expected={}, actual={}",
                                   expected_dim,
                                   source[0].dim()));
            AssertInfo(
                source[0].get_element_type() == expected_element_type,
                fmt::format("element_type mismatch: expected={}, actual={}",
                            static_cast<int>(expected_element_type),
                            static_cast<int>(source[0].get_element_type())));

            size_t total_bytes = 0;
            for (ssize_t i = 0; i < element_count; ++i) {
                total_bytes += source[i].byte_size();
            }

            size_t old_size = data_->size();
            size_t new_size = old_size + total_bytes;

            // If capacity is sufficient, resize won't realloc, safe for readers.
            // Otherwise, copy-on-write to protect existing readers.
            if (data_->capacity() < new_size) {
                auto new_data = std::make_shared<std::vector<char>>(*data_);
                new_data->resize(new_size);
                data_ = std::move(new_data);
            } else {
                data_->resize(new_size);
            }

            size_t write_offset = old_size;
            for (ssize_t i = 0; i < element_count; ++i) {
                const auto& va = source[i];
                std::memcpy(
                    data_->data() + write_offset, va.data(), va.byte_size());
                write_offset += va.byte_size();
                offsets_.push_back(offsets_.back() + va.length());
            }

            // Shrink when chunk is full
            if (chunk_offset + element_count == size_per_chunk_) {
                auto new_data = std::make_shared<std::vector<char>>(
                    data_->begin(), data_->end());
                data_ = std::move(new_data);
            }
        }

        int64_t
        size() const {
            return offsets_.size() - 1;
        }

        int64_t
        total_vectors() const {
            return offsets_.back();
        }

        VectorArrayView
        view_vector_array(ssize_t chunk_offset,
                          int64_t dim,
                          DataType element_type) const {
            auto num_vectors =
                offsets_[chunk_offset + 1] - offsets_[chunk_offset];
            auto bytes_per_vector = vector_bytes_per_element(element_type, dim);
            auto byte_offset = offsets_[chunk_offset] * bytes_per_vector;
            auto byte_size = num_vectors * bytes_per_vector;
            return VectorArrayView(
                const_cast<char*>(data_->data() + byte_offset),
                dim,
                num_vectors,
                byte_size,
                element_type);
        }

        const size_t*
        get_offsets() const {
            return offsets_.data();
        }

     private:
        std::shared_ptr<std::vector<char>> data_;
        std::vector<size_t> offsets_{};
        ssize_t size_per_chunk_;
    };

 private:
    int64_t dim_;
    DataType element_type_;

    mutable std::shared_mutex mutex_;
    std::deque<Chunk> chunks_;

    mutable std::vector<int64_t> offsets_{0};
    storage::MmapChunkDescriptorPtr mmap_descriptor_;
};

}  // namespace milvus::segcore
