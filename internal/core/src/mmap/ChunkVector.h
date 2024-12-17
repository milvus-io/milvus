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
#include "mmap/ChunkData.h"
#include "storage/MmapManager.h"
namespace milvus {
template <typename Type>
class ChunkVectorBase {
 public:
    virtual ~ChunkVectorBase() = default;
    virtual void
    emplace_to_at_least(int64_t chunk_num, int64_t chunk_size) = 0;
    virtual void
    copy_to_chunk(int64_t chunk_id,
                  int64_t offest,
                  const Type* data,
                  int64_t length) = 0;
    virtual void*
    get_chunk_data(int64_t index) = 0;
    virtual int64_t
    get_chunk_size(int64_t index) = 0;
    virtual ChunkViewType<Type>
    view_element(int64_t chunk_id, int64_t chunk_offset) = 0;
    int64_t
    size() const {
        return counter_;
    }
    virtual void
    clear() = 0;
    virtual SpanBase
    get_span(int64_t chunk_id) = 0;

    virtual bool
    is_mmap() const = 0;

 protected:
    std::atomic<int64_t> counter_ = 0;
};
template <typename Type>
using ChunkVectorPtr = std::unique_ptr<ChunkVectorBase<Type>>;

template <typename Type,
          typename ChunkImpl = FixedVector<Type>,
          bool IsMmap = false>
class ThreadSafeChunkVector : public ChunkVectorBase<Type> {
 public:
    ThreadSafeChunkVector(
        storage::MmapChunkDescriptorPtr descriptor = nullptr) {
        mmap_descriptor_ = descriptor;
    }

    void
    emplace_to_at_least(int64_t chunk_num, int64_t chunk_size) override {
        std::unique_lock<std::shared_mutex> lck(this->mutex_);
        if (chunk_num <= this->counter_) {
            return;
        }
        while (vec_.size() < chunk_num) {
            if constexpr (IsMmap) {
                vec_.emplace_back(chunk_size, mmap_descriptor_);
            } else {
                vec_.emplace_back(chunk_size);
            }
            ++this->counter_;
        }
    }

    void
    copy_to_chunk(int64_t chunk_id,
                  int64_t offset,
                  const Type* data,
                  int64_t length) override {
        std::unique_lock<std::shared_mutex> lck(mutex_);
        AssertInfo(chunk_id < this->counter_,
                   fmt::format("index out of range, index={}, counter_={}",
                               chunk_id,
                               this->counter_));
        if constexpr (!IsMmap || !IsVariableType<Type>) {
            auto ptr = (Type*)vec_[chunk_id].data();
            AssertInfo(
                offset + length <= vec_[chunk_id].size(),
                fmt::format(
                    "index out of chunk range, offset={}, length={}, size={}",
                    offset,
                    length,
                    vec_[chunk_id].size()));
            std::copy_n(data, length, ptr + offset);
        } else {
            vec_[chunk_id].set(data, offset, length);
        }
    }

    ChunkViewType<Type>
    view_element(int64_t chunk_id, int64_t chunk_offset) override {
        std::shared_lock<std::shared_mutex> lck(mutex_);
        auto& chunk = vec_[chunk_id];
        if constexpr (IsMmap) {
            return chunk.view(chunk_offset);
        } else if constexpr (std::is_same_v<std::string, Type>) {
            return std::string_view(chunk[chunk_offset].data(),
                                    chunk[chunk_offset].size());
        } else if constexpr (std::is_same_v<Array, Type>) {
            auto& src = chunk[chunk_offset];
            return ArrayView(const_cast<char*>(src.data()),
                             src.byte_size(),
                             src.get_element_type(),
                             src.get_offsets_in_copy());
        } else {
            return chunk[chunk_offset];
        }
    }

    void*
    get_chunk_data(int64_t index) override {
        std::shared_lock<std::shared_mutex> lck(mutex_);
        AssertInfo(index < this->counter_,
                   fmt::format("index out of range, index={}, counter_={}",
                               index,
                               this->counter_));
        return vec_[index].data();
    }

    int64_t
    get_chunk_size(int64_t index) override {
        std::shared_lock<std::shared_mutex> lck(mutex_);
        AssertInfo(index < this->counter_,
                   fmt::format("index out of range, index={}, counter_={}",
                               index,
                               this->counter_));
        return vec_[index].size();
    }

    void
    clear() override {
        std::unique_lock<std::shared_mutex> lck(mutex_);
        this->counter_ = 0;
        vec_.clear();
    }

    SpanBase
    get_span(int64_t chunk_id) override {
        std::shared_lock<std::shared_mutex> lck(mutex_);
        if constexpr (IsMmap && std::is_same_v<std::string, Type>) {
            return SpanBase(get_chunk_data(chunk_id),
                            get_chunk_size(chunk_id),
                            sizeof(ChunkViewType<Type>));
        } else {
            return SpanBase(get_chunk_data(chunk_id),
                            get_chunk_size(chunk_id),
                            sizeof(Type));
        }
    }

    bool
    is_mmap() const override {
        return mmap_descriptor_ != nullptr;
    }

 private:
    mutable std::shared_mutex mutex_;
    storage::MmapChunkDescriptorPtr mmap_descriptor_ = nullptr;
    std::deque<ChunkImpl> vec_;
};

template <typename Type>
ChunkVectorPtr<Type>
SelectChunkVectorPtr(storage::MmapChunkDescriptorPtr& mmap_descriptor) {
    if constexpr (!IsVariableType<Type>) {
        if (mmap_descriptor != nullptr) {
            return std::make_unique<
                ThreadSafeChunkVector<Type, FixedLengthChunk<Type>, true>>(
                mmap_descriptor);
        } else {
            return std::make_unique<ThreadSafeChunkVector<Type>>();
        }
    } else if constexpr (IsVariableTypeSupportInChunk<Type>) {
        if (mmap_descriptor != nullptr) {
            return std::make_unique<
                ThreadSafeChunkVector<Type, VariableLengthChunk<Type>, true>>(
                mmap_descriptor);
        } else {
            return std::make_unique<ThreadSafeChunkVector<Type>>();
        }
    } else {
        return std::make_unique<ThreadSafeChunkVector<Type>>();
    }
}
}  // namespace milvus