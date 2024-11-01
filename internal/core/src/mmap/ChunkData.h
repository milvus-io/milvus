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
#include "common/Array.h"
#include "storage/MmapManager.h"
namespace milvus {
/**
 * @brief FixedLengthChunk
 */
template <typename Type>
struct FixedLengthChunk {
 public:
    FixedLengthChunk() = delete;
    explicit FixedLengthChunk(const uint64_t size,
                              storage::MmapChunkDescriptorPtr descriptor)
        : mmap_descriptor_(descriptor), size_(size) {
        auto mcm = storage::MmapManager::GetInstance().GetMmapChunkManager();
        data_ = (Type*)(mcm->Allocate(mmap_descriptor_, sizeof(Type) * size));
        AssertInfo(data_ != nullptr,
                   "failed to create a mmapchunk: {}, map_size");
    };
    void*
    data() {
        return data_;
    };
    size_t
    size() {
        return size_;
    };
    const Type&
    view(const int i) const {
        return data_[i];
    };

 private:
    int64_t size_ = 0;
    Type* data_ = nullptr;
    storage::MmapChunkDescriptorPtr mmap_descriptor_ = nullptr;
};
/**
 * @brief VariableLengthChunk
 */
template <typename Type>
struct VariableLengthChunk {
    static_assert(IsVariableTypeSupportInChunk<Type>);

 public:
    VariableLengthChunk() = delete;
    explicit VariableLengthChunk(const uint64_t size,
                                 storage::MmapChunkDescriptorPtr descriptor)
        : mmap_descriptor_(descriptor), size_(size) {
        data_ = FixedVector<ChunkViewType<Type>>(size);
    };
    inline void
    set(const Type* src, uint32_t begin, uint32_t length) {
        throw std::runtime_error(
            "set should be a template specialization function");
    }
    const ChunkViewType<Type>&
    view(const int i) const {
        return data_[i];
    }
    void*
    data() {
        return data_.data();
    };
    size_t
    size() {
        return size_;
    };

 private:
    int64_t size_ = 0;
    FixedVector<ChunkViewType<Type>> data_;
    storage::MmapChunkDescriptorPtr mmap_descriptor_ = nullptr;
};

// Template specialization for string
template <>
inline void
VariableLengthChunk<std::string>::set(const std::string* src,
                                      uint32_t begin,
                                      uint32_t length) {
    auto mcm = storage::MmapManager::GetInstance().GetMmapChunkManager();
    milvus::ErrorCode err_code;
    AssertInfo(
        begin + length <= size_,
        "failed to set a chunk with length: {} from beign {}, map_size={}",
        length,
        begin,
        size_);
    size_t total_size = 0;
    size_t padding_size = 1;
    for (auto i = 0; i < length; i++) {
        total_size += src[i].size() + padding_size;
    }
    auto buf = (char*)mcm->Allocate(mmap_descriptor_, total_size);
    AssertInfo(buf != nullptr, "failed to allocate memory from mmap_manager.");
    for (auto i = 0, offset = 0; i < length; i++) {
        auto data_size = src[i].size() + padding_size;
        char* data_ptr = buf + offset;
        std::strcpy(data_ptr, src[i].c_str());
        data_[i + begin] = std::string_view(data_ptr, src[i].size());
        offset += data_size;
    }
}

// Template specialization for sparse vector
template <>
inline void
VariableLengthChunk<knowhere::sparse::SparseRow<float>>::set(
    const knowhere::sparse::SparseRow<float>* src,
    uint32_t begin,
    uint32_t length) {
    auto mcm = storage::MmapManager::GetInstance().GetMmapChunkManager();
    milvus::ErrorCode err_code;
    AssertInfo(
        begin + length <= size_,
        "failed to set a chunk with length: {} from beign {}, map_size={}",
        length,
        begin,
        size_);

    size_t total_size = 0;
    for (auto i = 0; i < length; i++) {
        total_size += src[i].data_byte_size();
    }
    auto buf = (uint8_t*)mcm->Allocate(mmap_descriptor_, total_size);
    AssertInfo(buf != nullptr, "failed to allocate memory from mmap_manager.");
    for (auto i = 0, offset = 0; i < length; i++) {
        auto data_size = src[i].data_byte_size();
        uint8_t* data_ptr = buf + offset;
        std::memcpy(data_ptr, (uint8_t*)src[i].data(), data_size);
        data_[i + begin] =
            knowhere::sparse::SparseRow<float>(src[i].size(), data_ptr, false);
        offset += data_size;
    }
}

// Template specialization for json
template <>
inline void
VariableLengthChunk<Json>::set(const Json* src,
                               uint32_t begin,
                               uint32_t length) {
    auto mcm = storage::MmapManager::GetInstance().GetMmapChunkManager();
    milvus::ErrorCode err_code;
    AssertInfo(
        begin + length <= size_,
        "failed to set a chunk with length: {} from beign {}, map_size={}",
        length,
        begin,
        size_);
    size_t total_size = 0;
    size_t padding_size = simdjson::SIMDJSON_PADDING + 1;
    for (auto i = 0; i < length; i++) {
        total_size += src[i].size() + padding_size;
    }
    auto buf = (char*)mcm->Allocate(mmap_descriptor_, total_size);
    AssertInfo(buf != nullptr, "failed to allocate memory from mmap_manager.");
    for (auto i = 0, offset = 0; i < length; i++) {
        auto data_size = src[i].size() + padding_size;
        char* data_ptr = buf + offset;
        std::strcpy(data_ptr, src[i].c_str());
        data_[i + begin] = Json(data_ptr, src[i].size());
        offset += data_size;
    }
}

// Template specialization for array
template <>
inline void
VariableLengthChunk<Array>::set(const Array* src,
                                uint32_t begin,
                                uint32_t length) {
    auto mcm = storage::MmapManager::GetInstance().GetMmapChunkManager();
    milvus::ErrorCode err_code;
    AssertInfo(
        begin + length <= size_,
        "failed to set a chunk with length: {} from beign {}, map_size={}",
        length,
        begin,
        size_);
    size_t total_size = 0;
    size_t padding_size = 0;
    for (auto i = 0; i < length; i++) {
        total_size += src[i].byte_size() + padding_size;
    }
    auto buf = (char*)mcm->Allocate(mmap_descriptor_, total_size);
    AssertInfo(buf != nullptr, "failed to allocate memory from mmap_manager.");
    for (auto i = 0, offset = 0; i < length; i++) {
        auto data_size = src[i].byte_size() + padding_size;
        char* data_ptr = buf + offset;
        std::copy(src[i].data(), src[i].data() + src[i].byte_size(), data_ptr);
        data_[i + begin] = ArrayView(data_ptr,
                                     data_size,
                                     src[i].get_element_type(),
                                     src[i].get_offsets_in_copy());
        offset += data_size;
    }
}

}  // namespace milvus