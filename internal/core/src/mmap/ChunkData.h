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
                              storage::MmapChunkDescriptor descriptor)
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
    Type
    get(const int i) const {
        return data_[i];
    }
    const Type&
    view(const int i) const {
        return data_[i];
    }

 private:
    int64_t size_ = 0;
    Type* data_ = nullptr;
    storage::MmapChunkDescriptor mmap_descriptor_ = nullptr;
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
                                 storage::MmapChunkDescriptor descriptor)
        : mmap_descriptor_(descriptor), size_(size) {
        data_ = FixedVector<ChunkViewType<Type>>(size);
    };
    inline void
    set(const Type* src, uint32_t begin, uint32_t length) {
        throw std::runtime_error(
            "set should be a template specialization function");
    }
    inline Type
    get(const int i) const {
        throw std::runtime_error(
            "get should be a template specialization function");
    }
    const ChunkViewType<Type>&
    view(const int i) const {
        return data_[i];
    }
    const ChunkViewType<Type>&
    operator[](const int i) const {
        return view(i);
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
    storage::MmapChunkDescriptor mmap_descriptor_ = nullptr;
};
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
    for (auto i = 0; i < length; i++) {
        auto buf_size = src[i].size() + 1;
        auto buf = (char*)mcm->Allocate(mmap_descriptor_, buf_size);
        AssertInfo(buf != nullptr,
                   "failed to allocate memory from mmap_manager, error_code");
        std::strcpy(buf, src[i].c_str());
        data_[i + begin] = std::string_view(buf, src[i].size());
    }
}
template <>
inline std::string
VariableLengthChunk<std::string>::get(const int i) const {
    // copy to a string
    return std::string(data_[i]);
}
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
    for (auto i = 0; i < length; i++) {
        auto buf_size = src[i].size() + simdjson::SIMDJSON_PADDING + 1;
        auto buf = (char*)mcm->Allocate(mmap_descriptor_, buf_size);
        AssertInfo(
            buf != nullptr,
            "failed to allocate memory from mmap_manager, error_code:{}");
        std::strcpy(buf, src[i].c_str());
        data_[i + begin] = Json(buf, src[i].size());
    }
}
template <>
inline Json
VariableLengthChunk<Json>::get(const int i) const {
    return std::move(Json(simdjson::padded_string(data_[i].data())));
}
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
    for (auto i = 0; i < length; i++) {
        auto array_data =
            (char*)mcm->Allocate(mmap_descriptor_, src[i].byte_size());
        AssertInfo(array_data != nullptr,
                   "failed to allocate memory from mmap_manager, error_code");
        std::copy(
            src[i].data(), src[i].data() + src[i].byte_size(), array_data);
        data_[i + begin] = ArrayView(array_data,
                                     src[i].byte_size(),
                                     src[i].get_element_type(),
                                     src[i].get_offsets_in_copy());
    }
}
template <>
inline Array
VariableLengthChunk<Array>::get(const int i) const {
    auto array_view_i = data_[i];
    char* data = static_cast<char*>(const_cast<void*>(array_view_i.data()));
    return Array(data,
                 array_view_i.byte_size(),
                 array_view_i.get_element_type(),
                 array_view_i.get_offsets_in_copy());
}
}  // namespace milvus