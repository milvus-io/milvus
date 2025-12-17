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

#include "segcore/ConcurrentVectorArray.h"

#include "common/EasyAssert.h"
#include "common/Utils.h"

namespace milvus::segcore {

ConcurrentVectorArray::ConcurrentVectorArray(
    int64_t dim,
    DataType element_type,
    int64_t size_per_chunk,
    storage::MmapChunkDescriptorPtr mmap_descriptor)
    : VectorBase(size_per_chunk),
      dim_(dim),
      element_type_(element_type),
      mmap_descriptor_(std::move(mmap_descriptor)) {
    AssertInfo(
        size_per_chunk != MAX_ROW_COUNT,
        "size_per_chunk must not be MAX_ROW_COUNT for ConcurrentVectorArray");
}

void
ConcurrentVectorArray::set_data_raw(ssize_t element_offset,
                                    ssize_t element_count,
                                    const DataArray* data,
                                    const FieldMeta& field_meta) {
    AssertInfo(field_meta.get_data_type() == DataType::VECTOR_ARRAY,
               "data type is not VECTOR_ARRAY");
    auto& vector_array = data->vectors().vector_array().data();
    std::vector<VectorArray> source;
    source.reserve(vector_array.size());
    for (auto& e : vector_array) {
        source.emplace_back(e);
    }
    set_data_raw(element_offset, source.data(), element_count);
}

void
ConcurrentVectorArray::set_data_raw(ssize_t element_offset,
                                    const std::vector<FieldDataPtr>& datas) {
    for (auto& field_data : datas) {
        auto num_rows = field_data->get_num_rows();
        auto src = static_cast<const VectorArray*>(field_data->Data());
        set_data_raw(element_offset, src, num_rows);
        element_offset += num_rows;
    }
}

void
ConcurrentVectorArray::set_data_raw(ssize_t element_offset,
                                    const void* source,
                                    ssize_t element_count) {
    if (element_count == 0) {
        return;
    }

    auto chunk_num = upper_div(element_offset + element_count, size_per_chunk_);
    emplace_to_at_least(chunk_num);

    auto source_ptr = static_cast<const VectorArray*>(source);
    set_data(element_offset, source_ptr, element_count);
}

void
ConcurrentVectorArray::set_data(ssize_t element_offset,
                                const VectorArray* source,
                                ssize_t element_count) {
    auto chunk_id = element_offset / size_per_chunk_;
    auto chunk_offset = element_offset % size_per_chunk_;
    ssize_t source_offset = 0;
    if (chunk_offset + element_count <= size_per_chunk_) {
        // only first
        fill_chunk(
            chunk_id, chunk_offset, element_count, source, source_offset);
        return;
    }

    auto first_size = size_per_chunk_ - chunk_offset;
    fill_chunk(chunk_id, chunk_offset, first_size, source, source_offset);

    source_offset += first_size;
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
ConcurrentVectorArray::fill_chunk(ssize_t chunk_id,
                                  ssize_t chunk_offset,
                                  ssize_t element_count,
                                  const VectorArray* source,
                                  ssize_t source_offset) {
    if (element_count <= 0) {
        return;
    }
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto chunk_num = chunks_.size();
    AssertInfo(
        chunk_id < chunk_num,
        fmt::format("chunk_id out of chunk num, chunk_id={}, chunk_num={}",
                    chunk_id,
                    chunk_num));
    chunks_[chunk_id].copy_to_chunk(source + source_offset,
                                    chunk_offset,
                                    element_count,
                                    dim_,
                                    element_type_);
}

SpanBase
ConcurrentVectorArray::get_span_base(int64_t chunk_id) const {
    ThrowInfo(NotImplemented, "unimplemented");
}

std::unique_ptr<VectorBase::ChunkDataAccessor>
ConcurrentVectorArray::get_chunk_data(ssize_t chunk_index) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    AssertInfo(chunk_index < chunks_.size(),
               fmt::format(
                   "chunk_index out of chunk num, chunk_index={}, chunk_num={}",
                   chunk_index,
                   chunks_.size()));
    return std::make_unique<SharedPtrChunkDataAccessor>(
        chunks_[chunk_index].get_data_ptr());
}

int64_t
ConcurrentVectorArray::get_chunk_size(ssize_t chunk_index) const {
    return size_per_chunk_;
}

int64_t
ConcurrentVectorArray::get_element_size() const {
    ThrowInfo(NotImplemented, "unimplemented");
}

int64_t
ConcurrentVectorArray::get_element_offset(ssize_t chunk_index) const {
    return chunk_index * size_per_chunk_;
}

VectorFieldProto
ConcurrentVectorArray::vector_field_data_at(ssize_t element_offset) const {
    auto chunk_id = element_offset / size_per_chunk_;
    auto chunk_offset = element_offset % size_per_chunk_;
    std::shared_lock<std::shared_mutex> lock(mutex_);
    AssertInfo(
        chunk_id < chunks_.size(),
        fmt::format("chunk_id out of chunk num, chunk_id={}, chunk_num={}",
                    chunk_id,
                    chunks_.size()));
    return chunks_[chunk_id].vector_field_data_at(
        chunk_offset, dim_, element_type_);
}

const size_t*
ConcurrentVectorArray::get_chunk_offsets(ssize_t chunk_index) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    AssertInfo(chunk_index < chunks_.size(),
               fmt::format(
                   "chunk_index out of chunk num, chunk_index={}, chunk_num={}",
                   chunk_index,
                   chunks_.size()));
    return chunks_[chunk_index].get_offsets();
}

ssize_t
ConcurrentVectorArray::num_chunk() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return chunks_.size();
}

bool
ConcurrentVectorArray::empty() {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return chunks_.empty();
}

void
ConcurrentVectorArray::clear() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    chunks_.clear();
}

}  // namespace milvus::segcore
