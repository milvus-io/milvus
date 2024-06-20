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

#include <folly/io/IOBuf.h>
#include <sys/mman.h>
#include <algorithm>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <queue>
#include <string>
#include <vector>

#include "common/Array.h"
#include "common/Common.h"
#include "common/EasyAssert.h"
#include "common/File.h"
#include "common/FieldMeta.h"
#include "common/FieldData.h"
#include "common/Span.h"
#include "fmt/format.h"
#include "log/Log.h"
#include "mmap/Utils.h"
#include "common/FieldData.h"
#include "common/FieldDataInterface.h"
#include "common/Array.h"
#include "knowhere/dataset.h"
#include "monitor/prometheus_client.h"
#include "storage/MmapChunkManager.h"

namespace milvus {

/*
* If string field's value all empty, need a string padding to avoid
* mmap failing because size_ is zero which causing invalid arguement
* array has the same problem
* TODO: remove it when support NULL value
*/
constexpr size_t STRING_PADDING = 1;
constexpr size_t ARRAY_PADDING = 1;

constexpr size_t BLOCK_SIZE = 8192;

class ColumnBase {
 public:
    enum MappingType {
        MAP_WITH_ANONYMOUS = 0,
        MAP_WITH_FILE = 1,
        MAP_WITH_MANAGER = 2,
    };
    // memory mode ctor
    ColumnBase(size_t reserve, const FieldMeta& field_meta)
        : mapping_type_(MappingType::MAP_WITH_ANONYMOUS) {
        auto data_type = field_meta.get_data_type();
        SetPaddingSize(data_type);

        if (IsVariableDataType(data_type)) {
            return;
        }

        if (!field_meta.is_vector()) {
            is_scalar = true;
        } else {
            AssertInfo(!field_meta.is_nullable(),
                       "only support null in scalar");
        }
        type_size_ = field_meta.get_sizeof();
        data_cap_size_ = field_meta.get_sizeof() * reserve;

        // use anon mapping so we are able to free these memory with munmap only
        size_t mapped_size = data_cap_size_ + padding_;
        data_ = static_cast<char*>(mmap(nullptr,
                                        mapped_size,
                                        PROT_READ | PROT_WRITE,
                                        MAP_PRIVATE | MAP_ANON,
                                        -1,
                                        0));
        AssertInfo(data_ != MAP_FAILED,
                   "failed to create anon map: {}, map_size={}",
                   strerror(errno),
                   mapped_size);

        if (field_meta.is_nullable()) {
            nullable = true;
            valid_data_cap_size_ = (reserve + 7) / 8;
            mapped_size += valid_data_cap_size_;
            valid_data_ = static_cast<uint8_t*>(mmap(nullptr,
                                                     valid_data_cap_size_,
                                                     PROT_READ | PROT_WRITE,
                                                     MAP_PRIVATE | MAP_ANON,
                                                     -1,
                                                     0));
            AssertInfo(valid_data_ != MAP_FAILED,
                       "failed to create anon map, err: {}",
                       strerror(errno));
        }
        UpdateMetricWhenMmap(mapped_size);
    }

    // use mmap manager ctor, used in growing segment fixed data type
    ColumnBase(size_t reserve,
               int dim,
               const DataType& data_type,
               storage::MmapChunkManagerPtr mcm,
               storage::MmapChunkDescriptorPtr descriptor,
               bool nullable)
        : mcm_(mcm),
          mmap_descriptor_(descriptor),
          type_size_(GetDataTypeSize(data_type, dim)),
          num_rows_(0),
          data_size_(0),
          data_cap_size_(reserve),
          mapping_type_(MAP_WITH_MANAGER),
          nullable(nullable) {
        AssertInfo((mcm != nullptr) && descriptor != nullptr,
                   "use wrong mmap chunk manager and mmap chunk descriptor to "
                   "create column.");

        SetPaddingSize(data_type);
        size_t mapped_size = data_cap_size_ + padding_;
        data_ = (char*)mcm_->Allocate(mmap_descriptor_, (uint64_t)mapped_size);
        AssertInfo(data_ != nullptr,
                   "fail to create with mmap manager: map_size = {}",
                   mapped_size);
        if (nullable) {
            nullable = true;
            valid_data_cap_size_ = (reserve + 7) / 8;
            mapped_size += valid_data_cap_size_;
            valid_data_ = (uint8_t*)mcm_->Allocate(
                mmap_descriptor_, (uint64_t)valid_data_cap_size_);
            AssertInfo(valid_data_ != MAP_FAILED,
                       "failed to create anon map, err: {}",
                       strerror(errno));
        }
    }

    // mmap mode ctor
    // User must call Seal to build the view for variable length column.
    ColumnBase(const File& file, size_t size, const FieldMeta& field_meta)
        : mapping_type_(MappingType::MAP_WITH_FILE) {
        auto data_type = field_meta.get_data_type();
        SetPaddingSize(data_type);
        if (!IsVariableDataType(data_type)) {
            type_size_ = field_meta.get_sizeof();
            num_rows_ = size / type_size_;
        }

        data_size_ = size;
        data_cap_size_ = size;
        // use exactly same size of file, padding shall be written in file already
        // see also https://github.com/milvus-io/milvus/issues/34442
        size_t mapped_size = data_cap_size_;
        data_ = static_cast<char*>(mmap(
            nullptr, mapped_size, PROT_READ, MAP_SHARED, file.Descriptor(), 0));
        AssertInfo(data_ != MAP_FAILED,
                   "failed to create file-backed map, err: {}",
                   strerror(errno));
        madvise(data_, mapped_size, MADV_WILLNEED);

        if (!field_meta.is_vector()) {
            is_scalar = true;
            if (field_meta.is_nullable()) {
                nullable = true;
                valid_data_cap_size_ = (num_rows_ + 7) / 8;
                valid_data_size_ = (num_rows_ + 7) / 8;
                mapped_size += valid_data_size_;
                valid_data_ = static_cast<uint8_t*>(mmap(nullptr,
                                                         valid_data_cap_size_,
                                                         PROT_READ | PROT_WRITE,
                                                         MAP_PRIVATE | MAP_ANON,
                                                         file.Descriptor(),
                                                         0));
                AssertInfo(valid_data_ != MAP_FAILED,
                           "failed to create file-backed map, err: {}",
                           strerror(errno));
                madvise(valid_data_, valid_data_cap_size_, MADV_WILLNEED);
            }
        }

        UpdateMetricWhenMmap(mapped_size);
    }

    // mmap mode ctor
    // User must call Seal to build the view for variable length column.
    ColumnBase(const File& file,
               size_t size,
               int dim,
               const DataType& data_type,
               bool nullable)
        : type_size_(IsSparseFloatVectorDataType(data_type)
                         ? 1
                         : GetDataTypeSize(data_type, dim)),
          num_rows_(
              IsSparseFloatVectorDataType(data_type) ? 1 : (size / type_size_)),
          data_size_(size),
          data_cap_size_(size),
          nullable(nullable),
          mapping_type_(MappingType::MAP_WITH_FILE) {
        SetPaddingSize(data_type);

        // use exact same size of file, padding shall be written in file already
        // see also https://github.com/milvus-io/milvus/issues/34442
        size_t mapped_size = cap_size_;
        if (!IsVariableDataType(data_type)) {
            type_size_ = GetDataTypeSize(data_type, dim);
            num_rows_ = size / type_size_;
        }
        data_ = static_cast<char*>(mmap(
            nullptr, mapped_size, PROT_READ, MAP_SHARED, file.Descriptor(), 0));
        AssertInfo(data_ != MAP_FAILED,
                   "failed to create file-backed map, err: {}",
                   strerror(errno));
        if (dim == 1) {
            is_scalar = true;
            if (nullable) {
                valid_data_cap_size_ = (num_rows_ + 7) / 8;
                valid_data_size_ = (num_rows_ + 7) / 8;
                mapped_size += valid_data_size_;
                valid_data_ = static_cast<uint8_t*>(mmap(nullptr,
                                                         valid_data_cap_size_,
                                                         PROT_READ | PROT_WRITE,
                                                         MAP_PRIVATE | MAP_ANON,
                                                         file.Descriptor(),
                                                         0));
                AssertInfo(valid_data_ != MAP_FAILED,
                           "failed to create file-backed map, err: {}",
                           strerror(errno));
            }
        }
        UpdateMetricWhenMmap(mapped_size);
    }

    virtual ~ColumnBase() {
        if (data_ != nullptr) {
            if (mapping_type_ != MappingType::MAP_WITH_MANAGER) {
                size_t mapped_size = data_cap_size_ + padding_;
                if (munmap(data_, mapped_size)) {
                    AssertInfo(true,
                               "failed to unmap variable field, err={}",
                               strerror(errno));
                }
            }
            UpdateMetricWhenMunmap(data_cap_size_ + padding_);
        }
        if (valid_data_ != nullptr) {
            if (munmap(valid_data_, valid_data_cap_size_)) {
                AssertInfo(true,
                           "failed to unmap variable field, err={}",
                           strerror(errno));
            }
            UpdateMetricWhenMunmap(valid_data_cap_size_);
        }
    }

    ColumnBase(ColumnBase&& column) noexcept
        : data_(column.data_),
          nullable(column.nullable),
          valid_data_(column.valid_data_),
          valid_data_cap_size_(column.valid_data_cap_size_),
          data_cap_size_(column.data_cap_size_),
          padding_(column.padding_),
          type_size_(column.type_size_),
          num_rows_(column.num_rows_),
          data_size_(column.data_size_),
          valid_data_size_(column.valid_data_size_) {
        column.data_ = nullptr;
        column.data_cap_size_ = 0;
        column.padding_ = 0;
        column.num_rows_ = 0;
        column.data_size_ = 0;
        column.nullable = false;
        column.valid_data_ = nullptr;
        column.valid_data_cap_size_ = 0;
        column.valid_data_size_ = 0;
    }

    // Data() points at an addr that contains the elements
    virtual const char*
    Data() const {
        return data_;
    }

    // MmappedData() returns the mmaped address
    const char*
    MmappedData() const {
        return data_;
    }

    const uint8_t*
    ValidData() const {
        return valid_data_;
    }

    bool
    IsNullable() const {
        return nullable;
    }

    size_t
    DataSize() const {
        return data_size_;
    }

    size_t
    ValidDataSize() const {
        return valid_data_size_;
    }

    size_t
    NumRows() const {
        return num_rows_;
    };

    virtual size_t
    ByteSize() const {
        return data_cap_size_ + padding_ + valid_data_cap_size_;
    }

    // The capacity of the column,
    // DO NOT call this for variable length column(including SparseFloatColumn).
    virtual size_t
    Capacity() const {
        return data_cap_size_ / type_size_;
    }

    virtual SpanBase
    Span() const = 0;

    // used for sequential access for search
    virtual BufferView
    GetBatchBuffer(int64_t start_offset, int64_t length) {
        PanicInfo(ErrorCode::Unsupported,
                  "GetBatchBuffer only supported for VariableColumn");
    }

    virtual std::vector<std::string_view>
    StringViews() const {
        PanicInfo(ErrorCode::Unsupported,
                  "StringViews only supported for VariableColumn");
    }

    virtual void
    AppendBatch(const FieldDataPtr data) {
        size_t required_size = data_size_ + data->DataSize();
        if (required_size > data_cap_size_) {
            ExpandData(required_size * 2 + padding_);
        }

        std::copy_n(static_cast<const char*>(data->Data()),
                    data->DataSize(),
                    data_ + data_size_);
        data_size_ = required_size;
        num_rows_ += data->Length();
        AppendValidData(data->ValidData(), data->ValidDataSize());
    }

    // Append one row
    virtual void
    Append(const char* data, size_t size) {
        size_t required_size = data_size_ + size;
        if (required_size > data_cap_size_) {
            ExpandData(required_size * 2);
        }

        std::copy_n(data, size, data_ + data_size_);
        data_size_ = required_size;
        num_rows_++;
    }

    // append valid_data don't need to change num_rows
    void
    AppendValidData(const uint8_t* valid_data, size_t size) {
        if (nullable == true) {
            size_t required_size = valid_data_size_ + size;
            if (required_size > valid_data_cap_size_) {
                ExpandValidData(required_size * 2);
            }
            std::copy(valid_data, valid_data + size, valid_data_);
        }
    }

    void
    SetPaddingSize(const DataType& type) {
        switch (type) {
            case DataType::JSON:
                // simdjson requires a padding following the json data
                padding_ = simdjson::SIMDJSON_PADDING;
                break;
            case DataType::VARCHAR:
            case DataType::STRING:
                padding_ = STRING_PADDING;
                break;
            case DataType::ARRAY:
                padding_ = ARRAY_PADDING;
                break;
            default:
                padding_ = 0;
                break;
        }
    }
    AssertInfo(mapping_type_ == MappingType::MAP_WITH_ANONYMOUS ||
                   mapping_type_ == MappingType::MAP_WITH_MANAGER,
               "expand function only use in anonymous or with mmap manager");
    if (mapping_type_ == MappingType::MAP_WITH_ANONYMOUS) {
        size_t new_mapped_size = new_size + padding_;
        auto valid_data = static_cast<uint8_t*>(mmap(nullptr,
                                                     new_mapped_size,
                                                     PROT_READ | PROT_WRITE,
                                                     MAP_PRIVATE | MAP_ANON,
                                                     -1,
                                                     0));
        UpdateMetricWhenMmap(true, new_mapped_size);

        AssertInfo(valid_data != MAP_FAILED,
                   "failed to expand map: {}, new_map_size={}",
                   strerror(errno),
                   new_size + padding_);

        if (valid_data_ != nullptr) {
            std::memcpy(valid_data, valid_data_, valid_data_size_);
            if (munmap(valid_data_, valid_data_cap_size_ + padding_)) {
                auto err = errno;
                size_t mapped_size = new_size + padding_;
                munmap(valid_data, mapped_size);
                UpdateMetricWhenMunmap(mapped_size);

                AssertInfo(
                    false,
                    "failed to unmap while expanding: {}, old_map_size={}",
                    strerror(err),
                    data_cap_size_ + padding_);
            }

            data_ = data;
            data_cap_size_ = new_size;
            mapping_type_ = MappingType::MAP_WITH_ANONYMOUS;
        } else if (mapping_type_ == MappingType::MAP_WITH_MANAGER) {
            size_t new_mapped_size = new_size + padding_;
            auto data = mcm_->Allocate(mmap_descriptor_, new_mapped_size);
            AssertInfo(data != nullptr,
                       "fail to create with mmap manager: map_size = {}",
                       new_mapped_size);
            std::memcpy(data, data_, data_cap_size_);
            // allocate space only append in one growing segment, so no need to munmap()
            data_ = (char*)data;
            data_cap_size_ = new_size;
            mapping_type_ = MappingType::MAP_WITH_MANAGER;
        }
    }

    // only for memory mode and mmap manager mode, not mmap
    void
    ExpandValidData(size_t new_size) {
        if (new_size == 0) {
            return;
        }
        AssertInfo(
            mapping_type_ == MappingType::MAP_WITH_ANONYMOUS ||
                mapping_type_ == MappingType::MAP_WITH_MANAGER,
            "expand function only use in anonymous or with mmap manager");
        if (mapping_type_ == MappingType::MAP_WITH_ANONYMOUS) {
            size_t new_mapped_size = new_size + padding_;
            auto valid_data = static_cast<uint8_t*>(mmap(nullptr,
                                                         new_mapped_size,
                                                         PROT_READ | PROT_WRITE,
                                                         MAP_PRIVATE | MAP_ANON,
                                                         -1,
                                                         0));
            UpdateMetricWhenMmap(true, new_mapped_size);

            AssertInfo(valid_data != MAP_FAILED,
                       "failed to expand map: {}, new_map_size={}",
                       strerror(errno),
                       new_size + padding_);

            if (valid_data_ != nullptr) {
                std::memcpy(valid_data, valid_data_, valid_size_);
                if (munmap(valid_data_, valid_data_cap_size_ + padding_)) {
                    auto err = errno;
                    size_t mapped_size = new_size + padding_;
                    munmap(valid_data, mapped_size);
                    UpdateMetricWhenMunmap(mapped_size);

                    AssertInfo(false,
                               "failed to unmap while expanding: {}, "
                               "old_map_size={}",
                               strerror(err),
                               data_cap_size_ + padding_);
                }
                UpdateMetricWhenMunmap(data_cap_size_ + padding_);
            }

            valid_data_ = valid_data;
            valid_data_cap_size_ = new_size;
            mapping_type_ = MappingType::MAP_WITH_ANONYMOUS;
        } else if (mapping_type_ == MappingType::MAP_WITH_MANAGER) {
            size_t new_mapped_size = new_size + padding_;
            auto valid_data = mcm_->Allocate(mmap_descriptor_, new_mapped_size);
            AssertInfo(valid_data != nullptr,
                       "fail to create with mmap manager: map_size = {}",
                       new_mapped_size);
            std::memcpy(valid_data, valid_data_, valid_data_cap_size_);
            // allocate space only append in one growing segment, so no need to munmap()
            valid_data_ = (uint8_t*)valid_data;
            valid_data_cap_size_ = new_size;
            mapping_type_ = MappingType::MAP_WITH_MANAGER;
        }
    }

    char* data_{nullptr};
    bool nullable{false};
    uint8_t* valid_data_{nullptr};
    size_t valid_data_cap_size_{0};
    // std::shared_ptr<uint8_t[]> valid_data_{nullptr};
    bool is_scalar{false};
    // capacity in bytes
    size_t data_cap_size_{0};
    size_t padding_{0};
    // type_size_ is not used for sparse float vector column.
    const size_t type_size_{1};
    size_t num_rows_{0};

    // length in bytes
    storage::MmapChunkDescriptorPtr mmap_descriptor_ = nullptr;
    size_t data_size_{0};
    size_t valid_data_size_{0};

 private:
    void
    UpdateMetricWhenMmap(size_t mmaped_size) {
        UpdateMetricWhenMmap(mapping_type_, mmaped_size);
    }

    void
    UpdateMetricWhenMmap(bool is_map_anonymous, size_t mapped_size) {
        if (mapping_type_ == MappingType::MAP_WITH_ANONYMOUS) {
            milvus::storage::internal_mmap_allocated_space_bytes_anon.Observe(
                mapped_size);
            milvus::storage::internal_mmap_in_used_space_bytes_anon.Increment(
                mapped_size);
        } else {
            milvus::storage::internal_mmap_allocated_space_bytes_file.Observe(
                mapped_size);
            milvus::storage::internal_mmap_in_used_space_bytes_file.Increment(
                mapped_size);
        }
    }

    void
    UpdateMetricWhenMunmap(size_t mapped_size) {
        if (mapping_type_ == MappingType::MAP_WITH_ANONYMOUS) {
            milvus::storage::internal_mmap_in_used_space_bytes_anon.Decrement(
                mapped_size);
        } else {
            milvus::storage::internal_mmap_in_used_space_bytes_file.Decrement(
                mapped_size);
        }
    }

 private:
    // mapping_type_
    MappingType mapping_type_;
    storage::MmapChunkManagerPtr mcm_ = nullptr;
};

class Column : public ColumnBase {
 public:
    // memory mode ctor
    Column(size_t cap, const FieldMeta& field_meta)
        : ColumnBase(cap, field_meta) {
    }

    // mmap mode ctor
    Column(const File& file, size_t size, const FieldMeta& field_meta)
        : ColumnBase(file, size, field_meta) {
    }

    // mmap mode ctor
    Column(const File& file,
           size_t size,
           int dim,
           DataType data_type,
           bool nullable)
        : ColumnBase(file, size, dim, data_type, nullable) {
    }

    Column(size_t reserve,
           int dim,
           const DataType& data_type,
           storage::MmapChunkManagerPtr mcm,
           storage::MmapChunkDescriptorPtr descriptor,
           bool nullable)
        : ColumnBase(reserve, dim, data_type, mcm, descriptor, nullable) {
    }

    Column(Column&& column) noexcept : ColumnBase(std::move(column)) {
    }

    ~Column() override = default;

    SpanBase
    Span() const override {
        return SpanBase(data_, num_rows_, data_cap_size_ / num_rows_);
    }
};

// when mmap is used, size_, data_ and num_rows_ of ColumnBase are used.
class SparseFloatColumn : public ColumnBase {
 public:
    // memory mode ctor
    SparseFloatColumn(const FieldMeta& field_meta) : ColumnBase(0, field_meta) {
    }
    // mmap mode ctor
    SparseFloatColumn(const File& file,
                      size_t size,
                      const FieldMeta& field_meta)
        : ColumnBase(file, size, field_meta) {
    }
    // mmap mode ctor
    SparseFloatColumn(const File& file,
                      size_t size,
                      int dim,
                      const DataType& data_type)
        : ColumnBase(file, size, dim, data_type, false) {
    }
    // mmap with mmap manager
    SparseFloatColumn(size_t reserve,
                      int dim,
                      const DataType& data_type,
                      storage::MmapChunkManagerPtr mcm,
                      storage::MmapChunkDescriptorPtr descriptor)
        : ColumnBase(reserve, dim, data_type, mcm, descriptor, false) {
    }

    SparseFloatColumn(SparseFloatColumn&& column) noexcept
        : ColumnBase(std::move(column)),
          dim_(column.dim_),
          vec_(std::move(column.vec_)) {
    }

    ~SparseFloatColumn() override = default;

    const char*
    Data() const override {
        return static_cast<const char*>(static_cast<const void*>(vec_.data()));
    }

    // This is used to advice mmap prefetch, we don't currently support mmap for
    // sparse float vector thus not implemented for now.
    size_t
    ByteSize() const override {
        PanicInfo(ErrorCode::Unsupported,
                  "ByteSize not supported for sparse float column");
    }

    size_t
    Capacity() const override {
        PanicInfo(ErrorCode::Unsupported,
                  "Capacity not supported for sparse float column");
    }

    SpanBase
    Span() const override {
        PanicInfo(ErrorCode::Unsupported,
                  "Span not supported for sparse float column");
    }

    void
    AppendBatch(const FieldDataPtr data) override {
        auto ptr = static_cast<const knowhere::sparse::SparseRow<float>*>(
            data->Data());
        vec_.insert(vec_.end(), ptr, ptr + data->Length());
        for (size_t i = 0; i < data->Length(); ++i) {
            dim_ = std::max(dim_, ptr[i].dim());
        }
        num_rows_ += data->Length();
    }

    void
    Append(const char* data, size_t size) override {
        PanicInfo(ErrorCode::Unsupported,
                  "Append not supported for sparse float column");
    }

    int64_t
    Dim() const {
        return dim_;
    }

    void
    Seal(std::vector<uint64_t> indices) {
        AssertInfo(!indices.empty(),
                   "indices should not be empty, Seal() of "
                   "SparseFloatColumn must be called only "
                   "at mmap mode");
        AssertInfo(data_,
                   "data_ should not be nullptr, Seal() of "
                   "SparseFloatColumn must be called only "
                   "at mmap mode");
        num_rows_ = indices.size();
        // so that indices[num_rows_] - indices[num_rows_ - 1] is the size of
        // the last row.
        indices.push_back(data_size_ + valid_data_size_);
        for (size_t i = 0; i < num_rows_; i++) {
            auto vec_size = indices[i + 1] - indices[i];
            AssertInfo(
                vec_size % knowhere::sparse::SparseRow<float>::element_size() ==
                    0,
                "Incorrect sparse vector size: {}",
                vec_size);
            vec_.emplace_back(
                vec_size / knowhere::sparse::SparseRow<float>::element_size(),
                (uint8_t*)(data_) + indices[i],
                false);
        }

        size_t Capacity() const override {
            PanicInfo(ErrorCode::Unsupported,
                      "Capacity not supported for sparse float column");
        }

        SpanBase Span() const override {
            PanicInfo(ErrorCode::Unsupported,
                      "Span not supported for sparse float column");
        }

        void AppendBatch(const FieldDataPtr data) override {
            auto ptr = static_cast<const knowhere::sparse::SparseRow<float>*>(
                data->Data());
            vec_.insert(vec_.end(), ptr, ptr + data->Length());
            for (size_t i = 0; i < data->Length(); ++i) {
                dim_ = std::max(dim_, ptr[i].dim());
            }
            num_rows_ += data->Length();
        }

        // mmap mode ctor
        VariableColumn(
            const File& file, size_t size, const FieldMeta& field_meta)
            : ColumnBase(file, size, field_meta) {
        }
        // mmap with mmap manager
        VariableColumn(size_t reserve,
                       int dim,
                       const DataType& data_type,
                       storage::MmapChunkManagerPtr mcm,
                       storage::MmapChunkDescriptorPtr descriptor,
                       bool nullable)
            : ColumnBase(reserve, dim, data_type, mcm, descriptor, nullable) {
        }

        int64_t Dim() const {
            return dim_;
        }

        void Seal(std::vector<uint64_t> indices) {
            AssertInfo(!indices.empty(),
                       "indices should not be empty, Seal() of "
                       "SparseFloatColumn must be called only "
                       "at mmap mode");
            AssertInfo(data_,
                       "data_ should not be nullptr, Seal() of "
                       "SparseFloatColumn must be called only "
                       "at mmap mode");
            num_rows_ = indices.size();
            // so that indices[num_rows_] - indices[num_rows_ - 1] is the size of
            // the last row.
            indices.push_back(size_);
            for (size_t i = 0; i < num_rows_; i++) {
                auto vec_size = indices[i + 1] - indices[i];
                AssertInfo(vec_size % knowhere::sparse::SparseRow<
                                          float>::element_size() ==
                               0,
                           "Incorrect sparse vector size: {}",
                           vec_size);
                vec_.emplace_back(
                    vec_size /
                        knowhere::sparse::SparseRow<float>::element_size(),
                    (uint8_t*)(data_) + indices[i],
                    false);
            }
        }

     private:
        int64_t dim_ = 0;
        std::vector<knowhere::sparse::SparseRow<float>> vec_;
    };

    template <typename T>
    class VariableColumn : public ColumnBase {
     public:
        using ViewType = std::
            conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;

        // memory mode ctor
        VariableColumn(size_t cap, const FieldMeta& field_meta)
            : ColumnBase(cap, field_meta) {
        }

        // mmap mode ctor
        VariableColumn(const File& file,
                       size_t size,
                       const FieldMeta& field_meta)
            : ColumnBase(file, size, field_meta) {
        }
        // mmap with mmap manager
        VariableColumn(size_t reserve,
                       int dim,
                       const DataType& data_type,
                       storage::MmapChunkManagerPtr mcm,
                       storage::MmapChunkDescriptorPtr descriptor)
            : ColumnBase(reserve, dim, data_type, mcm, descriptor) {
        }

        VariableColumn(VariableColumn&& column) noexcept
            : ColumnBase(std::move(column)),
              indices_(std::move(column.indices_)) {
        }

        ~VariableColumn() override = default;

        SpanBase
        Span() const override {
            PanicInfo(
                ErrorCode::NotImplemented,
                "span() interface is not implemented for variable column");
        }

        std::vector<std::string_view>
        StringViews() const override {
            std::vector<std::string_view> res;
            char* pos = data_;
            for (size_t i = 0; i < num_rows_; ++i) {
                uint32_t size;
                size = *reinterpret_cast<uint32_t*>(pos);
                pos += sizeof(uint32_t);
                res.emplace_back(std::string_view(pos, size));
                pos += size;
            }
            return res;
        }

        [[nodiscard]] std::vector<ViewType>
        Views() const {
            std::vector<ViewType> res;
            char* pos = data_;
            for (size_t i = 0; i < num_rows_; ++i) {
                uint32_t size;
                size = *reinterpret_cast<uint32_t*>(pos);
                pos += sizeof(uint32_t);
                res.emplace_back(ViewType(pos, size));
                pos += size;
            }
            return res;
        }

        BufferView
        GetBatchBuffer(int64_t start_offset, int64_t length) override {
            if (start_offset < 0 || start_offset > num_rows_ ||
                start_offset + length > num_rows_) {
                PanicInfo(ErrorCode::OutOfRange, "index out of range");
            }

            char* pos = data_ + indices_[start_offset / BLOCK_SIZE];
            for (size_t j = 0; j < start_offset % BLOCK_SIZE; j++) {
                uint32_t size;
                size = *reinterpret_cast<uint32_t*>(pos);
                pos += sizeof(uint32_t) + size;
            }

            return BufferView{pos, size_ - (pos - data_)};
        }

        ViewType
        operator[](const int i) const {
            if (i < 0 || i > num_rows_) {
                PanicInfo(ErrorCode::OutOfRange, "index out of range");
            }
            size_t batch_id = i / BLOCK_SIZE;
            size_t offset = i % BLOCK_SIZE;

            // located in batch start location
            char* pos = data_ + indices_[batch_id];
            for (size_t j = 0; j < offset; j++) {
                uint32_t size;
                size = *reinterpret_cast<uint32_t*>(pos);
                pos += sizeof(uint32_t) + size;
            }

            uint32_t size;
            size = *reinterpret_cast<uint32_t*>(pos);
            return ViewType(pos + sizeof(uint32_t), size);
        }

        std::string_view
        RawAt(const int i) const {
            return std::string_view((*this)[i]);
        }

        void
        Append(FieldDataPtr chunk) {
            for (auto i = 0; i < chunk->get_num_rows(); i++) {
                indices_.emplace_back(size_);
                auto data = static_cast<const T*>(chunk->RawValue(i));
                data_size_ += sizeof(uint32_t) + data->size();
            }
            load_buf_.emplace(std::move(chunk));
        }

        void
        Seal(std::vector<uint64_t> indices = {}) {
            if (!indices.empty()) {
                indices_ = std::move(indices);
            }

            ArrayColumn(size_t reserve,
                        int dim,
                        const DataType& data_type,
                        storage::MmapChunkManagerPtr mcm,
                        storage::MmapChunkDescriptorPtr descriptor,
                        bool nullable)
                : ColumnBase(
                      reserve, dim, data_type, mcm, descriptor, nullable) {
            }

            // for variable length column in memory mode only
            if (data_ == nullptr) {
                size_t total_data_size = data_size_;
                data_size_ = 0;
                ExpandData(total_data_size);

                size_t total_valid_data_size = valid_data_size_;
                valid_data_size_ = 0;
                ExpandValidData(total_valid_data_size);

                while (!load_buf_.empty()) {
                    auto chunk = std::move(load_buf_.front());
                    load_buf_.pop();

                    // data_ as: |size|data|size|data......
                    for (auto i = 0; i < chunk->get_num_rows(); i++) {
                        auto current_size = (uint32_t)chunk->Size(i);
                        std::memcpy(
                            data_ + size_, &current_size, sizeof(uint32_t));
                        size_ += sizeof(uint32_t);
                        auto data = static_cast<const T*>(chunk->RawValue(i));
                        std::memcpy(data_ + size_, data->c_str(), data->size());
                        data_size_ += data->size();
                    }
                    if (nullable == true) {
                        std::copy(chunk->ValidData(),
                                  chunk->ValidDataSize() + chunk->ValidData(),
                                  valid_data_);
                    }
                    valid_data_size_ += chunk->ValidDataSize();
                }
            }

            shrink_indice();
        }

     protected:
        void
        shrink_indice() {
            std::vector<uint64_t> tmp_indices;
            tmp_indices.reserve((indices_.size() + BLOCK_SIZE - 1) /
                                BLOCK_SIZE);

            for (size_t i = 0; i < indices_.size();) {
                tmp_indices.push_back(indices_[i]);
                i += BLOCK_SIZE;
            }

            indices_.swap(tmp_indices);
        }

     private:
        // loading states
        std::queue<FieldDataPtr> load_buf_{};

        // raw data index, record indices located 0, interval, 2 * interval, 3 * interval
        // ... just like page index, interval set to 8192 that matches search engine's batch size
        std::vector<uint64_t> indices_{};
    };

    class ArrayColumn : public ColumnBase {
     public:
        // memory mode ctor
        ArrayColumn(size_t num_rows, const FieldMeta& field_meta)
            : ColumnBase(num_rows, field_meta),
              element_type_(field_meta.get_element_type()) {
        }

        // mmap mode ctor
        ArrayColumn(const File& file, size_t size, const FieldMeta& field_meta)
            : ColumnBase(file, size, field_meta),
              element_type_(field_meta.get_element_type()) {
        }

        ArrayColumn(size_t reserve,
                    int dim,
                    const DataType& data_type,
                    storage::MmapChunkManagerPtr mcm,
                    storage::MmapChunkDescriptorPtr descriptor)
            : ColumnBase(reserve, dim, data_type, mcm, descriptor) {
        }

        ArrayColumn(ArrayColumn&& column) noexcept
            : ColumnBase(std::move(column)),
              indices_(std::move(column.indices_)),
              views_(std::move(column.views_)),
              element_type_(column.element_type_) {
        }

        ~ArrayColumn() override = default;

        SpanBase
        Span() const override {
            return SpanBase(views_.data(), views_.size(), sizeof(ArrayView));
        }

        [[nodiscard]] const std::vector<ArrayView>&
        Views() const {
            return views_;
        }

        ArrayView
        operator[](const int i) const {
            return views_[i];
        }

        ScalarArray
        RawAt(const int i) const {
            return views_[i].output_data();
        }

        void
        Append(const Array& array) {
            indices_.emplace_back(data_size_);
            element_indices_.emplace_back(array.get_offsets());
            ColumnBase::Append(static_cast<const char*>(array.data()),
                               array.byte_size());
        }

        void
        Seal(std::vector<uint64_t>&& indices = {},
             std::vector<std::vector<uint64_t>>&& element_indices = {}) {
            if (!indices.empty()) {
                indices_ = std::move(indices);
                element_indices_ = std::move(element_indices);
            }
            ConstructViews();
        }

     protected:
        void
        ConstructViews() {
            views_.reserve(indices_.size());
            for (size_t i = 0; i < indices_.size() - 1; i++) {
                views_.emplace_back(data_ + indices_[i],
                                    indices_[i + 1] - indices_[i],
                                    element_type_,
                                    std::move(element_indices_[i]));
            }
            views_.emplace_back(
                data_ + indices_.back(),
                data_size_ - indices_.back(),
                element_type_,
                std::move(element_indices_[indices_.size() - 1]));
            element_indices_.clear();
        }

     private:
        std::vector<uint64_t> indices_{};
        std::vector<std::vector<uint64_t>> element_indices_{};
        // Compatible with current Span type
        std::vector<ArrayView> views_{};
        DataType element_type_;
    };
}  // namespace milvus
