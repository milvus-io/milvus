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

#include "index/Index.h"
#include "storage/FileManager.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/MemFileManagerImpl.h"
#include "tantivy-binding.h"
#include "tantivy-wrapper.h"
#include "index/StringIndex.h"

namespace milvus::index {

using TantivyIndexWrapper = milvus::tantivy::TantivyIndexWrapper;
using RustArrayWrapper = milvus::tantivy::RustArrayWrapper;

struct TantivyConfig {
    DataType data_type_;

    TantivyDataType
    to_tantivy_data_type() {
        switch (data_type_) {
            case DataType::BOOL: {
                return TantivyDataType::Bool;
            }

            case DataType::INT8:
            case DataType::INT16:
            case DataType::INT32:
            case DataType::INT64: {
                return TantivyDataType::I64;
            }

            case DataType::FLOAT:
            case DataType::DOUBLE: {
                return TantivyDataType::F64;
            }

            case DataType::VARCHAR: {
                return TantivyDataType::Keyword;
            }

            default:
                PanicInfo(
                    ErrorCode::NotImplemented,
                    fmt::format("not implemented data type: {}", data_type_));
        }
    }
};

template <typename T>
class InvertedIndexTantivy : public ScalarIndex<T> {
 public:
    using MemFileManager = storage::MemFileManagerImpl;
    using MemFileManagerPtr = std::shared_ptr<MemFileManager>;
    using DiskFileManager = storage::DiskFileManagerImpl;
    using DiskFileManagerPtr = std::shared_ptr<DiskFileManager>;

    explicit InvertedIndexTantivy(const TantivyConfig& cfg,
                                  const storage::FileManagerContext& ctx);

    /*
     * deprecated.
     * TODO: why not remove this?
     */
    void
    Load(const BinarySet& binary_set, const Config& config = {}) override {
        PanicInfo(ErrorCode::NotImplemented, "load v1 should be deprecated");
    }

    void
    Load(const Config& config = {}) override;

    /*
     * deprecated.
     * TODO: why not remove this?
     */
    void
    BuildWithDataset(const DatasetPtr& dataset,
                     const Config& config = {}) override {
        PanicInfo(ErrorCode::NotImplemented,
                  "BuildWithDataset should be deprecated");
    }

    void
    Build(const Config& config = {}) override;

    int64_t
    Count() override {
        return wrapper_->count();
    }

    /*
     * deprecated.
     * TODO: why not remove this?
     */
    void
    BuildWithRawData(size_t n,
                     const void* values,
                     const Config& config = {}) override {
        PanicInfo(ErrorCode::NotImplemented,
                  "BuildWithRawData should be deprecated");
    }

    /*
     * deprecated.
     * TODO: why not remove this?
     */
    BinarySet
    Serialize(const Config& config /* not used */) override;

    BinarySet
    Upload(const Config& config = {}) override;

    /*
     * deprecated, only used in small chunk index.
     */
    void
    Build(size_t n, const T* values) override {
        PanicInfo(ErrorCode::NotImplemented, "Build should not be called");
    }

    const TargetBitmap
    In(size_t n, const T* values) override;

    const TargetBitmap
    NotIn(size_t n, const T* values) override;

    const TargetBitmap
    Range(T value, OpType op) override;

    const TargetBitmap
    Range(T lower_bound_value,
          bool lb_inclusive,
          T upper_bound_value,
          bool ub_inclusive) override;

    bool
    HasRawData() const override {
        return false;
    }

    T
    Reverse_Lookup(size_t offset) const override {
        PanicInfo(ErrorCode::NotImplemented,
                  "Reverse_Lookup should not be handled by inverted index");
    }

    int64_t
    Size() override {
        return Count();
    }

    const TargetBitmap
    PrefixMatch(const std::string_view prefix);

    const TargetBitmap
    Query(const DatasetPtr& dataset) override;

    ~InvertedIndexTantivy() override;

 private:
    void
    finish();

 private:
    std::shared_ptr<TantivyIndexWrapper> wrapper_;
    TantivyConfig cfg_;
    TantivyDataType d_type_;
    std::string path_;

    /*
     * To avoid IO amplification, we use both mem file manager & disk file manager
     * 1, build phase, we just need the raw data in memory, we use MemFileManager.CacheRawDataToMemory;
     * 2, upload phase, the index was already on the disk, we use DiskFileManager.AddFile directly;
     * 3, load phase, we need the index on the disk instead of memory, we use DiskFileManager.CacheIndexToDisk;
     * Btw, this approach can be applied to DiskANN also.
     */
    MemFileManagerPtr mem_file_manager_;
    DiskFileManagerPtr disk_file_manager_;
};

template class InvertedIndexTantivy<bool>;
template class InvertedIndexTantivy<int8_t>;
template class InvertedIndexTantivy<int16_t>;
template class InvertedIndexTantivy<int32_t>;
template class InvertedIndexTantivy<int64_t>;
template class InvertedIndexTantivy<float>;
template class InvertedIndexTantivy<double>;
template class InvertedIndexTantivy<std::string>;
}  // namespace milvus::index
