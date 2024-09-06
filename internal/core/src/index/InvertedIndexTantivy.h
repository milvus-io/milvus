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

#include <cstddef>
#include <vector>
#include "common/RegexQuery.h"
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

template <typename T>
class InvertedIndexTantivy : public ScalarIndex<T> {
 public:
    using MemFileManager = storage::MemFileManagerImpl;
    using MemFileManagerPtr = std::shared_ptr<MemFileManager>;
    using DiskFileManager = storage::DiskFileManagerImpl;
    using DiskFileManagerPtr = std::shared_ptr<DiskFileManager>;

    InvertedIndexTantivy() : ScalarIndex<T>(INVERTED_INDEX_TYPE) {
    }

    explicit InvertedIndexTantivy(const storage::FileManagerContext& ctx);

    ~InvertedIndexTantivy();

    /*
     * deprecated.
     * TODO: why not remove this?
     */
    void
    Load(const BinarySet& binary_set, const Config& config = {}) override {
        PanicInfo(ErrorCode::NotImplemented, "load v1 should be deprecated");
    }

    void
    Load(milvus::tracer::TraceContext ctx, const Config& config = {}) override;

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

    ScalarIndexType
    GetIndexType() const override {
        return ScalarIndexType::INVERTED;
    }

    void
    Build(const Config& config = {}) override;

    int64_t
    Count() override {
        return wrapper_->count();
    }

    // BuildWithRawData should be only used in ut. Only string is supported.
    void
    BuildWithRawData(size_t n,
                     const void* values,
                     const Config& config = {}) override;

    BinarySet
    Serialize(const Config& config) override;

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
    IsNull() override;

    const TargetBitmap
    IsNotNull() override;

    const TargetBitmap
    InApplyFilter(
        size_t n,
        const T* values,
        const std::function<bool(size_t /* offset */)>& filter) override;

    void
    InApplyCallback(
        size_t n,
        const T* values,
        const std::function<void(size_t /* offset */)>& callback) override;

    const TargetBitmap
    NotIn(size_t n, const T* values) override;

    const TargetBitmap
    Range(T value, OpType op) override;

    const TargetBitmap
    Range(T lower_bound_value,
          bool lb_inclusive,
          T upper_bound_value,
          bool ub_inclusive) override;

    const bool
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

    const TargetBitmap
    PatternMatch(const std::string& pattern) override {
        PatternMatchTranslator translator;
        auto regex_pattern = translator(pattern);
        return RegexQuery(regex_pattern);
    }

    bool
    SupportPatternMatch() const override {
        return SupportRegexQuery();
    }

    bool
    SupportRegexQuery() const override {
        return std::is_same_v<T, std::string>;
    }

    const TargetBitmap
    RegexQuery(const std::string& regex_pattern) override;

    void
    BuildWithFieldData(const std::vector<FieldDataPtr>& datas) override;

 private:
    void
    finish();

    void
    build_index_for_array(
        const std::vector<std::shared_ptr<FieldDataBase>>& field_datas);

 private:
    std::shared_ptr<TantivyIndexWrapper> wrapper_;
    TantivyDataType d_type_;
    std::string path_;
    proto::schema::FieldSchema schema_;

    /*
     * To avoid IO amplification, we use both mem file manager & disk file manager
     * 1, build phase, we just need the raw data in memory, we use MemFileManager.CacheRawDataToMemory;
     * 2, upload phase, the index was already on the disk, we use DiskFileManager.AddFile directly;
     * 3, load phase, we need the index on the disk instead of memory, we use DiskFileManager.CacheIndexToDisk;
     * Btw, this approach can be applied to DiskANN also.
     */
    MemFileManagerPtr mem_file_manager_;
    DiskFileManagerPtr disk_file_manager_;

    // all data need to be built to align the offset
    // so need to store null_offset in inverted index additionally
    std::vector<size_t> null_offset{};
};
}  // namespace milvus::index
