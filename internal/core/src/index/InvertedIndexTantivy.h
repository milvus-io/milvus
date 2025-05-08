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
#include <folly/SharedMutex.h>
#include "common/RegexQuery.h"
#include "index/Index.h"
#include "storage/FileManager.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/MemFileManagerImpl.h"
#include "tantivy-binding.h"
#include "tantivy-wrapper.h"
#include "index/StringIndex.h"

namespace milvus::index {

inline TantivyDataType
get_tantivy_data_type(proto::schema::DataType data_type) {
    switch (data_type) {
        case proto::schema::DataType::Bool: {
            return TantivyDataType::Bool;
        }

        case proto::schema::DataType::Int8:
        case proto::schema::DataType::Int16:
        case proto::schema::DataType::Int32:
        case proto::schema::DataType::Int64: {
            return TantivyDataType::I64;
        }

        case proto::schema::DataType::Float:
        case proto::schema::DataType::Double: {
            return TantivyDataType::F64;
        }

        case proto::schema::DataType::String:
        case proto::schema::DataType::VarChar: {
            return TantivyDataType::Keyword;
        }

        default:
            PanicInfo(ErrorCode::NotImplemented,
                      fmt::format("not implemented data type: {}", data_type));
    }
}

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

    // Default, we build tantivy index with version 7 (newest version now).
    explicit InvertedIndexTantivy(uint32_t tantivy_index_version,
                                  const storage::FileManagerContext& ctx,
                                  bool inverted_index_single_segment = false);

    ~InvertedIndexTantivy();

    void
    InitForBuildIndex();
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

    // BuildWithRawDataForUT should be only used in ut. Only string is supported.
    void
    BuildWithRawDataForUT(size_t n,
                          const void* values,
                          const Config& config = {}) override;

    BinarySet
    Serialize(const Config& config) override;

    IndexStatsPtr
    Upload(const Config& config = {}) override;

    /*
     * deprecated, only used in small chunk index.
     */
    void
    Build(size_t n, const T* values, const bool* valid_data) override {
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

    std::optional<T>
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
    PatternMatch(const std::string& pattern, proto::plan::OpType op) override {
        switch (op) {
            case proto::plan::OpType::PrefixMatch: {
                return PrefixMatch(pattern);
            }
            case proto::plan::OpType::PostfixMatch: {
                PatternMatchTranslator translator;
                auto regex_pattern = translator(fmt::format("%{}", pattern));
                return RegexQuery(regex_pattern);
            }
            case proto::plan::OpType::InnerMatch: {
                PatternMatchTranslator translator;
                auto regex_pattern = translator(fmt::format("%{}%", pattern));
                return RegexQuery(regex_pattern);
            }
            case proto::plan::OpType::Match: {
                PatternMatchTranslator translator;
                auto regex_pattern = translator(pattern);
                return RegexQuery(regex_pattern);
            }
            default:
                PanicInfo(
                    ErrorCode::OpTypeInvalid,
                    "not supported op type: {} for inverted index PatternMatch",
                    op);
        }
    }

    bool
    SupportPatternMatch() const override {
        return SupportRegexQuery();
    }

    bool
    SupportRegexQuery() const override {
        return std::is_same_v<T, std::string>;
    }

    bool
    TryUseRegexQuery() const override {
        // for inverted index, not use regex query to implement match
        return false;
    }

    const TargetBitmap
    RegexQuery(const std::string& regex_pattern) override;

    void
    BuildWithFieldData(const std::vector<FieldDataPtr>& datas) override;

 protected:
    void
    finish();

    void
    build_index_for_array(
        const std::vector<std::shared_ptr<FieldDataBase>>& field_datas);

    virtual void
    build_index_for_json(
        const std::vector<std::shared_ptr<FieldDataBase>>& field_datas) {
        PanicInfo(ErrorCode::NotImplemented,
                  "build_index_for_json not implemented");
    };

 protected:
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

    folly::SharedMutexWritePriority mutex_{};
    // all data need to be built to align the offset
    // so need to store null_offset_ in inverted index additionally
    std::vector<size_t> null_offset_{};

    // `inverted_index_single_segment_` is used to control whether to build tantivy index with single segment.
    //
    // In the older version of milvus, the query node can only read tantivy index built whtin single segment
    // where the newer version builds and reads index of multi segments by default.
    // However, the index may be built from a separate node from the query node where the index buliding node is a
    // new version while the query node is a older version. So we have this `inverted_index_single_segment_` to control the index
    // building node to build specific type of tantivy index.
    bool inverted_index_single_segment_{false};

    // `tantivy_index_version_` is used to control which kind of tantivy index should be used.
    // There could be the case where milvus version of read node is lower than the version of index builder node(and read node
    // may not be upgraded to a higher version in a predictable time), so we are using a lower version of tantivy to read index
    // built from a higher version of tantivy which is not supported.
    // Therefore, we should provide a way to allow higher version of milvus to build tantivy index with low version.
    uint32_t tantivy_index_version_{0};
};
}  // namespace milvus::index
