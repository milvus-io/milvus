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

#include <map>
#include <memory>
#include <string>

#include "index/ScalarIndex.h"
#include "index/BitmapIndex.h"
#include "index/ScalarIndexSort.h"
#include "index/StringIndexMarisa.h"
#include "index/InvertedIndexTantivy.h"
#include "storage/FileManager.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/MemFileManagerImpl.h"

namespace milvus {
namespace index {

/*
* @brief Implementation of hybrid index  
* @details This index only for scalar type.
* dynamically choose bitmap/stlsort/marisa type index
* according to data distribution
*/
template <typename T>
class HybridScalarIndex : public ScalarIndex<T> {
 public:
    explicit HybridScalarIndex(
        uint32_t index_engine_version,
        const storage::FileManagerContext& file_manager_context =
            storage::FileManagerContext());

    ~HybridScalarIndex() override = default;

    BinarySet
    Serialize(const Config& config) override;

    void
    Load(const BinarySet& index_binary, const Config& config = {}) override;

    void
    Load(milvus::tracer::TraceContext ctx, const Config& config = {}) override;

    int64_t
    Count() override {
        return internal_index_->Count();
    }

    ScalarIndexType
    GetIndexType() const override {
        return ScalarIndexType::HYBRID;
    }

    void
    Build(size_t n,
          const T* values,
          const bool* valid_data = nullptr) override {
        SelectIndexBuildType(n, values);
        auto index = GetInternalIndex();
        index->Build(n, values, valid_data);
        is_built_ = true;
    }

    void
    Build(const Config& config = {}) override;

    const TargetBitmap
    In(size_t n, const T* values) override {
        return internal_index_->In(n, values);
    }

    const TargetBitmap
    NotIn(size_t n, const T* values) override {
        return internal_index_->NotIn(n, values);
    }

    const TargetBitmap
    IsNull() override {
        return internal_index_->IsNull();
    }

    const TargetBitmap
    IsNotNull() override {
        return internal_index_->IsNotNull();
    }

    const TargetBitmap
    Query(const DatasetPtr& dataset) override {
        return internal_index_->Query(dataset);
    }

    const TargetBitmap
    PatternMatch(const std::string& pattern) override {
        PatternMatchTranslator translator;
        auto regex_pattern = translator(pattern);
        return RegexQuery(regex_pattern);
    }

    bool
    SupportRegexQuery() const override {
        return internal_index_->SupportRegexQuery();
    }

    const TargetBitmap
    RegexQuery(const std::string& pattern) override {
        return internal_index_->RegexQuery(pattern);
    }

    const TargetBitmap
    Range(T value, OpType op) override {
        return internal_index_->Range(value, op);
    }

    const TargetBitmap
    Range(T lower_bound_value,
          bool lb_inclusive,
          T upper_bound_value,
          bool ub_inclusive) override {
        return internal_index_->Range(
            lower_bound_value, lb_inclusive, upper_bound_value, ub_inclusive);
    }

    std::optional<T>
    Reverse_Lookup(size_t offset) const override {
        return internal_index_->Reverse_Lookup(offset);
    }

    int64_t
    Size() override {
        return internal_index_->Size();
    }

    const bool
    HasRawData() const override {
        if (field_type_ == proto::schema::DataType::Array) {
            return false;
        }
        return internal_index_->HasRawData();
    }

    IndexStatsPtr
    Upload(const Config& config = {}) override;

 private:
    ScalarIndexType
    SelectBuildTypeForPrimitiveType(
        const std::vector<FieldDataPtr>& field_datas);

    ScalarIndexType
    SelectBuildTypeForArrayType(const std::vector<FieldDataPtr>& field_datas);

    ScalarIndexType
    SelectIndexBuildType(const std::vector<FieldDataPtr>& field_datas);

    ScalarIndexType
    SelectIndexBuildType(size_t n, const T* values);

    BinarySet
    SerializeIndexType();

    void
    DeserializeIndexType(const BinarySet& binary_set);

    void
    BuildInternal(const std::vector<FieldDataPtr>& field_datas);

    std::shared_ptr<ScalarIndex<T>>
    GetInternalIndex();

    std::string
    GetRemoteIndexTypeFile(const std::vector<std::string>& files);

 public:
    bool is_built_{false};
    int32_t bitmap_index_cardinality_limit_;
    proto::schema::DataType field_type_;
    ScalarIndexType internal_index_type_;
    std::shared_ptr<ScalarIndex<T>> internal_index_{nullptr};
    storage::FileManagerContext file_manager_context_;
    std::shared_ptr<storage::MemFileManagerImpl> mem_file_manager_{nullptr};

    // `tantivy_index_version_` is used to control which kind of tantivy index should be used.
    // There could be the case where milvus version of read node is lower than the version of index builder node(and read node
    // may not be upgraded to a higher version in a predictable time), so we are using a lower version of tantivy to read index
    // built from a higher version of tantivy which is not supported.
    // Therefore, we should provide a way to allow higher version of milvus to build tantivy index with low version.
    uint32_t index_engine_version_{0};
};

}  // namespace index
}  // namespace milvus