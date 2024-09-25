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
#include <roaring/roaring.hh>

#include "common/RegexQuery.h"
#include "index/ScalarIndex.h"
#include "storage/FileManager.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/MemFileManagerImpl.h"

namespace milvus {
namespace index {

struct BitmapInfo {
    size_t offset_;
    size_t size_;
};

enum class BitmapIndexBuildMode {
    ROARING,
    BITSET,
};

/*
* @brief Implementation of Bitmap Index 
* @details This index only for scalar Integral type.
*/
template <typename T>
class BitmapIndex : public ScalarIndex<T> {
 public:
    explicit BitmapIndex(
        const storage::FileManagerContext& file_manager_context =
            storage::FileManagerContext());

    ~BitmapIndex() {
        if (is_mmap_) {
            UnmapIndexData();
        }
    }

    BinarySet
    Serialize(const Config& config) override;

    void
    Load(const BinarySet& index_binary, const Config& config = {}) override;

    void
    Load(milvus::tracer::TraceContext ctx, const Config& config = {}) override;

    int64_t
    Count() override {
        return total_num_rows_;
    }

    ScalarIndexType
    GetIndexType() const override {
        return ScalarIndexType::BITMAP;
    }

    void
    Build(size_t n, const T* values) override;

    void
    Build(const Config& config = {}) override;

    void
    BuildWithFieldData(const std::vector<FieldDataPtr>& datas) override;

    const TargetBitmap
    In(size_t n, const T* values) override;

    const TargetBitmap
    NotIn(size_t n, const T* values) override;

    const TargetBitmap
    IsNull() override;

    const TargetBitmap
    IsNotNull() override;

    const TargetBitmap
    Range(T value, OpType op) override;

    const TargetBitmap
    Range(T lower_bound_value,
          bool lb_inclusive,
          T upper_bound_value,
          bool ub_inclusive) override;

    T
    Reverse_Lookup(size_t offset) const override;

    int64_t
    Size() override {
        return Count();
    }

    BinarySet
    Upload(const Config& config = {}) override;

    const bool
    HasRawData() const override {
        if (schema_.data_type() == proto::schema::DataType::Array) {
            return false;
        }
        return true;
    }

    void
    LoadWithoutAssemble(const BinarySet& binary_set,
                        const Config& config) override;

    const TargetBitmap
    Query(const DatasetPtr& dataset) override;

    bool
    SupportPatternMatch() const override {
        return SupportRegexQuery();
    }

    const TargetBitmap
    PatternMatch(const std::string& pattern) override {
        PatternMatchTranslator translator;
        auto regex_pattern = translator(pattern);
        return RegexQuery(regex_pattern);
    }

    bool
    SupportRegexQuery() const override {
        return std::is_same_v<T, std::string>;
    }

    const TargetBitmap
    RegexQuery(const std::string& regex_pattern) override;

 public:
    int64_t
    Cardinality() {
        if (is_mmap_) {
            return bitmap_info_map_.size();
        }

        if (build_mode_ == BitmapIndexBuildMode::ROARING) {
            return data_.size();
        } else {
            return bitsets_.size();
        }
    }

 private:
    void
    BuildPrimitiveField(const std::vector<FieldDataPtr>& datas);

    void
    BuildArrayField(const std::vector<FieldDataPtr>& datas);

    size_t
    GetIndexDataSize();

    void
    SerializeIndexData(uint8_t* index_data_ptr);

    std::pair<std::shared_ptr<uint8_t[]>, size_t>
    SerializeIndexMeta();

    std::pair<size_t, size_t>
    DeserializeIndexMeta(const uint8_t* data_ptr, size_t data_size);

    void
    DeserializeIndexDataForMmap(const char* data_ptr, size_t index_length);

    void
    DeserializeIndexData(const uint8_t* data_ptr, size_t index_length);

    void
    BuildOffsetCache();

    T
    Reverse_Lookup_InCache(size_t idx) const;

    void
    ChooseIndexLoadMode(int64_t index_length);

    bool
    ShouldSkip(const T lower_value, const T upper_value, const OpType op);

    TargetBitmap
    ConvertRoaringToBitset(const roaring::Roaring& values);

    TargetBitmap
    RangeForRoaring(T value, OpType op);

    TargetBitmap
    RangeForBitset(T value, OpType op);

    TargetBitmap
    RangeForMmap(T value, OpType op);

    TargetBitmap
    RangeForRoaring(T lower_bound_value,
                    bool lb_inclusive,
                    T upper_bound_value,
                    bool ub_inclusive);

    TargetBitmap
    RangeForBitset(T lower_bound_value,
                   bool lb_inclusive,
                   T upper_bound_value,
                   bool ub_inclusive);

    TargetBitmap
    RangeForMmap(T lower_bound_value,
                 bool lb_inclusive,
                 T upper_bound_value,
                 bool ub_inclusive);

    void
    MMapIndexData(const std::string& filepath,
                  const uint8_t* data,
                  size_t data_size,
                  size_t index_length);

    roaring::Roaring
    AccessBitmap(const BitmapInfo& info) const {
        return roaring::Roaring::read(mmap_data_ + info.offset_, info.size_);
    }

    void
    UnmapIndexData();

 public:
    bool is_built_{false};
    BitmapIndexBuildMode build_mode_;
    std::map<T, roaring::Roaring> data_;
    std::map<T, TargetBitmap> bitsets_;
    bool is_mmap_{false};
    char* mmap_data_;
    int64_t mmap_size_;
    std::map<T, BitmapInfo> bitmap_info_map_;
    size_t total_num_rows_{0};
    proto::schema::FieldSchema schema_;
    bool use_offset_cache_{false};
    std::vector<typename std::map<T, roaring::Roaring>::iterator>
        data_offsets_cache_;
    std::vector<typename std::map<T, TargetBitmap>::iterator>
        bitsets_offsets_cache_;
    std::vector<typename std::map<T, BitmapInfo>::iterator> mmap_offsets_cache_;
    std::shared_ptr<storage::MemFileManagerImpl> file_manager_;

    // generate valid_bitset to speed up NotIn and IsNull and IsNotNull operate
    TargetBitmap valid_bitset;
};

}  // namespace index
}  // namespace milvus