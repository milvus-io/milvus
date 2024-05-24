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

#include "index/ScalarIndex.h"
#include "storage/FileManager.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/space.h"

namespace milvus {
namespace index {

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

    explicit BitmapIndex(
        const storage::FileManagerContext& file_manager_context,
        std::shared_ptr<milvus_storage::Space> space);

    ~BitmapIndex() override = default;

    BinarySet
    Serialize(const Config& config) override;

    void
    Load(const BinarySet& index_binary, const Config& config = {}) override;

    void
    Load(milvus::tracer::TraceContext ctx, const Config& config = {}) override;

    void
    LoadV2(const Config& config = {}) override;

    int64_t
    Count() override {
        return bitsets_.begin()->second.size();
    }

    void
    Build(size_t n, const T* values) override;

    void
    Build(const Config& config = {}) override;

    void
    BuildV2(const Config& config = {}) override;

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

    T
    Reverse_Lookup(size_t offset) const override;

    int64_t
    Size() override {
        return Count();
    }

    BinarySet
    Upload(const Config& config = {}) override;
    BinarySet
    UploadV2(const Config& config = {}) override;

    const bool
    HasRawData() const override {
        return true;
    }

    int64_t
    Cardinality() {
        return bitsets_.size();
    }

 private:
    size_t
    GetIndexDataSize();

    void
    SerializeIndexData(uint8_t* index_data_ptr);

    void
    DeserializeIndexData(const uint8_t* data_ptr, size_t index_length);

    bool
    ShouldSkip(const T lower_value, const T upper_value, const OpType op);

    TargetBitmap
    ConvertRoaringToBitset(const roaring::Roaring& values);

    void
    LoadWithoutAssemble(const BinarySet& binary_set, const Config& config);

 private:
    bool is_built_;
    Config config_;
    std::map<T, roaring::Roaring> data_;
    std::map<T, TargetBitmap> bitsets_;
    size_t total_num_rows_;
    std::shared_ptr<storage::MemFileManagerImpl> file_manager_;
    std::shared_ptr<milvus_storage::Space> space_;
};

}  // namespace index
}  // namespace milvus