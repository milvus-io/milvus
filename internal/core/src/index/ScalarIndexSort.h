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

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>
#include <string>
#include <map>

#include "index/IndexStructure.h"
#include "index/ScalarIndex.h"
#include "storage/MemFileManagerImpl.h"

namespace milvus::index {

template <typename T>
class ScalarIndexSort : public ScalarIndex<T> {
 public:
    explicit ScalarIndexSort(
        storage::FileManagerImplPtr file_manager = nullptr);

    BinarySet
    Serialize(const Config& config) override;

    void
    Load(const BinarySet& index_binary, const Config& config = {}) override;

    void
    Load(const Config& config = {}) override;

    int64_t
    Count() override {
        return data_.size();
    }

    void
    Build(size_t n, const T* values) override;

    void
    Build(const Config& config = {}) override;

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
        return (int64_t)data_.size();
    }

    BinarySet
    Upload(const Config& config = {}) override;

 public:
    const std::vector<IndexStructure<T>>&
    GetData() {
        return data_;
    }

    bool
    IsBuilt() const {
        return is_built_;
    }

    void
    LoadWithoutAssemble(const BinarySet& binary_set, const Config& config);

 private:
    bool is_built_;
    Config config_;
    std::vector<int32_t> idx_to_offsets_;  // used to retrieve.
    std::vector<IndexStructure<T>> data_;
    std::shared_ptr<storage::MemFileManagerImpl> file_manager_;
};

template <typename T>
using ScalarIndexSortPtr = std::unique_ptr<ScalarIndexSort<T>>;

}  // namespace milvus::index

#include "index/ScalarIndexSort-inl.h"

namespace milvus::index {
template <typename T>
inline ScalarIndexSortPtr<T>
CreateScalarIndexSort(storage::FileManagerImplPtr file_manager = nullptr) {
    return std::make_unique<ScalarIndexSort<T>>(file_manager);
}
}  // namespace milvus::index
