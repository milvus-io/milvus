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
#include "storage/DiskFileManagerImpl.h"
#include "storage/FileWriter.h"
#include "common/File.h"

#if defined(__clang__) || defined(__GNUC__)
#define ALWAYS_INLINE inline __attribute__((always_inline))
#elif defined(_MSC_VER)
#define ALWAYS_INLINE __forceinline
#else
#define ALWAYS_INLINE inline
#endif

namespace milvus::index {

template <typename T>
class ScalarIndexSort : public ScalarIndex<T> {
    static_assert(std::is_arithmetic_v<T>,
                  "ScalarIndexSort only supports arithmetic types");

 public:
    explicit ScalarIndexSort(
        const storage::FileManagerContext& file_manager_context =
            storage::FileManagerContext());

    ~ScalarIndexSort() {
        if (is_mmap_ && mmap_data_ != nullptr && mmap_data_ != MAP_FAILED) {
            munmap(mmap_data_, mmap_size_);
            unlink(mmap_filepath_.c_str());
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
        return ScalarIndexType::STLSORT;
    }

    void
    Build(size_t n, const T* values, const bool* valid_data = nullptr) override;

    void
    Build(const Config& config = {}) override;

    const TargetBitmap
    In(size_t n, const T* values) override;

    const TargetBitmap
    NotIn(size_t n, const T* values) override;

    const TargetBitmap
    IsNull() override;

    TargetBitmap
    IsNotNull() override;

    const TargetBitmap
    Range(T value, OpType op) override;

    const TargetBitmap
    Range(T lower_bound_value,
          bool lb_inclusive,
          T upper_bound_value,
          bool ub_inclusive) override;

    std::optional<T>
    Reverse_Lookup(size_t offset) const override;

    int64_t
    Size() override {
        return (int64_t)size_;
    }

    bool
    Empty() const {
        return size_ == 0;
    }

    IndexStatsPtr
    Upload(const Config& config = {}) override;

    const bool
    HasRawData() const override {
        return true;
    }

    void
    BuildWithFieldData(const std::vector<FieldDataPtr>& datas) override;

 private:
    bool
    ShouldSkip(const T lower_value, const T upper_value, const OpType op);

 public:
    const IndexStructure<T>*
    GetData() {
        return data_ptr_;
    }

    bool
    IsBuilt() const {
        return is_built_;
    }

    void
    LoadWithoutAssemble(const BinarySet& binary_set,
                        const Config& config) override;

 public:
    // zero-cost data acess api
    ALWAYS_INLINE const IndexStructure<T>&
    operator[](size_t idx) const {
        assert(idx < size_);
        return data_ptr_[idx];
    }

    ALWAYS_INLINE const IndexStructure<T>*
    begin() const {
        return data_ptr_;
    }

    using const_iterator = const IndexStructure<T>*;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

    ALWAYS_INLINE const_reverse_iterator
    rbegin() const {
        return const_reverse_iterator(end());
    }

    ALWAYS_INLINE const IndexStructure<T>*
    end() const {
        return end_ptr_;
    }

 private:
    void
    setup_data_pointers() const {
        if (is_mmap_) {
            data_ptr_ = reinterpret_cast<IndexStructure<T>*>(mmap_data_);
            size_ = data_size_ / sizeof(IndexStructure<T>);
            end_ptr_ = data_ptr_ + size_;
        } else {
            data_ptr_ = data_.data();
            end_ptr_ = data_ptr_ + data_.size();
            size_ = data_.size();
        }
    }

    int64_t field_id_ = 0;

    bool is_built_ = false;
    Config config_;
    std::vector<int32_t> idx_to_offsets_;  // used to retrieve.
    std::shared_ptr<storage::MemFileManagerImpl> file_manager_;
    std::shared_ptr<storage::DiskFileManagerImpl> disk_file_manager_;
    size_t total_num_rows_{0};
    // generate valid_bitset_ to speed up NotIn and IsNull and IsNotNull operate
    TargetBitmap valid_bitset_;

    // for ram and also used for building index.
    // Note: it should not be used directly for accessing data. Use data_ptr_ instead.
    std::vector<IndexStructure<T>> data_;

    // for mmap
    bool is_mmap_{false};
    int64_t mmap_size_ = 0;
    int64_t data_size_ = 0;
    // Note: it should not be used directly for accessing data. Use data_ptr_ instead.
    char* mmap_data_ = nullptr;
    std::string mmap_filepath_;

    mutable const IndexStructure<T>* data_ptr_ = nullptr;
    mutable const IndexStructure<T>* end_ptr_ = nullptr;
    mutable size_t size_ = 0;

    std::chrono::time_point<std::chrono::system_clock> index_build_begin_;
};

template <typename T>
using ScalarIndexSortPtr = std::unique_ptr<ScalarIndexSort<T>>;

}  // namespace milvus::index

namespace milvus::index {
template <typename T>
inline ScalarIndexSortPtr<T>
CreateScalarIndexSort(const storage::FileManagerContext& file_manager_context =
                          storage::FileManagerContext()) {
    return std::make_unique<ScalarIndexSort<T>>(file_manager_context);
}
}  // namespace milvus::index
