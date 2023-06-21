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

#include <algorithm>
#include <memory>
#include <utility>
#include <pb/schema.pb.h>
#include <vector>
#include <string>
#include "knowhere/log.h"
#include "Meta.h"
#include "common/Utils.h"
#include "common/Slice.h"
#include "index/Utils.h"

namespace milvus::index {

template <typename T>
inline ScalarIndexSort<T>::ScalarIndexSort(
    storage::FileManagerImplPtr file_manager)
    : is_built_(false), data_() {
    if (file_manager != nullptr) {
        file_manager_ = std::dynamic_pointer_cast<storage::MemFileManagerImpl>(
            file_manager);
    }
}

template <typename T>
inline void
ScalarIndexSort<T>::Build(const Config& config) {
    if (is_built_)
        return;
    auto insert_files =
        GetValueFromConfig<std::vector<std::string>>(config, "insert_files");
    AssertInfo(insert_files.has_value(),
               "insert file paths is empty when build index");
    auto field_datas =
        file_manager_->CacheRawDataToMemory(insert_files.value());

    int64_t total_num_rows = 0;
    for (auto data : field_datas) {
        total_num_rows += data->get_num_rows();
    }
    if (total_num_rows == 0) {
        // todo: throw an exception
        throw std::invalid_argument(
            "ScalarIndexSort cannot build null values!");
    }

    data_.reserve(total_num_rows);
    int64_t offset = 0;
    for (auto data : field_datas) {
        auto slice_num = data->get_num_rows();
        for (size_t i = 0; i < slice_num; ++i) {
            auto value = reinterpret_cast<const T*>(data->RawValue(i));
            data_.emplace_back(IndexStructure(*value, offset));
            offset++;
        }
    }

    std::sort(data_.begin(), data_.end());
    idx_to_offsets_.resize(total_num_rows);
    for (size_t i = 0; i < total_num_rows; ++i) {
        idx_to_offsets_[data_[i].idx_] = i;
    }
    is_built_ = true;
}

template <typename T>
inline void
ScalarIndexSort<T>::Build(size_t n, const T* values) {
    if (is_built_)
        return;
    if (n == 0) {
        // todo: throw an exception
        throw std::invalid_argument(
            "ScalarIndexSort cannot build null values!");
    }
    data_.reserve(n);
    idx_to_offsets_.resize(n);
    T* p = const_cast<T*>(values);
    for (size_t i = 0; i < n; ++i) {
        data_.emplace_back(IndexStructure(*p++, i));
    }
    std::sort(data_.begin(), data_.end());
    for (size_t i = 0; i < data_.size(); ++i) {
        idx_to_offsets_[data_[i].idx_] = i;
    }
    is_built_ = true;
}

template <typename T>
inline BinarySet
ScalarIndexSort<T>::Serialize(const Config& config) {
    AssertInfo(is_built_, "index has not been built");

    auto index_data_size = data_.size() * sizeof(IndexStructure<T>);
    std::shared_ptr<uint8_t[]> index_data(new uint8_t[index_data_size]);
    memcpy(index_data.get(), data_.data(), index_data_size);

    std::shared_ptr<uint8_t[]> index_length(new uint8_t[sizeof(size_t)]);
    auto index_size = data_.size();
    memcpy(index_length.get(), &index_size, sizeof(size_t));

    BinarySet res_set;
    res_set.Append("index_data", index_data, index_data_size);
    res_set.Append("index_length", index_length, sizeof(size_t));

    milvus::Disassemble(res_set);

    return res_set;
}

template <typename T>
inline BinarySet
ScalarIndexSort<T>::Upload(const Config& config) {
    auto binary_set = Serialize(config);
    file_manager_->AddFile(binary_set);

    auto remote_paths_to_size = file_manager_->GetRemotePathsToFileSize();
    BinarySet ret;
    for (auto& file : remote_paths_to_size) {
        ret.Append(file.first, nullptr, file.second);
    }

    return ret;
}

template <typename T>
inline void
ScalarIndexSort<T>::LoadWithoutAssemble(const BinarySet& index_binary,
                                        const Config& config) {
    size_t index_size;
    auto index_length = index_binary.GetByName("index_length");
    memcpy(&index_size, index_length->data.get(), (size_t)index_length->size);

    auto index_data = index_binary.GetByName("index_data");
    data_.resize(index_size);
    idx_to_offsets_.resize(index_size);
    memcpy(data_.data(), index_data->data.get(), (size_t)index_data->size);
    for (size_t i = 0; i < data_.size(); ++i) {
        idx_to_offsets_[data_[i].idx_] = i;
    }
    is_built_ = true;
}

template <typename T>
inline void
ScalarIndexSort<T>::Load(const BinarySet& index_binary, const Config& config) {
    milvus::Assemble(const_cast<BinarySet&>(index_binary));
    LoadWithoutAssemble(index_binary, config);
}

template <typename T>
inline void
ScalarIndexSort<T>::Load(const Config& config) {
    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load disk ann index");
    auto index_datas = file_manager_->LoadIndexToMemory(index_files.value());
    AssembleIndexDatas(index_datas);
    BinarySet binary_set;
    for (auto& [key, data] : index_datas) {
        auto size = data->Size();
        auto deleter = [&](uint8_t*) {};  // avoid repeated deconstruction
        auto buf = std::shared_ptr<uint8_t[]>(
            (uint8_t*)const_cast<void*>(data->Data()), deleter);
        binary_set.Append(key, buf, size);
    }

    LoadWithoutAssemble(binary_set, config);
}

template <typename T>
inline const TargetBitmap
ScalarIndexSort<T>::In(const size_t n, const T* values) {
    AssertInfo(is_built_, "index has not been built");
    TargetBitmap bitset(data_.size());
    for (size_t i = 0; i < n; ++i) {
        auto lb = std::lower_bound(
            data_.begin(), data_.end(), IndexStructure<T>(*(values + i)));
        auto ub = std::upper_bound(
            data_.begin(), data_.end(), IndexStructure<T>(*(values + i)));
        for (; lb < ub; ++lb) {
            if (lb->a_ != *(values + i)) {
                std::cout << "error happens in ScalarIndexSort<T>::In, "
                             "experted value is: "
                          << *(values + i) << ", but real value is: " << lb->a_;
            }
            bitset[lb->idx_] = true;
        }
    }
    return bitset;
}

template <typename T>
inline const TargetBitmap
ScalarIndexSort<T>::NotIn(const size_t n, const T* values) {
    AssertInfo(is_built_, "index has not been built");
    TargetBitmap bitset(data_.size(), true);
    for (size_t i = 0; i < n; ++i) {
        auto lb = std::lower_bound(
            data_.begin(), data_.end(), IndexStructure<T>(*(values + i)));
        auto ub = std::upper_bound(
            data_.begin(), data_.end(), IndexStructure<T>(*(values + i)));
        for (; lb < ub; ++lb) {
            if (lb->a_ != *(values + i)) {
                std::cout << "error happens in ScalarIndexSort<T>::NotIn, "
                             "experted value is: "
                          << *(values + i) << ", but real value is: " << lb->a_;
            }
            bitset[lb->idx_] = false;
        }
    }
    return bitset;
}

template <typename T>
inline const TargetBitmap
ScalarIndexSort<T>::Range(const T value, const OpType op) {
    AssertInfo(is_built_, "index has not been built");
    TargetBitmap bitset(data_.size());
    auto lb = data_.begin();
    auto ub = data_.end();
    switch (op) {
        case OpType::LessThan:
            ub = std::lower_bound(
                data_.begin(), data_.end(), IndexStructure<T>(value));
            break;
        case OpType::LessEqual:
            ub = std::upper_bound(
                data_.begin(), data_.end(), IndexStructure<T>(value));
            break;
        case OpType::GreaterThan:
            lb = std::upper_bound(
                data_.begin(), data_.end(), IndexStructure<T>(value));
            break;
        case OpType::GreaterEqual:
            lb = std::lower_bound(
                data_.begin(), data_.end(), IndexStructure<T>(value));
            break;
        default:
            throw std::invalid_argument(std::string("Invalid OperatorType: ") +
                                        std::to_string((int)op) + "!");
    }
    for (; lb < ub; ++lb) {
        bitset[lb->idx_] = true;
    }
    return bitset;
}

template <typename T>
inline const TargetBitmap
ScalarIndexSort<T>::Range(T lower_bound_value,
                          bool lb_inclusive,
                          T upper_bound_value,
                          bool ub_inclusive) {
    AssertInfo(is_built_, "index has not been built");
    TargetBitmap bitset(data_.size());
    if (lower_bound_value > upper_bound_value ||
        (lower_bound_value == upper_bound_value &&
         !(lb_inclusive && ub_inclusive))) {
        return bitset;
    }
    auto lb = data_.begin();
    auto ub = data_.end();
    if (lb_inclusive) {
        lb = std::lower_bound(
            data_.begin(), data_.end(), IndexStructure<T>(lower_bound_value));
    } else {
        lb = std::upper_bound(
            data_.begin(), data_.end(), IndexStructure<T>(lower_bound_value));
    }
    if (ub_inclusive) {
        ub = std::upper_bound(
            data_.begin(), data_.end(), IndexStructure<T>(upper_bound_value));
    } else {
        ub = std::lower_bound(
            data_.begin(), data_.end(), IndexStructure<T>(upper_bound_value));
    }
    for (; lb < ub; ++lb) {
        bitset[lb->idx_] = true;
    }
    return bitset;
}

template <typename T>
inline T
ScalarIndexSort<T>::Reverse_Lookup(size_t idx) const {
    AssertInfo(idx < idx_to_offsets_.size(), "out of range of total count");
    AssertInfo(is_built_, "index has not been built");

    auto offset = idx_to_offsets_[idx];
    return data_[offset].a_;
}
}  // namespace milvus::index
