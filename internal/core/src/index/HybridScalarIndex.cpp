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

#include "index/HybridScalarIndex.h"
#include "common/Slice.h"
#include "common/Common.h"
#include "index/Meta.h"
#include "index/ScalarIndex.h"
#include "index/Utils.h"
#include "storage/Util.h"
#include "storage/space.h"

namespace milvus {
namespace index {

template <typename T>
HybridScalarIndex<T>::HybridScalarIndex(
    const storage::FileManagerContext& file_manager_context)
    : is_built_(false),
      bitmap_index_cardinality_limit_(DEFAULT_BITMAP_INDEX_CARDINALITY_BOUND) {
    if (file_manager_context.Valid()) {
        file_manager_ =
            std::make_shared<storage::MemFileManagerImpl>(file_manager_context);
        AssertInfo(file_manager_ != nullptr, "create file manager failed!");
    }
    internal_index_type_ = InternalIndexType::NONE;
}

template <typename T>
HybridScalarIndex<T>::HybridScalarIndex(
    const storage::FileManagerContext& file_manager_context,
    std::shared_ptr<milvus_storage::Space> space)
    : is_built_(false),
      bitmap_index_cardinality_limit_(DEFAULT_BITMAP_INDEX_CARDINALITY_BOUND),
      space_(space) {
    if (file_manager_context.Valid()) {
        file_manager_ = std::make_shared<storage::MemFileManagerImpl>(
            file_manager_context, space);
        AssertInfo(file_manager_ != nullptr, "create file manager failed!");
    }
    internal_index_type_ = InternalIndexType::NONE;
}

template <typename T>
InternalIndexType
HybridScalarIndex<T>::SelectIndexBuildType(size_t n, const T* values) {
    std::set<T> distinct_vals;
    for (size_t i = 0; i < n; i++) {
        distinct_vals.insert(values[i]);
    }

    // Decide whether to select bitmap index or stl sort
    if (distinct_vals.size() >= bitmap_index_cardinality_limit_) {
        internal_index_type_ = InternalIndexType::STLSORT;
    } else {
        internal_index_type_ = InternalIndexType::BITMAP;
    }
    return internal_index_type_;
}

template <>
InternalIndexType
HybridScalarIndex<std::string>::SelectIndexBuildType(
    size_t n, const std::string* values) {
    std::set<std::string> distinct_vals;
    for (size_t i = 0; i < n; i++) {
        distinct_vals.insert(values[i]);
        if (distinct_vals.size() >= bitmap_index_cardinality_limit_) {
            break;
        }
    }

    // Decide whether to select bitmap index or marisa index
    if (distinct_vals.size() >= bitmap_index_cardinality_limit_) {
        internal_index_type_ = InternalIndexType::MARISA;
    } else {
        internal_index_type_ = InternalIndexType::BITMAP;
    }
    return internal_index_type_;
}

template <typename T>
InternalIndexType
HybridScalarIndex<T>::SelectIndexBuildType(
    const std::vector<FieldDataPtr>& field_datas) {
    std::set<T> distinct_vals;
    for (const auto& data : field_datas) {
        auto slice_row_num = data->get_num_rows();
        for (size_t i = 0; i < slice_row_num; ++i) {
            auto val = reinterpret_cast<const T*>(data->RawValue(i));
            distinct_vals.insert(*val);
            if (distinct_vals.size() >= bitmap_index_cardinality_limit_) {
                break;
            }
        }
    }

    // Decide whether to select bitmap index or stl sort
    if (distinct_vals.size() >= bitmap_index_cardinality_limit_) {
        internal_index_type_ = InternalIndexType::STLSORT;
    } else {
        internal_index_type_ = InternalIndexType::BITMAP;
    }
    return internal_index_type_;
}

template <>
InternalIndexType
HybridScalarIndex<std::string>::SelectIndexBuildType(
    const std::vector<FieldDataPtr>& field_datas) {
    std::set<std::string> distinct_vals;
    for (const auto& data : field_datas) {
        auto slice_row_num = data->get_num_rows();
        for (size_t i = 0; i < slice_row_num; ++i) {
            auto val = reinterpret_cast<const std::string*>(data->RawValue(i));
            distinct_vals.insert(*val);
            if (distinct_vals.size() >= bitmap_index_cardinality_limit_) {
                break;
            }
        }
    }

    // Decide whether to select bitmap index or marisa sort
    if (distinct_vals.size() >= bitmap_index_cardinality_limit_) {
        internal_index_type_ = InternalIndexType::MARISA;
    } else {
        internal_index_type_ = InternalIndexType::BITMAP;
    }
    return internal_index_type_;
}

template <typename T>
std::shared_ptr<ScalarIndex<T>>
HybridScalarIndex<T>::GetInternalIndex() {
    if (internal_index_ != nullptr) {
        return internal_index_;
    }
    if (internal_index_type_ == InternalIndexType::BITMAP) {
        internal_index_ = std::make_shared<BitmapIndex<T>>(file_manager_);
    } else if (internal_index_type_ == InternalIndexType::STLSORT) {
        internal_index_ = std::make_shared<ScalarIndexSort<T>>(file_manager_);
    } else {
        PanicInfo(UnexpectedError,
                  "unknown index type when get internal index");
    }
    return internal_index_;
}

template <>
std::shared_ptr<ScalarIndex<std::string>>
HybridScalarIndex<std::string>::GetInternalIndex() {
    if (internal_index_ != nullptr) {
        return internal_index_;
    }

    if (internal_index_type_ == InternalIndexType::BITMAP) {
        internal_index_ =
            std::make_shared<BitmapIndex<std::string>>(file_manager_);
    } else if (internal_index_type_ == InternalIndexType::MARISA) {
        internal_index_ = std::make_shared<StringIndexMarisa>(file_manager_);
    } else {
        PanicInfo(UnexpectedError,
                  "unknown index type when get internal index");
    }
    return internal_index_;
}

template <typename T>
void
HybridScalarIndex<T>::BuildInternal(
    const std::vector<FieldDataPtr>& field_datas) {
    auto index = GetInternalIndex();
    index->BuildWithFieldData(field_datas);
}

template <typename T>
void
HybridScalarIndex<T>::Build(const Config& config) {
    if (is_built_) {
        return;
    }

    bitmap_index_cardinality_limit_ =
        GetBitmapCardinalityLimitFromConfig(config);
    LOG_INFO("config bitmap cardinality limit to {}",
             bitmap_index_cardinality_limit_);

    auto insert_files =
        GetValueFromConfig<std::vector<std::string>>(config, "insert_files");
    AssertInfo(insert_files.has_value(),
               "insert file paths is empty when build index");

    auto field_datas =
        file_manager_->CacheRawDataToMemory(insert_files.value());

    SelectIndexBuildType(field_datas);
    BuildInternal(field_datas);
    is_built_ = true;
}

template <typename T>
void
HybridScalarIndex<T>::BuildV2(const Config& config) {
    if (is_built_) {
        return;
    }
    bitmap_index_cardinality_limit_ =
        GetBitmapCardinalityLimitFromConfig(config);
    LOG_INFO("config bitmap cardinality limit to {}",
             bitmap_index_cardinality_limit_);

    auto field_name = file_manager_->GetIndexMeta().field_name;
    auto reader = space_->ScanData();
    std::vector<FieldDataPtr> field_datas;
    for (auto rec = reader->Next(); rec != nullptr; rec = reader->Next()) {
        if (!rec.ok()) {
            PanicInfo(DataFormatBroken, "failed to read data");
        }
        auto data = rec.ValueUnsafe();
        auto total_num_rows = data->num_rows();
        auto col_data = data->GetColumnByName(field_name);
        auto field_data = storage::CreateFieldData(
            DataType(GetDType<T>()), 0, total_num_rows);
        field_data->FillFieldData(col_data);
        field_datas.push_back(field_data);
    }

    SelectIndexBuildType(field_datas);
    BuildInternal(field_datas);
    is_built_ = true;
}

template <typename T>
BinarySet
HybridScalarIndex<T>::Serialize(const Config& config) {
    AssertInfo(is_built_, "index has not been built yet");

    auto ret_set = internal_index_->Serialize(config);

    // Add index type info to storage for future restruct index
    std::shared_ptr<uint8_t[]> index_type_buf(new uint8_t[sizeof(uint8_t)]);
    index_type_buf[0] = static_cast<uint8_t>(internal_index_type_);
    ret_set.Append(INDEX_TYPE, index_type_buf, sizeof(uint8_t));

    return ret_set;
}

template <typename T>
BinarySet
HybridScalarIndex<T>::Upload(const Config& config) {
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
BinarySet
HybridScalarIndex<T>::UploadV2(const Config& config) {
    auto binary_set = Serialize(config);
    file_manager_->AddFileV2(binary_set);

    auto remote_paths_to_size = file_manager_->GetRemotePathsToFileSize();
    BinarySet ret;
    for (auto& file : remote_paths_to_size) {
        ret.Append(file.first, nullptr, file.second);
    }

    return ret;
}

template <typename T>
void
HybridScalarIndex<T>::DeserializeIndexType(const BinarySet& binary_set) {
    uint8_t index_type;
    auto index_type_buffer = binary_set.GetByName(INDEX_TYPE);
    memcpy(&index_type, index_type_buffer->data.get(), index_type_buffer->size);
    internal_index_type_ = static_cast<InternalIndexType>(index_type);
}

template <typename T>
void
HybridScalarIndex<T>::LoadInternal(const BinarySet& binary_set,
                                   const Config& config) {
    auto index = GetInternalIndex();
    index->LoadWithoutAssemble(binary_set, config);
}

template <typename T>
void
HybridScalarIndex<T>::Load(const BinarySet& binary_set, const Config& config) {
    milvus::Assemble(const_cast<BinarySet&>(binary_set));
    DeserializeIndexType(binary_set);

    LoadInternal(binary_set, config);
    is_built_ = true;
}

template <typename T>
void
HybridScalarIndex<T>::LoadV2(const Config& config) {
    auto blobs = space_->StatisticsBlobs();
    std::vector<std::string> index_files;
    auto prefix = file_manager_->GetRemoteIndexObjectPrefixV2();
    for (auto& b : blobs) {
        if (b.name.rfind(prefix, 0) == 0) {
            index_files.push_back(b.name);
        }
    }
    std::map<std::string, FieldDataPtr> index_datas{};
    for (auto& file_name : index_files) {
        auto res = space_->GetBlobByteSize(file_name);
        if (!res.ok()) {
            PanicInfo(S3Error, "unable to read index blob");
        }
        auto index_blob_data =
            std::shared_ptr<uint8_t[]>(new uint8_t[res.value()]);
        auto status = space_->ReadBlob(file_name, index_blob_data.get());
        if (!status.ok()) {
            PanicInfo(S3Error, "unable to read index blob");
        }
        auto raw_index_blob =
            storage::DeserializeFileData(index_blob_data, res.value());
        auto key = file_name.substr(file_name.find_last_of('/') + 1);
        index_datas[key] = raw_index_blob->GetFieldData();
    }
    AssembleIndexDatas(index_datas);

    BinarySet binary_set;
    for (auto& [key, data] : index_datas) {
        auto size = data->Size();
        auto deleter = [&](uint8_t*) {};  // avoid repeated deconstruction
        auto buf = std::shared_ptr<uint8_t[]>(
            (uint8_t*)const_cast<void*>(data->Data()), deleter);
        binary_set.Append(key, buf, size);
    }

    DeserializeIndexType(binary_set);

    LoadInternal(binary_set, config);

    is_built_ = true;
}

template <typename T>
void
HybridScalarIndex<T>::Load(milvus::tracer::TraceContext ctx,
                           const Config& config) {
    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load bitmap index");
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

    DeserializeIndexType(binary_set);

    LoadInternal(binary_set, config);

    is_built_ = true;
}

template class HybridScalarIndex<bool>;
template class HybridScalarIndex<int8_t>;
template class HybridScalarIndex<int16_t>;
template class HybridScalarIndex<int32_t>;
template class HybridScalarIndex<int64_t>;
template class HybridScalarIndex<float>;
template class HybridScalarIndex<double>;
template class HybridScalarIndex<std::string>;

}  // namespace index
}  // namespace milvus