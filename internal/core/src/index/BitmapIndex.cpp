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
#include <yaml-cpp/yaml.h>

#include "index/BitmapIndex.h"

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
BitmapIndex<T>::BitmapIndex(
    const storage::FileManagerContext& file_manager_context)
    : is_built_(false),
      schema_(file_manager_context.fieldDataMeta.field_schema) {
    if (file_manager_context.Valid()) {
        file_manager_ =
            std::make_shared<storage::MemFileManagerImpl>(file_manager_context);
        AssertInfo(file_manager_ != nullptr, "create file manager failed!");
    }
}

template <typename T>
BitmapIndex<T>::BitmapIndex(
    const storage::FileManagerContext& file_manager_context,
    std::shared_ptr<milvus_storage::Space> space)
    : is_built_(false),
      schema_(file_manager_context.fieldDataMeta.field_schema),
      space_(space) {
    if (file_manager_context.Valid()) {
        file_manager_ = std::make_shared<storage::MemFileManagerImpl>(
            file_manager_context, space);
        AssertInfo(file_manager_ != nullptr, "create file manager failed!");
    }
}

template <typename T>
void
BitmapIndex<T>::Build(const Config& config) {
    if (is_built_) {
        return;
    }
    auto insert_files =
        GetValueFromConfig<std::vector<std::string>>(config, "insert_files");
    AssertInfo(insert_files.has_value(),
               "insert file paths is empty when build index");

    auto field_datas =
        file_manager_->CacheRawDataToMemory(insert_files.value());

    BuildWithFieldData(field_datas);
}

template <typename T>
void
BitmapIndex<T>::Build(size_t n, const T* data) {
    if (is_built_) {
        return;
    }
    if (n == 0) {
        throw SegcoreError(DataIsEmpty,
                           "BitmapIndex can not build null values");
    }

    T* p = const_cast<T*>(data);
    for (int i = 0; i < n; ++i, ++p) {
        data_[*p].add(i);
    }
    total_num_rows_ = n;

    if (data_.size() < DEFAULT_BITMAP_INDEX_CARDINALITY_BOUND) {
        for (auto it = data_.begin(); it != data_.end(); ++it) {
            bitsets_[it->first] = ConvertRoaringToBitset(it->second);
        }
        build_mode_ = BitmapIndexBuildMode::BITSET;
    } else {
        build_mode_ = BitmapIndexBuildMode::ROARING;
    }

    is_built_ = true;
}

template <typename T>
void
BitmapIndex<T>::BuildV2(const Config& config) {
    if (is_built_) {
        return;
    }
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

    BuildWithFieldData(field_datas);
}

template <typename T>
void
BitmapIndex<T>::BuildPrimitiveField(
    const std::vector<FieldDataPtr>& field_datas) {
    int64_t offset = 0;
    for (const auto& data : field_datas) {
        auto slice_row_num = data->get_num_rows();
        for (size_t i = 0; i < slice_row_num; ++i) {
            auto val = reinterpret_cast<const T*>(data->RawValue(i));
            data_[*val].add(offset);
            offset++;
        }
    }
}

template <typename T>
void
BitmapIndex<T>::BuildWithFieldData(
    const std::vector<FieldDataPtr>& field_datas) {
    int total_num_rows = 0;
    for (auto& field_data : field_datas) {
        total_num_rows += field_data->get_num_rows();
    }
    if (total_num_rows == 0) {
        throw SegcoreError(DataIsEmpty,
                           "scalar bitmap index can not build null values");
    }
    total_num_rows_ = total_num_rows;

    switch (schema_.data_type()) {
        case proto::schema::DataType::Bool:
        case proto::schema::DataType::Int8:
        case proto::schema::DataType::Int16:
        case proto::schema::DataType::Int32:
        case proto::schema::DataType::Int64:
        case proto::schema::DataType::Float:
        case proto::schema::DataType::Double:
        case proto::schema::DataType::String:
        case proto::schema::DataType::VarChar:
            BuildPrimitiveField(field_datas);
            break;
        case proto::schema::DataType::Array:
            BuildArrayField(field_datas);
            break;
        default:
            PanicInfo(
                DataTypeInvalid,
                fmt::format("Invalid data type: {} for build bitmap index",
                            proto::schema::DataType_Name(schema_.data_type())));
    }
    is_built_ = true;
}

template <typename T>
void
BitmapIndex<T>::BuildArrayField(const std::vector<FieldDataPtr>& field_datas) {
    int64_t offset = 0;
    for (const auto& data : field_datas) {
        auto slice_row_num = data->get_num_rows();
        for (size_t i = 0; i < slice_row_num; ++i) {
            auto array =
                reinterpret_cast<const milvus::Array*>(data->RawValue(i));
            for (size_t j = 0; j < array->length(); ++j) {
                auto val = array->template get_data<T>(j);
                data_[val].add(offset);
            }
            offset++;
        }
    }
}

template <typename T>
size_t
BitmapIndex<T>::GetIndexDataSize() {
    auto index_data_size = 0;
    for (auto& pair : data_) {
        index_data_size += pair.second.getSizeInBytes() + sizeof(T);
    }
    return index_data_size;
}

template <>
size_t
BitmapIndex<std::string>::GetIndexDataSize() {
    auto index_data_size = 0;
    for (auto& pair : data_) {
        index_data_size +=
            pair.second.getSizeInBytes() + pair.first.size() + sizeof(size_t);
    }
    return index_data_size;
}

template <typename T>
void
BitmapIndex<T>::SerializeIndexData(uint8_t* data_ptr) {
    for (auto& pair : data_) {
        memcpy(data_ptr, &pair.first, sizeof(T));
        data_ptr += sizeof(T);

        pair.second.write(reinterpret_cast<char*>(data_ptr));
        data_ptr += pair.second.getSizeInBytes();
    }
}

template <typename T>
std::pair<std::shared_ptr<uint8_t[]>, size_t>
BitmapIndex<T>::SerializeIndexMeta() {
    YAML::Node node;
    node[BITMAP_INDEX_LENGTH] = data_.size();
    node[BITMAP_INDEX_NUM_ROWS] = total_num_rows_;

    std::stringstream ss;
    ss << node;
    auto json_string = ss.str();
    auto str_size = json_string.size();
    std::shared_ptr<uint8_t[]> res(new uint8_t[str_size]);
    memcpy(res.get(), json_string.data(), str_size);
    return std::make_pair(res, str_size);
}

template <>
void
BitmapIndex<std::string>::SerializeIndexData(uint8_t* data_ptr) {
    for (auto& pair : data_) {
        size_t key_size = pair.first.size();
        memcpy(data_ptr, &key_size, sizeof(size_t));
        data_ptr += sizeof(size_t);

        memcpy(data_ptr, pair.first.data(), key_size);
        data_ptr += key_size;

        pair.second.write(reinterpret_cast<char*>(data_ptr));
        data_ptr += pair.second.getSizeInBytes();
    }
}

template <typename T>
BinarySet
BitmapIndex<T>::Serialize(const Config& config) {
    AssertInfo(is_built_, "index has not been built yet");

    auto index_data_size = GetIndexDataSize();

    std::shared_ptr<uint8_t[]> index_data(new uint8_t[index_data_size]);
    uint8_t* data_ptr = index_data.get();
    SerializeIndexData(data_ptr);

    auto index_meta = SerializeIndexMeta();

    BinarySet ret_set;
    ret_set.Append(BITMAP_INDEX_DATA, index_data, index_data_size);
    ret_set.Append(BITMAP_INDEX_META, index_meta.first, index_meta.second);

    LOG_INFO("build bitmap index with cardinality = {}, num_rows = {}",
             Cardinality(),
             total_num_rows_);

    Disassemble(ret_set);
    return ret_set;
}

template <typename T>
BinarySet
BitmapIndex<T>::Upload(const Config& config) {
    auto binary_set = Serialize(config);

    file_manager_->AddFile(binary_set);

    auto remote_path_to_size = file_manager_->GetRemotePathsToFileSize();
    BinarySet ret;
    for (auto& file : remote_path_to_size) {
        ret.Append(file.first, nullptr, file.second);
    }
    return ret;
}

template <typename T>
BinarySet
BitmapIndex<T>::UploadV2(const Config& config) {
    auto binary_set = Serialize(config);

    file_manager_->AddFileV2(binary_set);

    auto remote_path_to_size = file_manager_->GetRemotePathsToFileSize();
    BinarySet ret;
    for (auto& file : remote_path_to_size) {
        ret.Append(file.first, nullptr, file.second);
    }
    return ret;
}

template <typename T>
void
BitmapIndex<T>::Load(const BinarySet& binary_set, const Config& config) {
    milvus::Assemble(const_cast<BinarySet&>(binary_set));
    LoadWithoutAssemble(binary_set, config);
}

template <typename T>
TargetBitmap
BitmapIndex<T>::ConvertRoaringToBitset(const roaring::Roaring& values) {
    AssertInfo(total_num_rows_ != 0, "total num rows should not be 0");
    TargetBitmap res(total_num_rows_, false);
    for (const auto& val : values) {
        res.set(val);
    }
    return res;
}

template <typename T>
std::pair<size_t, size_t>
BitmapIndex<T>::DeserializeIndexMeta(const uint8_t* data_ptr,
                                     size_t data_size) {
    YAML::Node node = YAML::Load(
        std::string(reinterpret_cast<const char*>(data_ptr), data_size));

    auto index_length = node[BITMAP_INDEX_LENGTH].as<size_t>();
    auto index_num_rows = node[BITMAP_INDEX_NUM_ROWS].as<size_t>();

    return std::make_pair(index_length, index_num_rows);
}

template <typename T>
void
BitmapIndex<T>::ChooseIndexBuildMode() {
    if (data_.size() <= DEFAULT_BITMAP_INDEX_CARDINALITY_BOUND) {
        build_mode_ = BitmapIndexBuildMode::BITSET;
    } else {
        build_mode_ = BitmapIndexBuildMode::ROARING;
    }
}

template <typename T>
void
BitmapIndex<T>::DeserializeIndexData(const uint8_t* data_ptr,
                                     size_t index_length) {
    for (size_t i = 0; i < index_length; ++i) {
        T key;
        memcpy(&key, data_ptr, sizeof(T));
        data_ptr += sizeof(T);

        roaring::Roaring value;
        value = roaring::Roaring::read(reinterpret_cast<const char*>(data_ptr));
        data_ptr += value.getSizeInBytes();

        ChooseIndexBuildMode();

        if (build_mode_ == BitmapIndexBuildMode::BITSET) {
            bitsets_[key] = ConvertRoaringToBitset(value);
            data_.erase(key);
        }
    }
}

template <>
void
BitmapIndex<std::string>::DeserializeIndexData(const uint8_t* data_ptr,
                                               size_t index_length) {
    for (size_t i = 0; i < index_length; ++i) {
        size_t key_size;
        memcpy(&key_size, data_ptr, sizeof(size_t));
        data_ptr += sizeof(size_t);

        std::string key(reinterpret_cast<const char*>(data_ptr), key_size);
        data_ptr += key_size;

        roaring::Roaring value;
        value = roaring::Roaring::read(reinterpret_cast<const char*>(data_ptr));
        data_ptr += value.getSizeInBytes();

        bitsets_[key] = ConvertRoaringToBitset(value);
    }
}

template <typename T>
void
BitmapIndex<T>::LoadWithoutAssemble(const BinarySet& binary_set,
                                    const Config& config) {
    auto index_meta_buffer = binary_set.GetByName(BITMAP_INDEX_META);
    auto index_meta = DeserializeIndexMeta(index_meta_buffer->data.get(),
                                           index_meta_buffer->size);
    auto index_length = index_meta.first;
    total_num_rows_ = index_meta.second;

    auto index_data_buffer = binary_set.GetByName(BITMAP_INDEX_DATA);
    DeserializeIndexData(index_data_buffer->data.get(), index_length);

    LOG_INFO("load bitmap index with cardinality = {}, num_rows = {}",
             Cardinality(),
             total_num_rows_);

    is_built_ = true;
}

template <typename T>
void
BitmapIndex<T>::LoadV2(const Config& config) {
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

    LoadWithoutAssemble(binary_set, config);
}

template <typename T>
void
BitmapIndex<T>::Load(milvus::tracer::TraceContext ctx, const Config& config) {
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

    LoadWithoutAssemble(binary_set, config);
}

template <typename T>
const TargetBitmap
BitmapIndex<T>::In(const size_t n, const T* values) {
    AssertInfo(is_built_, "index has not been built");
    TargetBitmap res(total_num_rows_, false);

    if (build_mode_ == BitmapIndexBuildMode::ROARING) {
        for (size_t i = 0; i < n; ++i) {
            auto val = values[i];
            auto it = data_.find(val);
            if (it != data_.end()) {
                for (const auto& v : it->second) {
                    res.set(v);
                }
            }
        }
    } else {
        for (size_t i = 0; i < n; ++i) {
            auto val = values[i];
            if (bitsets_.find(val) != bitsets_.end()) {
                res |= bitsets_.at(val);
            }
        }
    }
    return res;
}

template <typename T>
const TargetBitmap
BitmapIndex<T>::NotIn(const size_t n, const T* values) {
    AssertInfo(is_built_, "index has not been built");

    if (build_mode_ == BitmapIndexBuildMode::ROARING) {
        TargetBitmap res(total_num_rows_, true);
        for (int i = 0; i < n; ++i) {
            auto val = values[i];
            auto it = data_.find(val);
            if (it != data_.end()) {
                for (const auto& v : it->second) {
                    res.reset(v);
                }
            }
        }
        return res;
    } else {
        TargetBitmap res(total_num_rows_, false);
        for (size_t i = 0; i < n; ++i) {
            auto val = values[i];
            if (bitsets_.find(val) != bitsets_.end()) {
                res |= bitsets_.at(val);
            }
        }
        res.flip();
        return res;
    }
}

template <typename T>
TargetBitmap
BitmapIndex<T>::RangeForBitset(const T value, const OpType op) {
    AssertInfo(is_built_, "index has not been built");
    TargetBitmap res(total_num_rows_, false);
    if (ShouldSkip(value, value, op)) {
        return res;
    }
    auto lb = bitsets_.begin();
    auto ub = bitsets_.end();

    switch (op) {
        case OpType::LessThan: {
            ub = std::lower_bound(bitsets_.begin(),
                                  bitsets_.end(),
                                  std::make_pair(value, TargetBitmap()),
                                  [](const auto& lhs, const auto& rhs) {
                                      return lhs.first < rhs.first;
                                  });
            break;
        }
        case OpType::LessEqual: {
            ub = std::upper_bound(bitsets_.begin(),
                                  bitsets_.end(),
                                  std::make_pair(value, TargetBitmap()),
                                  [](const auto& lhs, const auto& rhs) {
                                      return lhs.first < rhs.first;
                                  });
            break;
        }
        case OpType::GreaterThan: {
            lb = std::upper_bound(bitsets_.begin(),
                                  bitsets_.end(),
                                  std::make_pair(value, TargetBitmap()),
                                  [](const auto& lhs, const auto& rhs) {
                                      return lhs.first < rhs.first;
                                  });
            break;
        }
        case OpType::GreaterEqual: {
            lb = std::lower_bound(bitsets_.begin(),
                                  bitsets_.end(),
                                  std::make_pair(value, TargetBitmap()),
                                  [](const auto& lhs, const auto& rhs) {
                                      return lhs.first < rhs.first;
                                  });
            break;
        }
        default: {
            throw SegcoreError(OpTypeInvalid,
                               fmt::format("Invalid OperatorType: {}", op));
        }
    }

    for (; lb != ub; lb++) {
        res |= lb->second;
    }
    return res;
}

template <typename T>
const TargetBitmap
BitmapIndex<T>::Range(const T value, OpType op) {
    if (build_mode_ == BitmapIndexBuildMode::ROARING) {
        return std::move(RangeForRoaring(value, op));
    } else {
        return std::move(RangeForBitset(value, op));
    }
}

template <typename T>
TargetBitmap
BitmapIndex<T>::RangeForRoaring(const T value, const OpType op) {
    AssertInfo(is_built_, "index has not been built");
    TargetBitmap res(total_num_rows_, false);
    if (ShouldSkip(value, value, op)) {
        return res;
    }
    auto lb = data_.begin();
    auto ub = data_.end();

    switch (op) {
        case OpType::LessThan: {
            ub = std::lower_bound(data_.begin(),
                                  data_.end(),
                                  std::make_pair(value, TargetBitmap()),
                                  [](const auto& lhs, const auto& rhs) {
                                      return lhs.first < rhs.first;
                                  });
            break;
        }
        case OpType::LessEqual: {
            ub = std::upper_bound(data_.begin(),
                                  data_.end(),
                                  std::make_pair(value, TargetBitmap()),
                                  [](const auto& lhs, const auto& rhs) {
                                      return lhs.first < rhs.first;
                                  });
            break;
        }
        case OpType::GreaterThan: {
            lb = std::upper_bound(data_.begin(),
                                  data_.end(),
                                  std::make_pair(value, TargetBitmap()),
                                  [](const auto& lhs, const auto& rhs) {
                                      return lhs.first < rhs.first;
                                  });
            break;
        }
        case OpType::GreaterEqual: {
            lb = std::lower_bound(data_.begin(),
                                  data_.end(),
                                  std::make_pair(value, TargetBitmap()),
                                  [](const auto& lhs, const auto& rhs) {
                                      return lhs.first < rhs.first;
                                  });
            break;
        }
        default: {
            throw SegcoreError(OpTypeInvalid,
                               fmt::format("Invalid OperatorType: {}", op));
        }
    }

    for (; lb != ub; lb++) {
        for (const auto& v : lb->second) {
            res.set(v);
        }
    }
    return res;
}

template <typename T>
TargetBitmap
BitmapIndex<T>::RangeForBitset(const T lower_value,
                               bool lb_inclusive,
                               const T upper_value,
                               bool ub_inclusive) {
    AssertInfo(is_built_, "index has not been built");
    TargetBitmap res(total_num_rows_, false);
    if (lower_value > upper_value ||
        (lower_value == upper_value && !(lb_inclusive && ub_inclusive))) {
        return res;
    }
    if (ShouldSkip(lower_value, upper_value, OpType::Range)) {
        return res;
    }

    auto lb = bitsets_.begin();
    auto ub = bitsets_.end();

    if (lb_inclusive) {
        lb = std::lower_bound(bitsets_.begin(),
                              bitsets_.end(),
                              std::make_pair(lower_value, TargetBitmap()),
                              [](const auto& lhs, const auto& rhs) {
                                  return lhs.first < rhs.first;
                              });
    } else {
        lb = std::upper_bound(bitsets_.begin(),
                              bitsets_.end(),
                              std::make_pair(lower_value, TargetBitmap()),
                              [](const auto& lhs, const auto& rhs) {
                                  return lhs.first < rhs.first;
                              });
    }

    if (ub_inclusive) {
        ub = std::upper_bound(bitsets_.begin(),
                              bitsets_.end(),
                              std::make_pair(upper_value, TargetBitmap()),
                              [](const auto& lhs, const auto& rhs) {
                                  return lhs.first < rhs.first;
                              });
    } else {
        ub = std::lower_bound(bitsets_.begin(),
                              bitsets_.end(),
                              std::make_pair(upper_value, TargetBitmap()),
                              [](const auto& lhs, const auto& rhs) {
                                  return lhs.first < rhs.first;
                              });
    }

    for (; lb != ub; lb++) {
        res |= lb->second;
    }
    return res;
}

template <typename T>
const TargetBitmap
BitmapIndex<T>::Range(const T lower_value,
                      bool lb_inclusive,
                      const T upper_value,
                      bool ub_inclusive) {
    if (build_mode_ == BitmapIndexBuildMode::ROARING) {
        return RangeForRoaring(
            lower_value, lb_inclusive, upper_value, ub_inclusive);
    } else {
        return RangeForBitset(
            lower_value, lb_inclusive, upper_value, ub_inclusive);
    }
}

template <typename T>
TargetBitmap
BitmapIndex<T>::RangeForRoaring(const T lower_value,
                                bool lb_inclusive,
                                const T upper_value,
                                bool ub_inclusive) {
    AssertInfo(is_built_, "index has not been built");
    TargetBitmap res(total_num_rows_, false);
    if (lower_value > upper_value ||
        (lower_value == upper_value && !(lb_inclusive && ub_inclusive))) {
        return res;
    }
    if (ShouldSkip(lower_value, upper_value, OpType::Range)) {
        return res;
    }

    auto lb = data_.begin();
    auto ub = data_.end();

    if (lb_inclusive) {
        lb = std::lower_bound(data_.begin(),
                              data_.end(),
                              std::make_pair(lower_value, TargetBitmap()),
                              [](const auto& lhs, const auto& rhs) {
                                  return lhs.first < rhs.first;
                              });
    } else {
        lb = std::upper_bound(data_.begin(),
                              data_.end(),
                              std::make_pair(lower_value, TargetBitmap()),
                              [](const auto& lhs, const auto& rhs) {
                                  return lhs.first < rhs.first;
                              });
    }

    if (ub_inclusive) {
        ub = std::upper_bound(data_.begin(),
                              data_.end(),
                              std::make_pair(upper_value, TargetBitmap()),
                              [](const auto& lhs, const auto& rhs) {
                                  return lhs.first < rhs.first;
                              });
    } else {
        ub = std::lower_bound(data_.begin(),
                              data_.end(),
                              std::make_pair(upper_value, TargetBitmap()),
                              [](const auto& lhs, const auto& rhs) {
                                  return lhs.first < rhs.first;
                              });
    }

    for (; lb != ub; lb++) {
        for (const auto& v : lb->second) {
            res.set(v);
        }
    }
    return res;
}

template <typename T>
T
BitmapIndex<T>::Reverse_Lookup(size_t idx) const {
    AssertInfo(is_built_, "index has not been built");
    AssertInfo(idx < total_num_rows_, "out of range of total coun");

    if (build_mode_ == BitmapIndexBuildMode::ROARING) {
        for (auto it = data_.begin(); it != data_.end(); it++) {
            for (const auto& v : it->second) {
                if (v == idx) {
                    return it->first;
                }
            }
        }
    } else {
        for (auto it = bitsets_.begin(); it != bitsets_.end(); it++) {
            if (it->second[idx]) {
                return it->first;
            }
        }
    }
    throw SegcoreError(
        UnexpectedError,
        fmt::format(
            "scalar bitmap index can not lookup target value of index {}",
            idx));
}

template <typename T>
bool
BitmapIndex<T>::ShouldSkip(const T lower_value,
                           const T upper_value,
                           const OpType op) {
    auto skip = [&](OpType op, T lower_bound, T upper_bound) -> bool {
        bool should_skip = false;
        switch (op) {
            case OpType::LessThan: {
                // lower_value == upper_value
                should_skip = lower_bound >= lower_value;
                break;
            }
            case OpType::LessEqual: {
                // lower_value == upper_value
                should_skip = lower_bound > lower_value;
                break;
            }
            case OpType::GreaterThan: {
                // lower_value == upper_value
                should_skip = upper_bound <= lower_value;
                break;
            }
            case OpType::GreaterEqual: {
                // lower_value == upper_value
                should_skip = upper_bound < lower_value;
                break;
            }
            case OpType::Range: {
                // lower_value == upper_value
                should_skip =
                    lower_bound > upper_value || upper_bound < lower_value;
                break;
            }
            default:
                throw SegcoreError(
                    OpTypeInvalid,
                    fmt::format("Invalid OperatorType for "
                                "checking scalar index optimization: {}",
                                op));
        }
        return should_skip;
    };

    if (build_mode_ == BitmapIndexBuildMode::ROARING) {
        if (!data_.empty()) {
            auto lower_bound = data_.begin()->first;
            auto upper_bound = data_.rbegin()->first;
            bool should_skip = skip(op, lower_bound, upper_bound);
            return should_skip;
        }
    } else {
        if (!bitsets_.empty()) {
            auto lower_bound = bitsets_.begin()->first;
            auto upper_bound = bitsets_.rbegin()->first;
            bool should_skip = skip(op, lower_bound, upper_bound);
            return should_skip;
        }
    }
    return true;
}

template class BitmapIndex<bool>;
template class BitmapIndex<int8_t>;
template class BitmapIndex<int16_t>;
template class BitmapIndex<int32_t>;
template class BitmapIndex<int64_t>;
template class BitmapIndex<float>;
template class BitmapIndex<double>;
template class BitmapIndex<std::string>;

}  // namespace index
}  // namespace milvus
