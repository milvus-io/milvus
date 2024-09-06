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
#include <boost/algorithm/string.hpp>
#include <sys/errno.h>
#include <unistd.h>
#include <yaml-cpp/yaml.h>

#include "index/BitmapIndex.h"

#include "common/File.h"
#include "common/Slice.h"
#include "common/Common.h"
#include "index/Meta.h"
#include "index/ScalarIndex.h"
#include "index/Utils.h"
#include "storage/Util.h"
#include "query/Utils.h"

namespace milvus {
namespace index {

template <typename T>
BitmapIndex<T>::BitmapIndex(
    const storage::FileManagerContext& file_manager_context)
    : ScalarIndex<T>(BITMAP_INDEX_TYPE),
      is_built_(false),
      schema_(file_manager_context.fieldDataMeta.field_schema),
      is_mmap_(false) {
    if (file_manager_context.Valid()) {
        file_manager_ =
            std::make_shared<storage::MemFileManagerImpl>(file_manager_context);
        AssertInfo(file_manager_ != nullptr, "create file manager failed!");
    }
}

template <typename T>
void
BitmapIndex<T>::UnmapIndexData() {
    if (mmap_data_ != nullptr && mmap_data_ != MAP_FAILED) {
        if (munmap(mmap_data_, mmap_size_) != 0) {
            AssertInfo(
                true, "failed to unmap bitmap index, err={}", strerror(errno));
        }
        mmap_data_ = nullptr;
        mmap_size_ = 0;
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
        PanicInfo(DataIsEmpty, "BitmapIndex can not build null values");
    }

    total_num_rows_ = n;
    valid_bitset = TargetBitmap(total_num_rows_, false);

    T* p = const_cast<T*>(data);
    for (int i = 0; i < n; ++i, ++p) {
        data_[*p].add(i);
        valid_bitset.set(i);
    }

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
BitmapIndex<T>::BuildPrimitiveField(
    const std::vector<FieldDataPtr>& field_datas) {
    int64_t offset = 0;
    for (const auto& data : field_datas) {
        auto slice_row_num = data->get_num_rows();
        for (size_t i = 0; i < slice_row_num; ++i) {
            if (data->is_valid(i)) {
                auto val = reinterpret_cast<const T*>(data->RawValue(i));
                data_[*val].add(offset);
                valid_bitset.set(offset);
            }
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
        PanicInfo(DataIsEmpty, "scalar bitmap index can not build null values");
    }
    total_num_rows_ = total_num_rows;
    valid_bitset = TargetBitmap(total_num_rows_, false);

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
    using GetType = std::conditional_t<std::is_same_v<T, int8_t> ||
                                           std::is_same_v<T, int16_t> ||
                                           std::is_same_v<T, int32_t>,
                                       int32_t,
                                       T>;
    for (const auto& data : field_datas) {
        auto slice_row_num = data->get_num_rows();
        for (size_t i = 0; i < slice_row_num; ++i) {
            if (data->is_valid(i)) {
                auto array =
                    reinterpret_cast<const milvus::Array*>(data->RawValue(i));
                for (size_t j = 0; j < array->length(); ++j) {
                    auto val = array->template get_data<T>(j);
                    data_[val].add(offset);
                }
                valid_bitset.set(offset);
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
             data_.size(),
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
BitmapIndex<T>::ChooseIndexLoadMode(int64_t index_length) {
    if (index_length <= DEFAULT_BITMAP_INDEX_CARDINALITY_BOUND) {
        LOG_DEBUG("load bitmap index with bitset mode");
        build_mode_ = BitmapIndexBuildMode::BITSET;
    } else {
        LOG_DEBUG("load bitmap index with raw roaring mode");
        build_mode_ = BitmapIndexBuildMode::ROARING;
    }
}

template <typename T>
void
BitmapIndex<T>::DeserializeIndexData(const uint8_t* data_ptr,
                                     size_t index_length) {
    ChooseIndexLoadMode(index_length);
    for (size_t i = 0; i < index_length; ++i) {
        T key;
        memcpy(&key, data_ptr, sizeof(T));
        data_ptr += sizeof(T);

        roaring::Roaring value;
        value = roaring::Roaring::read(reinterpret_cast<const char*>(data_ptr));
        data_ptr += value.getSizeInBytes();

        if (build_mode_ == BitmapIndexBuildMode::BITSET) {
            bitsets_[key] = ConvertRoaringToBitset(value);
        } else {
            data_[key] = value;
        }
        for (const auto& v : value) {
            valid_bitset.set(v);
        }
    }
}

template <typename T>
void
BitmapIndex<T>::BuildOffsetCache() {
    if (is_mmap_) {
        mmap_offsets_cache_.resize(total_num_rows_);
        for (auto it = bitmap_info_map_.begin(); it != bitmap_info_map_.end();
             ++it) {
            for (const auto& v : AccessBitmap(it->second)) {
                mmap_offsets_cache_[v] = it;
            }
        }
    } else {
        if (build_mode_ == BitmapIndexBuildMode::ROARING) {
            data_offsets_cache_.resize(total_num_rows_);
            for (auto it = data_.begin(); it != data_.end(); it++) {
                for (const auto& v : it->second) {
                    data_offsets_cache_[v] = it;
                }
            }
        } else {
            bitsets_offsets_cache_.resize(total_num_rows_);
            for (auto it = bitsets_.begin(); it != bitsets_.end(); it++) {
                const auto& bits = it->second;
                for (int i = 0; i < bits.size(); i++) {
                    if (bits[i]) {
                        bitsets_offsets_cache_[i] = it;
                    }
                }
            }
        }
    }
    use_offset_cache_ = true;
    LOG_INFO("build offset cache for bitmap index");
}

template <>
void
BitmapIndex<std::string>::DeserializeIndexData(const uint8_t* data_ptr,
                                               size_t index_length) {
    ChooseIndexLoadMode(index_length);
    for (size_t i = 0; i < index_length; ++i) {
        size_t key_size;
        memcpy(&key_size, data_ptr, sizeof(size_t));
        data_ptr += sizeof(size_t);

        std::string key(reinterpret_cast<const char*>(data_ptr), key_size);
        data_ptr += key_size;

        roaring::Roaring value;
        value = roaring::Roaring::read(reinterpret_cast<const char*>(data_ptr));
        data_ptr += value.getSizeInBytes();

        if (build_mode_ == BitmapIndexBuildMode::BITSET) {
            bitsets_[key] = ConvertRoaringToBitset(value);
        } else {
            data_[key] = value;
        }
        for (const auto& v : value) {
            valid_bitset.set(v);
        }
    }
}

template <typename T>
void
BitmapIndex<T>::DeserializeIndexDataForMmap(const char* data_ptr,
                                            size_t index_length) {
    for (size_t i = 0; i < index_length; ++i) {
        T key;
        memcpy(&key, data_ptr, sizeof(T));
        data_ptr += sizeof(T);

        roaring::Roaring value;
        value = roaring::Roaring::read(reinterpret_cast<const char*>(data_ptr));
        auto size = value.getSizeInBytes();

        bitmap_info_map_[key] = {static_cast<size_t>(data_ptr - mmap_data_),
                                 size};
        data_ptr += size;
    }
}

template <>
void
BitmapIndex<std::string>::DeserializeIndexDataForMmap(const char* data_ptr,
                                                      size_t index_length) {
    for (size_t i = 0; i < index_length; ++i) {
        size_t key_size;
        memcpy(&key_size, data_ptr, sizeof(size_t));
        data_ptr += sizeof(size_t);

        std::string key(reinterpret_cast<const char*>(data_ptr), key_size);
        data_ptr += key_size;

        roaring::Roaring value;
        value = roaring::Roaring::read(reinterpret_cast<const char*>(data_ptr));
        auto size = value.getSizeInBytes();

        bitmap_info_map_[key] = {static_cast<size_t>(data_ptr - mmap_data_),
                                 size};
        data_ptr += size;
    }
}

template <typename T>
void
BitmapIndex<T>::MMapIndexData(const std::string& file_name,
                              const uint8_t* data_ptr,
                              size_t data_size,
                              size_t index_length) {
    std::filesystem::create_directories(
        std::filesystem::path(file_name).parent_path());

    auto file = File::Open(file_name, O_RDWR | O_CREAT | O_TRUNC);
    auto written = file.Write(data_ptr, data_size);
    if (written != data_size) {
        file.Close();
        remove(file_name.c_str());
        PanicInfo(ErrorCode::UnistdError,
                  fmt::format("write index to fd error: {}", strerror(errno)));
    }

    file.Seek(0, SEEK_SET);
    mmap_data_ = static_cast<char*>(
        mmap(NULL, data_size, PROT_READ, MAP_PRIVATE, file.Descriptor(), 0));
    if (mmap_data_ == MAP_FAILED) {
        file.Close();
        remove(file_name.c_str());
        PanicInfo(
            ErrorCode::UnexpectedError, "failed to mmap: {}", strerror(errno));
    }

    mmap_size_ = data_size;
    unlink(file_name.c_str());

    char* ptr = mmap_data_;
    DeserializeIndexDataForMmap(ptr, index_length);
    is_mmap_ = true;
}

template <typename T>
void
BitmapIndex<T>::LoadWithoutAssemble(const BinarySet& binary_set,
                                    const Config& config) {
    auto enable_offset_cache =
        GetValueFromConfig<bool>(config, ENABLE_OFFSET_CACHE);

    auto index_meta_buffer = binary_set.GetByName(BITMAP_INDEX_META);
    auto index_meta = DeserializeIndexMeta(index_meta_buffer->data.get(),
                                           index_meta_buffer->size);
    auto index_length = index_meta.first;
    total_num_rows_ = index_meta.second;
    valid_bitset = TargetBitmap(total_num_rows_, false);

    auto index_data_buffer = binary_set.GetByName(BITMAP_INDEX_DATA);

    ChooseIndexLoadMode(index_length);

    // only using mmap when build mode is raw roaring bitmap
    if (config.contains(MMAP_FILE_PATH) &&
        build_mode_ == BitmapIndexBuildMode::ROARING) {
        auto mmap_filepath =
            GetValueFromConfig<std::string>(config, MMAP_FILE_PATH);
        AssertInfo(mmap_filepath.has_value(),
                   "mmap filepath is empty when load index");
        MMapIndexData(mmap_filepath.value(),
                      index_data_buffer->data.get(),
                      index_data_buffer->size,
                      index_length);
    } else {
        DeserializeIndexData(index_data_buffer->data.get(), index_length);
    }

    if (enable_offset_cache.has_value() && enable_offset_cache.value()) {
        BuildOffsetCache();
    }

    auto file_index_meta = file_manager_->GetIndexMeta();
    LOG_INFO(
        "load bitmap index with cardinality = {}, num_rows = {} for segment_id "
        "= {}, field_id = {}, mmap = {}",
        Cardinality(),
        total_num_rows_,
        file_index_meta.segment_id,
        file_index_meta.field_id,
        is_mmap_);

    is_built_ = true;
}

template <typename T>
void
BitmapIndex<T>::Load(milvus::tracer::TraceContext ctx, const Config& config) {
    LOG_DEBUG("load bitmap index with config {}", config.dump());
    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load bitmap index");
    auto index_datas = file_manager_->LoadIndexToMemory(index_files.value());
    AssembleIndexDatas(index_datas);
    BinarySet binary_set;
    for (auto& [key, data] : index_datas) {
        auto size = data->DataSize();
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

    if (is_mmap_) {
        for (size_t i = 0; i < n; ++i) {
            auto val = values[i];
            auto it = bitmap_info_map_.find(val);
            if (it != bitmap_info_map_.end()) {
                for (const auto& v : AccessBitmap(it->second)) {
                    res.set(v);
                }
            }
        }
        return res;
    }
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

    if (is_mmap_) {
        TargetBitmap res(total_num_rows_, true);
        for (int i = 0; i < n; ++i) {
            auto val = values[i];
            auto it = bitmap_info_map_.find(val);
            if (it != bitmap_info_map_.end()) {
                for (const auto& v : AccessBitmap(it->second)) {
                    res.reset(v);
                }
            }
        }
        return res;
    }
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
        // NotIn(null) and In(null) is both false, need to mask with IsNotNull operate
        res &= valid_bitset;
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
        // NotIn(null) and In(null) is both false, need to mask with IsNotNull operate
        res &= valid_bitset;
        return res;
    }
}

template <typename T>
const TargetBitmap
BitmapIndex<T>::IsNull() {
    AssertInfo(is_built_, "index has not been built");
    TargetBitmap res(total_num_rows_, true);
    res &= valid_bitset;
    res.flip();
    return res;
}

template <typename T>
const TargetBitmap
BitmapIndex<T>::IsNotNull() {
    AssertInfo(is_built_, "index has not been built");
    TargetBitmap res(total_num_rows_, true);
    res &= valid_bitset;
    return res;
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
            PanicInfo(OpTypeInvalid,
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
    if (is_mmap_) {
        return std::move(RangeForMmap(value, op));
    }
    if (build_mode_ == BitmapIndexBuildMode::ROARING) {
        return std::move(RangeForRoaring(value, op));
    } else {
        return std::move(RangeForBitset(value, op));
    }
}
template <typename T>
TargetBitmap
BitmapIndex<T>::RangeForMmap(const T value, const OpType op) {
    AssertInfo(is_built_, "index has not been built");
    TargetBitmap res(total_num_rows_, false);
    if (ShouldSkip(value, value, op)) {
        return res;
    }
    auto lb = bitmap_info_map_.begin();
    auto ub = bitmap_info_map_.end();

    switch (op) {
        case OpType::LessThan: {
            ub = std::lower_bound(bitmap_info_map_.begin(),
                                  bitmap_info_map_.end(),
                                  std::make_pair(value, TargetBitmap()),
                                  [](const auto& lhs, const auto& rhs) {
                                      return lhs.first < rhs.first;
                                  });
            break;
        }
        case OpType::LessEqual: {
            ub = std::upper_bound(bitmap_info_map_.begin(),
                                  bitmap_info_map_.end(),
                                  std::make_pair(value, TargetBitmap()),
                                  [](const auto& lhs, const auto& rhs) {
                                      return lhs.first < rhs.first;
                                  });
            break;
        }
        case OpType::GreaterThan: {
            lb = std::upper_bound(bitmap_info_map_.begin(),
                                  bitmap_info_map_.end(),
                                  std::make_pair(value, TargetBitmap()),
                                  [](const auto& lhs, const auto& rhs) {
                                      return lhs.first < rhs.first;
                                  });
            break;
        }
        case OpType::GreaterEqual: {
            lb = std::lower_bound(bitmap_info_map_.begin(),
                                  bitmap_info_map_.end(),
                                  std::make_pair(value, TargetBitmap()),
                                  [](const auto& lhs, const auto& rhs) {
                                      return lhs.first < rhs.first;
                                  });
            break;
        }
        default: {
            PanicInfo(OpTypeInvalid,
                      fmt::format("Invalid OperatorType: {}", op));
        }
    }

    for (; lb != ub; lb++) {
        for (const auto& v : AccessBitmap(lb->second)) {
            res.set(v);
        }
    }
    return res;
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
            PanicInfo(OpTypeInvalid,
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
    if (is_mmap_) {
        return RangeForMmap(
            lower_value, lb_inclusive, upper_value, ub_inclusive);
    }
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
BitmapIndex<T>::RangeForMmap(const T lower_value,
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

    auto lb = bitmap_info_map_.begin();
    auto ub = bitmap_info_map_.end();

    if (lb_inclusive) {
        lb = std::lower_bound(bitmap_info_map_.begin(),
                              bitmap_info_map_.end(),
                              std::make_pair(lower_value, TargetBitmap()),
                              [](const auto& lhs, const auto& rhs) {
                                  return lhs.first < rhs.first;
                              });
    } else {
        lb = std::upper_bound(bitmap_info_map_.begin(),
                              bitmap_info_map_.end(),
                              std::make_pair(lower_value, TargetBitmap()),
                              [](const auto& lhs, const auto& rhs) {
                                  return lhs.first < rhs.first;
                              });
    }

    if (ub_inclusive) {
        ub = std::upper_bound(bitmap_info_map_.begin(),
                              bitmap_info_map_.end(),
                              std::make_pair(upper_value, TargetBitmap()),
                              [](const auto& lhs, const auto& rhs) {
                                  return lhs.first < rhs.first;
                              });
    } else {
        ub = std::lower_bound(bitmap_info_map_.begin(),
                              bitmap_info_map_.end(),
                              std::make_pair(upper_value, TargetBitmap()),
                              [](const auto& lhs, const auto& rhs) {
                                  return lhs.first < rhs.first;
                              });
    }

    for (; lb != ub; lb++) {
        for (const auto& v : AccessBitmap(lb->second)) {
            res.set(v);
        }
    }
    return res;
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
BitmapIndex<T>::Reverse_Lookup_InCache(size_t idx) const {
    if (is_mmap_) {
        Assert(build_mode_ == BitmapIndexBuildMode::ROARING);
        return mmap_offsets_cache_[idx]->first;
    }

    if (build_mode_ == BitmapIndexBuildMode::ROARING) {
        return data_offsets_cache_[idx]->first;
    } else {
        return bitsets_offsets_cache_[idx]->first;
    }
}

template <typename T>
T
BitmapIndex<T>::Reverse_Lookup(size_t idx) const {
    AssertInfo(is_built_, "index has not been built");
    AssertInfo(idx < total_num_rows_, "out of range of total coun");

    if (use_offset_cache_) {
        return Reverse_Lookup_InCache(idx);
    }

    if (is_mmap_) {
        for (auto it = bitmap_info_map_.begin(); it != bitmap_info_map_.end();
             it++) {
            for (const auto& v : AccessBitmap(it->second)) {
                if (v == idx) {
                    return it->first;
                }
            }
        }
    } else {
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
    }
    PanicInfo(UnexpectedError,
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
                PanicInfo(OpTypeInvalid,
                          fmt::format("Invalid OperatorType for "
                                      "checking scalar index optimization: {}",
                                      op));
        }
        return should_skip;
    };

    if (is_mmap_) {
        if (!bitmap_info_map_.empty()) {
            auto lower_bound = bitmap_info_map_.begin()->first;
            auto upper_bound = bitmap_info_map_.rbegin()->first;
            bool should_skip = skip(op, lower_bound, upper_bound);
            return should_skip;
        }
    }

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

template <typename T>
const TargetBitmap
BitmapIndex<T>::Query(const DatasetPtr& dataset) {
    return ScalarIndex<T>::Query(dataset);
}

template <>
const TargetBitmap
BitmapIndex<std::string>::Query(const DatasetPtr& dataset) {
    AssertInfo(is_built_, "index has not been built");

    auto op = dataset->Get<OpType>(OPERATOR_TYPE);
    if (op == OpType::PrefixMatch) {
        auto prefix = dataset->Get<std::string>(PREFIX_VALUE);
        TargetBitmap res(total_num_rows_, false);
        if (is_mmap_) {
            for (auto it = bitmap_info_map_.begin();
                 it != bitmap_info_map_.end();
                 ++it) {
                const auto& key = it->first;
                if (milvus::query::Match(key, prefix, op)) {
                    for (const auto& v : AccessBitmap(it->second)) {
                        res.set(v);
                    }
                }
            }
            return res;
        }
        if (build_mode_ == BitmapIndexBuildMode::ROARING) {
            for (auto it = data_.begin(); it != data_.end(); ++it) {
                const auto& key = it->first;
                if (milvus::query::Match(key, prefix, op)) {
                    for (const auto& v : it->second) {
                        res.set(v);
                    }
                }
            }
        } else {
            for (auto it = bitsets_.begin(); it != bitsets_.end(); ++it) {
                const auto& key = it->first;
                if (milvus::query::Match(key, prefix, op)) {
                    res |= it->second;
                }
            }
        }

        return res;
    } else {
        PanicInfo(OpTypeInvalid,
                  fmt::format("unsupported op_type:{} for bitmap query", op));
    }
}

template <typename T>
const TargetBitmap
BitmapIndex<T>::RegexQuery(const std::string& regex_pattern) {
    return ScalarIndex<T>::RegexQuery(regex_pattern);
}

template <>
const TargetBitmap
BitmapIndex<std::string>::RegexQuery(const std::string& regex_pattern) {
    AssertInfo(is_built_, "index has not been built");
    RegexMatcher matcher(regex_pattern);
    TargetBitmap res(total_num_rows_, false);
    if (is_mmap_) {
        for (auto it = bitmap_info_map_.begin(); it != bitmap_info_map_.end();
             ++it) {
            const auto& key = it->first;
            if (matcher(key)) {
                for (const auto& v : AccessBitmap(it->second)) {
                    res.set(v);
                }
            }
        }
        return res;
    }
    if (build_mode_ == BitmapIndexBuildMode::ROARING) {
        for (auto it = data_.begin(); it != data_.end(); ++it) {
            const auto& key = it->first;
            if (matcher(key)) {
                for (const auto& v : it->second) {
                    res.set(v);
                }
            }
        }
    } else {
        for (auto it = bitsets_.begin(); it != bitsets_.end(); ++it) {
            const auto& key = it->first;
            if (matcher(key)) {
                res |= it->second;
            }
        }
    }
    return res;
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
