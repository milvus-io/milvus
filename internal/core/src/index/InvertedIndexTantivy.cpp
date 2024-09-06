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

#include "tantivy-binding.h"
#include "common/Slice.h"
#include "common/RegexQuery.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "index/InvertedIndexTantivy.h"
#include "log/Log.h"
#include "index/Utils.h"
#include "storage/Util.h"

#include <boost/filesystem.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <cstddef>
#include <vector>
#include "InvertedIndexTantivy.h"

namespace milvus::index {
constexpr const char* TMP_INVERTED_INDEX_PREFIX = "/tmp/milvus/inverted-index/";

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

inline TantivyDataType
get_tantivy_data_type(const proto::schema::FieldSchema& schema) {
    switch (schema.data_type()) {
        case proto::schema::Array:
            return get_tantivy_data_type(schema.element_type());
        default:
            return get_tantivy_data_type(schema.data_type());
    }
}

template <typename T>
InvertedIndexTantivy<T>::InvertedIndexTantivy(
    const storage::FileManagerContext& ctx)
    : ScalarIndex<T>(INVERTED_INDEX_TYPE),
      schema_(ctx.fieldDataMeta.field_schema) {
    mem_file_manager_ = std::make_shared<MemFileManager>(ctx);
    disk_file_manager_ = std::make_shared<DiskFileManager>(ctx);
    auto field =
        std::to_string(disk_file_manager_->GetFieldDataMeta().field_id);
    auto prefix = disk_file_manager_->GetIndexIdentifier();
    path_ = std::string(TMP_INVERTED_INDEX_PREFIX) + prefix;
    boost::filesystem::create_directories(path_);
    d_type_ = get_tantivy_data_type(schema_);
    if (tantivy_index_exist(path_.c_str())) {
        LOG_INFO(
            "index {} already exists, which should happen in loading progress",
            path_);
    } else {
        wrapper_ = std::make_shared<TantivyIndexWrapper>(
            field.c_str(), d_type_, path_.c_str());
    }
}

template <typename T>
InvertedIndexTantivy<T>::~InvertedIndexTantivy() {
    auto local_chunk_manager =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    auto prefix = path_;
    local_chunk_manager->RemoveDir(prefix);
}

template <typename T>
void
InvertedIndexTantivy<T>::finish() {
    wrapper_->finish();
}

template <typename T>
BinarySet
InvertedIndexTantivy<T>::Serialize(const Config& config) {
    auto index_valid_data_length = null_offset.size() * sizeof(size_t);
    std::shared_ptr<uint8_t[]> index_valid_data(
        new uint8_t[index_valid_data_length]);
    memcpy(index_valid_data.get(), null_offset.data(), index_valid_data_length);
    BinarySet res_set;
    res_set.Append(
        "index_null_offset", index_valid_data, index_valid_data_length);
    milvus::Disassemble(res_set);
    return res_set;
}

template <typename T>
BinarySet
InvertedIndexTantivy<T>::Upload(const Config& config) {
    finish();

    boost::filesystem::path p(path_);
    boost::filesystem::directory_iterator end_iter;

    for (boost::filesystem::directory_iterator iter(p); iter != end_iter;
         iter++) {
        if (boost::filesystem::is_directory(*iter)) {
            LOG_WARN("{} is a directory", iter->path().string());
        } else {
            LOG_INFO("trying to add index file: {}", iter->path().string());
            AssertInfo(disk_file_manager_->AddFile(iter->path().string()),
                       "failed to add index file: {}",
                       iter->path().string());
            LOG_INFO("index file: {} added", iter->path().string());
        }
    }

    BinarySet ret;

    auto remote_paths_to_size = disk_file_manager_->GetRemotePathsToFileSize();
    for (auto& file : remote_paths_to_size) {
        ret.Append(file.first, nullptr, file.second);
    }
    auto binary_set = Serialize(config);
    mem_file_manager_->AddFile(binary_set);
    auto remote_mem_path_to_size =
        mem_file_manager_->GetRemotePathsToFileSize();
    for (auto& file : remote_mem_path_to_size) {
        ret.Append(file.first, nullptr, file.second);
    }
    return ret;
}

template <typename T>
void
InvertedIndexTantivy<T>::Build(const Config& config) {
    auto insert_files =
        GetValueFromConfig<std::vector<std::string>>(config, "insert_files");
    AssertInfo(insert_files.has_value(), "insert_files were empty");
    auto field_datas =
        mem_file_manager_->CacheRawDataToMemory(insert_files.value());
    BuildWithFieldData(field_datas);
}

template <typename T>
void
InvertedIndexTantivy<T>::Load(milvus::tracer::TraceContext ctx,
                              const Config& config) {
    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load disk ann index data");
    auto prefix = disk_file_manager_->GetLocalIndexObjectPrefix();
    auto files_value = index_files.value();
    // need erase the index type file that has been readed
    auto index_type_file =
        disk_file_manager_->GetRemoteIndexPrefix() + std::string("/index_type");
    files_value.erase(std::remove_if(files_value.begin(),
                                     files_value.end(),
                                     [&](const std::string& file) {
                                         return file == index_type_file;
                                     }),
                      files_value.end());

    auto index_valid_data_file =
        mem_file_manager_->GetRemoteIndexObjectPrefix() +
        std::string("/index_null_offset");
    auto it = std::find(
        files_value.begin(), files_value.end(), index_valid_data_file);
    if (it != files_value.end()) {
        files_value.erase(it);
        std::vector<std::string> file;
        file.push_back(index_valid_data_file);
        auto index_datas = mem_file_manager_->LoadIndexToMemory(file);
        AssembleIndexDatas(index_datas);
        BinarySet binary_set;
        for (auto& [key, data] : index_datas) {
            auto size = data->DataSize();
            auto deleter = [&](uint8_t*) {};  // avoid repeated deconstruction
            auto buf = std::shared_ptr<uint8_t[]>(
                (uint8_t*)const_cast<void*>(data->Data()), deleter);
            binary_set.Append(key, buf, size);
        }
        auto index_valid_data = binary_set.GetByName("index_null_offset");
        null_offset.resize((size_t)index_valid_data->size / sizeof(size_t));
        memcpy(null_offset.data(),
               index_valid_data->data.get(),
               (size_t)index_valid_data->size);
    }
    disk_file_manager_->CacheIndexToDisk(files_value);
    wrapper_ = std::make_shared<TantivyIndexWrapper>(prefix.c_str());
}

inline void
apply_hits(TargetBitmap& bitset, const RustArrayWrapper& w, bool v) {
    for (size_t j = 0; j < w.array_.len; j++) {
        bitset[w.array_.array[j]] = v;
    }
}

inline void
apply_hits_with_filter(TargetBitmap& bitset,
                       const RustArrayWrapper& w,
                       const std::function<bool(size_t /* offset */)>& filter) {
    for (size_t j = 0; j < w.array_.len; j++) {
        auto the_offset = w.array_.array[j];
        bitset[the_offset] = filter(the_offset);
    }
}

inline void
apply_hits_with_callback(
    const RustArrayWrapper& w,
    const std::function<void(size_t /* offset */)>& callback) {
    for (size_t j = 0; j < w.array_.len; j++) {
        callback(w.array_.array[j]);
    }
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::In(size_t n, const T* values) {
    TargetBitmap bitset(Count());
    for (size_t i = 0; i < n; ++i) {
        auto array = wrapper_->term_query(values[i]);
        apply_hits(bitset, array, true);
    }
    return bitset;
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::IsNull() {
    TargetBitmap bitset(Count());

    for (size_t i = 0; i < null_offset.size(); ++i) {
        bitset.set(null_offset[i]);
    }
    return bitset;
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::IsNotNull() {
    TargetBitmap bitset(Count(), true);
    for (size_t i = 0; i < null_offset.size(); ++i) {
        bitset.reset(null_offset[i]);
    }
    return bitset;
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::InApplyFilter(
    size_t n, const T* values, const std::function<bool(size_t)>& filter) {
    TargetBitmap bitset(Count());
    for (size_t i = 0; i < n; ++i) {
        auto array = wrapper_->term_query(values[i]);
        apply_hits_with_filter(bitset, array, filter);
    }
    return bitset;
}

template <typename T>
void
InvertedIndexTantivy<T>::InApplyCallback(
    size_t n, const T* values, const std::function<void(size_t)>& callback) {
    for (size_t i = 0; i < n; ++i) {
        auto array = wrapper_->term_query(values[i]);
        apply_hits_with_callback(array, callback);
    }
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::NotIn(size_t n, const T* values) {
    TargetBitmap bitset(Count(), true);
    for (size_t i = 0; i < n; ++i) {
        auto array = wrapper_->term_query(values[i]);
        apply_hits(bitset, array, false);
    }
    for (size_t i = 0; i < null_offset.size(); ++i) {
        bitset.reset(null_offset[i]);
    }
    return bitset;
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::Range(T value, OpType op) {
    TargetBitmap bitset(Count());

    switch (op) {
        case OpType::LessThan: {
            auto array = wrapper_->upper_bound_range_query(value, false);
            apply_hits(bitset, array, true);
        } break;
        case OpType::LessEqual: {
            auto array = wrapper_->upper_bound_range_query(value, true);
            apply_hits(bitset, array, true);
        } break;
        case OpType::GreaterThan: {
            auto array = wrapper_->lower_bound_range_query(value, false);
            apply_hits(bitset, array, true);
        } break;
        case OpType::GreaterEqual: {
            auto array = wrapper_->lower_bound_range_query(value, true);
            apply_hits(bitset, array, true);
        } break;
        default:
            PanicInfo(OpTypeInvalid,
                      fmt::format("Invalid OperatorType: {}", op));
    }

    return bitset;
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::Range(T lower_bound_value,
                               bool lb_inclusive,
                               T upper_bound_value,
                               bool ub_inclusive) {
    TargetBitmap bitset(Count());
    auto array = wrapper_->range_query(
        lower_bound_value, upper_bound_value, lb_inclusive, ub_inclusive);
    apply_hits(bitset, array, true);
    return bitset;
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::PrefixMatch(const std::string_view prefix) {
    TargetBitmap bitset(Count());
    std::string s(prefix);
    auto array = wrapper_->prefix_query(s);
    apply_hits(bitset, array, true);
    return bitset;
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::Query(const DatasetPtr& dataset) {
    return ScalarIndex<T>::Query(dataset);
}

template <>
const TargetBitmap
InvertedIndexTantivy<std::string>::Query(const DatasetPtr& dataset) {
    auto op = dataset->Get<OpType>(OPERATOR_TYPE);
    if (op == OpType::PrefixMatch) {
        auto prefix = dataset->Get<std::string>(PREFIX_VALUE);
        return PrefixMatch(prefix);
    }
    return ScalarIndex<std::string>::Query(dataset);
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::RegexQuery(const std::string& regex_pattern) {
    TargetBitmap bitset(Count());
    auto array = wrapper_->regex_query(regex_pattern);
    apply_hits(bitset, array, true);
    return bitset;
}

template <typename T>
void
InvertedIndexTantivy<T>::BuildWithRawData(size_t n,
                                          const void* values,
                                          const Config& config) {
    if constexpr (std::is_same_v<bool, T>) {
        schema_.set_data_type(proto::schema::DataType::Bool);
    }
    if constexpr (std::is_same_v<int8_t, T>) {
        schema_.set_data_type(proto::schema::DataType::Int8);
    }
    if constexpr (std::is_same_v<int16_t, T>) {
        schema_.set_data_type(proto::schema::DataType::Int16);
    }
    if constexpr (std::is_same_v<int32_t, T>) {
        schema_.set_data_type(proto::schema::DataType::Int32);
    }
    if constexpr (std::is_same_v<int64_t, T>) {
        schema_.set_data_type(proto::schema::DataType::Int64);
    }
    if constexpr (std::is_same_v<float, T>) {
        schema_.set_data_type(proto::schema::DataType::Float);
    }
    if constexpr (std::is_same_v<double, T>) {
        schema_.set_data_type(proto::schema::DataType::Double);
    }
    if constexpr (std::is_same_v<std::string, T>) {
        schema_.set_data_type(proto::schema::DataType::VarChar);
    }
    boost::uuids::random_generator generator;
    auto uuid = generator();
    auto prefix = boost::uuids::to_string(uuid);
    path_ = fmt::format("/tmp/{}", prefix);
    boost::filesystem::create_directories(path_);
    d_type_ = get_tantivy_data_type(schema_);
    std::string field = "test_inverted_index";
    wrapper_ = std::make_shared<TantivyIndexWrapper>(
        field.c_str(), d_type_, path_.c_str());
    if (config.find("is_array") != config.end()) {
        // only used in ut.
        auto arr = static_cast<const boost::container::vector<T>*>(values);
        for (size_t i = 0; i < n; i++) {
            wrapper_->template add_multi_data(arr[i].data(), arr[i].size(), i);
        }
    } else {
        wrapper_->add_data<T>(static_cast<const T*>(values), n, 0);
    }
    finish();
}

template <typename T>
void
InvertedIndexTantivy<T>::BuildWithFieldData(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas) {
    if (schema_.nullable()) {
        int64_t total = 0;
        for (const auto& data : field_datas) {
            total += data->get_null_count();
        }
        null_offset.reserve(total);
    }
    switch (schema_.data_type()) {
        case proto::schema::DataType::Bool:
        case proto::schema::DataType::Int8:
        case proto::schema::DataType::Int16:
        case proto::schema::DataType::Int32:
        case proto::schema::DataType::Int64:
        case proto::schema::DataType::Float:
        case proto::schema::DataType::Double:
        case proto::schema::DataType::String:
        case proto::schema::DataType::VarChar: {
            int64_t offset = 0;
            if (schema_.nullable()) {
                for (const auto& data : field_datas) {
                    auto n = data->get_num_rows();
                    for (int i = 0; i < n; i++) {
                        if (!data->is_valid(i)) {
                            null_offset.push_back(i);
                        }
                        wrapper_->add_multi_data<T>(
                            static_cast<const T*>(data->RawValue(i)),
                            data->is_valid(i),
                            offset++);
                    }
                }
            } else {
                for (const auto& data : field_datas) {
                    auto n = data->get_num_rows();
                    wrapper_->add_data<T>(
                        static_cast<const T*>(data->Data()), n, offset);
                    offset += n;
                }
            }
            break;
        }

        case proto::schema::DataType::Array: {
            build_index_for_array(field_datas);
            break;
        }

        default:
            PanicInfo(ErrorCode::NotImplemented,
                      fmt::format("Inverted index not supported on {}",
                                  schema_.data_type()));
    }
}

template <typename T>
void
InvertedIndexTantivy<T>::build_index_for_array(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas) {
    int64_t offset = 0;
    for (const auto& data : field_datas) {
        auto n = data->get_num_rows();
        auto array_column = static_cast<const Array*>(data->Data());
        for (int64_t i = 0; i < n; i++) {
            assert(array_column[i].get_element_type() ==
                   static_cast<DataType>(schema_.element_type()));
            if (schema_.nullable() && !data->is_valid(i)) {
                null_offset.push_back(i);
            }
            auto length = data->is_valid(i) ? array_column[i].length() : 0;
            wrapper_->template add_multi_data(
                reinterpret_cast<const T*>(array_column[i].data()),
                length,
                offset++);
        }
    }
}

template <>
void
InvertedIndexTantivy<std::string>::build_index_for_array(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas) {
    int64_t offset = 0;
    for (const auto& data : field_datas) {
        auto n = data->get_num_rows();
        auto array_column = static_cast<const Array*>(data->Data());
        for (int64_t i = 0; i < n; i++) {
            Assert(IsStringDataType(array_column[i].get_element_type()));
            Assert(IsStringDataType(
                static_cast<DataType>(schema_.element_type())));
            if (schema_.nullable() && !data->is_valid(i)) {
                null_offset.push_back(i);
            }
            std::vector<std::string> output;
            for (int64_t j = 0; j < array_column[i].length(); j++) {
                output.push_back(
                    array_column[i].template get_data<std::string>(j));
            }
            auto length = data->is_valid(i) ? output.size() : 0;
            wrapper_->template add_multi_data(output.data(), length, offset++);
        }
    }
}

template class InvertedIndexTantivy<bool>;
template class InvertedIndexTantivy<int8_t>;
template class InvertedIndexTantivy<int16_t>;
template class InvertedIndexTantivy<int32_t>;
template class InvertedIndexTantivy<int64_t>;
template class InvertedIndexTantivy<float>;
template class InvertedIndexTantivy<double>;
template class InvertedIndexTantivy<std::string>;
}  // namespace milvus::index
