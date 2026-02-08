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

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <string.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <list>
#include <map>
#include <type_traits>
#include <utility>
#include <vector>

#include "InvertedIndexTantivy.h"
#include "boost/container/vector.hpp"
#include "boost/filesystem/directory.hpp"
#include "boost/filesystem/operations.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/iterator/iterator_facade.hpp"
#include "common/Array.h"
#include "common/FieldDataInterface.h"
#include "common/Slice.h"
#include "common/Tracer.h"
#include "folly/SharedMutex.h"
#include "glog/logging.h"
#include "index/InvertedIndexTantivy.h"
#include "index/InvertedIndexUtil.h"
#include "index/Utils.h"
#include "knowhere/dataset.h"
#include "log/Log.h"
#include "nlohmann/detail/iterators/iter_impl.hpp"
#include "nlohmann/json.hpp"
#include "pb/common.pb.h"
#include "storage/DataCodec.h"
#include "storage/FileManager.h"
#include "storage/LocalChunkManager.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "tantivy-binding.h"

namespace milvus::index {
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
void
InvertedIndexTantivy<T>::InitForBuildIndex() {
    auto field =
        std::to_string(disk_file_manager_->GetFieldDataMeta().field_id);
    path_ = disk_file_manager_->GetLocalTempIndexObjectPrefix();
    boost::filesystem::create_directories(path_);
    d_type_ = get_tantivy_data_type(schema_);
    if (tantivy_index_exist(path_.c_str())) {
        ThrowInfo(IndexBuildError,
                  "build inverted index temp dir:{} not empty",
                  path_);
    }
    wrapper_ =
        std::make_shared<TantivyIndexWrapper>(field.c_str(),
                                              d_type_,
                                              path_.c_str(),
                                              tantivy_index_version_,
                                              inverted_index_single_segment_,
                                              user_specified_doc_id_);
}

template <typename T>
InvertedIndexTantivy<T>::InvertedIndexTantivy(
    uint32_t tantivy_index_version,
    const storage::FileManagerContext& ctx,
    bool inverted_index_single_segment,
    bool user_specified_doc_id,
    bool is_nested_index)
    : ScalarIndex<T>(INVERTED_INDEX_TYPE),
      schema_(ctx.fieldDataMeta.field_schema),
      tantivy_index_version_(tantivy_index_version),
      inverted_index_single_segment_(inverted_index_single_segment),
      user_specified_doc_id_(user_specified_doc_id),
      is_nested_index_(is_nested_index) {
    mem_file_manager_ = std::make_shared<MemFileManager>(ctx);
    disk_file_manager_ = std::make_shared<DiskFileManager>(ctx);
    // push init wrapper to load process
    if (ctx.for_loading_index) {
        return;
    }
    InitForBuildIndex();
}

template <typename T>
InvertedIndexTantivy<T>::~InvertedIndexTantivy() {
    if (wrapper_) {
        wrapper_->free();
    }
    if (path_.empty()) {
        return;
    }
    auto local_chunk_manager =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    auto prefix = path_;
    LOG_INFO("inverted index remove path:{}", path_);
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
    std::shared_lock<folly::SharedMutex> lock(mutex_);
    auto index_valid_data_length = null_offset_.size() * sizeof(size_t);
    std::shared_ptr<uint8_t[]> index_valid_data(
        new uint8_t[index_valid_data_length]);
    memcpy(
        index_valid_data.get(), null_offset_.data(), index_valid_data_length);
    lock.unlock();
    BinarySet res_set;
    if (index_valid_data_length > 0) {
        res_set.Append(INDEX_NULL_OFFSET_FILE_NAME,
                       index_valid_data,
                       index_valid_data_length);
    }
    milvus::Disassemble(res_set);
    return res_set;
}

template <typename T>
IndexStatsPtr
InvertedIndexTantivy<T>::Upload(const Config& config) {
    finish();

    boost::filesystem::path p(path_);
    boost::filesystem::directory_iterator end_iter;

    // TODO: remove this log when #45590 is solved
    auto segment_id = disk_file_manager_->GetFieldDataMeta().segment_id;
    auto field_id = disk_file_manager_->GetFieldDataMeta().field_id;
    LOG_INFO(
        "InvertedIndexTantivy::Upload: segment_id={}, field_id={}, path={}",
        segment_id,
        field_id,
        path_);

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

    auto remote_paths_to_size = disk_file_manager_->GetRemotePathsToFileSize();

    auto binary_set = Serialize(config);
    mem_file_manager_->AddFile(binary_set);
    auto remote_mem_path_to_size =
        mem_file_manager_->GetRemotePathsToFileSize();

    std::vector<SerializedIndexFileInfo> index_files;
    index_files.reserve(remote_paths_to_size.size() +
                        remote_mem_path_to_size.size());
    for (auto& file : remote_paths_to_size) {
        index_files.emplace_back(file.first, file.second);
    }
    for (auto& file : remote_mem_path_to_size) {
        index_files.emplace_back(file.first, file.second);
    }
    return IndexStats::New(mem_file_manager_->GetAddedTotalMemSize() +
                               disk_file_manager_->GetAddedTotalFileSize(),
                           std::move(index_files));
}

template <typename T>
void
InvertedIndexTantivy<T>::Build(const Config& config) {
    auto field_datas =
        storage::CacheRawDataAndFillMissing(mem_file_manager_, config);
    BuildWithFieldData(field_datas);
}

template <typename T>
void
InvertedIndexTantivy<T>::Load(milvus::tracer::TraceContext ctx,
                              const Config& config) {
    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, INDEX_FILES);
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load disk ann index data");
    auto inverted_index_files = index_files.value();

    // TODO: remove this log when #45590 is solved
    auto segment_id = disk_file_manager_->GetFieldDataMeta().segment_id;
    auto field_id = disk_file_manager_->GetFieldDataMeta().field_id;
    LOG_INFO("InvertedIndexTantivy::Load: segment_id={}, field_id={}",
             segment_id,
             field_id);

    LoadIndexMetas(inverted_index_files, config);
    RetainTantivyIndexFiles(inverted_index_files);
    auto load_priority =
        GetValueFromConfig<milvus::proto::common::LoadPriority>(
            config, milvus::LOAD_PRIORITY)
            .value_or(milvus::proto::common::LoadPriority::HIGH);
    disk_file_manager_->CacheIndexToDisk(inverted_index_files, load_priority);
    auto prefix = disk_file_manager_->GetLocalIndexObjectPrefix();
    path_ = prefix;
    auto load_in_mmap =
        GetValueFromConfig<bool>(config, ENABLE_MMAP).value_or(true);
    wrapper_ = std::make_shared<TantivyIndexWrapper>(
        prefix.c_str(), load_in_mmap, milvus::index::SetBitsetSealed);

    if (!load_in_mmap) {
        // the index is loaded in ram, so we can remove files in advance
        disk_file_manager_->RemoveIndexFiles();
    }
    ComputeByteSize();
}

template <typename T>
void
InvertedIndexTantivy<T>::LoadIndexMetas(
    const std::vector<std::string>& index_files, const Config& config) {
    auto fill_null_offsets = [&](const uint8_t* data, int64_t size) {
        null_offset_.resize((size_t)size / sizeof(size_t));
        memcpy(null_offset_.data(), data, (size_t)size);
    };
    auto null_offset_file_itr = std::find_if(
        index_files.begin(), index_files.end(), [&](const std::string& file) {
            return boost::filesystem::path(file).filename().string() ==
                   INDEX_NULL_OFFSET_FILE_NAME;
        });
    auto load_priority =
        GetValueFromConfig<milvus::proto::common::LoadPriority>(
            config, milvus::LOAD_PRIORITY)
            .value_or(milvus::proto::common::LoadPriority::HIGH);

    if (null_offset_file_itr != index_files.end()) {
        // null offset file is not sliced
        auto index_datas = mem_file_manager_->LoadIndexToMemory(
            {*null_offset_file_itr}, load_priority);
        auto null_offset_data =
            std::move(index_datas.at(INDEX_NULL_OFFSET_FILE_NAME));
        fill_null_offsets(null_offset_data->PayloadData(),
                          null_offset_data->PayloadSize());
        return;
    }
    std::vector<std::string> null_offset_files;
    for (auto& file : index_files) {
        auto file_name = boost::filesystem::path(file).filename().string();
        if (file_name.find(INDEX_NULL_OFFSET_FILE_NAME) != std::string::npos) {
            null_offset_files.push_back(file);
        }

        // add slice meta file for null offset file compact
        if (file_name == INDEX_FILE_SLICE_META) {
            null_offset_files.push_back(file);
        }
    }

    if (null_offset_files.size() > 0) {
        // null offset file is sliced
        auto index_datas = mem_file_manager_->LoadIndexToMemory(
            null_offset_files, load_priority);

        auto null_offsets_data = CompactIndexDatas(index_datas);
        auto null_offsets_data_codecs =
            std::move(null_offsets_data.at(INDEX_NULL_OFFSET_FILE_NAME));
        for (auto&& null_offsets_codec : null_offsets_data_codecs.codecs_) {
            fill_null_offsets(null_offsets_codec->PayloadData(),
                              null_offsets_codec->PayloadSize());
        }
    }
}

template <typename T>
void
InvertedIndexTantivy<T>::RetainTantivyIndexFiles(
    std::vector<std::string>& index_files) {
    index_files.erase(
        std::remove_if(
            index_files.begin(),
            index_files.end(),
            [&](const std::string& file) {
                auto file_name =
                    boost::filesystem::path(file).filename().string();
                return file_name == "index_type" ||
                       // Slice meta is only used to compact null_offset files and non_exist_offset files.
                       // It can be removed after compaction is complete.
                       // Other index files are compacted by slice index instead of meta.
                       file_name == INDEX_FILE_SLICE_META ||
                       file_name.find(INDEX_NULL_OFFSET_FILE_NAME) !=
                           std::string::npos;
            }),
        index_files.end());
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::In(size_t n, const T* values) {
    tracer::AutoSpan span("InvertedIndexTantivy::In", tracer::GetRootSpan());
    TargetBitmap bitset(Count());
    wrapper_->terms_query(values, n, &bitset);
    return bitset;
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::IsNull() {
    tracer::AutoSpan span("InvertedIndexTantivy::IsNull",
                          tracer::GetRootSpan());
    int64_t count = Count();
    TargetBitmap bitset(count);

    auto fill_bitset = [this, count, &bitset]() {
        auto end =
            std::lower_bound(null_offset_.begin(), null_offset_.end(), count);
        for (auto iter = null_offset_.begin(); iter != end; ++iter) {
            bitset.set(*iter);
        }
    };

    if (is_growing_) {
        std::shared_lock<folly::SharedMutex> lock(mutex_);
        fill_bitset();
    } else {
        fill_bitset();
    }

    return bitset;
}

template <typename T>
TargetBitmap
InvertedIndexTantivy<T>::IsNotNull() {
    tracer::AutoSpan span("InvertedIndexTantivy::IsNotNull",
                          tracer::GetRootSpan());
    int64_t count = Count();
    TargetBitmap bitset(count, true);

    auto fill_bitset = [this, count, &bitset]() {
        auto end =
            std::lower_bound(null_offset_.begin(), null_offset_.end(), count);
        for (auto iter = null_offset_.begin(); iter != end; ++iter) {
            bitset.reset(*iter);
        }
    };

    if (is_growing_) {
        std::shared_lock<folly::SharedMutex> lock(mutex_);
        fill_bitset();
    } else {
        fill_bitset();
    }

    return bitset;
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::InApplyFilter(
    size_t n, const T* values, const std::function<bool(size_t)>& filter) {
    tracer::AutoSpan span("InvertedIndexTantivy::InApplyFilter",
                          tracer::GetRootSpan());
    TargetBitmap bitset(Count());
    wrapper_->terms_query(values, n, &bitset);
    // todo(SpadeA): could push-down the filter to tantivy query
    apply_hits_with_filter(bitset, filter);
    return bitset;
}

template <typename T>
void
InvertedIndexTantivy<T>::InApplyCallback(
    size_t n, const T* values, const std::function<void(size_t)>& callback) {
    tracer::AutoSpan span("InvertedIndexTantivy::InApplyCallback",
                          tracer::GetRootSpan());
    TargetBitmap bitset(Count());
    wrapper_->terms_query(values, n, &bitset);
    // todo(SpadeA): could push-down the callback to tantivy query
    apply_hits_with_callback(bitset, callback);
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::NotIn(size_t n, const T* values) {
    tracer::AutoSpan span("InvertedIndexTantivy::NotIn", tracer::GetRootSpan());
    int64_t count = Count();
    TargetBitmap bitset(count);
    wrapper_->terms_query(values, n, &bitset);
    // The expression is "not" in, so we flip the bit.
    bitset.flip();

    auto fill_bitset = [this, count, &bitset]() {
        auto end =
            std::lower_bound(null_offset_.begin(), null_offset_.end(), count);
        for (auto iter = null_offset_.begin(); iter != end; ++iter) {
            bitset.reset(*iter);
        }
    };

    if (is_growing_) {
        std::shared_lock<folly::SharedMutex> lock(mutex_);
        fill_bitset();
    } else {
        fill_bitset();
    }

    return bitset;
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::Range(T value, OpType op) {
    tracer::AutoSpan span("InvertedIndexTantivy::Range", tracer::GetRootSpan());
    TargetBitmap bitset(Count());

    switch (op) {
        case OpType::LessThan: {
            wrapper_->upper_bound_range_query(value, false, &bitset);
        } break;
        case OpType::LessEqual: {
            wrapper_->upper_bound_range_query(value, true, &bitset);
        } break;
        case OpType::GreaterThan: {
            wrapper_->lower_bound_range_query(value, false, &bitset);
        } break;
        case OpType::GreaterEqual: {
            wrapper_->lower_bound_range_query(value, true, &bitset);
        } break;
        default:
            ThrowInfo(OpTypeInvalid,
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
    tracer::AutoSpan span("InvertedIndexTantivy::RangeWithBounds",
                          tracer::GetRootSpan());
    TargetBitmap bitset(Count());
    wrapper_->range_query(lower_bound_value,
                          upper_bound_value,
                          lb_inclusive,
                          ub_inclusive,
                          &bitset);
    return bitset;
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::PrefixMatch(const std::string_view prefix) {
    tracer::AutoSpan span("InvertedIndexTantivy::PrefixMatch",
                          tracer::GetRootSpan());
    TargetBitmap bitset(Count());
    std::string s(prefix);
    wrapper_->prefix_query(s, &bitset);
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
    tracer::AutoSpan span("InvertedIndexTantivy::Query", tracer::GetRootSpan());
    auto op = dataset->Get<OpType>(OPERATOR_TYPE);
    if (op == OpType::PrefixMatch) {
        auto prefix = dataset->Get<std::string>(MATCH_VALUE);
        return PrefixMatch(prefix);
    }
    return ScalarIndex<std::string>::Query(dataset);
}

template <typename T>
const TargetBitmap
InvertedIndexTantivy<T>::PatternQuery(const std::string& pattern) {
    tracer::AutoSpan span("InvertedIndexTantivy::PatternQuery",
                          tracer::GetRootSpan());
    TargetBitmap bitset(Count());
    wrapper_->regex_query(pattern, &bitset);
    return bitset;
}

template <typename T>
void
InvertedIndexTantivy<T>::BuildWithRawDataForUT(size_t n,
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
    if (!wrapper_) {
        boost::uuids::random_generator generator;
        auto uuid = generator();
        auto prefix = boost::uuids::to_string(uuid);
        path_ = fmt::format("/tmp/{}", prefix);
        boost::filesystem::create_directories(path_);
        d_type_ = get_tantivy_data_type(schema_);
        std::string field = "test_inverted_index";
        inverted_index_single_segment_ =
            GetValueFromConfig<int32_t>(
                config, milvus::index::SCALAR_INDEX_ENGINE_VERSION)
                .value_or(1) == 0;
        tantivy_index_version_ =
            GetValueFromConfig<int32_t>(config,
                                        milvus::index::TANTIVY_INDEX_VERSION)
                .value_or(milvus::index::TANTIVY_INDEX_LATEST_VERSION);
        wrapper_ = std::make_shared<TantivyIndexWrapper>(
            field.c_str(),
            d_type_,
            path_.c_str(),
            tantivy_index_version_,
            inverted_index_single_segment_);
    }
    bool is_nested_index = config.find("is_nested_index") != config.end();
    if (!inverted_index_single_segment_) {
        if (config.find("is_array") != config.end()) {
            // only used in ut.
            auto arr = static_cast<const boost::container::vector<T>*>(values);
            if (is_nested_index) {
                is_nested_index_ = true;
                // For nested index, each array element is a separate document
                int64_t offset = 0;
                for (size_t i = 0; i < n; i++) {
                    wrapper_->template add_data<T>(
                        arr[i].data(), arr[i].size(), offset);
                    offset += arr[i].size();
                }
            } else {
                for (size_t i = 0; i < n; i++) {
                    wrapper_->template add_array_data(
                        arr[i].data(), arr[i].size(), i);
                }
            }
        } else {
            wrapper_->add_data<T>(static_cast<const T*>(values), n, 0);
        }
    } else {
        if (config.find("is_array") != config.end()) {
            // only used in ut.
            auto arr = static_cast<const boost::container::vector<T>*>(values);
            for (size_t i = 0; i < n; i++) {
                wrapper_->template add_array_data_by_single_segment_writer(
                    arr[i].data(), arr[i].size());
            }
        } else {
            wrapper_->add_data_by_single_segment_writer<T>(
                static_cast<const T*>(values), n);
        }
    }
    wrapper_->create_reader(milvus::index::SetBitsetSealed);
    finish();
    wrapper_->reload();
    ComputeByteSize();
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
        null_offset_.reserve(total);
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
            // Generally, we will not build inverted index with single segment except for building index
            // for query node with older version(2.4). See more comments above `inverted_index_single_segment_`.
            if (!inverted_index_single_segment_) {
                int64_t offset = 0;
                if (schema_.nullable()) {
                    for (const auto& data : field_datas) {
                        auto n = data->get_num_rows();
                        for (int i = 0; i < n; i++) {
                            if (!data->is_valid(i)) {
                                null_offset_.push_back(offset);
                            }
                            wrapper_->add_array_data<T>(
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
            } else {
                for (const auto& data : field_datas) {
                    auto n = data->get_num_rows();
                    if (schema_.nullable()) {
                        for (int i = 0; i < n; i++) {
                            if (!data->is_valid(i)) {
                                null_offset_.push_back(i);
                            }
                            wrapper_
                                ->add_array_data_by_single_segment_writer<T>(
                                    static_cast<const T*>(data->RawValue(i)),
                                    data->is_valid(i));
                        }
                        continue;
                    }
                    wrapper_->add_data_by_single_segment_writer<T>(
                        static_cast<const T*>(data->Data()), n);
                }
            }
            break;
        }

        case proto::schema::DataType::Array: {
            if (is_nested_index_) {
                build_index_for_array_nested(field_datas);
            } else {
                build_index_for_array(field_datas);
            }
            break;
        }

        case proto::schema::DataType::JSON: {
            build_index_for_json(field_datas);
            break;
        }

        default:
            ThrowInfo(ErrorCode::NotImplemented,
                      fmt::format("Inverted index not supported on {}",
                                  schema_.data_type()));
    }
}

template <typename T>
void
InvertedIndexTantivy<T>::build_index_for_array(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas) {
    using ElementType = std::conditional_t<std::is_same<T, int8_t>::value ||
                                               std::is_same<T, int16_t>::value,
                                           int32_t,
                                           T>;
    int64_t offset = 0;
    for (const auto& data : field_datas) {
        auto n = data->get_num_rows();
        auto array_column = static_cast<const Array*>(data->Data());
        for (int64_t i = 0; i < n; i++) {
            if (schema_.nullable() && !data->is_valid(i)) {
                null_offset_.push_back(offset);
            }
            auto length = data->is_valid(i) ? array_column[i].length() : 0;
            if (!inverted_index_single_segment_) {
                wrapper_->template add_array_data(
                    reinterpret_cast<const ElementType*>(
                        array_column[i].data()),
                    length,
                    offset++);
            } else {
                wrapper_->template add_array_data_by_single_segment_writer(
                    reinterpret_cast<const ElementType*>(
                        array_column[i].data()),
                    length);
                offset++;
            }
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
            if (schema_.nullable() && !data->is_valid(i)) {
                null_offset_.push_back(offset);
            } else {
                Assert(IsStringDataType(array_column[i].get_element_type()));
                Assert(IsStringDataType(
                    static_cast<DataType>(schema_.element_type())));
            }
            std::vector<std::string> output;
            for (int64_t j = 0; j < array_column[i].length(); j++) {
                output.push_back(
                    array_column[i].template get_data<std::string>(j));
            }
            auto length = data->is_valid(i) ? output.size() : 0;
            if (!inverted_index_single_segment_) {
                wrapper_->template add_array_data(
                    output.data(), length, offset++);
            } else {
                wrapper_->template add_array_data_by_single_segment_writer(
                    output.data(), length);
            }
        }
    }
}

template <typename T>
void
InvertedIndexTantivy<T>::build_index_for_array_nested(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas) {
    using ElementType = std::conditional_t<std::is_same<T, int8_t>::value ||
                                               std::is_same<T, int16_t>::value,
                                           int32_t,
                                           T>;

    int64_t offset = 0;
    int64_t row_offset = 0;
    for (const auto& data : field_datas) {
        auto n = data->get_num_rows();
        auto array_column = static_cast<const Array*>(data->Data());
        for (int64_t i = 0; i < n; i++, row_offset++) {
            if (schema_.nullable() && !data->is_valid(i)) {
                // Record null row offset, no elements to add
                null_offset_.push_back(row_offset);
                continue;
            }
            auto length = array_column[i].length();
            wrapper_->template add_data<ElementType>(
                reinterpret_cast<const ElementType*>(array_column[i].data()),
                length,
                offset);
            offset += length;
        }
    }
}

template <>
void
InvertedIndexTantivy<std::string>::build_index_for_array_nested(
    const std::vector<std::shared_ptr<FieldDataBase>>& field_datas) {
    int64_t offset = 0;
    int64_t row_offset = 0;
    for (const auto& data : field_datas) {
        auto n = data->get_num_rows();
        auto array_column = static_cast<const Array*>(data->Data());
        for (int64_t i = 0; i < n; i++, row_offset++) {
            if (schema_.nullable() && !data->is_valid(i)) {
                // Record null row offset, no elements to add
                null_offset_.push_back(row_offset);
                continue;
            }
            Assert(IsStringDataType(array_column[i].get_element_type()));
            Assert(IsStringDataType(
                static_cast<DataType>(schema_.element_type())));

            std::vector<std::string> output;
            auto length = array_column[i].length();
            output.reserve(length);
            for (int64_t j = 0; j < length; j++) {
                output.push_back(
                    array_column[i].template get_data<std::string>(j));
            }
            wrapper_->add_data(output.data(), length, offset);
            offset += length;
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
