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
#include "storage/LocalChunkManagerSingleton.h"
#include "index/InvertedIndexTantivy.h"
#include "log/Log.h"
#include "index/Utils.h"
#include "storage/Util.h"

#include <boost/filesystem.hpp>

namespace milvus::index {
template <typename T>
InvertedIndexTantivy<T>::InvertedIndexTantivy(
    const TantivyConfig& cfg,
    const storage::FileManagerContext& ctx,
    std::shared_ptr<milvus_storage::Space> space)
    : cfg_(cfg), space_(space) {
    mem_file_manager_ = std::make_shared<MemFileManager>(ctx, ctx.space_);
    disk_file_manager_ = std::make_shared<DiskFileManager>(ctx, ctx.space_);
    auto field =
        std::to_string(disk_file_manager_->GetFieldDataMeta().field_id);
    auto prefix = disk_file_manager_->GetLocalIndexObjectPrefix();
    path_ = fmt::format("/tmp/{}", prefix);
    boost::filesystem::create_directories(path_);
    d_type_ = cfg_.to_tantivy_data_type();
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
    BinarySet res_set;

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

    return ret;
}

template <typename T>
BinarySet
InvertedIndexTantivy<T>::UploadV2(const Config& config) {
    return Upload(config);
}

template <typename T>
void
InvertedIndexTantivy<T>::Build(const Config& config) {
    auto insert_files =
        GetValueFromConfig<std::vector<std::string>>(config, "insert_files");
    AssertInfo(insert_files.has_value(), "insert_files were empty");
    auto field_datas =
        mem_file_manager_->CacheRawDataToMemory(insert_files.value());
    switch (cfg_.data_type_) {
        case DataType::BOOL: {
            for (const auto& data : field_datas) {
                auto n = data->get_num_rows();
                wrapper_->add_data<bool>(static_cast<const bool*>(data->Data()),
                                         n);
            }
            break;
        }

        case DataType::INT8: {
            for (const auto& data : field_datas) {
                auto n = data->get_num_rows();
                wrapper_->add_data<int8_t>(
                    static_cast<const int8_t*>(data->Data()), n);
            }
            break;
        }

        case DataType::INT16: {
            for (const auto& data : field_datas) {
                auto n = data->get_num_rows();
                wrapper_->add_data<int16_t>(
                    static_cast<const int16_t*>(data->Data()), n);
            }
            break;
        }

        case DataType::INT32: {
            for (const auto& data : field_datas) {
                auto n = data->get_num_rows();
                wrapper_->add_data<int32_t>(
                    static_cast<const int32_t*>(data->Data()), n);
            }
            break;
        }

        case DataType::INT64: {
            for (const auto& data : field_datas) {
                auto n = data->get_num_rows();
                wrapper_->add_data<int64_t>(
                    static_cast<const int64_t*>(data->Data()), n);
            }
            break;
        }

        case DataType::FLOAT: {
            for (const auto& data : field_datas) {
                auto n = data->get_num_rows();
                wrapper_->add_data<float>(
                    static_cast<const float*>(data->Data()), n);
            }
            break;
        }

        case DataType::DOUBLE: {
            for (const auto& data : field_datas) {
                auto n = data->get_num_rows();
                wrapper_->add_data<double>(
                    static_cast<const double*>(data->Data()), n);
            }
            break;
        }

        case DataType::VARCHAR: {
            for (const auto& data : field_datas) {
                auto n = data->get_num_rows();
                wrapper_->add_data<std::string>(
                    static_cast<const std::string*>(data->Data()), n);
            }
            break;
        }

        default:
            PanicInfo(ErrorCode::NotImplemented,
                      fmt::format("todo: not supported, {}", cfg_.data_type_));
    }
}

template <typename T>
void
InvertedIndexTantivy<T>::BuildV2(const Config& config) {
    auto field_name = mem_file_manager_->GetIndexMeta().field_name;
    auto res = space_->ScanData();
    if (!res.ok()) {
        PanicInfo(S3Error, "failed to create scan iterator");
    }
    auto reader = res.value();
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

    switch (cfg_.data_type_) {
        case DataType::BOOL: {
            for (const auto& data : field_datas) {
                auto n = data->get_num_rows();
                wrapper_->add_data<bool>(static_cast<const bool*>(data->Data()),
                                         n);
            }
            break;
        }

        case DataType::INT8: {
            for (const auto& data : field_datas) {
                auto n = data->get_num_rows();
                wrapper_->add_data<int8_t>(
                    static_cast<const int8_t*>(data->Data()), n);
            }
            break;
        }

        case DataType::INT16: {
            for (const auto& data : field_datas) {
                auto n = data->get_num_rows();
                wrapper_->add_data<int16_t>(
                    static_cast<const int16_t*>(data->Data()), n);
            }
            break;
        }

        case DataType::INT32: {
            for (const auto& data : field_datas) {
                auto n = data->get_num_rows();
                wrapper_->add_data<int32_t>(
                    static_cast<const int32_t*>(data->Data()), n);
            }
            break;
        }

        case DataType::INT64: {
            for (const auto& data : field_datas) {
                auto n = data->get_num_rows();
                wrapper_->add_data<int64_t>(
                    static_cast<const int64_t*>(data->Data()), n);
            }
            break;
        }

        case DataType::FLOAT: {
            for (const auto& data : field_datas) {
                auto n = data->get_num_rows();
                wrapper_->add_data<float>(
                    static_cast<const float*>(data->Data()), n);
            }
            break;
        }

        case DataType::DOUBLE: {
            for (const auto& data : field_datas) {
                auto n = data->get_num_rows();
                wrapper_->add_data<double>(
                    static_cast<const double*>(data->Data()), n);
            }
            break;
        }

        case DataType::VARCHAR: {
            for (const auto& data : field_datas) {
                auto n = data->get_num_rows();
                wrapper_->add_data<std::string>(
                    static_cast<const std::string*>(data->Data()), n);
            }
            break;
        }

        default:
            PanicInfo(ErrorCode::NotImplemented,
                      fmt::format("todo: not supported, {}", cfg_.data_type_));
    }
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
    disk_file_manager_->CacheIndexToDisk(index_files.value());
    wrapper_ = std::make_shared<TantivyIndexWrapper>(prefix.c_str());
}

template <typename T>
void
InvertedIndexTantivy<T>::LoadV2(const Config& config) {
    disk_file_manager_->CacheIndexToDisk();
    auto prefix = disk_file_manager_->GetLocalIndexObjectPrefix();
    wrapper_ = std::make_shared<TantivyIndexWrapper>(prefix.c_str());
}

inline void
apply_hits(TargetBitmap& bitset, const RustArrayWrapper& w, bool v) {
    for (size_t j = 0; j < w.array_.len; j++) {
        bitset[w.array_.array[j]] = v;
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
InvertedIndexTantivy<T>::NotIn(size_t n, const T* values) {
    TargetBitmap bitset(Count(), true);
    for (size_t i = 0; i < n; ++i) {
        auto array = wrapper_->term_query(values[i]);
        apply_hits(bitset, array, false);
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
            throw SegcoreError(OpTypeInvalid,
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

template class InvertedIndexTantivy<bool>;
template class InvertedIndexTantivy<int8_t>;
template class InvertedIndexTantivy<int16_t>;
template class InvertedIndexTantivy<int32_t>;
template class InvertedIndexTantivy<int64_t>;
template class InvertedIndexTantivy<float>;
template class InvertedIndexTantivy<double>;
template class InvertedIndexTantivy<std::string>;
}  // namespace milvus::index
