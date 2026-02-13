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

#include <string.h>
#include <cstdint>
#include <exception>
#include <map>
#include <set>
#include <utility>

#include "common/Array.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/FieldDataInterface.h"
#include "common/Tracer.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "index/BitmapIndex.h"
#include "index/HybridScalarIndex.h"
#include "index/InvertedIndexTantivy.h"
#include "index/Meta.h"
#include "index/ScalarIndex.h"
#include "index/ScalarIndexSort.h"
#include "index/StringIndexSort.h"
#include "index/StringIndexMarisa.h"
#include "index/Utils.h"
#include "knowhere/binaryset.h"
#include "log/Log.h"
#include "nlohmann/json.hpp"
#include "pb/common.pb.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "storage/IndexEntryReader.h"
#include "storage/IndexEntryWriter.h"

namespace milvus {
namespace index {

template <typename T>
HybridScalarIndex<T>::HybridScalarIndex(
    uint32_t tantivy_index_version,
    const storage::FileManagerContext& file_manager_context)
    : ScalarIndex<T>(HYBRID_INDEX_TYPE),
      is_built_(false),
      tantivy_index_version_(tantivy_index_version),
      bitmap_index_cardinality_limit_(
          DEFAULT_HYBRID_INDEX_BITMAP_CARDINALITY_LIMIT),
      low_cardinality_index_type_(ScalarIndexType::BITMAP),
      high_cardinality_index_type_(ScalarIndexType::STLSORT),
      file_manager_context_(file_manager_context) {
    if (file_manager_context.Valid()) {
        this->file_manager_ =
            std::make_shared<storage::MemFileManagerImpl>(file_manager_context);
        AssertInfo(this->file_manager_ != nullptr,
                   "create file manager failed!");
    }
    field_type_ = file_manager_context.fieldDataMeta.field_schema.data_type();
    internal_index_type_ = ScalarIndexType::NONE;
}

template <typename T>
ScalarIndexType
HybridScalarIndex<T>::SelectIndexTypeByCardinality(size_t cardinality) {
    if (cardinality >= bitmap_index_cardinality_limit_) {
        if (high_cardinality_index_type_ == ScalarIndexType::NONE) {
            // Legacy mode: use type-dependent selection
            if constexpr (std::is_same_v<T, std::string>) {
                internal_index_type_ = ScalarIndexType::INVERTED;
            } else if constexpr (std::is_integral_v<T>) {
                internal_index_type_ = ScalarIndexType::STLSORT;
            } else {
                internal_index_type_ = ScalarIndexType::INVERTED;
            }
        } else {
            internal_index_type_ = high_cardinality_index_type_;
        }
    } else {
        internal_index_type_ = low_cardinality_index_type_;
    }
    return internal_index_type_;
}

template <typename T>
ScalarIndexType
HybridScalarIndex<T>::SelectIndexBuildType(size_t n, const T* values) {
    std::set<T> distinct_vals;
    for (size_t i = 0; i < n; i++) {
        distinct_vals.insert(values[i]);
        if (distinct_vals.size() >= bitmap_index_cardinality_limit_) {
            break;
        }
    }
    return SelectIndexTypeByCardinality(distinct_vals.size());
}

template <typename T>
ScalarIndexType
HybridScalarIndex<T>::SelectBuildTypeForPrimitiveType(
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
    return SelectIndexTypeByCardinality(distinct_vals.size());
}

template <typename T>
ScalarIndexType
HybridScalarIndex<T>::SelectBuildTypeForArrayType(
    const std::vector<FieldDataPtr>& field_datas) {
    std::set<T> distinct_vals;
    for (const auto& data : field_datas) {
        auto slice_row_num = data->get_num_rows();
        for (size_t i = 0; i < slice_row_num; ++i) {
            auto array =
                reinterpret_cast<const milvus::Array*>(data->RawValue(i));
            for (size_t j = 0; j < array->length(); ++j) {
                auto val = array->template get_data<T>(j);
                distinct_vals.insert(val);

                // Limit the bitmap index cardinality because of memory usage
                if (distinct_vals.size() > bitmap_index_cardinality_limit_) {
                    break;
                }
            }
        }
    }
    // For array types, always use BITMAP for low cardinality and INVERTED for high cardinality.
    // These are hardcoded because STL_SORT doesn't support arrays, and BITMAP is the only
    // suitable choice for low cardinality arrays. Config parameters don't apply to arrays.
    if (distinct_vals.size() >= bitmap_index_cardinality_limit_) {
        internal_index_type_ = ScalarIndexType::INVERTED;
    } else {
        internal_index_type_ = ScalarIndexType::BITMAP;
    }
    return internal_index_type_;
}

template <typename T>
ScalarIndexType
HybridScalarIndex<T>::SelectIndexBuildType(
    const std::vector<FieldDataPtr>& field_datas) {
    std::set<T> distinct_vals;
    if (IsPrimitiveType(field_type_)) {
        return SelectBuildTypeForPrimitiveType(field_datas);
    } else if (IsArrayType(field_type_)) {
        return SelectBuildTypeForArrayType(field_datas);
    } else {
        ThrowInfo(Unsupported,
                  fmt::format("unsupported build index for type {}",
                              DataType_Name(field_type_)));
    }
}

template <typename T>
std::shared_ptr<ScalarIndex<T>>
HybridScalarIndex<T>::GetInternalIndex() {
    if (internal_index_ != nullptr) {
        return internal_index_;
    }
    if (internal_index_type_ == ScalarIndexType::BITMAP) {
        internal_index_ =
            std::make_shared<BitmapIndex<T>>(this->file_manager_context_);
    } else if (internal_index_type_ == ScalarIndexType::STLSORT) {
        internal_index_ =
            std::make_shared<ScalarIndexSort<T>>(this->file_manager_context_);
    } else if (internal_index_type_ == ScalarIndexType::INVERTED) {
        internal_index_ = std::make_shared<InvertedIndexTantivy<T>>(
            tantivy_index_version_, this->file_manager_context_);
    } else {
        ThrowInfo(UnexpectedError,
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

    if (internal_index_type_ == ScalarIndexType::BITMAP) {
        internal_index_ = std::make_shared<BitmapIndex<std::string>>(
            this->file_manager_context_);
    } else if (internal_index_type_ == ScalarIndexType::MARISA) {
        internal_index_ =
            std::make_shared<StringIndexMarisa>(this->file_manager_context_);
    } else if (internal_index_type_ == ScalarIndexType::STLSORT) {
        internal_index_ =
            std::make_shared<StringIndexSort>(this->file_manager_context_);
    } else if (internal_index_type_ == ScalarIndexType::INVERTED) {
        internal_index_ = std::make_shared<InvertedIndexTantivy<std::string>>(
            tantivy_index_version_, this->file_manager_context_);
    } else {
        ThrowInfo(UnexpectedError,
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

    // Check scalar index version for compatibility
    auto scalar_index_version =
        GetValueFromConfig<int32_t>(config, SCALAR_INDEX_ENGINE_VERSION)
            .value_or(kLastVersionWithoutHybridIndexConfig);

    if (scalar_index_version >= kHybridIndexConfigVersion) {
        // Version 3+: Use configurable index types
        low_cardinality_index_type_ =
            GetHybridLowCardinalityIndexTypeFromConfig(config);
        high_cardinality_index_type_ =
            GetHybridHighCardinalityIndexTypeFromConfig(config);
    } else {
        // Version <= 2: Use legacy defaults
        // Low cardinality always uses BITMAP
        low_cardinality_index_type_ = ScalarIndexType::BITMAP;
        // High cardinality uses NONE as sentinel to trigger legacy type-dependent logic
        // in SelectIndexBuildType (STLSORT for integral, INVERTED for others)
        high_cardinality_index_type_ = ScalarIndexType::NONE;
    }

    LOG_INFO(
        "config bitmap cardinality limit to {}, low cardinality index type: "
        "{}, high cardinality index type: {}, scalar_index_version: {}",
        bitmap_index_cardinality_limit_,
        ToString(low_cardinality_index_type_),
        ToString(high_cardinality_index_type_),
        scalar_index_version);
    auto field_datas =
        storage::CacheRawDataAndFillMissing(this->file_manager_, config);

    SelectIndexBuildType(field_datas);
    BuildInternal(field_datas);
    auto index_meta = this->file_manager_context_.indexMeta;
    LOG_INFO(
        "build hybrid index with internal index:{}, for segment_id:{}, "
        "field_id:{}",
        ToString(internal_index_type_),
        index_meta.segment_id,
        index_meta.field_id);
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
HybridScalarIndex<T>::SerializeIndexType() {
    // Add index type info to storage for future restruct index
    BinarySet index_binary_set;
    std::shared_ptr<uint8_t[]> index_type_buf(new uint8_t[sizeof(uint8_t)]);
    index_type_buf[0] = static_cast<uint8_t>(internal_index_type_);
    index_binary_set.Append(index::INDEX_TYPE, index_type_buf, sizeof(uint8_t));
    this->file_manager_->AddFile(index_binary_set);

    auto remote_paths_to_size = this->file_manager_->GetRemotePathsToFileSize();
    BinarySet ret_set;
    Assert(remote_paths_to_size.size() == 1);
    for (auto& file : remote_paths_to_size) {
        ret_set.Append(file.first, nullptr, file.second);
    }
    return ret_set;
}

template <typename T>
IndexStatsPtr
HybridScalarIndex<T>::Upload(const Config& config) {
    if (kScalarIndexUseV3) {
        return this->UploadV3(config);
    }
    auto internal_index = GetInternalIndex();
    auto index_ret = internal_index->Upload(config);

    auto index_type_ret = SerializeIndexType();

    for (auto& [key, value] : index_type_ret.binary_map_) {
        index_ret->AppendSerializedIndexFileInfo(
            SerializedIndexFileInfo(key, value->size));
    }
    return index_ret;
}

template <typename T>
void
HybridScalarIndex<T>::DeserializeIndexType(const BinarySet& binary_set) {
    uint8_t index_type;
    auto index_type_buffer = binary_set.GetByName(INDEX_TYPE);
    memcpy(&index_type, index_type_buffer->data.get(), index_type_buffer->size);
    internal_index_type_ = static_cast<ScalarIndexType>(index_type);
}

template <typename T>
std::string
HybridScalarIndex<T>::GetRemoteIndexTypeFile(
    const std::vector<std::string>& files) {
    std::string ret;
    for (auto& file : files) {
        auto file_name = file.substr(file.find_last_of('/') + 1);
        if (file_name == index::INDEX_TYPE) {
            ret = file;
        }
    }
    AssertInfo(!ret.empty(), "index type file not found for hybrid index");
    return ret;
}

template <typename T>
void
HybridScalarIndex<T>::Load(const BinarySet& binary_set, const Config& config) {
    DeserializeIndexType(binary_set);

    auto index = GetInternalIndex();
    LOG_INFO("load hybrid index with internal index:{}",
             ToString(internal_index_type_));
    index->Load(binary_set, config);

    is_built_ = true;
    ComputeByteSize();
}

template <typename T>
void
HybridScalarIndex<T>::Load(milvus::tracer::TraceContext ctx,
                           const Config& config) {
    if (kScalarIndexUseV3) {
        this->LoadV3(config);
        return;
    }
    auto index_files =
        GetValueFromConfig<std::vector<std::string>>(config, "index_files");
    AssertInfo(index_files.has_value(),
               "index file paths is empty when load hybrid index");

    auto index_type_file = GetRemoteIndexTypeFile(index_files.value());

    auto load_priority =
        GetValueFromConfig<milvus::proto::common::LoadPriority>(
            config, milvus::LOAD_PRIORITY)
            .value_or(milvus::proto::common::LoadPriority::HIGH);
    auto index_datas = this->file_manager_->LoadIndexToMemory(
        std::vector<std::string>{index_type_file}, load_priority);
    BinarySet binary_set;
    AssembleIndexDatas(index_datas, binary_set);
    // clear index_datas to free memory early
    index_datas.clear();
    DeserializeIndexType(binary_set);

    auto index = GetInternalIndex();
    LOG_INFO("load hybrid index with internal index:{}",
             ToString(internal_index_type_));
    index->Load(ctx, config);

    is_built_ = true;
    ComputeByteSize();
}

template <typename T>
void
HybridScalarIndex<T>::WriteEntries(storage::IndexEntryWriter* writer) {
    AssertInfo(is_built_, "index has not been built yet");

    // Write internal index entries (which adds internal meta via PutMeta)
    internal_index_->WriteEntries(writer);

    // Add hybrid-level meta
    writer->PutMeta("index_type", static_cast<uint8_t>(internal_index_type_));

    LOG_INFO("write hybrid index entries with internal index type: {}",
             ToString(internal_index_type_));
}

template <typename T>
void
HybridScalarIndex<T>::LoadEntries(storage::IndexEntryReader& reader,
                                  const Config& config) {
    internal_index_type_ =
        static_cast<ScalarIndexType>(reader.GetMeta<uint8_t>("index_type"));

    LOG_INFO("LoadEntries hybrid index with internal index type: {}",
             ToString(internal_index_type_));

    auto index = GetInternalIndex();
    index->LoadEntries(reader, config);

    is_built_ = true;
    ComputeByteSize();
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
