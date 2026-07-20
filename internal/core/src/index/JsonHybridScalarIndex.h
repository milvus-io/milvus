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
#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <vector>

#include <boost/filesystem.hpp>

#include "common/FastMem.h"
#include "common/FieldDataInterface.h"
#include "common/JsonCastFunction.h"
#include "common/JsonCastType.h"
#include "index/HybridScalarIndex.h"
#include "index/Utils.h"
#include "index/JsonIndexBuilder.h"
#include "index/JsonScalarIndexWrapper.h"
#include "log/Log.h"
#include "pb/schema.pb.h"
#include "storage/IndexEntryReader.h"
#include "storage/IndexEntryWriter.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/Util.h"

namespace milvus::index {

template <typename T>
class JsonHybridScalarIndex : public HybridScalarIndex<T> {
 public:
    JsonHybridScalarIndex(const JsonCastType& cast_type,
                          const std::string& nested_path,
                          const JsonCastFunction& cast_function,
                          const proto::schema::FieldSchema& json_schema,
                          uint32_t tantivy_index_version,
                          const storage::FileManagerContext& original_ctx)
        : HybridScalarIndex<T>(tantivy_index_version,
                               MakeJsonCastContext(original_ctx, cast_type)),
          cast_type_(cast_type),
          nested_path_(nested_path),
          cast_function_(cast_function),
          json_schema_(json_schema) {
        json_file_manager_ =
            std::make_shared<storage::MemFileManagerImpl>(original_ctx);
    }

    void
    BuildWithFieldData(const std::vector<FieldDataPtr>& field_datas) override {
        auto result = ConvertJsonToTypedFieldData<T>(field_datas,
                                                     json_schema_,
                                                     nested_path_,
                                                     cast_type_,
                                                     cast_function_);
        non_exist_offsets_ = std::move(result.non_exist_offsets);
        field_null_offsets_ = std::move(result.field_null_offsets);

        auto n = result.field_data->get_num_rows();
        int64_t total_rows = n;
        std::set<T> distinct_vals;
        for (size_t i = 0; i < n; ++i) {
            if (result.field_data->is_valid(i)) {
                auto val =
                    reinterpret_cast<const T*>(result.field_data->RawValue(i));
                distinct_vals.insert(*val);
                if (distinct_vals.size() >=
                    this->bitmap_index_cardinality_limit_) {
                    break;
                }
            }
        }
        this->SelectIndexTypeByCardinality(distinct_vals.size());
        this->BuildInternal({result.field_data});

        this->is_built_ = true;
        this->ComputeByteSize();
        BuildExistsBitset(total_rows);
        BuildFieldValidityBitset(total_rows);
    }

    void
    Build(const Config& config) override {
        if (this->is_built_) {
            return;
        }

        this->bitmap_index_cardinality_limit_ =
            GetBitmapCardinalityLimitFromConfig(config);

        auto scalar_index_version =
            GetValueFromConfig<int32_t>(config, SCALAR_INDEX_ENGINE_VERSION)
                .value_or(kLastVersionWithoutHybridIndexConfig);

        if (scalar_index_version >= kHybridIndexConfigVersion) {
            this->low_cardinality_index_type_ =
                GetHybridLowCardinalityIndexTypeFromConfig(config);
            this->high_cardinality_index_type_ =
                GetHybridHighCardinalityIndexTypeFromConfig(config);
        } else {
            this->low_cardinality_index_type_ = ScalarIndexType::BITMAP;
            this->high_cardinality_index_type_ = ScalarIndexType::NONE;
        }

        auto json_field_datas =
            storage::CacheRawDataAndFillMissing(json_file_manager_, config);

        auto result = ConvertJsonToTypedFieldData<T>(json_field_datas,
                                                     json_schema_,
                                                     nested_path_,
                                                     cast_type_,
                                                     cast_function_);

        non_exist_offsets_ = std::move(result.non_exist_offsets);
        field_null_offsets_ = std::move(result.field_null_offsets);

        auto n = result.field_data->get_num_rows();
        int64_t total_rows = n;
        // Do cardinality counting that respects is_valid(), unlike the base
        // class SelectBuildTypeForPrimitiveType which counts invalid rows too.
        std::set<T> distinct_vals;
        for (size_t i = 0; i < n; ++i) {
            if (result.field_data->is_valid(i)) {
                auto val =
                    reinterpret_cast<const T*>(result.field_data->RawValue(i));
                distinct_vals.insert(*val);
                if (distinct_vals.size() >=
                    this->bitmap_index_cardinality_limit_) {
                    break;
                }
            }
        }
        this->SelectIndexTypeByCardinality(distinct_vals.size());
        this->BuildInternal({result.field_data});

        this->is_built_ = true;
        this->ComputeByteSize();
        BuildExistsBitset(total_rows);
        BuildFieldValidityBitset(total_rows);
    }

    TargetBitmap
    Exists() override {
        return exists_bitset_.clone();
    }

    std::optional<TargetBitmap>
    FieldIsNotNull(milvus::OpContext* op_ctx = nullptr) override {
        (void)op_ctx;
        if (!field_validity_available_) {
            return std::nullopt;
        }
        return field_validity_bitset_.clone();
    }

    BinarySet
    Serialize(const Config& config) override {
        auto binary_set = HybridScalarIndex<T>::Serialize(config);
        AppendNonExistOffsets(binary_set);
        AppendFieldNullOffsets(binary_set);
        return binary_set;
    }

    IndexStatsPtr
    Upload(const Config& config = {}) override {
        auto stats = HybridScalarIndex<T>::Upload(config);

        BinarySet sidecars;
        auto sidecar_size =
            AppendNonExistOffsets(sidecars) + AppendFieldNullOffsets(sidecars);
        this->file_manager_->AddFile(sidecars);

        auto files = stats->GetSerializedIndexFileInfo();
        auto remote_files = this->file_manager_->GetRemotePathsToFileSize();
        for (const auto* sidecar_name : {INDEX_NON_EXIST_OFFSET_FILE_NAME,
                                         INDEX_FIELD_NULL_OFFSET_FILE_NAME}) {
            auto sidecar_file =
                std::find_if(remote_files.begin(),
                             remote_files.end(),
                             [sidecar_name](const auto& entry) {
                                 return boost::filesystem::path(entry.first)
                                            .filename()
                                            .string() == sidecar_name;
                             });
            if (sidecar_name == INDEX_NON_EXIST_OFFSET_FILE_NAME &&
                non_exist_offsets_.empty()) {
                continue;
            }
            AssertInfo(sidecar_file != remote_files.end(),
                       "uploaded JSON sidecar {} was not found",
                       sidecar_name);
            files.emplace_back(sidecar_file->first, sidecar_file->second);
        }
        return IndexStats::New(stats->GetMemSize() + sidecar_size,
                               std::move(files));
    }

    void
    Load(const BinarySet& binary_set, const Config& config = {}) override {
        non_exist_offsets_.clear();
        if (binary_set.Contains(INDEX_NON_EXIST_OFFSET_FILE_NAME)) {
            auto data = binary_set.GetByName(INDEX_NON_EXIST_OFFSET_FILE_NAME);
            DecodeNonExistOffsets(data->data.get(), data->size);
        }
        field_validity_available_ = false;
        if (binary_set.Contains(INDEX_FIELD_NULL_OFFSET_FILE_NAME)) {
            auto data = binary_set.GetByName(INDEX_FIELD_NULL_OFFSET_FILE_NAME);
            DecodeFieldNullOffsets(data->data.get(), data->size);
        }
        HybridScalarIndex<T>::Load(binary_set, config);
        BuildExistsBitset(this->Count());
        BuildFieldValidityBitset(this->Count());
    }

    void
    Load(milvus::tracer::TraceContext ctx, const Config& config = {}) override {
        auto index_files =
            GetValueFromConfig<std::vector<std::string>>(config, INDEX_FILES);
        AssertInfo(index_files.has_value(),
                   "index files are required to load JSON field validity");
        LoadNonExistOffsets(*index_files, config);
        LoadFieldNullOffsets(*index_files, config);
        HybridScalarIndex<T>::Load(ctx, config);
        BuildExistsBitset(this->Count());
        BuildFieldValidityBitset(this->Count());
    }

    void
    WriteEntries(storage::IndexEntryWriter* writer) override {
        HybridScalarIndex<T>::WriteEntries(writer);

        bool has_non_exist = !non_exist_offsets_.empty();
        writer->PutMeta("has_non_exist", has_non_exist);
        if (has_non_exist) {
            writer->WriteEntry(INDEX_NON_EXIST_OFFSET_FILE_NAME,
                               non_exist_offsets_.data(),
                               non_exist_offsets_.size() * sizeof(size_t));
        }
        auto field_null_data = EncodeFieldNullOffsets();
        writer->WriteEntry(INDEX_FIELD_NULL_OFFSET_FILE_NAME,
                           field_null_data.data(),
                           field_null_data.size() * sizeof(size_t));
    }

    void
    LoadEntries(storage::IndexEntryReader& reader,
                const Config& config) override {
        HybridScalarIndex<T>::LoadEntries(reader, config);

        bool has_non_exist = reader.GetMeta<bool>("has_non_exist", false);
        if (has_non_exist) {
            auto e = reader.ReadEntry(INDEX_NON_EXIST_OFFSET_FILE_NAME);
            non_exist_offsets_.resize(e.data.size() / sizeof(size_t));
            milvus::fastmem::FastMemcpy(
                non_exist_offsets_.data(), e.data.data(), e.data.size());
        }
        field_validity_available_ = false;
        if (reader.HasEntry(INDEX_FIELD_NULL_OFFSET_FILE_NAME)) {
            auto e = reader.ReadEntry(INDEX_FIELD_NULL_OFFSET_FILE_NAME);
            DecodeFieldNullOffsets(e.data.data(), e.data.size());
        }
        LOG_INFO("LoadEntries JsonHybridScalarIndex done, has_non_exist: {}",
                 has_non_exist);
        BuildExistsBitset(this->Count());
        BuildFieldValidityBitset(this->Count());
    }

    JsonCastType
    GetCastType() const override {
        return cast_type_;
    }

 private:
    void
    BuildExistsBitset(int64_t count) {
        exists_bitset_ = TargetBitmap(count, true);
        for (auto offset : non_exist_offsets_) {
            if (static_cast<int64_t>(offset) >= count) {
                break;
            }
            exists_bitset_.reset(offset);
        }
    }

    void
    BuildFieldValidityBitset(int64_t count) {
        if (!field_validity_available_) {
            field_validity_bitset_ = TargetBitmap();
            return;
        }
        field_validity_bitset_ = TargetBitmap(count, true);
        for (auto offset : field_null_offsets_) {
            if (static_cast<int64_t>(offset) >= count) {
                break;
            }
            field_validity_bitset_.reset(offset);
        }
    }

    std::vector<size_t>
    EncodeFieldNullOffsets() const {
        std::vector<size_t> encoded;
        encoded.reserve(field_null_offsets_.size() + 1);
        encoded.push_back(std::numeric_limits<size_t>::max());
        encoded.insert(encoded.end(),
                       field_null_offsets_.begin(),
                       field_null_offsets_.end());
        return encoded;
    }

    size_t
    AppendNonExistOffsets(BinarySet& binary_set) const {
        if (non_exist_offsets_.empty()) {
            return 0;
        }
        auto byte_size = non_exist_offsets_.size() * sizeof(size_t);
        std::shared_ptr<uint8_t[]> bytes(new uint8_t[byte_size]);
        milvus::fastmem::FastMemcpy(
            bytes.get(), non_exist_offsets_.data(), byte_size);
        binary_set.Append(INDEX_NON_EXIST_OFFSET_FILE_NAME, bytes, byte_size);
        return byte_size;
    }

    size_t
    AppendFieldNullOffsets(BinarySet& binary_set) const {
        auto field_null_data = EncodeFieldNullOffsets();
        auto field_null_len = field_null_data.size() * sizeof(size_t);
        std::shared_ptr<uint8_t[]> field_null_bytes(
            new uint8_t[field_null_len]);
        milvus::fastmem::FastMemcpy(
            field_null_bytes.get(), field_null_data.data(), field_null_len);
        binary_set.Append(INDEX_FIELD_NULL_OFFSET_FILE_NAME,
                          field_null_bytes,
                          field_null_len);
        return field_null_len;
    }

    void
    DecodeFieldNullOffsets(const uint8_t* data, size_t size) {
        AssertInfo(size >= sizeof(size_t) && size % sizeof(size_t) == 0,
                   "invalid JSON field-null sidecar size: {}",
                   size);
        size_t marker;
        milvus::fastmem::FastMemcpy(&marker, data, sizeof(size_t));
        AssertInfo(marker == std::numeric_limits<size_t>::max(),
                   "invalid JSON field-null sidecar marker");
        auto count = size / sizeof(size_t) - 1;
        field_null_offsets_.resize(count);
        if (count > 0) {
            milvus::fastmem::FastMemcpy(field_null_offsets_.data(),
                                        data + sizeof(size_t),
                                        count * sizeof(size_t));
        }
        field_validity_available_ = true;
    }

    void
    DecodeNonExistOffsets(const uint8_t* data, size_t size) {
        AssertInfo(size % sizeof(size_t) == 0,
                   "invalid JSON non-exist sidecar size: {}",
                   size);
        non_exist_offsets_.resize(size / sizeof(size_t));
        if (size > 0) {
            milvus::fastmem::FastMemcpy(non_exist_offsets_.data(), data, size);
        }
    }

    void
    LoadNonExistOffsets(const std::vector<std::string>& index_files,
                        const Config& config) {
        non_exist_offsets_.clear();
        auto file = std::find_if(
            index_files.begin(), index_files.end(), [](const std::string& f) {
                return boost::filesystem::path(f).filename().string() ==
                       INDEX_NON_EXIST_OFFSET_FILE_NAME;
            });
        if (file == index_files.end()) {
            return;
        }

        auto load_priority =
            GetValueFromConfig<milvus::proto::common::LoadPriority>(
                config, milvus::LOAD_PRIORITY)
                .value_or(milvus::proto::common::LoadPriority::HIGH);
        auto datas =
            this->file_manager_->LoadIndexToMemory({*file}, load_priority);
        auto& data = datas.at(INDEX_NON_EXIST_OFFSET_FILE_NAME);
        DecodeNonExistOffsets(data->PayloadData(), data->PayloadSize());
    }

    void
    LoadFieldNullOffsets(const std::vector<std::string>& index_files,
                         const Config& config) {
        field_validity_available_ = false;
        auto file = std::find_if(
            index_files.begin(), index_files.end(), [](const std::string& f) {
                return boost::filesystem::path(f).filename().string() ==
                       INDEX_FIELD_NULL_OFFSET_FILE_NAME;
            });
        if (file == index_files.end()) {
            return;
        }

        auto load_priority =
            GetValueFromConfig<milvus::proto::common::LoadPriority>(
                config, milvus::LOAD_PRIORITY)
                .value_or(milvus::proto::common::LoadPriority::HIGH);
        auto datas =
            this->file_manager_->LoadIndexToMemory({*file}, load_priority);
        auto& data = datas.at(INDEX_FIELD_NULL_OFFSET_FILE_NAME);
        DecodeFieldNullOffsets(data->PayloadData(), data->PayloadSize());
    }

    JsonCastType cast_type_;
    std::string nested_path_;
    JsonCastFunction cast_function_;
    proto::schema::FieldSchema json_schema_;
    storage::MemFileManagerImplPtr json_file_manager_;
    std::vector<size_t> non_exist_offsets_;
    std::vector<size_t> field_null_offsets_;
    TargetBitmap exists_bitset_;
    TargetBitmap field_validity_bitset_;
    bool field_validity_available_{true};
};

}  // namespace milvus::index
