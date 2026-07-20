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
#include <type_traits>
#include <vector>

#include "common/FastMem.h"
#include "common/FieldDataInterface.h"
#include "common/JsonCastFunction.h"
#include "common/JsonCastType.h"
#include "common/Slice.h"
#include "index/InvertedIndexTantivy.h"
#include "index/JsonIndexBuilder.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "log/Log.h"
#include "nlohmann/json.hpp"
#include "pb/common.pb.h"
#include "pb/schema.pb.h"
#include "storage/IndexEntryReader.h"
#include "storage/IndexEntryWriter.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/Types.h"
#include "storage/Util.h"

namespace milvus::index {

inline storage::FileManagerContext
MakeJsonCastContext(const storage::FileManagerContext& ctx,
                    const JsonCastType& cast_type) {
    auto modified = ctx;
    modified.fieldDataMeta.field_schema.set_data_type(
        static_cast<proto::schema::DataType>(cast_type.ToMilvusDataType()));
    // JSON path extraction produces nullable data (rows where path is missing
    // or cast fails are marked invalid), so the cast-type schema must be
    // nullable for base indexes to handle null_offset_ correctly.
    modified.fieldDataMeta.field_schema.set_nullable(true);
    return modified;
}

template <typename T, typename BaseIndex>
class JsonScalarIndexWrapper : public BaseIndex {
    static constexpr bool kIsInverted =
        std::is_base_of_v<InvertedIndexTantivy<T>, BaseIndex>;

 public:
    template <typename... Args>
    JsonScalarIndexWrapper(const JsonCastType& cast_type,
                           const std::string& nested_path,
                           const JsonCastFunction& cast_function,
                           const proto::schema::FieldSchema& json_schema,
                           const storage::FileManagerContext& original_ctx,
                           Args&&... args)
        : BaseIndex(std::forward<Args>(args)...,
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
        if constexpr (kIsInverted) {
            BuildInvertedWithJsonFieldData(field_datas);
        } else {
            auto result = ConvertJsonToTypedFieldData<T>(field_datas,
                                                         json_schema_,
                                                         nested_path_,
                                                         cast_type_,
                                                         cast_function_);
            non_exist_offsets_ = std::move(result.non_exist_offsets);
            field_null_offsets_ = std::move(result.field_null_offsets);
            auto total_rows = result.field_data->get_num_rows();
            BaseIndex::BuildWithFieldData({result.field_data});
            BuildExistsBitset(total_rows);
            BuildFieldValidityBitset(total_rows);
        }
    }

    void
    Build(const Config& config) override {
        auto json_field_datas =
            storage::CacheRawDataAndFillMissing(json_file_manager_, config);

        if constexpr (kIsInverted) {
            BuildInvertedWithJsonFieldData(json_field_datas);
        } else {
            auto result = ConvertJsonToTypedFieldData<T>(json_field_datas,
                                                         json_schema_,
                                                         nested_path_,
                                                         cast_type_,
                                                         cast_function_);

            non_exist_offsets_ = std::move(result.non_exist_offsets);
            field_null_offsets_ = std::move(result.field_null_offsets);
            auto total_rows = result.field_data->get_num_rows();
            BaseIndex::BuildWithFieldData({result.field_data});
            BuildExistsBitset(total_rows);
            BuildFieldValidityBitset(total_rows);
        }
    }

    // Returns a bitmap indicating which rows have the indexed JSON path
    // present. Returned by value (the copy cost matches the caller-side
    // clone that would otherwise be required because TargetBitmap has a
    // deleted copy constructor, and the caller typically wraps the result
    // in a shared_ptr anyway).
    TargetBitmap
    Exists() override {
        return exists_bitset_.clone();
    }

    // v2 format: serialize non_exist_offsets (and null_offset for inverted).
    BinarySet
    Serialize(const Config& config) override {
        if constexpr (kIsInverted) {
            std::shared_lock<folly::SharedMutex> lock(this->mutex_);
            BinarySet res_set;
            auto null_len = this->null_offset_.size() * sizeof(size_t);
            if (null_len > 0) {
                std::shared_ptr<uint8_t[]> null_data(new uint8_t[null_len]);
                milvus::fastmem::FastMemcpy(
                    null_data.get(), this->null_offset_.data(), null_len);
                res_set.Append(
                    INDEX_NULL_OFFSET_FILE_NAME, null_data, null_len);
            }
            auto ne_len = non_exist_offsets_.size() * sizeof(size_t);
            if (ne_len > 0) {
                std::shared_ptr<uint8_t[]> ne_data(new uint8_t[ne_len]);
                milvus::fastmem::FastMemcpy(
                    ne_data.get(), non_exist_offsets_.data(), ne_len);
                res_set.Append(
                    INDEX_NON_EXIST_OFFSET_FILE_NAME, ne_data, ne_len);
            }
            AppendFieldNullOffsets(res_set);
            lock.unlock();
            milvus::Disassemble(res_set);
            return res_set;
        } else {
            // Legacy V2 scalar indexes serialize their own entries before
            // returning the BinarySet. Append the JSON column-validity
            // sidecar as a separate entry so nullable index-only execution is
            // still self-contained when scalar_index_engine_version < 3.
            auto res_set = BaseIndex::Serialize(config);
            AppendNonExistOffsets(res_set);
            AppendFieldNullOffsets(res_set);
            return res_set;
        }
    }

    // V3 format: write non_exist_offsets on top of base entries. This is the
    // sealed-segment entry point, called after the tantivy index is ready —
    // safe to eagerly compute the exists bitmap.
    void
    WriteEntries(storage::IndexEntryWriter* writer) override {
        BaseIndex::WriteEntries(writer);

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
        BaseIndex::LoadEntries(reader, config);

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
        LOG_INFO("LoadEntries JsonScalarIndexWrapper done, has_non_exist: {}",
                 has_non_exist);
        // BaseIndex::LoadEntries has fully initialized the base index, so
        // Count() is safe to call and we can eagerly build the exists bitmap.
        BuildExistsBitset(this->Count());
        BuildFieldValidityBitset(this->Count());
    }

    void
    Load(const BinarySet& binary_set, const Config& config = {}) override {
        if constexpr (!kIsInverted) {
            non_exist_offsets_.clear();
            if (binary_set.Contains(INDEX_NON_EXIST_OFFSET_FILE_NAME)) {
                auto data =
                    binary_set.GetByName(INDEX_NON_EXIST_OFFSET_FILE_NAME);
                DecodeNonExistOffsets(data->data.get(), data->size);
            }
            field_validity_available_ = false;
            if (binary_set.Contains(INDEX_FIELD_NULL_OFFSET_FILE_NAME)) {
                auto data =
                    binary_set.GetByName(INDEX_FIELD_NULL_OFFSET_FILE_NAME);
                DecodeFieldNullOffsets(data->data.get(), data->size);
            }
        }
        BaseIndex::Load(binary_set, config);
        BuildExistsBitset(this->Count());
        BuildFieldValidityBitset(this->Count());
    }

    // v2 format: override Load() to defer the eager exists bitmap build
    // until after the base Load finishes. LoadIndexMetas (called from within
    // base Load) runs before the tantivy reader is initialized, so we can
    // only safely compute Count() here, not inside LoadIndexMetas.
    void
    Load(milvus::tracer::TraceContext ctx, const Config& config = {}) override {
        if constexpr (!kIsInverted) {
            auto index_files = GetValueFromConfig<std::vector<std::string>>(
                config, INDEX_FILES);
            AssertInfo(index_files.has_value(),
                       "index files are required to load JSON field validity");
            LoadNonExistOffsets(*index_files, config);
            LoadFieldNullOffsets(*index_files, config);
        }
        BaseIndex::Load(ctx, config);
        BuildExistsBitset(this->Count());
        BuildFieldValidityBitset(this->Count());
    }

    std::optional<TargetBitmap>
    FieldIsNotNull(milvus::OpContext* op_ctx = nullptr) override {
        (void)op_ctx;
        if (!field_validity_available_) {
            return std::nullopt;
        }
        return field_validity_bitset_.clone();
    }

    JsonCastType
    GetCastType() const override {
        return cast_type_;
    }

    // ----------------------------------------------------------------
    // InvertedIndexTantivy-specific methods.
    // When BaseIndex is not InvertedIndexTantivy, these are dead code.
    // ----------------------------------------------------------------

    // Expose finish/create_reader for tests that build indexes directly
    // (production code calls these via Upload/Load).
    template <typename B = BaseIndex>
    std::enable_if_t<std::is_base_of_v<InvertedIndexTantivy<T>, B>>
    finish() {
        this->wrapper_->finish();
    }

    template <typename B = BaseIndex>
    std::enable_if_t<std::is_base_of_v<InvertedIndexTantivy<T>, B>>
    create_reader(SetBitsetFn set_bitset) {
        this->wrapper_->create_reader(set_bitset);
    }

    // v2: load non_exist_offsets from index files, with v2.5.x fallback.
    // Called by InvertedIndexTantivy::Load() before the tantivy reader is
    // initialized, so we defer the exists bitmap build to our Load() override.
    void
    LoadIndexMetas(const std::vector<std::string>& index_files,
                   const Config& config) {
        if constexpr (kIsInverted) {
            InvertedIndexTantivy<T>::LoadIndexMetas(index_files, config);

            LoadFieldNullOffsets(index_files, config);

            auto fill = [&](const uint8_t* data, int64_t size) {
                non_exist_offsets_.resize((size_t)size / sizeof(size_t));
                milvus::fastmem::FastMemcpy(
                    non_exist_offsets_.data(), data, (size_t)size);
            };

            auto load_priority =
                GetValueFromConfig<milvus::proto::common::LoadPriority>(
                    config, milvus::LOAD_PRIORITY)
                    .value_or(milvus::proto::common::LoadPriority::HIGH);

            // Try exact file name first
            auto it = std::find_if(
                index_files.begin(),
                index_files.end(),
                [](const std::string& f) {
                    return boost::filesystem::path(f).filename().string() ==
                           INDEX_NON_EXIST_OFFSET_FILE_NAME;
                });
            if (it != index_files.end()) {
                auto datas = this->file_manager_->LoadIndexToMemory(
                    {*it}, load_priority);
                auto& d = datas.at(INDEX_NON_EXIST_OFFSET_FILE_NAME);
                fill(d->PayloadData(), d->PayloadSize());
                return;
            }

            // Try sliced files
            std::vector<std::string> sliced;
            std::optional<std::string> slice_meta_file;
            for (auto& f : index_files) {
                auto name = boost::filesystem::path(f).filename().string();
                if (name.find(INDEX_NON_EXIST_OFFSET_FILE_NAME) !=
                    std::string::npos) {
                    sliced.push_back(f);
                }
                if (name == INDEX_FILE_SLICE_META) {
                    slice_meta_file = f;
                }
            }
            if (!sliced.empty()) {
                AssertInfo(
                    slice_meta_file.has_value(),
                    "non_exist_offset slices found but _meta_slice is missing");
                sliced.push_back(slice_meta_file.value());
                auto datas = this->file_manager_->LoadIndexToMemory(
                    sliced, load_priority);
                auto slice_meta = std::move(datas.at(INDEX_FILE_SLICE_META));
                auto non_exist_codecs =
                    CompactIndexDatasByKey(INDEX_NON_EXIST_OFFSET_FILE_NAME,
                                           std::move(slice_meta),
                                           datas);
                auto non_exist_codec = AssembleIndexDataCodec(non_exist_codecs);
                fill(non_exist_codec->PayloadData(),
                     non_exist_codec->PayloadSize());
                return;
            }

            // Fallback: v2.5.x data — use null_offset_ as non_exist_offsets_
            non_exist_offsets_ = this->null_offset_;
        }
    }

    nlohmann::json
    BuildTantivyMeta(const std::vector<std::string>& file_names,
                     bool has_null) {
        if constexpr (kIsInverted) {
            auto meta =
                InvertedIndexTantivy<T>::BuildTantivyMeta(file_names, has_null);
            std::shared_lock<folly::SharedMutex> lock(this->mutex_);
            meta["has_non_exist"] = !non_exist_offsets_.empty();
            return meta;
        } else {
            return {};
        }
    }

    void
    RetainTantivyIndexFiles(std::vector<std::string>& index_files) {
        if constexpr (kIsInverted) {
            index_files.erase(
                std::remove_if(
                    index_files.begin(),
                    index_files.end(),
                    [](const std::string& f) {
                        return boost::filesystem::path(f)
                                       .filename()
                                       .string()
                                       .find(
                                           INDEX_NON_EXIST_OFFSET_FILE_NAME) !=
                                   std::string::npos ||
                               boost::filesystem::path(f)
                                       .filename()
                                       .string()
                                       .find(
                                           INDEX_FIELD_NULL_OFFSET_FILE_NAME) !=
                                   std::string::npos;
                    }),
                index_files.end());
            InvertedIndexTantivy<T>::RetainTantivyIndexFiles(index_files);
        }
    }

 protected:
    template <typename B = BaseIndex>
    std::enable_if_t<std::is_base_of_v<InvertedIndexTantivy<T>, B>>
    BuildInvertedWithJsonFieldData(
        const std::vector<FieldDataPtr>& field_datas) {
        if (cast_type_.data_type() != JsonCastType::DataType::ARRAY) {
            auto result = ConvertJsonToTypedFieldData<T>(field_datas,
                                                         json_schema_,
                                                         nested_path_,
                                                         cast_type_,
                                                         cast_function_);
            non_exist_offsets_ = std::move(result.non_exist_offsets);
            field_null_offsets_ = std::move(result.field_null_offsets);
            auto total_rows = result.field_data->get_num_rows();
            BaseIndex::BuildWithFieldData({result.field_data});
            BuildExistsBitset(total_rows);
            BuildFieldValidityBitset(total_rows);
            return;
        }

        int64_t total_rows = 0;
        for (const auto& data : field_datas) {
            total_rows += data->get_num_rows();
        }

        ProcessJsonFieldData<T>(
            field_datas,
            json_schema_,
            nested_path_,
            cast_type_,
            cast_function_,
            [this](const T* data, int64_t size, int64_t offset) {
                if (!this->inverted_index_single_segment_) {
                    this->wrapper_->template add_array_data<T>(
                        data, size, offset);
                } else {
                    this->wrapper_
                        ->template add_array_data_by_single_segment_writer<T>(
                            data, size);
                }
            },
            [this](int64_t offset) {
                this->null_offset_.push_back(offset);
                field_null_offsets_.push_back(offset);
            },
            [this](int64_t offset) { non_exist_offsets_.push_back(offset); },
            [](const Json&, const std::string&, simdjson::error_code) {});

        BuildExistsBitset(total_rows);
        BuildFieldValidityBitset(total_rows);
    }

    // Build the exists bitmap. The caller must supply the total row count
    // explicitly at build time because InvertedIndexTantivy::Count() requires
    // finish()+create_reader() which haven't been called yet during
    // Build/BuildWithFieldData.
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

    void
    AppendNonExistOffsets(BinarySet& binary_set) const {
        if (non_exist_offsets_.empty()) {
            return;
        }
        auto byte_size = non_exist_offsets_.size() * sizeof(size_t);
        std::shared_ptr<uint8_t[]> bytes(new uint8_t[byte_size]);
        milvus::fastmem::FastMemcpy(
            bytes.get(), non_exist_offsets_.data(), byte_size);
        binary_set.Append(INDEX_NON_EXIST_OFFSET_FILE_NAME, bytes, byte_size);
    }

    void
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
        auto load_priority =
            GetValueFromConfig<milvus::proto::common::LoadPriority>(
                config, milvus::LOAD_PRIORITY)
                .value_or(milvus::proto::common::LoadPriority::HIGH);
        auto exact = std::find_if(
            index_files.begin(), index_files.end(), [](const std::string& f) {
                return boost::filesystem::path(f).filename().string() ==
                       INDEX_FIELD_NULL_OFFSET_FILE_NAME;
            });
        if (exact != index_files.end()) {
            auto datas =
                this->file_manager_->LoadIndexToMemory({*exact}, load_priority);
            auto& d = datas.at(INDEX_FIELD_NULL_OFFSET_FILE_NAME);
            DecodeFieldNullOffsets(d->PayloadData(), d->PayloadSize());
            return;
        }

        std::vector<std::string> sliced;
        std::optional<std::string> slice_meta_file;
        for (const auto& f : index_files) {
            auto name = boost::filesystem::path(f).filename().string();
            if (name.find(INDEX_FIELD_NULL_OFFSET_FILE_NAME) !=
                std::string::npos) {
                sliced.push_back(f);
            }
            if (name == INDEX_FILE_SLICE_META) {
                slice_meta_file = f;
            }
        }
        if (sliced.empty()) {
            field_validity_available_ = false;
            return;
        }
        AssertInfo(slice_meta_file.has_value(),
                   "JSON field-null slices found but _meta_slice is missing");
        sliced.push_back(*slice_meta_file);
        auto datas =
            this->file_manager_->LoadIndexToMemory(sliced, load_priority);
        auto slice_meta = std::move(datas.at(INDEX_FILE_SLICE_META));
        auto codecs = CompactIndexDatasByKey(
            INDEX_FIELD_NULL_OFFSET_FILE_NAME, std::move(slice_meta), datas);
        auto assembled = AssembleIndexDataCodec(codecs);
        DecodeFieldNullOffsets(assembled->PayloadData(),
                               assembled->PayloadSize());
    }

    std::vector<size_t> non_exist_offsets_;
    std::vector<size_t> field_null_offsets_;
    TargetBitmap exists_bitset_;
    TargetBitmap field_validity_bitset_;
    bool field_validity_available_{true};

 private:
    JsonCastType cast_type_;
    std::string nested_path_;
    JsonCastFunction cast_function_;
    proto::schema::FieldSchema json_schema_;
    storage::MemFileManagerImplPtr json_file_manager_;
};

// JsonInvertedIndex<T> is a type alias for the wrapper over
// InvertedIndexTantivy<T>. It preserves the historical name and can be used
// interchangeably with the wrapper.
template <typename T>
using JsonInvertedIndex = JsonScalarIndexWrapper<T, InvertedIndexTantivy<T>>;

}  // namespace milvus::index
