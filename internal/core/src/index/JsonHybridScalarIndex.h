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
#include <string>
#include <vector>

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
    BuildWithFieldData(
        const std::vector<FieldDataPtr>& field_datas) override {
        auto result = ConvertJsonToTypedFieldData<T>(
            field_datas, json_schema_, nested_path_, cast_type_,
            cast_function_);
        non_exist_offsets_ = std::move(result.non_exist_offsets);

        std::set<T> distinct_vals;
        for (const auto& data : result.field_datas) {
            auto n = data->get_num_rows();
            for (size_t i = 0; i < n; ++i) {
                if (data->is_valid(i)) {
                    auto val =
                        reinterpret_cast<const T*>(data->RawValue(i));
                    distinct_vals.insert(*val);
                    if (distinct_vals.size() >=
                        this->bitmap_index_cardinality_limit_) {
                        break;
                    }
                }
            }
        }
        this->SelectIndexTypeByCardinality(distinct_vals.size());
        this->BuildInternal(result.field_datas);
        this->is_built_ = true;
        this->ComputeByteSize();
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

        // Do cardinality counting that respects is_valid(), unlike the base
        // class SelectBuildTypeForPrimitiveType which counts invalid rows too.
        std::set<T> distinct_vals;
        for (const auto& data : result.field_datas) {
            auto n = data->get_num_rows();
            for (size_t i = 0; i < n; ++i) {
                if (data->is_valid(i)) {
                    auto val = reinterpret_cast<const T*>(data->RawValue(i));
                    distinct_vals.insert(*val);
                    if (distinct_vals.size() >=
                        this->bitmap_index_cardinality_limit_) {
                        break;
                    }
                }
            }
        }
        this->SelectIndexTypeByCardinality(distinct_vals.size());
        this->BuildInternal(result.field_datas);

        this->is_built_ = true;
        this->ComputeByteSize();
        BuildExistsBitset();
    }

    const TargetBitmap&
    Exists() override {
        return exists_bitset_;
    }

    void
    WriteEntries(storage::IndexEntryWriter* writer) override {
        HybridScalarIndex<T>::WriteEntries(writer);

        bool has_non_exist = !non_exist_offsets_.empty();
        writer->PutMeta("has_non_exist", has_non_exist);
        if (has_non_exist) {
            writer->WriteEntry(kJsonNonExistOffsetFileName,
                               non_exist_offsets_.data(),
                               non_exist_offsets_.size() * sizeof(size_t));
        }
    }

    void
    LoadEntries(storage::IndexEntryReader& reader,
                const Config& config) override {
        HybridScalarIndex<T>::LoadEntries(reader, config);

        bool has_non_exist = reader.GetMeta<bool>("has_non_exist", false);
        if (has_non_exist) {
            auto e = reader.ReadEntry(kJsonNonExistOffsetFileName);
            non_exist_offsets_.resize(e.data.size() / sizeof(size_t));
            std::memcpy(
                non_exist_offsets_.data(), e.data.data(), e.data.size());
        }
        LOG_INFO("LoadEntries JsonHybridScalarIndex done, has_non_exist: {}",
                 has_non_exist);
        BuildExistsBitset();
    }

    JsonCastType
    GetCastType() const override {
        return cast_type_;
    }

 private:
    void
    BuildExistsBitset() {
        int64_t count = this->Count();
        exists_bitset_ = TargetBitmap(count, true);
        for (auto offset : non_exist_offsets_) {
            if (static_cast<int64_t>(offset) >= count) {
                break;
            }
            exists_bitset_.reset(offset);
        }
    }

    JsonCastType cast_type_;
    std::string nested_path_;
    JsonCastFunction cast_function_;
    proto::schema::FieldSchema json_schema_;
    storage::MemFileManagerImplPtr json_file_manager_;
    std::vector<size_t> non_exist_offsets_;
    TargetBitmap exists_bitset_;
};

}  // namespace milvus::index
