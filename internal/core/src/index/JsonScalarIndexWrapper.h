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
#include "index/JsonIndexBuilder.h"
#include "log/Log.h"
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
    return modified;
}

// File name for serializing non_exist_offsets in JSON path indexes.
// Shared with JsonInvertedIndex which uses the same name.
constexpr const char* kJsonNonExistOffsetFileName =
    "json_index_non_exist_offsets";

template <typename T, typename BaseIndex>
class JsonScalarIndexWrapper : public BaseIndex {
 public:
    template <typename... Args>
    JsonScalarIndexWrapper(const JsonCastType& cast_type,
                           const std::string& nested_path,
                           const JsonCastFunction& cast_function,
                           const proto::schema::FieldSchema& json_schema,
                           const storage::FileManagerContext& original_ctx,
                           Args&&... args)
        : BaseIndex(MakeJsonCastContext(original_ctx, cast_type),
                    std::forward<Args>(args)...),
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
        BaseIndex::BuildWithFieldData(result.field_datas);
    }

    void
    Build(const Config& config) override {
        auto json_field_datas =
            storage::CacheRawDataAndFillMissing(json_file_manager_, config);

        auto result = ConvertJsonToTypedFieldData<T>(json_field_datas,
                                                     json_schema_,
                                                     nested_path_,
                                                     cast_type_,
                                                     cast_function_);

        non_exist_offsets_ = std::move(result.non_exist_offsets);
        BaseIndex::BuildWithFieldData(result.field_datas);
    }

    TargetBitmap
    Exists() override {
        int64_t count = this->Count();
        TargetBitmap bitset(count, true);
        auto end = std::lower_bound(non_exist_offsets_.begin(),
                                    non_exist_offsets_.end(),
                                    static_cast<size_t>(count));
        for (auto iter = non_exist_offsets_.begin(); iter != end; ++iter) {
            bitset.reset(*iter);
        }
        return bitset;
    }

    void
    WriteEntries(storage::IndexEntryWriter* writer) override {
        BaseIndex::WriteEntries(writer);

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
        BaseIndex::LoadEntries(reader, config);

        bool has_non_exist = reader.GetMeta<bool>("has_non_exist", false);
        if (has_non_exist) {
            auto e = reader.ReadEntry(kJsonNonExistOffsetFileName);
            non_exist_offsets_.resize(e.data.size() / sizeof(size_t));
            std::memcpy(
                non_exist_offsets_.data(), e.data.data(), e.data.size());
        }
        LOG_INFO("LoadEntries JsonScalarIndexWrapper done, has_non_exist: {}",
                 has_non_exist);
    }

    JsonCastType
    GetCastType() const override {
        return cast_type_;
    }

 private:
    JsonCastType cast_type_;
    std::string nested_path_;
    JsonCastFunction cast_function_;
    proto::schema::FieldSchema json_schema_;
    storage::MemFileManagerImplPtr json_file_manager_;
    std::vector<size_t> non_exist_offsets_;
};

}  // namespace milvus::index
