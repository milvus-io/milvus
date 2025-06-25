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

#pragma once

#include <memory>
#include <string_view>
#include <utility>

#include "common/JsonCastType.h"
#include "common/LoadInfo.h"
#include "common/Types.h"
#include "index/Index.h"
#include "index/JsonInvertedIndex.h"
#include "index/JsonFlatIndex.h"
#include "pb/segcore.pb.h"
#include "segcore/InsertRecord.h"
#include "segcore/SegmentInterface.h"
#include "segcore/Types.h"
#include "index/NgramInvertedIndex.h"

namespace milvus::segcore {

class SegmentSealed : public SegmentInternalInterface {
 public:
    virtual void
    LoadIndex(const LoadIndexInfo& info) = 0;
    virtual void
    LoadSegmentMeta(const milvus::proto::segcore::LoadSegmentMeta& meta) = 0;
    virtual void
    DropIndex(const FieldId field_id) = 0;
    virtual void
    DropFieldData(const FieldId field_id) = 0;

    virtual void
    AddFieldDataInfoForSealed(const LoadFieldDataInfo& field_data_info) = 0;
    virtual void
    RemoveFieldFile(const FieldId field_id) = 0;
    virtual void
    ClearData() = 0;
    virtual std::unique_ptr<DataArray>
    get_vector(FieldId field_id, const int64_t* ids, int64_t count) const = 0;

    virtual void
    LoadTextIndex(FieldId field_id,
                  std::unique_ptr<index::TextMatchIndex> index) = 0;

    virtual InsertRecord<true>&
    get_insert_record() = 0;

    virtual index::IndexBase*
    GetJsonIndex(FieldId field_id, std::string path) const override {
        int path_len_diff = std::numeric_limits<int>::max();
        index::IndexBase* best_match = nullptr;
        std::string_view path_view = path;
        for (const auto& index : json_indices) {
            if (index.field_id != field_id) {
                continue;
            }
            switch (index.cast_type.data_type()) {
                case JsonCastType::DataType::JSON:
                    if (path_view.length() < index.nested_path.length()) {
                        continue;
                    }
                    if (path_view.substr(0, index.nested_path.length()) ==
                        index.nested_path) {
                        int current_len_diff =
                            path_view.length() - index.nested_path.length();
                        if (current_len_diff < path_len_diff) {
                            path_len_diff = current_len_diff;
                            best_match = index.index.get();
                        }
                        if (path_len_diff == 0) {
                            return best_match;
                        }
                    }
                    break;
                default:
                    if (index.nested_path == path) {
                        return index.index.get();
                    }
            }
        }
        return best_match;
    }

    virtual void
    LoadJsonKeyIndex(
        FieldId field_id,
        std::unique_ptr<index::JsonKeyStatsInvertedIndex> index) = 0;

    virtual bool
    HasNgramIndex(FieldId field_id) const = 0;

    virtual PinWrapper<index::NgramInvertedIndex*>
    GetNgramIndex(FieldId field_id) const override = 0;

    SegmentType
    type() const override {
        return SegmentType::Sealed;
    }

    virtual bool
    HasIndex(FieldId field_id) const override = 0;
    bool
    HasIndex(FieldId field_id,
             const std::string& path,
             DataType data_type,
             bool any_type = false,
             bool is_json_contain = false) const override {
        auto it = std::find_if(
            json_indices.begin(),
            json_indices.end(),
            [field_id, path, data_type, any_type, is_json_contain](
                const JsonIndex& index) {
                if (index.field_id != field_id) {
                    return false;
                }
                if (index.cast_type.data_type() ==
                    JsonCastType::DataType::JSON) {
                    // for json flat index, path should be a subpath of nested_path
                    return path.substr(0, index.nested_path.length()) ==
                           index.nested_path;
                }
                if (any_type) {
                    return true;
                }
                return index.nested_path == path &&
                       index.index->IsDataTypeSupported(data_type,
                                                        is_json_contain);
            });
        return it != json_indices.end();
    }

 protected:
    virtual PinWrapper<const index::IndexBase*>
    chunk_index_impl(FieldId field_id, int64_t chunk_id) const override = 0;

    PinWrapper<const index::IndexBase*>
    chunk_index_impl(FieldId field_id,
                     const std::string& path,
                     int64_t chunk_id) const override {
        return GetJsonIndex(field_id, path);
    }
    struct JsonIndex {
        FieldId field_id;
        std::string nested_path;
        JsonCastType cast_type{JsonCastType::UNKNOWN};
        index::IndexBasePtr index;
    };

    std::vector<JsonIndex> json_indices;
};

using SegmentSealedSPtr = std::shared_ptr<SegmentSealed>;
using SegmentSealedUPtr = std::unique_ptr<SegmentSealed>;

}  // namespace milvus::segcore
