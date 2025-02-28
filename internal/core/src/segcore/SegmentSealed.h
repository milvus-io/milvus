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
#include <utility>
#include <tuple>

#include "common/LoadInfo.h"
#include "index/JsonInvertedIndex.h"
#include "index/JsonFlatIndex.h"
#include "pb/segcore.pb.h"
#include "segcore/SegmentInterface.h"
#include "segcore/Types.h"

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
    LoadFieldData(FieldId field_id, FieldDataInfo& data) = 0;
    virtual void
    MapFieldData(const FieldId field_id, FieldDataInfo& data) = 0;
    virtual void
    AddFieldDataInfoForSealed(const LoadFieldDataInfo& field_data_info) = 0;
    virtual void
    WarmupChunkCache(const FieldId field_id, bool mmap_enabled) = 0;
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

    SegmentType
    type() const override {
        return SegmentType::Sealed;
    }

    virtual bool
    HasIndex(FieldId field_id) const override = 0;
    bool
    HasIndex(FieldId field_id,
             const std::string& path,
             DataType data_type) const override {
        auto it = std::find_if(
            json_indices.begin(),
            json_indices.end(),
            [field_id, path, data_type](const JsonIndex& index) {
                if (index.field_id != field_id) {
                    return false;
                }
                if (index.cast_type != DataType::JSON) {
                    return index.nested_path == path &&
                           index.cast_type == data_type;
                }
                return path.substr(0, index.nested_path.length()) ==
                       index.nested_path;
            });
        return it != json_indices.end();
    }

 protected:
    virtual const index::IndexBase*
    chunk_index_impl(FieldId field_id, int64_t chunk_id) const override = 0;

    const index::IndexBase*
    chunk_index_impl(FieldId field_id,
                     std::string path,
                     int64_t chunk_id) const override {
        auto it = std::find_if(json_indices.begin(),
                               json_indices.end(),
                               [field_id, path](const JsonIndex& index) {
                                   if (index.field_id != field_id) {
                                       return false;
                                   }
                                   if (index.cast_type != DataType::JSON) {
                                       // json path index
                                       return index.nested_path == path;
                                   }
                                   // json flat index
                                   return path.substr(
                                              0, index.nested_path.length()) ==
                                          index.nested_path;
                               });
        return it != json_indices.end() ? it->index.get() : nullptr;
    }
    struct JsonIndex {
        FieldId field_id;
        std::string nested_path;
        DataType cast_type;
        index::IndexBasePtr index;
    };

    std::vector<JsonIndex> json_indices;
};

using SegmentSealedSPtr = std::shared_ptr<SegmentSealed>;
using SegmentSealedUPtr = std::unique_ptr<SegmentSealed>;

}  // namespace milvus::segcore
