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

    index::IndexBase*
    chunk_index_impl(FieldId field_id,
                     std::string path,
                     int64_t chunk_id) const override {
        JSONIndexKey key;
        key.field_id = field_id;
        key.nested_path = path;
        AssertInfo(json_indexings_.find(key) != json_indexings_.end(),
                   "Cannot find json index with path: " + path);
        return json_indexings_.at(key).get();
    }

    virtual bool
    HasIndex(FieldId field_id) const override = 0;
    bool
    HasIndex(FieldId field_id, const std::string& path) const override {
        JSONIndexKey key;
        key.field_id = field_id;
        key.nested_path = path;
        return json_indexings_.find(key) != json_indexings_.end();
    }

 protected:
    struct JSONIndexKey {
        FieldId field_id;
        std::string nested_path;
        bool
        operator==(const JSONIndexKey& other) const {
            return field_id == other.field_id &&
                   nested_path == other.nested_path;
        }
    };

    struct hash_helper {
        size_t
        operator()(const JSONIndexKey& k) const {
            std::hash<int64_t> h1;
            std::hash<std::string> h2;
            size_t hash_result = 0;
            boost::hash_combine(hash_result, h1(k.field_id.get()));
            boost::hash_combine(hash_result, h2(k.nested_path));
            return hash_result;
        }
    };
    std::unordered_map<JSONIndexKey, index::IndexBasePtr, hash_helper>
        json_indexings_;
};

using SegmentSealedSPtr = std::shared_ptr<SegmentSealed>;
using SegmentSealedUPtr = std::unique_ptr<SegmentSealed>;

}  // namespace milvus::segcore
