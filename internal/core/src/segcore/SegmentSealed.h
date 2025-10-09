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
#include "cachinglayer/Utils.h"
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
    DropJSONIndex(const FieldId field_id, const std::string& nested_path) = 0;
    virtual void
    DropFieldData(const FieldId field_id) = 0;

    virtual void
    AddFieldDataInfoForSealed(const LoadFieldDataInfo& field_data_info) = 0;
    virtual void
    RemoveFieldFile(const FieldId field_id) = 0;
    virtual void
    ClearData() = 0;
    virtual std::unique_ptr<DataArray>
    get_vector(milvus::OpContext* op_ctx,
               FieldId field_id,
               const int64_t* ids,
               int64_t count) const = 0;

    virtual void
    LoadTextIndex(FieldId field_id,
                  std::unique_ptr<index::TextMatchIndex> index) = 0;

    virtual InsertRecord<true>&
    get_insert_record() = 0;

    virtual std::vector<PinWrapper<const index::IndexBase*>>
    PinJsonIndex(milvus::OpContext* op_ctx,
                 FieldId field_id,
                 const std::string& path,
                 DataType data_type,
                 bool any_type,
                 bool is_array) const override {
        int path_len_diff = std::numeric_limits<int>::max();
        index::CacheIndexBasePtr best_match = nullptr;
        std::string_view path_view = path;
        auto res = json_indices.withRLock(
            [&](auto vec) -> PinWrapper<const index::IndexBase*> {
                for (const auto& index : vec) {
                    if (index.field_id != field_id) {
                        continue;
                    }
                    switch (index.cast_type.data_type()) {
                        case JsonCastType::DataType::JSON:
                            if (path_view.length() <
                                index.nested_path.length()) {
                                continue;
                            }
                            if (path_view.substr(0,
                                                 index.nested_path.length()) ==
                                index.nested_path) {
                                int current_len_diff =
                                    path_view.length() -
                                    index.nested_path.length();
                                if (current_len_diff < path_len_diff) {
                                    path_len_diff = current_len_diff;
                                    best_match = index.index;
                                }
                                if (path_len_diff == 0) {
                                    break;
                                }
                            }
                            break;
                        default:
                            if (index.nested_path != path) {
                                continue;
                            }
                            if (any_type) {
                                best_match = index.index;
                                break;
                            }
                            if (milvus::index::json::IsDataTypeSupported(
                                    index.cast_type, data_type, is_array)) {
                                best_match = index.index;
                                break;
                            }
                    }
                }
                if (best_match == nullptr) {
                    return nullptr;
                }
                auto ca = SemiInlineGet(best_match->PinCells(op_ctx, {0}));
                auto index = ca->get_cell_of(0);
                return PinWrapper<const index::IndexBase*>(ca, index);
            });
        if (res.get() == nullptr) {
            return {};
        }
        return {res};
    }

    virtual PinWrapper<index::NgramInvertedIndex*>
    GetNgramIndex(milvus::OpContext* op_ctx,
                  FieldId field_id) const override = 0;

    virtual PinWrapper<index::NgramInvertedIndex*>
    GetNgramIndexForJson(milvus::OpContext* op_ctx,
                         FieldId field_id,
                         const std::string& nested_path) const override = 0;

    SegmentType
    type() const override {
        return SegmentType::Sealed;
    }

 protected:
    struct JsonIndex {
        FieldId field_id;
        std::string nested_path;
        JsonCastType cast_type{JsonCastType::UNKNOWN};
        index::CacheIndexBasePtr index;
    };

    folly::Synchronized<std::vector<JsonIndex>> json_indices;
};

using SegmentSealedSPtr = std::shared_ptr<SegmentSealed>;
using SegmentSealedUPtr = std::unique_ptr<SegmentSealed>;

}  // namespace milvus::segcore
