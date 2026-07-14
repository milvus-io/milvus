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
#include "index/JsonScalarIndexWrapper.h"
#include "index/JsonFlatIndex.h"
#include "pb/index_cgo_msg.pb.h"
#include "pb/segcore.pb.h"
#include "segcore/InsertRecord.h"
#include "segcore/SegmentInterface.h"
#include "segcore/Types.h"
#include "index/NgramInvertedIndex.h"

namespace milvus::segcore {

class SegmentSealed : public SegmentInternalInterface {
 public:
    virtual void
    LoadIndex(LoadIndexInfo& info) = 0;
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

    virtual InsertRecord<true>&
    get_insert_record() = 0;

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
};

using SegmentSealedSPtr = std::shared_ptr<SegmentSealed>;
using SegmentSealedUPtr = std::unique_ptr<SegmentSealed>;

}  // namespace milvus::segcore
