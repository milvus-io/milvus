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

#include "SegmentInterface.h"
#include "base/LoadInfo.h"
#include "pb/segcore.pb.h"
#include "mmap/Column.h"

namespace milvus::segment {

class SegmentSealed : public SegmentInternalInterface {
 public:
    virtual void
    LoadIndex(const milvus::index::LoadIndexInfo& info) = 0;
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
    WarmupChunkCache(const FieldId field_id) = 0;
    virtual void
    AddFieldDataInfoForSealed(
        const milvus::base::LoadFieldDataInfo& field_data_info) = 0;

    SegmentType
    type() const override {
        return SegmentType::Sealed;
    }
};

using SegmentSealedSPtr = std::shared_ptr<SegmentSealed>;
using SegmentSealedUPtr = std::unique_ptr<SegmentSealed>;

}  // namespace milvus::segment
