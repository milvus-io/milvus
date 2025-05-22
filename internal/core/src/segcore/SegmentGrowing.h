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
#include <vector>

#include "common/LoadInfo.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "query/Plan.h"
#include "segcore/SegmentInterface.h"

namespace milvus::segcore {

class SegmentGrowing : public SegmentInternalInterface {
 public:
    virtual int64_t
    PreInsert(int64_t size) = 0;

    virtual void
    Insert(int64_t reserved_offset,
           int64_t size,
           const int64_t* row_ids,
           const Timestamp* timestamps,
           InsertRecordProto* insert_record_proto) = 0;

    SegmentType
    type() const override {
        return SegmentType::Growing;
    }

    // virtual int64_t
    // PreDelete(int64_t size) = 0;

    // virtual Status
    // Delete(int64_t reserved_offset, int64_t size, const int64_t* row_ids, const Timestamp* timestamps) = 0;
};

using SegmentGrowingPtr = std::unique_ptr<SegmentGrowing>;

}  // namespace milvus::segcore
