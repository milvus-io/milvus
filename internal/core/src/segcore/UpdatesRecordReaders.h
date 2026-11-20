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

#include "segcore/UpdatesRecord.h"
#include "index/contracts/query/IIndexReaderBase.h"

namespace milvus::segcore {
// Borrowed from the physical segment pinned by the query.
struct UpdateReadBinding {
    const UpdatesRecord& records;
    FieldId field;
    Timestamp timestamp;
    DataType type;
    bool nullable;

    std::unique_ptr<index::IIndexReaderBase>
    CorrectOriginalIndex(const index::IIndexReaderBase* original) const;

    std::vector<UpdatesRecord::Patch>
    Patches(int64_t start,
            size_t count,
            const int32_t* offsets,
            const TargetBitmapView* mask,
            size_t mask_start = 0) const {
        return records.ReadRangeUpdates(field,
                                        timestamp,
                                        start,
                                        count,
                                        offsets,
                                        mask ? *mask : TargetBitmapView{},
                                        mask_start);
    }
};
}  // namespace milvus::segcore
