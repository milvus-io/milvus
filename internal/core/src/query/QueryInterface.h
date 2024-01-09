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

#include <cstdint>

#include "base/Utils.h"
#include "common/EasyAssert.h"
#include "base/SystemProperty.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "segment/SegmentInterface.h"
#include "query/generated/ExecPlanNodeVisitor.h"

namespace milvus {
namespace query {

std::unique_ptr<milvus::base::SearchResult>
Search(const milvus::segment::SegmentInternalInterface* segment,
       const Plan* plan,
       const PlaceholderGroup* placeholder_group,
       Timestamp timestamp);

std::unique_ptr<proto::segcore::RetrieveResults>
Retrieve(const milvus::segment::SegmentInternalInterface* segment,
         const RetrievePlan* plan,
         Timestamp timestamp,
         int64_t limit_size);

int64_t
GetRealCount(const milvus::segment::SegmentInternalInterface* segment);

}  // namespace query
}  // namespace milvus