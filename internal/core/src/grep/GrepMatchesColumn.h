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

#include <cstdint>
#include <memory>

#include "common/OpContext.h"
#include "common/Types.h"
#include "pb/plan.pb.h"
#include "query/ProjectionItem.h"
#include "segcore/SegmentInterface.h"

namespace milvus::grep {

// Builds the column of a GREP_MATCHES(field, args) output item for `size`
// rows of one segment, in the order of `offsets`.
//
// The column is nullable JSON. A row whose field value has at least one
// matching line gets a UTF-8 JSON array of its matching and context lines:
//   [{"type":"match","content":"...","line_number":1,"line_start_offset":0,
//     "starts":[...],"ends":[...]}, {"type":"context", ...}]
// Line numbers are one-based; line_start_offset is a zero-based UTF-8 byte
// offset in the field value; starts/ends are byte spans inside the line and
// are present only on matching lines. A row whose field is NULL or has no
// matching line is NULL.
//
// `plan` is the item's compiled patterns from the request plan; it is null
// exactly when the item selects no line. The
// serialized column may not exceed the configured result budget; exceeding it
// fails the request. Cancellation is taken from op_ctx.
std::unique_ptr<DataArray>
BuildGrepMatchesColumn(const segcore::SegmentInternalInterface* segment,
                       FieldId output_id,
                       const proto::plan::GrepMatches& item,
                       const Plan* plan,
                       const int64_t* offsets,
                       int64_t size,
                       milvus::OpContext* op_ctx);

// Builds the column of any projection item for rows of one segment.
inline std::unique_ptr<DataArray>
BuildProjectionColumn(const segcore::SegmentInternalInterface* segment,
                      const query::ProjectionItem& item,
                      const int64_t* offsets,
                      int64_t size,
                      milvus::OpContext* op_ctx) {
    if (item.grep_matches != nullptr) {
        return BuildGrepMatchesColumn(segment,
                                      item.output_id,
                                      *item.grep_matches,
                                      item.grep_plan.get(),
                                      offsets,
                                      size,
                                      op_ctx);
    }
    return query::BuildPlacementProjectionColumn(
        item, segment->get_segment_id(), offsets, size);
}

}  // namespace milvus::grep
