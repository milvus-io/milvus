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

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include "segcore/collection_c.h"
#include "segcore/plan_c.h"
#include <stdint.h>

typedef void* CSegmentBase;

CSegmentBase
NewSegment(CCollection collection, uint64_t segment_id);

void
DeleteSegment(CSegmentBase segment);

//////////////////////////////////////////////////////////////////

int
Insert(CSegmentBase c_segment,
       int64_t reserved_offset,
       int64_t size,
       const int64_t* row_ids,
       const uint64_t* timestamps,
       void* raw_data,
       int sizeof_per_row,
       int64_t count);

int64_t
PreInsert(CSegmentBase c_segment, int64_t size);

int
Delete(
    CSegmentBase c_segment, int64_t reserved_offset, int64_t size, const int64_t* row_ids, const uint64_t* timestamps);

int64_t
PreDelete(CSegmentBase c_segment, int64_t size);

int
Search(CSegmentBase c_segment,
       CPlan plan,
       CPlaceholderGroup* placeholder_groups,
       uint64_t* timestamps,
       int num_groups,
       int64_t* result_ids,
       float* result_distances);

//////////////////////////////////////////////////////////////////

int
Close(CSegmentBase c_segment);

int
BuildIndex(CCollection c_collection, CSegmentBase c_segment);

bool
IsOpened(CSegmentBase c_segment);

int64_t
GetMemoryUsageInBytes(CSegmentBase c_segment);

//////////////////////////////////////////////////////////////////

int64_t
GetRowCount(CSegmentBase c_segment);

int64_t
GetDeletedCount(CSegmentBase c_segment);

#ifdef __cplusplus
}
#endif
