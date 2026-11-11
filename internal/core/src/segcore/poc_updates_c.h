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
#include "segcore/segment_c.h"
#ifdef __cplusplus
extern "C" {
#endif
// The caller pins the unpublished segment until DestroyPOCUpdates.
CStatus
BeginPOCUpdates(CSegmentInterface segment,
                const int64_t* fields,
                int64_t field_count,
                int64_t row_count,
                bool enabled_temp_index,
                void** session,
                uint64_t* base_timestamp);
CStatus
ApplyPOCUpdates(void* session,
                const char* json,
                int64_t length,
                int64_t* matched_rows);
CStatus
CommitPOCUpdates(void* session);
void
DestroyPOCUpdates(void* session);
CStatus
GetPOCReadCounts(CSegmentInterface segment,
                 uint64_t* index_reads,
                 uint64_t* value_reads);
#ifdef __cplusplus
}
#endif
