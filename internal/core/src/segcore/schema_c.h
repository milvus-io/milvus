// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <stdint.h>

#include "common/common_type_c.h"

#ifdef __cplusplus
extern "C" {
#endif

// A heap-boxed SchemaPtr. Every successful Acquire/Clone must be paired with
// Release.
typedef void* CSchemaHandle;

CStatus
AcquireSchemaHandle(int64_t collection_id,
                    const void* schema_proto_blob,
                    int64_t length,
                    CSchemaHandle* schema_handle);

// Parses an effective load schema without inserting it into the global logical
// schema cache. The returned SchemaPtr carries collection-level field loading,
// mmap, and warmup policy and can be shared by all segments in one collection
// state.
CStatus
AcquireLoadSchemaHandle(const void* schema_proto_blob,
                        int64_t length,
                        const int64_t* load_fields,
                        int64_t load_field_count,
                        CSchemaHandle* schema_handle);

CStatus
CloneSchemaHandle(CSchemaHandle schema_handle,
                  CSchemaHandle* cloned_schema_handle);

void
ReleaseSchemaHandle(CSchemaHandle schema_handle);

#ifdef __cplusplus
}
#endif
