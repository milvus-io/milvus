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

#include <stdint.h>

#include "common/common_type_c.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void* CTextFstHandle;

typedef struct CTextFstBuildResult {
    CStatus status;
    const uint8_t* data;
    int64_t data_size;
    int64_t term_count;
    // Owns data and must be released with DeleteTextFst.
    CTextFstHandle handle;
} CTextFstBuildResult;

// BuildTextFst builds a Milvus text FST artifact.
// encoded_terms is little-endian: u64 count followed by count repetitions of
// u64 byte_length and the raw term bytes.
CTextFstBuildResult
BuildTextFst(const uint8_t* encoded_terms, int64_t encoded_size);

void
DeleteTextFst(CTextFstHandle handle);

#ifdef __cplusplus
}
#endif
