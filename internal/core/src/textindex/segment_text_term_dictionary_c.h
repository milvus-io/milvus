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

#include <stdbool.h>
#include <stdint.h>

#include "common/common_type_c.h"
#include "common/type_c.h"
#include "textindex/fst/text_fst_c.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct CTextTermTrieUpdateResult {
    CStatus status;
    int64_t term_count;
    int64_t memory_size;
    bool is_data_integrity_error;
} CTextTermTrieUpdateResult;

// Adds one field's message-deduplicated terms to the mutable Trie owned by a
// growing segcore segment. encoded_terms uses the same little-endian
// count/length/bytes representation as BuildTextFst.
CTextTermTrieUpdateResult
UpdateSegmentTextTermTrie(CSegmentInterface c_segment,
                          int64_t field_id,
                          const uint8_t* encoded_terms,
                          int64_t encoded_size);

// Streams every term from the supplied persisted FSTs into the mutable Trie
// owned by a growing segment.
CTextTermTrieUpdateResult
AddSegmentTextTermFstsToTrie(CSegmentInterface c_segment,
                             int64_t field_id,
                             const CTextFstHandle* fst_handles,
                             int64_t fst_count);

#ifdef __cplusplus
}
#endif
