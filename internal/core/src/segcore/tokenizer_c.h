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

#include <stdint.h>

#include "common/common_type_c.h"
#include "segcore/token_stream_c.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void* CTokenizer;

CStatus
set_tokenizer_option(const char* params);

CStatus
create_tokenizer(const char* params,
                 const char* extra_info,
                 CTokenizer* tokenizer);

CStatus
clone_tokenizer(CTokenizer* tokenizer, CTokenizer* rst);

void
free_tokenizer(CTokenizer tokenizer);

typedef struct CValidateResult {
    int64_t* resource_ids;
    uint64_t resource_ids_count;
    CStatus status;
} CValidateResult;

CValidateResult
validate_tokenizer(const char* params, const char* extra_info);

CStatus
create_token_stream(CTokenizer tokenizer,
                    const char* text,
                    uint32_t text_len,
                    CTokenStream* token_stream);

// All buffers are owned by handle; offsets contains num_rows + 1 byte offsets.
// Copy the rows before releasing handle with free_bm25_batch.
typedef struct CBM25Batch {
    const uint8_t* data;
    uint64_t data_size;
    const uint64_t* offsets;
    void* handle;
} CBM25Batch;

CStatus
batch_tokenize_bm25(CTokenizer tokenizer,
                    const uint8_t* data,
                    uint64_t data_size,
                    const uint64_t* offsets,
                    uint64_t num_rows,
                    CBM25Batch* output);

void
free_bm25_batch(void* handle);

#ifdef __cplusplus
}
#endif
