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

#include "segcore/map_c.h"
#include "segcore/token_stream_c.h"
#include "common/type_c.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void* CTokenizer;

CStatus
create_tokenizer(CMap m, CTokenizer* tokenizer);

void
free_tokenizer(CTokenizer tokenizer);

CTokenStream
create_token_stream(CTokenizer tokenizer, const char* text, uint32_t text_len);

#ifdef __cplusplus
}
#endif
