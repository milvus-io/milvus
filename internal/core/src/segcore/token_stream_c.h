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

#include "map_c.h"
#include "common/type_c.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void* CTokenStream;

void free_token_stream(CTokenStream);

bool token_stream_advance(CTokenStream);

// Note: returned string must be freed by the caller.
const char* token_stream_get_token(CTokenStream);

void
free_token(void* token);

#ifdef __cplusplus
}
#endif
