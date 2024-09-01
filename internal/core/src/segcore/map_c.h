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

#ifdef __cplusplus
extern "C" {
#endif

typedef void* CMap;

CMap
create_cmap();

void
free_cmap(CMap m);

void
cmap_set(CMap m,
         const char* key,
         uint32_t key_len,
         const char* value,
         uint32_t value_len);

#ifdef __cplusplus
}
#endif
