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
#include "common/type_c.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void* CCollection;

CStatus
NewCollection(const void* schema_proto_blob,
              const int64_t length,
              CCollection* collection);

CStatus
UpdateSchema(CCollection collection,
             const void* proto_blob,
             const int64_t length);

CStatus
SetIndexMeta(CCollection collection,
             const void* proto_blob,
             const int64_t length);

void
DeleteCollection(CCollection collection);

const char*
GetCollectionName(CCollection collection);

#ifdef __cplusplus
}
#endif
