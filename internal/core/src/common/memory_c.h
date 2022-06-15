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

#ifdef __cplusplus
extern "C" {
#endif

#include "common/type_c.h"

/*
 * In glibc, free chunks are stored in various lists based on size and history,
 * so that the library can quickly find suitable chunks to satisfy allocation requests.
 * The lists, called "bins".
 * ref: <https://sourceware.org/glibc/wiki/MallocInternals>
 */
CStatus
PurgeMemory(uint64_t max_bins_size);

#ifdef __cplusplus
}
#endif
