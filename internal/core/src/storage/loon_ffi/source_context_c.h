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
#ifdef __cplusplus
extern "C" {
#endif
// Trusted process callback. The output is an owned native handle; no Go pointer
// or token is serialized. Return values are LoonSourceCredentialStatus, not RPC
// error codes. Registration happens once before readers are constructed.
typedef int (*MilvusSourceContextFactory)(const char*,
                                          const char*,
                                          int64_t,
                                          uintptr_t*);
void
MilvusSetSourceContextFactory(MilvusSourceContextFactory factory);
// Resolve through the already scoped native filesystem BEFORE removing URI
// authority. Returned path is malloc-owned; result is zero or a source FFI code.
int
MilvusSourceFilesystemPath(uintptr_t filesystem, const char* uri, char** out);
#ifdef __cplusplus
}
#endif
