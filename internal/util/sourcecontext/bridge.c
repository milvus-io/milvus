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

#include "bridge.h"
#include <stdlib.h>
#include <string.h>

extern void
MilvusSourceFetch(uintptr_t,
                  const LoonSourceCredentialRequest*,
                  LoonSourceCredentialResult*);
extern void
MilvusSourceDestroy(uintptr_t);

static void
fetch(void* userdata,
      const LoonSourceCredentialRequest* request,
      LoonSourceCredentialResult* result) {
    MilvusSourceFetch((uintptr_t)userdata, request, result);
}
void
MilvusSourceResultRelease(LoonSourceCredentialResult* result) {
    if (!result)
        return;
    // Best effort erasure before freeing callback-owned C buffers.
    if (result->sas_token) {
        volatile char* token = (volatile char*)result->sas_token;
        for (size_t i = 0; i < result->sas_token_length; ++i) token[i] = 0;
    }
    free((void*)result->account_name);
    free((void*)result->sas_token);
    free((void*)result->scope_root_uri);
    result->account_name = NULL;
    result->sas_token = NULL;
    result->scope_root_uri = NULL;
}
static void
release_result(void* userdata, LoonSourceCredentialResult* result) {
    (void)userdata;
    MilvusSourceResultRelease(result);
}
static void
destroy(void* userdata) {
    MilvusSourceDestroy((uintptr_t)userdata);
}

LoonFFIResult
MilvusSourceContextCreate(uintptr_t userdata,
                          const LoonSourceReadContextOptions* options,
                          LoonSourceReadContextHandle* out) {
    LoonSourceCredentialCallbacks callbacks = {
        .abi_version = 1,
        .struct_size = sizeof(LoonSourceCredentialCallbacks),
        .userdata = (void*)userdata,
        .fetch = fetch,
        .release_result = release_result,
        .destroy_userdata = destroy,
    };
    return loon_source_read_context_create(options, &callbacks, out);
}
