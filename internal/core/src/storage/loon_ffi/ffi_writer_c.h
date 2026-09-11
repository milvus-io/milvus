// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "common/common_type_c.h"
#include "common/type_c.h"
#include "milvus-storage/ffi_c.h"

#ifdef __cplusplus
extern "C" {
#endif

// Creates an encrypted Loon writer without transporting binary DEKs through
// NUL-terminated FFI properties. The caller owns the returned handle and uses
// loon_writer_write/close/destroy normally. The schema is consumed on import.
CStatus
NewPackedFFIWriterWithCMEK(const char* base_path,
                           struct ArrowSchema* schema,
                           const LoonProperties* properties,
                           CPluginContext* plugin_context,
                           LoonWriterHandle* out_handle);

#ifdef __cplusplus
}
#endif
