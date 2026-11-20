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
#ifdef __cplusplus
extern "C" {
#endif

#include "common/type_c.h"
#include <arrow/c/abi.h>
#include "segcore/column_groups_c.h"

typedef void* CPackedWriter;

CStatus
NewPackedWriterWithStorageConfig(struct ArrowSchema* schema,
                                 const int64_t buffer_size,
                                 char** paths,
                                 int64_t num_paths,
                                 int64_t part_upload_size,
                                 CColumnGroups column_groups,
                                 CStorageConfig c_storage_config,
                                 CPackedWriter* c_packed_writer,
                                 CPluginContext* c_plugin_context);

CStatus
NewPackedWriter(struct ArrowSchema* schema,
                const int64_t buffer_size,
                char** paths,
                int64_t num_paths,
                int64_t part_upload_size,
                CColumnGroups column_groups,
                CPackedWriter* c_packed_writer,
                CPluginContext* c_plugin_context);

CStatus
WriteRecordBatch(CPackedWriter c_packed_writer,
                 struct ArrowArray* arrays,
                 struct ArrowSchema* array_schemas,
                 struct ArrowSchema* schema);

CStatus
CloseWriter(CPackedWriter c_packed_writer);

CStatus
GetFileSize(const char* path, int64_t* size);

CStatus
GetFileSizeWithStorageConfig(const char* path,
                             int64_t* size,
                             CStorageConfig c_storage_config);

#ifdef __cplusplus
}
#endif
