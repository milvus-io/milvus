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

#include <stdint.h>

#include "common/type_c.h"

#ifdef __cplusplus
extern "C" {
#endif

CStatus
InitLocalArrowFileSystemSingleton(const char* c_path);

void
CleanArrowFileSystemSingleton();

CStatus
InitRemoteArrowFileSystemSingleton(CStorageConfig c_storage_config);

CStatus
GetFileStats(const char* c_path,
             int64_t* out_size,
             char*** out_keys,
             char*** out_values,
             int* out_count);
CStatus
ReadFileData(const char* c_path, uint8_t** out_data, int64_t* out_size);

CStatus
WriteFileData(const char* c_path,
              const uint8_t* data,
              int64_t data_size,
              const char** metadata_keys,
              const char** metadata_values,
              int metadata_count);

CStatus
DeleteFile(const char* c_path);

CStatus
GetFileInfo(const char* c_path,
            bool* out_exists,
            bool* out_is_dir,
            int64_t* out_mtime_ns,
            int64_t* out_ctime_ns);

CStatus
CreateDir(const char* c_path, bool recursive);


CStatus
ListDir(const char* c_path,
        bool recursive,
        char*** out_paths,
        bool** out_is_dirs,
        int64_t** out_sizes,
        int64_t** out_mtime_ns,
        int* out_count);

 void
 FreeMemory(void* ptr);

#ifdef __cplusplus
}
#endif
