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

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "common/type_c.h"
#include "common/binary_set_c.h"
#include "clustering/type_c.h"

CStatus
Analyze(CAnalyze* res_analyze,
        const uint8_t* serialized_analyze_info,
        const uint64_t len);

CStatus
DeleteAnalyze(CAnalyze analyze);

CStatus
GetAnalyzeResultMeta(CAnalyze analyze,
                     char** centroid_path,
                     int64_t* centroid_file_size,
                     void* id_mapping_paths,
                     int64_t* id_mapping_sizes);

#ifdef __cplusplus
};
#endif
