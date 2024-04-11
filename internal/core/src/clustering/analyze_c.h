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
Analyze(CAnalyze* res_analyze, CAnalyzeInfo c_analyze_info);

CStatus
DeleteAnalyze(CAnalyze analyze);

CStatus
NewAnalyzeInfo(CAnalyzeInfo* c_analyze_info, CStorageConfig c_storage_config);

void
DeleteAnalyzeInfo(CAnalyzeInfo c_analyze_info);

CStatus
AppendAnalyzeInfo(CAnalyzeInfo c_analyze_info,
                  int64_t collection_id,
                  int64_t partition_id,
                  int64_t field_id,
                  int64_t task_id,
                  int64_t version,
                  const char* field_name,
                  enum CDataType field_type,
                  int64_t dim,
                  int64_t segment_size,
                  int64_t train_size);

CStatus
AppendSegmentInsertFile(CAnalyzeInfo c_analyze_info,
                        int64_t segID,
                        const char* file_path);

CStatus
AppendSegmentNumRows(CAnalyzeInfo c_analyze_info,
                     int64_t segID,
                     int64_t num_rows);

CStatus
GetAnalyzeResultMeta(CAnalyze analysis,
                     char* centroid_path,
                     int64_t* centroid_file_size,
                     void* id_mapping_paths,
                     void* id_mapping_sizes);

#ifdef __cplusplus
};
#endif
