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
#include "indexbuilder/type_c.h"

CStatus
Analysis(CAnalysis* res_analysis, CAnalysisInfo c_analysis_info);

//CStatus
//CreateMajorCompaction(CAnalysis* res_analysis,
//                      CBuildIndexInfo c_build_index_info);

CStatus
DeleteAnalysis(CAnalysis analysis);

CStatus
CleanAnalysisLocalData(CAnalysis analysis);

CStatus
NewAnalysisInfo(CAnalysisInfo* c_analysis_info,
                CStorageConfig c_storage_config);

void
DeleteAnalysisInfo(CAnalysisInfo c_analysis_info);

CStatus
AppendAnalysisFieldMetaInfo(CAnalysisInfo c_analysis_info,
                            int64_t collection_id,
                            int64_t partition_id,
                            int64_t field_id,
                            const char* field_name,
                            enum CDataType field_type,
                            int64_t dim);

CStatus
AppendAnalysisInfo(CAnalysisInfo c_analysis_info,
                   int64_t task_id,
                   int64_t version);

// no use
CStatus
AppendSegmentID(CAnalysisInfo c_analysis_info, int64_t segment_id);

CStatus
AppendSegmentInsertFile(CAnalysisInfo c_analysis_info,
                        int64_t segID,
                        const char* file_path);

CStatus
AppendSegmentSize(CAnalysisInfo c_analysis_info, int64_t size);

CStatus
AppendTrainSize(CAnalysisInfo c_analysis_info, int64_t size);

CStatus
SerializeAnalysisAndUpLoad(CAnalysis analysis);

CStatus
GetCentroidsFile(const char* file);

CStatus
GetSegmentOffsetMapping(int64_t segment_id, const char* file);

#ifdef __cplusplus
};
#endif
