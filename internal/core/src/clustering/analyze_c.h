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

#include "clustering/type_c.h"
#include "common/common_type_c.h"

CStatus
Analyze(CAnalyze* res_analyze,
        const uint8_t* serialized_analyze_info,
        const uint64_t len,
        const CPluginContext* plugin_context);

CStatus
DeleteAnalyze(CAnalyze analyze);

CStatus
GetAnalyzeResultMeta(CAnalyze analyze,
                     const char** centroid_path,
                     int64_t* centroid_file_size,
                     void* id_mapping_paths,
                     int64_t* id_mapping_sizes);

typedef struct CClusteringCentroidGroup {
    uint32_t centroid_group_id;
    uint64_t rows;
    uint64_t centroid_count;
    const uint32_t* centroids;
} CClusteringCentroidGroup;

CStatus
BuildClusteringCompactionPlan(CClusteringCompactionPlan* result,
                              const uint8_t* serialized_centroids,
                              uint64_t serialized_centroids_len,
                              int32_t field_type,
                              const uint64_t* centroid_counts,
                              uint64_t centroid_count,
                              const char* cluster_type,
                              const char* params_json);

CStatus
GetClusteringCompactionPlanMeta(CClusteringCompactionPlan plan,
                                uint64_t* row_count,
                                uint32_t* centroid_count,
                                const uint64_t** centroid_counts,
                                uint64_t* group_count);

CStatus
GetClusteringCompactionPlanGroup(CClusteringCompactionPlan plan,
                                 uint64_t group_offset,
                                 CClusteringCentroidGroup* group);

CStatus
DeleteClusteringCompactionPlan(CClusteringCompactionPlan plan);

#ifdef __cplusplus
};
#endif
