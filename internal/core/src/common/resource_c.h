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

typedef struct LoadResourceRequest {
    float max_memory_cost;    //memory cost (GB) during loading
    float max_disk_cost;      // disk cost (GB) during loading
    float final_memory_cost;  // final memory (GB) cost after loading
    float final_disk_cost;    // final disk cost (GB) after loading
    bool has_raw_data;        // the filed contains raw data or not
} LoadResourceRequest;

#ifdef __cplusplus
}
#endif
