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

struct LoadResourceRequest {
    float maxMemoryCost;    //memory cost (GB) during loading
    float maxDiskCost;      // disk cost (GB) during loading
    float finalMemoryCost;  // final memory (GB) cost after loading
    float finalDiskCost;    // final disk cost (GB) after loading
    bool hasRawData;        // the filed contains raw data or not
};

#ifdef __cplusplus
}
#endif
