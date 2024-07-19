// Copyright (C) 2019-2023 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <string.h>

#include "monitor_c.h"
#include "prometheus_client.h"

char*
GetCoreMetrics() {
    auto str = milvus::monitor::prometheusClient->GetMetrics();
    auto len = str.length();
    char* res = (char*)malloc(len + 1);
    memcpy(res, str.data(), len);
    res[len] = '\0';
    return res;
}