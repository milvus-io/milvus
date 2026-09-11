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

#include <stdlib.h>
#include <string.h>
#include <mutex>
#include <string>

#include "cachinglayer/Metrics.h"
#include "common/FastMem.h"
#include "common/init_c.h"
#include "common/PrometheusClient.h"
#include "monitor_c.h"
#include "storage/LoadAdmissionController.h"

char*
GetCoreMetrics() {
    // Concurrent Go gathers must not interleave snapshot publication with
    // collection and return mixed or stale admission gauges.
    static std::mutex scrape_mutex;
    std::lock_guard lock(scrape_mutex);
    milvus::storage::LoadAdmissionController::GetInstance().UpdateMetrics();
    UpdateArrowIOThreadPoolMetrics();
    static_cast<void>(
        milvus::cachinglayer::monitor::collect_cache_shard_usage_stats());
    auto str = milvus::monitor::getPrometheusClient().GetMetrics();
    auto len = str.length();
    char* res = static_cast<char*>(malloc(len + 1));
    milvus::fastmem::FastMemcpy(res, str.data(), len);
    res[len] = '\0';
    return res;
}
