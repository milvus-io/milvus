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

#include <stdint.h>
#include <stdbool.h>

#include "common/type_c.h"

void
InitCpuNum(const int);

void
SetIndexSliceSize(const int64_t);

void
SetHighPriorityThreadCoreCoefficient(const float);

void
SetMiddlePriorityThreadCoreCoefficient(const float);

void
SetLowPriorityThreadCoreCoefficient(const float);

void
SetThreadPoolMaxThreadsSize(const int);

void
SetDefaultExprEvalBatchSize(int64_t val);

void
SetDefaultDeleteDumpBatchSize(int64_t val);

void
SetDefaultOptimizeExprEnable(bool val);

void
SetDefaultGrowingJSONKeyStatsEnable(bool val);

void
SetDefaultConfigParamTypeCheck(bool val);

void
SetDefaultEnableParquetStatsSkipIndex(bool val);

void
SetEnableLatestDeleteSnapshotOptimization(bool val);

// dynamic update segcore params
void
SetLogLevel(const char* level);

void
InitTrace(CTraceConfig* config);

void
SetTrace(CTraceConfig* config);

// OpenSSL FIPS status
void
LogOpenSSLFIPSStatus();

// Expr result cache
void
SetExprResCacheEnable(bool val);

void
SetExprResCacheCapacityBytes(int64_t bytes);

// Set the capacity of arrow's internal IO thread pool. This pool runs
// async range reads (ReadRangeCache) that issue actual S3 GetObject
// requests, so it's the true ceiling on parallel object-storage reads —
// independent of the segcore HIGH/MIDDLE pools and aws-sdk-cpp's
// ClientConfiguration::maxConnections. Arrow's built-in default is a
// fixed constant (kDefaultNumIoThreads = 8 in arrow 17), which is almost
// always the hidden bottleneck for retrieve/load workloads. The Go side
// resolves common.arrow.ioThreadPoolCoefficient (× CPU cores, clamped by
// common.arrow.ioThreadPoolMaxCapacity) into a final thread count and
// passes it here. Values <= 0 are ignored (keep the current capacity).
void
SetArrowIOThreadPoolCapacity(int threads);

#ifdef __cplusplus
};
#endif
