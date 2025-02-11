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

#include <stdbool.h>
#include <stdint.h>
#include "common/type_c.h"

void
InitIndexSliceSize(const int64_t);

void
InitHighPriorityThreadCoreCoefficient(const int64_t);

void
InitMiddlePriorityThreadCoreCoefficient(const int64_t);

void
InitLowPriorityThreadCoreCoefficient(const int64_t);

void
InitDefaultExprEvalBatchSize(int64_t val);

void
InitCpuNum(const int);

void
InitTrace(CTraceConfig* config);

void
SetTrace(CTraceConfig* config);

void
InitDefaultJSONKeyIndexMemoryBudget(int64_t val);

void
InitDefaultJSONKeyIndexCommitInterval(int64_t val);

void
InitDefaultJSONKeyIndexEnable(bool val);

#ifdef __cplusplus
};
#endif
