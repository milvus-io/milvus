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

#include "future_c_types.h"
#include "common/type_c.h"

#ifdef __cplusplus
extern "C" {
#endif

void
future_cancel(CFuture* future);

bool
future_is_ready(CFuture* future);

void
future_register_ready_callback(CFuture* future,
                               CUnlockGoMutexFn unlockFn,
                               CLockedGoMutex* mutex);

CStatus
future_leak_and_get(CFuture* future, void** result);

// TODO: only for testing, add test macro for this function.
CFuture*
future_create_test_case(int interval, int loop_cnt, int caseNo);

void
future_destroy(CFuture* future);

#ifdef __cplusplus
}
#endif
