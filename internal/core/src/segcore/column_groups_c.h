// Copyright 2025 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void* CColumnSplits;
// Returns NULL on allocation failure; callers must check.
CColumnSplits
NewCColumnSplits();

// Returns false when the split could not be recorded (null input or
// allocation failure) so the caller can abort instead of silently writing a
// wrong column grouping.
bool
AddCColumnSplit(CColumnSplits cgs, int* group, int group_size);

int
CColumnSplitsSize(CColumnSplits cgs);

void
FreeCColumnSplits(CColumnSplits cgs);

#ifdef __cplusplus
}
#endif