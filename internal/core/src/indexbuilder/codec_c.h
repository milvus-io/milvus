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

#include "stdint.h"
#include "index_c.h"

// FIXME(dragondriver): necessary to separate this to an independent shared library? It's heavy for a tool to depend on
//                      indexbuilder.
// AssembleBinarySet assemble the binary set by the slice meta. This function can only be used to parse index meta.
CStatus
AssembleBinarySet(const char* original, int32_t size, CBinary* assembled);

#ifdef __cplusplus
};
#endif
