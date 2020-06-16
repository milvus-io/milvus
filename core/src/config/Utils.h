// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <string>
#include <vector>

#include "utils/Status.h"

namespace milvus {
namespace server {

extern int64_t
parse_bytes(const std::string& str, std::string& err);

extern bool
GetSystemMemInfo(int64_t& total_mem, int64_t& free_mem);

extern bool
GetSystemAvailableThreads(int64_t& thread_count);

extern Status
ValidateGpuIndex(int32_t gpu_index);

#ifdef MILVUS_GPU_VERSION
extern Status
GetGpuMemory(int32_t gpu_index, int64_t& memory);
#endif

extern Status
ValidateIpAddress(const std::string& ip_address);

extern Status
ValidateStringIsNumber(const std::string& str);

extern Status
ValidateStringIsBool(const std::string& str);

extern Status
ValidateStringIsFloat(const std::string& str);

extern Status
ValidateDbURI(const std::string& uri);

extern Status
ValidateStoragePath(const std::string& path);

extern Status
ValidateLogLevel(const std::string& level);

extern bool
IsNumber(const std::string& s);
}  // namespace server
}  // namespace milvus
