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

#include "db/Types.h"
#include "db/meta/MetaTypes.h"
#include "utils/Json.h"
#include "utils/Status.h"

#include <string>
#include <vector>

namespace milvus {
namespace server {

class ValidationUtil {
 private:
    ValidationUtil() = default;

 public:
    static Status
    ValidateGpuIndex(int32_t gpu_index);

#ifdef MILVUS_GPU_VERSION
    static Status
    GetGpuMemory(int32_t gpu_index, int64_t& memory);
#endif

    static Status
    ValidateIpAddress(const std::string& ip_address);

    static Status
    ValidateStringIsNumber(const std::string& str);

    static Status
    ValidateStringIsBool(const std::string& str);

    static Status
    ValidateStringIsFloat(const std::string& str);

    static Status
    ValidateDbURI(const std::string& uri);

    static Status
    ValidateStoragePath(const std::string& path);

    static Status
    ValidateLogLevel(const std::string& level);

    static bool
    IsNumber(const std::string& s);
};

}  // namespace server
}  // namespace milvus
