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

#include "utils/Status.h"

#include <time.h>
#include <string>

namespace milvus {

class CommonUtil {
 public:
    static bool
    IsFileExist(const std::string& path);
    static uint64_t
    GetFileSize(const std::string& path);
    static bool
    IsDirectoryExist(const std::string& path);
    static Status
    CreateDirectory(const std::string& path);
    static Status
    DeleteDirectory(const std::string& path);

    static std::string
    GetFileName(std::string filename);
    static std::string
    GetExePath();

    static bool
    TimeStrToTime(const std::string& time_str,
                  time_t& time_integer,
                  tm& time_struct,
                  const std::string& format = "%d-%d-%d %d:%d:%d");

    static void
    GetCurrentTimeStr(std::string& time_str);

    static void
    ConvertTime(time_t time_integer, tm& time_struct);
    static void
    ConvertTime(tm time_struct, time_t& time_integer);

    static std::string
    ConvertSize(int64_t size);

#ifdef ENABLE_CPU_PROFILING
    static std::string
    GetCurrentTimeStr();
#endif
};

}  // namespace milvus
