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

#include <chrono>
#include <string>
#include "utils/Log.h"

namespace milvus {

inline void
print_timestamp(const std::string& message) {
    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
    micros %= 1000000;
    double millisecond = (double)micros / 1000.0;

    LOG_SERVER_DEBUG_ << std::fixed << " " << millisecond << "(ms) [timestamp]" << message;
}

class TimeRecorder {
    using stdclock = std::chrono::high_resolution_clock;

 public:
    // trace = 0, debug = 1, info = 2, warn = 3, error = 4, critical = 5
    explicit TimeRecorder(std::string hdr, int64_t log_level = 1);
    virtual ~TimeRecorder() = default;

    double
    RecordSection(const std::string& msg);

    double
    ElapseFromBegin(const std::string& msg);

    static std::string
    GetTimeSpanStr(double span);

 private:
    void
    PrintTimeRecord(const std::string& msg, double span);

 private:
    std::string header_;
    stdclock::time_point start_;
    stdclock::time_point last_;
    int64_t log_level_;
};

class TimeRecorderAuto : public TimeRecorder {
 public:
    explicit TimeRecorderAuto(std::string hdr, int64_t log_level = 1);
    ~TimeRecorderAuto() override;
};

}  // namespace milvus
