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
#include <chrono>

class Timer {
 public:
    Timer() {
        reset();
    }

    double
    get_overall_seconds() {
        using namespace std::chrono;
        auto now = high_resolution_clock::now();
        auto diff = now - init_record;
        step_record = now;
        return (double)duration_cast<microseconds>(diff).count() * 1e-6;
    }

    double
    get_step_seconds() {
        using namespace std::chrono;
        auto now = high_resolution_clock::now();
        auto diff = now - step_record;
        step_record = now;
        return (double)duration_cast<microseconds>(diff).count() * 1e-6;
    }

    void
    reset() {
        using namespace std::chrono;
        step_record = init_record = high_resolution_clock::now();
    }

 private:
    using nanosecond_t = std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>;

 private:
    nanosecond_t init_record;
    nanosecond_t step_record;
};