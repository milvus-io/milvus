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

#include "TimeRecorder.h"

#include <iostream>

namespace milvus_sdk {

TimeRecorder::TimeRecorder(const std::string& title) : title_(title) {
    std::cout << title_ << " begin..." << std::endl;
}

void TimeRecorder::Start() {
    start_ = std::chrono::system_clock::now();
}

void TimeRecorder::End() {
    end_ = std::chrono::system_clock::now();
    int64_t span = (std::chrono::duration_cast<std::chrono::milliseconds>(end_ - start_)).count();
    total_time_ = total_time_ + span;
}

void TimeRecorder::Print(int loop) {
    uint64_t per_cost = total_time_ / loop;
    std::cout << title_ << " totally cost: " << total_time_ << " ms" << std::endl;
    std::cout << title_ << " per cost: " << per_cost << " ms" << std::endl;
}

TimeRecorder::~TimeRecorder() {
}

}  // namespace milvus_sdk
