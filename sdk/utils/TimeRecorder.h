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

namespace milvus_sdk {

class TimeRecorder {
 public:
    explicit TimeRecorder(const std::string& title);
    void Start();
    void End();
    void Print(int loop);
    ~TimeRecorder();

 private:
    std::string title_;
    std::chrono::system_clock::time_point start_;
    std::chrono::system_clock::time_point end_;
    int64_t total_time_ = 0;
};

}  // namespace milvus_sdk
