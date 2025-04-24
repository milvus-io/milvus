// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once
#include <chrono>
#include <functional>
#include <string>
#include <string_view>
#include <log/Log.h>

namespace milvus {

class ScopedTimer {
 public:
    ScopedTimer(std::string_view name,
                std::function<void(double /*ms*/)> reporter)
        : name_(name),
          reporter_(std::move(reporter)),
          start_(std::chrono::steady_clock::now()) {
    }

    ~ScopedTimer() {
        auto end = std::chrono::steady_clock::now();
        auto duration_us =
            std::chrono::duration<double, std::micro>(end - start_).count();
        reporter_(duration_us / 1000.0);  // report in milliseconds
        LOG_DEBUG("{} time: {} ms", name_, duration_us / 1000.0);
    }

 private:
    std::string name_;
    std::function<void(double)> reporter_;
    std::chrono::steady_clock::time_point start_;
};
}  // namespace milvus
