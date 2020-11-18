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
#include <functional>
#include <memory>
#include <ostream>
#include <sstream>

#include <prometheus/registry.h>
#include <prometheus/text_serializer.h>

namespace milvus {

class ScopedTimer {
 public:
    explicit ScopedTimer(std::function<void(double)> callback) : callback_(callback) {
        start_ = std::chrono::system_clock::now();
    }

    ~ScopedTimer() {
        auto end = std::chrono::system_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(end - start_).count();
        callback_(duration);
    }

 private:
    std::function<void(double)> callback_;
    std::chrono::time_point<std::chrono::system_clock> start_;
};

class Prometheus {
 public:
    prometheus::Registry&
    registry() {
        return *registry_;
    }

    std::string
    get_metrics() {
        std::ostringstream ss;
        prometheus::TextSerializer serializer;
        serializer.Serialize(ss, registry_->Collect());
        return ss.str();
    }

 private:
    std::shared_ptr<prometheus::Registry> registry_ = std::make_shared<prometheus::Registry>();
};

extern Prometheus prometheus;

}  // namespace milvus
