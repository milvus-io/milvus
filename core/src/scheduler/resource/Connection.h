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

#include <sstream>
#include <string>
#include <utility>

namespace milvus {
namespace scheduler {

class Connection {
 public:
    // TODO: update construct function, speed: double->uint64_t
    Connection(std::string name, double speed) : name_(std::move(name)), speed_(speed) {
    }

    const std::string&
    name() const {
        return name_;
    }

    uint64_t
    speed() const {
        return speed_;
    }

    uint64_t
    transport_cost() {
        return 1024 / speed_;
    }

 public:
    std::string
    Dump() const {
        std::stringstream ss;
        ss << "<name: " << name_ << ", speed: " << speed_ << ">";
        return ss.str();
    }

 private:
    std::string name_;
    uint64_t speed_;
};

}  // namespace scheduler
}  // namespace milvus
