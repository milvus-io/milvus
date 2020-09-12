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

namespace milvus {
namespace scheduler {

class Path {
 public:
    Path() = default;

    Path(std::vector<std::string>& path, uint64_t index) : path_(path), index_(index) {
    }

    void
    push_back(const std::string& str) {
        path_.push_back(str);
    }

    std::vector<std::string>
    Dump() {
        return path_;
    }

    std::string
    Current() {
        if (!path_.empty() && path_.size() > index_) {
            return path_[index_];
        } else {
            return "";
        }
    }

    std::string
    Next() {
        if (index_ > 0 && !path_.empty()) {
            --index_;
            return path_[index_];
        } else {
            return "";
        }
    }

    std::string
    Last() {
        if (!path_.empty()) {
            return path_[0];
        } else {
            return "";
        }
    }

    std::string
    ToString() {
        std::string str = path_[index_];
        for (int64_t i = index_; i > 0; i--) {
            str += "->" + path_[i - 1];
        }
        return str;
    }

 public:
    std::string& operator[](uint64_t index) {
        return path_[index];
    }

    std::vector<std::string>::iterator
    begin() {
        return path_.begin();
    }

    std::vector<std::string>::iterator
    end() {
        return path_.end();
    }

 public:
    std::vector<std::string> path_;
    uint64_t index_ = 0;
};

}  // namespace scheduler
}  // namespace milvus
