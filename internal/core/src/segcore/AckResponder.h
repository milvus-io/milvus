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
#include <shared_mutex>
#include <mutex>
#include <set>
#include <atomic>
namespace milvus::segcore {
class AckResponder {
 public:
    void
    AddSegment(int64_t seg_begin, int64_t seg_end) {
        std::lock_guard lck(mutex_);
        fetch_and_flip(seg_end);
        auto old_begin = fetch_and_flip(seg_begin);
        if (old_begin) {
            minimum_ = *acks_.begin();
        }
    }

    int64_t
    GetAck() const {
        return minimum_;
    }

 private:
    bool
    fetch_and_flip(int64_t endpoint) {
        if (acks_.count(endpoint)) {
            acks_.erase(endpoint);
            return true;
        } else {
            acks_.insert(endpoint);
            return false;
        }
    }

 private:
    std::shared_mutex mutex_;
    std::set<int64_t> acks_ = {0};
    std::atomic<int64_t> minimum_ = 0;
};
}  // namespace milvus::segcore
