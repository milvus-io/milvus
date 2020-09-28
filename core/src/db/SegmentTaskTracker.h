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

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "utils/Status.h"

namespace milvus {
namespace engine {

using SegmentFailedMap = std::unordered_map<int64_t, Status>;

// this class is to track segment tasks status such as build index and compact, for these purpose:
// record failed segment id and error status
// ignore failed segments in case of necessery
// return error status to client
class SegmentTaskTracker {
 public:
    explicit SegmentTaskTracker(int64_t retry_times);

    void
    MarkFailedSegment(const std::string& collection_name, int64_t segment_id, const Status& status);

    void
    MarkFailedSegments(const std::string& collection_name, const SegmentFailedMap& failed_segments);

    void
    IgnoreFailedSegments(const std::string& collection_name, std::vector<int64_t>& segment_ids);

    void
    ClearFailedRecords(const std::string& collection_name);

    void
    GetFailedRecords(const std::string& collection_name, SegmentFailedMap& failed_map);

 private:
    int64_t retry_times_ = 3;

    struct FailedRecord {
        int64_t failed_count_ = 0;
        Status error_status;
    };
    using SegmentRetryMap = std::unordered_map<int64_t, FailedRecord>;
    using CollectionRetryMap = std::unordered_map<std::string, SegmentRetryMap>;

    CollectionRetryMap retry_map_;
    std::mutex retry_map_mutex_;
};

}  // namespace engine
}  // namespace milvus
