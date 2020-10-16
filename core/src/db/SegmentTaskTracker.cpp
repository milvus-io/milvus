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

#include "db/SegmentTaskTracker.h"

#include <utility>

namespace milvus {
namespace engine {

SegmentTaskTracker::SegmentTaskTracker(int64_t retry_times) : retry_times_(retry_times) {
    if (retry_times_ <= 0) {
        retry_times_ = 1;
    }
}

void
SegmentTaskTracker::MarkFailedSegment(const std::string& collection_name, int64_t segment_id, const Status& status) {
    std::lock_guard<std::mutex> lock(retry_map_mutex_);

    SegmentRetryMap& retry_map = retry_map_[collection_name];
    FailedRecord& record = retry_map[segment_id];
    ++record.failed_count_;
    record.error_status = status;
}

void
SegmentTaskTracker::MarkFailedSegments(const std::string& collection_name, const SegmentFailedMap& failed_segments) {
    std::lock_guard<std::mutex> lock(retry_map_mutex_);

    SegmentRetryMap& retry_map = retry_map_[collection_name];
    for (auto& pair : failed_segments) {
        FailedRecord& record = retry_map[pair.first];
        ++record.failed_count_;
        record.error_status = pair.second;
    }
}

void
SegmentTaskTracker::IgnoreFailedSegments(const std::string& collection_name, std::vector<int64_t>& segment_ids) {
    std::lock_guard<std::mutex> lock(retry_map_mutex_);

    SegmentRetryMap& retry_map = retry_map_[collection_name];
    if (retry_map.empty()) {
        return;
    }

    std::vector<int64_t> temp_segment_ids;
    for (auto id : segment_ids) {
        if (retry_map.find(id) == retry_map.end()) {
            temp_segment_ids.push_back(id);
            continue;
        }

        FailedRecord& record = retry_map[id];
        if (record.failed_count_ < retry_times_) {
            temp_segment_ids.push_back(id);
        }
    }
    segment_ids.swap(temp_segment_ids);
}

void
SegmentTaskTracker::ClearFailedRecords(const std::string& collection_name) {
    std::lock_guard<std::mutex> lock(retry_map_mutex_);
    retry_map_.erase(collection_name);
}

void
SegmentTaskTracker::GetFailedRecords(const std::string& collection_name, SegmentFailedMap& failed_map) {
    std::lock_guard<std::mutex> lock(retry_map_mutex_);

    SegmentRetryMap& retry_map = retry_map_[collection_name];
    for (auto& pair : retry_map) {
        FailedRecord& record = pair.second;
        failed_map.insert(std::make_pair(pair.first, record.error_status));
    }
}

}  // namespace engine
}  // namespace milvus
