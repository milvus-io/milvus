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

#include <atomic>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

#include "common/Types.h"
#include "common/FieldMeta.h"

namespace milvus::segcore {

// statistics for LOB data
struct LOBStats {
    int64_t total_count = 0;  // total number of LOB entries
    int64_t total_bytes = 0;  // total bytes stored in file
};

// per-field LOB storage state
struct FieldLOBState {
    std::ofstream write_stream;
    std::atomic<uint64_t> current_offset{0};
    std::atomic<int64_t> total_bytes{0};
    std::atomic<int64_t> total_count{0};
    std::string file_path;

    FieldLOBState() = default;
    FieldLOBState(FieldLOBState&&) = default;
    FieldLOBState&
    operator=(FieldLOBState&&) = default;
};

// GrowingLOBManager manages LOB (Large Object) data for growing segments.
// it provides a unified interface for storing and retrieving TEXT data.
//
// file layout:
// - separate file per field: {segment_id}/lob/{field_id}
// - data is appended sequentially, LOBReference stores (offset, size)
class GrowingLOBManager {
 public:
    explicit GrowingLOBManager(int64_t segment_id,
                               const std::string& local_data_path);

    ~GrowingLOBManager();

    // disable copy
    GrowingLOBManager(const GrowingLOBManager&) = delete;
    GrowingLOBManager&
    operator=(const GrowingLOBManager&) = delete;

    // write LOB data to file for a specific field and return a LOB reference
    // the reference contains (offset, size) for later random read
    // this method is thread-safe
    LOBReference
    WriteLOB(FieldId field_id, std::string_view text_data);

    // read LOB data from file by reference for a specific field
    // uses offset and size from reference for random read
    // returns the data as a string
    // this method is thread-safe
    std::string
    ReadLOB(FieldId field_id, const LOBReference& ref) const;

    // get total statistics across all fields
    LOBStats
    GetTotalStats() const;

    // get statistics for a specific field
    LOBStats
    GetFieldStats(FieldId field_id) const;

    // get segment ID
    int64_t
    GetSegmentID() const {
        return segment_id_;
    }

    // get LOB file path for a specific field
    std::string
    GetLOBFilePath(FieldId field_id) const;

 private:
    // get or create field state
    FieldLOBState&
    GetOrCreateFieldState(FieldId field_id);

    // get field state for reading (const)
    const FieldLOBState*
    GetFieldState(FieldId field_id) const;

    int64_t segment_id_;
    std::string segment_dir_;  // {local_data_path}/{segment_id}
    std::string lob_dir_;      // {segment_dir}/lob

    // per-field state
    mutable std::mutex mutex_;
    std::unordered_map<int64_t, std::unique_ptr<FieldLOBState>> field_states_;
};

}  // namespace milvus::segcore
