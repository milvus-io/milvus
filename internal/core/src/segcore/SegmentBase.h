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
#include <vector>

#include "common/Types.h"
#include "common/Schema.h"
#include <memory>

#include "query/deprecated/GeneralQuery.h"
#include "query/Plan.h"
#include "common/LoadIndex.h"

namespace milvus {
namespace segcore {
// using engine::DataChunk;
// using engine::DataChunkPtr;
using QueryResult = milvus::QueryResult;
struct RowBasedRawData {
    void* raw_data;      // schema
    int sizeof_per_row;  // alignment
    int64_t count;
};

int
TestABI();

class SegmentBase {
 public:
    // definitions
    enum class SegmentState {
        Invalid = 0,
        Open,   // able to insert data
        Closed  // able to build index
    };

 public:
    virtual ~SegmentBase() = default;
    // SegmentBase(std::shared_ptr<FieldsInfo> collection);

    virtual int64_t
    PreInsert(int64_t size) = 0;

    virtual Status
    Insert(int64_t reserved_offset,
           int64_t size,
           const int64_t* row_ids,
           const Timestamp* timestamps,
           const RowBasedRawData& values) = 0;

    virtual int64_t
    PreDelete(int64_t size) = 0;
    // TODO: add id into delete log, possibly bitmap

    virtual Status
    Delete(int64_t reserved_offset, int64_t size, const int64_t* row_ids, const Timestamp* timestamps) = 0;

 public:
    virtual Status
    Search(const query::Plan* Plan,
           const query::PlaceholderGroup* placeholder_groups[],
           const Timestamp timestamps[],
           int num_groups,
           QueryResult& results) = 0;

    virtual Status
    FillTargetEntry(const query::Plan* Plan, QueryResult& results) = 0;

    // stop receive insert requests
    virtual Status
    Close() = 0;

    virtual Status
    LoadIndexing(const LoadIndexInfo& info) = 0;

    virtual int64_t
    GetMemoryUsageInBytes() = 0;

 public:
    virtual ssize_t
    get_row_count() const = 0;

    virtual SegmentState
    get_state() const = 0;

    virtual ssize_t
    get_deleted_count() const = 0;
};

using SegmentBasePtr = std::unique_ptr<SegmentBase>;

SegmentBasePtr
CreateSegment(SchemaPtr schema, int64_t chunk_size = 32 * 1024);

}  // namespace segcore
}  // namespace milvus
