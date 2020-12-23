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

#include <tbb/concurrent_priority_queue.h>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_vector.h>

#include <knowhere/index/vector_index/VecIndex.h>

#include "query/deprecated/GeneralQuery.h"
#include "utils/Status.h"
#include "utils/EasyAssert.h"
#include "segcore/SegmentBase.h"
#include "segcore/AckResponder.h"
#include "segcore/ConcurrentVector.h"
#include "segcore/DeletedRecord.h"
#include "segcore/InsertRecord.h"
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace milvus::segcore {
class SegmentNaive : public SegmentBase {
 public:
    // SegmentBase(std::shared_ptr<FieldsInfo> collection);

    int64_t
    PreInsert(int64_t size) override;

    // TODO: originally, id should be put into data_chunk
    // TODO: Is it ok to put them the other side?
    Status
    Insert(int64_t reserverd_offset,
           int64_t size,
           const int64_t* row_ids,
           const Timestamp* timestamps,
           const RowBasedRawData& values) override;

    int64_t
    PreDelete(int64_t size) override;

    // TODO: add id into delete log, possibly bitmap
    Status
    Delete(int64_t reserverd_offset, int64_t size, const int64_t* row_ids, const Timestamp* timestamps) override;

 private:
    // NOTE: now deprecated, remains for further copy out
    Status
    QueryDeprecated(query::QueryDeprecatedPtr query_info, Timestamp timestamp, QueryResult& results);

 public:
    Status
    Search(const query::Plan* Plan,
           const query::PlaceholderGroup* placeholder_groups[],
           const Timestamp timestamps[],
           int num_groups,
           QueryResult& results) override {
        PanicInfo("unimplemented");
    }

    // stop receive insert requests
    // will move data to immutable vector or something
    Status
    Close() override;

    // using IndexType = knowhere::IndexType;
    // using IndexMode = knowhere::IndexMode;
    // using IndexConfig = knowhere::Config;
    // BuildIndex With Paramaters, must with Frozen State
    // NOTE: index_params contains serveral policies for several index
    // TODO: currently, index has to be set at startup, and can't be modified
    // AddIndex and DropIndex will be added later
    Status
    BuildIndex(IndexMetaPtr index_meta) override;

    Status
    FillTargetEntry(const query::Plan* Plan, QueryResult& results) override {
        PanicInfo("unimplemented");
    }

    Status
    DropRawData(std::string_view field_name) override {
        // TODO: NO-OP
        return Status::OK();
    }

    Status
    LoadRawData(std::string_view field_name, const char* blob, int64_t blob_size) override {
        // TODO: NO-OP
        return Status::OK();
    }

    int64_t
    GetMemoryUsageInBytes() override;

 public:
    ssize_t
    get_row_count() const override {
        return record_.ack_responder_.GetAck();
    }

    SegmentState
    get_state() const override {
        return state_.load(std::memory_order_relaxed);
    }

    ssize_t
    get_deleted_count() const override {
        return 0;
    }

 public:
    friend std::unique_ptr<SegmentBase>
    CreateSegment(SchemaPtr schema);

    static constexpr int64_t deprecated_fixed_chunk_size = 32 * 1024;
    explicit SegmentNaive(const SchemaPtr& schema) : schema_(schema), record_(*schema, deprecated_fixed_chunk_size) {
    }

 private:
    std::shared_ptr<DeletedRecord::TmpBitmap>
    get_deleted_bitmap(int64_t del_barrier, Timestamp query_timestamp, int64_t insert_barrier, bool force = false);

    Status
    QueryImpl(query::QueryDeprecatedPtr query, Timestamp timestamp, QueryResult& results);

    Status
    QuerySlowImpl(query::QueryDeprecatedPtr query, Timestamp timestamp, QueryResult& results);

    Status
    QueryBruteForceImpl(query::QueryDeprecatedPtr query, Timestamp timestamp, QueryResult& results);

    template <typename Type>
    knowhere::IndexPtr
    BuildVecIndexImpl(const IndexMeta::Entry& entry);

 private:
    SchemaPtr schema_;
    std::atomic<SegmentState> state_ = SegmentState::Open;
    InsertRecord record_;
    DeletedRecord deleted_record_;

    std::atomic<bool> index_ready_ = false;
    IndexMetaPtr index_meta_;
    std::unordered_map<std::string, knowhere::IndexPtr> indexings_;  // index_name => indexing
    tbb::concurrent_unordered_multimap<idx_t, int64_t> uid2offset_;
};
}  // namespace milvus::segcore
