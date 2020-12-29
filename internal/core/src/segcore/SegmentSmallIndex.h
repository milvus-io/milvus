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

#include <shared_mutex>
#include <knowhere/index/vector_index/VecIndex.h>
#include <query/PlanNode.h>

#include "AckResponder.h"
#include "SealedIndexingRecord.h"
#include "ConcurrentVector.h"
#include "segcore/SegmentBase.h"
#include "query/deprecated/GeneralQuery.h"
#include "utils/Status.h"
#include "segcore/DeletedRecord.h"
#include "utils/EasyAssert.h"
#include "IndexingEntry.h"
#include "InsertRecord.h"
#include <utility>
#include <memory>

namespace milvus::segcore {

class SegmentSmallIndex : public SegmentBase {
 public:
    int64_t
    PreInsert(int64_t size) override;

    // TODO: originally, id should be put into data_chunk
    // TODO: Is it ok to put them the other side?
    Status
    Insert(int64_t reserved_offset,
           int64_t size,
           const int64_t* row_ids,
           const Timestamp* timestamps,
           const RowBasedRawData& values) override;

    int64_t
    PreDelete(int64_t size) override;

    // TODO: add id into delete log, possibly bitmap
    Status
    Delete(int64_t reserverd_offset, int64_t size, const int64_t* row_ids, const Timestamp* timestamps) override;

    Status
    Search(const query::Plan* Plan,
           const query::PlaceholderGroup* placeholder_groups[],
           const Timestamp timestamps[],
           int num_groups,
           QueryResult& results) override;

    // stop receive insert requests
    // will move data to immutable vector or something
    Status
    Close() override;

    // using IndexType = knowhere::IndexType;
    // using IndexMode = knowhere::IndexMode;
    // using IndexConfig = knowhere::Config;
    // BuildIndex With Paramaters, must with Frozen State
    // NOTE: index_params contains several policies for several index
    // TODO: currently, index has to be set at startup, and can't be modified
    // AddIndex and DropIndex will be added later
    Status
    BuildIndex(IndexMetaPtr index_meta) override;

    Status
    DropRawData(std::string_view field_name) override {
        PanicInfo("unimplemented");
    }

    Status
    LoadRawData(std::string_view field_name, const char* blob, int64_t blob_size) override {
        PanicInfo("unimplemented");
    }

    int64_t
    GetMemoryUsageInBytes() override;

 public:
    const InsertRecord&
    get_insert_record() const {
        return record_;
    }

    const IndexingRecord&
    get_indexing_record() const {
        return indexing_record_;
    }

    const DeletedRecord&
    get_deleted_record() const {
        return deleted_record_;
    }

    const SealedIndexingRecord&
    get_sealed_indexing_record() const {
        return sealed_indexing_record_;
    }

    const Schema&
    get_schema() const {
        return *schema_;
    }

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

    Status
    LoadIndexing(const LoadIndexInfo& info) override;

 public:
    friend std::unique_ptr<SegmentBase>
    CreateSegment(SchemaPtr schema, int64_t chunk_size);

    explicit SegmentSmallIndex(SchemaPtr schema, int64_t chunk_size)
        : chunk_size_(chunk_size),
          schema_(std::move(schema)),
          record_(*schema_, chunk_size),
          indexing_record_(*schema_, chunk_size) {
    }

 public:
    std::shared_ptr<DeletedRecord::TmpBitmap>
    get_deleted_bitmap(int64_t del_barrier, Timestamp query_timestamp, int64_t insert_barrier, bool force = false);

    template <typename Type>
    knowhere::IndexPtr
    BuildVecIndexImpl(const IndexMeta::Entry& entry);

    Status
    FillTargetEntry(const query::Plan* Plan, QueryResult& results) override;

 private:
    int64_t chunk_size_;
    SchemaPtr schema_;
    std::atomic<SegmentState> state_ = SegmentState::Open;
    IndexMetaPtr index_meta_;

    InsertRecord record_;
    DeletedRecord deleted_record_;
    IndexingRecord indexing_record_;
    SealedIndexingRecord sealed_indexing_record_;

    tbb::concurrent_unordered_multimap<idx_t, int64_t> uid2offset_;
};

}  // namespace milvus::segcore
