#pragma once

#include <tbb/concurrent_priority_queue.h>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_vector.h>

#include <shared_mutex>
#include <knowhere/index/vector_index/VecIndex.h>
#include <query/PlanNode.h>

#include "AckResponder.h"
#include "ConcurrentVector.h"
#include "segcore/SegmentBase.h"
// #include "knowhere/index/structured_index/StructuredIndex.h"
#include "query/deprecated/GeneralQuery.h"
#include "utils/Status.h"
#include "segcore/DeletedRecord.h"
#include "utils/EasyAssert.h"
#include "IndexingEntry.h"
#include "InsertRecord.h"

namespace milvus::segcore {
// struct ColumnBasedDataChunk {
//    std::vector<std::vector<float>> entity_vecs;
//
//    static ColumnBasedDataChunk
//    from(const RowBasedRawData& source, const Schema& schema) {
//        ColumnBasedDataChunk dest;
//        auto count = source.count;
//        auto raw_data = reinterpret_cast<const char*>(source.raw_data);
//        auto align = source.sizeof_per_row;
//        for (auto& field : schema) {
//            auto len = field.get_sizeof();
//            Assert(len % sizeof(float) == 0);
//            std::vector<float> new_col(len * count / sizeof(float));
//            for (int64_t i = 0; i < count; ++i) {
//                memcpy(new_col.data() + i * len / sizeof(float), raw_data + i * align, len);
//            }
//            dest.entity_vecs.push_back(std::move(new_col));
//            // offset the raw_data
//            raw_data += len / sizeof(float);
//        }
//        return dest;
//    }
//};

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

 public:
    friend std::unique_ptr<SegmentBase>
    CreateSegment(SchemaPtr schema);

    explicit SegmentSmallIndex(SchemaPtr schema)
        : schema_(std::move(schema)), record_(*schema_), indexing_record_(*schema_) {
    }

 public:
    std::shared_ptr<DeletedRecord::TmpBitmap>
    get_deleted_bitmap(int64_t del_barrier, Timestamp query_timestamp, int64_t insert_barrier, bool force = false);

    // Status
    // QueryBruteForceImpl(query::QueryPtr query, Timestamp timestamp, QueryResult& results);

    //    Status
    //    QueryBruteForceImpl(const query::QueryInfo& info,
    //                        const float* query_data,
    //                        int64_t num_queries,
    //                        Timestamp timestamp,
    //                        QueryResult& results);

    template <typename Type>
    knowhere::IndexPtr
    BuildVecIndexImpl(const IndexMeta::Entry& entry);

 private:
    SchemaPtr schema_;
    std::atomic<SegmentState> state_ = SegmentState::Open;
    IndexMetaPtr index_meta_;

    InsertRecord record_;
    DeletedRecord deleted_record_;
    IndexingRecord indexing_record_;

    // std::atomic<bool> index_ready_ = false;
    // std::unordered_map<std::string, knowhere::IndexPtr> indexings_;  // index_name => indexing
    tbb::concurrent_unordered_multimap<idx_t, int64_t> uid2offset_;
};

}  // namespace milvus::segcore
