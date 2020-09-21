#pragma once

#include <tbb/concurrent_priority_queue.h>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_vector.h>

#include <shared_mutex>
#include <knowhere/index/vector_index/VecIndex.h>

#include "AckResponder.h"
#include "ConcurrentVector.h"
#include "dog_segment/SegmentBase.h"
// #include "knowhere/index/structured_index/StructuredIndex.h"
#include "query/GeneralQuery.h"
#include "utils/Status.h"
#include "dog_segment/DeletedRecord.h"

namespace milvus::dog_segment {
struct ColumnBasedDataChunk {
    std::vector<std::vector<float>> entity_vecs;

    static ColumnBasedDataChunk
    from(const DogDataChunk &source, const Schema &schema) {
        ColumnBasedDataChunk dest;
        auto count = source.count;
        auto raw_data = reinterpret_cast<const char *>(source.raw_data);
        auto align = source.sizeof_per_row;
        for (auto &field : schema) {
            auto len = field.get_sizeof();
            assert(len % sizeof(float) == 0);
            std::vector<float> new_col(len * count / sizeof(float));
            for (int64_t i = 0; i < count; ++i) {
                memcpy(new_col.data() + i * len / sizeof(float), raw_data + i * align, len);
            }
            dest.entity_vecs.push_back(std::move(new_col));
            // offset the raw_data
            raw_data += len / sizeof(float);
        }
        return dest;
    }
};

class SegmentNaive : public SegmentBase {
public:
    virtual ~SegmentNaive() = default;

    // SegmentBase(std::shared_ptr<FieldsInfo> collection);

    int64_t PreInsert(int64_t size) override;

    // TODO: originally, id should be put into data_chunk
    // TODO: Is it ok to put them the other side?
    Status
    Insert(int64_t reserverd_offset, int64_t size, const int64_t *primary_keys, const Timestamp *timestamps,
           const DogDataChunk &values) override;

    int64_t PreDelete(int64_t size) override;

    // TODO: add id into delete log, possibly bitmap
    Status
    Delete(int64_t reserverd_offset, int64_t size, const int64_t *primary_keys, const Timestamp *timestamps) override;

    // query contains metadata of
    Status
    Query(query::QueryPtr query_info, Timestamp timestamp, QueryResult &results) override;

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
    DropRawData(std::string_view field_name) override {
        // TODO: NO-OP
        return Status::OK();
    }

    Status
    LoadRawData(std::string_view field_name, const char *blob, int64_t blob_size) override {
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

    explicit SegmentNaive(SchemaPtr schema)
            : schema_(schema), record_(*schema) {
    }

private:
//    struct MutableRecord {
//        ConcurrentVector<uint64_t> uids_;
//        tbb::concurrent_vector<Timestamp> timestamps_;
//        std::vector<tbb::concurrent_vector<float>> entity_vecs_;
//
//        MutableRecord(int entity_size) : entity_vecs_(entity_size) {
//        }
//    };

    struct Record {
        std::atomic<int64_t> reserved = 0;
        AckResponder ack_responder_;
        ConcurrentVector<Timestamp, true> timestamps_;
        ConcurrentVector<idx_t, true> uids_;
        std::vector<std::shared_ptr<VectorBase>> entity_vec_;

        Record(const Schema &schema);

        template<typename Type>
        auto get_vec_entity(int offset) {
            return std::static_pointer_cast<ConcurrentVector<Type>>(entity_vec_[offset]);
        }
    };


    std::shared_ptr<DeletedRecord::TmpBitmap>
    get_deleted_bitmap(int64_t del_barrier, Timestamp query_timestamp, int64_t insert_barrier, bool force = false);

    Status
    QueryImpl(query::QueryPtr query, Timestamp timestamp, QueryResult &results);

    Status
    QuerySlowImpl(query::QueryPtr query, Timestamp timestamp, QueryResult &results);

    Status
    QueryBruteForceImpl(query::QueryPtr query, Timestamp timestamp, QueryResult &results);

    template<typename Type>
    knowhere::IndexPtr BuildVecIndexImpl(const IndexMeta::Entry &entry);

private:
    SchemaPtr schema_;
    std::atomic<SegmentState> state_ = SegmentState::Open;
    Record record_;
    DeletedRecord deleted_record_;

    std::atomic<bool> index_ready_ = false;
    IndexMetaPtr index_meta_;
    std::unordered_map<std::string, knowhere::IndexPtr> indexings_; // index_name => indexing
    tbb::concurrent_unordered_multimap<idx_t, int64_t> uid2offset_;
};
}  // namespace milvus::dog_segment
