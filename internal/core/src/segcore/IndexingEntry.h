#pragma once
#include "AckResponder.h"
#include <tbb/concurrent_vector.h>
#include "common/Schema.h"
#include <optional>
#include "InsertRecord.h"
#include <knowhere/index/vector_index/IndexIVF.h>

namespace milvus::segcore {

// this should be concurrent
// All concurrent
class IndexingEntry {
 public:
    explicit IndexingEntry(const FieldMeta& field_meta) : field_meta_(field_meta) {
    }

    // concurrent
    knowhere::VecIndex*
    get_indexing(int64_t chunk_id) const {
        return data_.at(chunk_id).get();
    }

    // Do this in parallel
    void
    BuildIndexRange(int64_t ack_beg, int64_t ack_end, const VectorBase* vec_base);

    const FieldMeta&
    get_field_meta() {
        return field_meta_;
    }

    knowhere::Config
    get_build_conf() const;
    knowhere::Config
    get_search_conf(int top_k) const;

 private:
    // additional info
    const FieldMeta& field_meta_;

 private:
    tbb::concurrent_vector<std::unique_ptr<knowhere::VecIndex>> data_;
};

class IndexingRecord {
 public:
    explicit IndexingRecord(const Schema& schema) : schema_(schema) {
        Initialize();
    }

    void
    Initialize() {
        int offset = 0;
        for (auto& field : schema_) {
            if (field.is_vector()) {
                entries_.try_emplace(offset, field);
            }
            ++offset;
        }
        assert(offset == schema_.size());
    }

    // concurrent, reentrant
    void
    UpdateResourceAck(int64_t chunk_ack, const InsertRecord& record);

    // concurrent
    int64_t
    get_finished_ack() {
        return finished_ack_.GetAck();
    }

    const IndexingEntry&
    get_indexing(int i) const {
        assert(entries_.count(i));
        return entries_.at(i);
    }

 private:
    const Schema& schema_;

 private:
    // control info
    std::atomic<int64_t> resource_ack_ = 0;
    //    std::atomic<int64_t> finished_ack_ = 0;
    AckResponder finished_ack_;
    std::mutex mutex_;

 private:
    // field_offset => indexing
    std::map<int, IndexingEntry> entries_;
};

}  // namespace milvus::segcore