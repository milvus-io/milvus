#include <shared_mutex>

#include "dog_segment/SegmentBase.h"
#include "utils/Status.h"
#include <tbb/concurrent_vector.h>
#include <tbb/concurrent_unordered_map.h>
#include <atomic>

namespace milvus::dog_segment {

int
TestABI() {
    return 42;
}

struct ColumnBasedDataChunk {
    std::vector<std::vector<float>> entity_vecs;
    static ColumnBasedDataChunk from(const DogDataChunk& source, const Schema& schema){
        ColumnBasedDataChunk dest;
        auto count = source.count;
        auto raw_data = reinterpret_cast<const char*>(source.raw_data);
        auto align = source.sizeof_per_row;
        for(auto& field: schema) {
            auto len = field.get_sizeof();
            assert(len % sizeof(float) == 0);
            std::vector<float> new_col(len * count / sizeof(float));
            for(int64_t i = 0; i < count; ++i) {
                memcpy(new_col.data() + i * len / sizeof(float), raw_data + i * align , len);
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

    // TODO: originally, id should be put into data_chunk
    // TODO: Is it ok to put them the other side?
    Status
    Insert(int64_t size, const uint64_t* primary_keys, const Timestamp* timestamps,
           const DogDataChunk& values) override;

    // TODO: add id into delete log, possibly bitmap
    Status
    Delete(int64_t size, const uint64_t* primary_keys, const Timestamp* timestamps) override;

    // query contains metadata of
    Status
    Query(const query::QueryPtr& query, Timestamp timestamp, QueryResult& results) override;

    // // THIS FUNCTION IS REMOVED
    // virtual Status
    // GetEntityByIds(Timestamp timestamp, const std::vector<Id>& ids, DataChunkPtr& results) = 0;

    // stop receive insert requests
    Status
    Close() override {
        std::lock_guard<std::shared_mutex> lck(mutex_);
        assert(state_ == SegmentState::Open);
        state_ = SegmentState::Closed;
        return Status::OK();
    }

    //    // to make all data inserted visible
    //    // maybe a no-op?
    //    virtual Status
    //    Flush(Timestamp timestamp) = 0;

    // BuildIndex With Paramaters, must with Frozen State
    // This function is atomic
    // NOTE: index_params contains serveral policies for several index
    Status
    BuildIndex(std::shared_ptr<IndexConfig> index_params) override {
        throw std::runtime_error("not implemented");
    }

    // Remove Index
    Status
    DropIndex(std::string_view field_name) override {
        throw std::runtime_error("not implemented");
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

 public:
    ssize_t
    get_row_count() const override {
        return ack_count_.load(std::memory_order_relaxed);
    }

    //    const FieldsInfo&
    //    get_fields_info() const override {
    //
    //    }
    //
    //    // check is_indexed here
    //    virtual const IndexConfig&
    //    get_index_param() const = 0;
    //
    SegmentState
    get_state() const override {
        return state_.load(std::memory_order_relaxed);
    }
    //
    //    std::shared_ptr<IndexData>
    //    get_index_data();

    ssize_t
    get_deleted_count() const override {
        return 0;
    }

 public:
    friend std::unique_ptr<SegmentBase>
    CreateSegment(SchemaPtr schema);

    friend SegmentBase*
    CreateSegment();

 private:
    SchemaPtr schema_;
    std::shared_mutex mutex_;
    std::atomic<SegmentState> state_ = SegmentState::Open;
    std::atomic<int64_t> ack_count_ = 0;
    tbb::concurrent_vector<uint64_t> uids_;
    tbb::concurrent_vector<Timestamp> timestamps_;
    std::vector<tbb::concurrent_vector<float>> entity_vecs_;
    tbb::concurrent_unordered_map<uint64_t, int> internal_indexes_;

    tbb::concurrent_unordered_multimap<int, Timestamp> delete_logs_;
};

std::unique_ptr<SegmentBase>
CreateSegment(SchemaPtr schema) {
    auto segment = std::make_unique<SegmentNaive>();
    segment->schema_ = schema;
    segment->entity_vecs_.resize(schema->size());
    return segment;
}

SegmentBase* CreateSegment() {
  auto segment = new SegmentNaive();
  auto schema = std::make_shared<Schema>();
  schema->AddField("fakevec", DataType::VECTOR_FLOAT, 16);
  schema->AddField("age", DataType::INT32);
  segment->schema_ = schema;
  segment->entity_vecs_.resize(schema->size());
  return segment;
}

Status
SegmentNaive::Insert(int64_t size, const uint64_t* primary_keys, const Timestamp* timestamps,
                     const DogDataChunk& row_values) {
    const auto& schema = *schema_;
    auto data_chunk = ColumnBasedDataChunk::from(row_values, schema);

    std::cout << "key:" << std::endl;
    // insert datas
    // TODO: use shared_lock
    std::lock_guard lck(mutex_);
    assert(state_ == SegmentState::Open);
    auto ack_id = ack_count_.load();
    uids_.grow_by(primary_keys, primary_keys + size);
    for(int64_t i = 0; i < size; ++i) {
        auto key = primary_keys[i];
        std::cout << key << std::endl;
        auto internal_index = i + ack_id;
        internal_indexes_[key] = internal_index;
    }
    timestamps_.grow_by(timestamps, timestamps + size);
    for(int fid = 0; fid < schema.size(); ++fid) {
        auto field = schema[fid];
        auto total_len = field.get_sizeof() * size / sizeof(float);
        auto source_vec = data_chunk.entity_vecs[fid];
        entity_vecs_[fid].grow_by(source_vec.data(), source_vec.data() + total_len);
    }

    // finish insert
    ack_count_ += size;
    return Status::OK();
}

Status SegmentNaive::Delete(int64_t size, const uint64_t *primary_keys, const Timestamp *timestamps) {
    for(int i = 0; i < size; ++i) {
        auto key = primary_keys[i];
        auto time = timestamps[i];
        delete_logs_.insert(std::make_pair(key, time));
    }
    return Status::OK();
}

// TODO: remove mock
Status
SegmentNaive::Query(const query::QueryPtr &query, Timestamp timestamp, QueryResult &result) {
    std::shared_lock lck(mutex_);
    auto ack_count = ack_count_.load();
    assert(query == nullptr);
    assert(schema_->size() >= 1);
    const auto& field = schema_->operator[](0);
    assert(field.get_data_type() == DataType::VECTOR_FLOAT);
    assert(field.get_name() == "fakevec");
    auto dim = field.get_dim();
    // assume query vector is [0, 0, ..., 0]
    std::vector<float> query_vector(dim, 0);
    auto& target_vec = entity_vecs_[0];
    int current_index = -1;
    float min_diff = std::numeric_limits<float>::max();
    for(int index = 0; index < ack_count; ++index) {
        float diff = 0;
        int offset = index * dim;
        for(auto d = 0; d < dim; ++d) {
            auto v = target_vec[offset + d] - query_vector[d];
            diff += v * v;
        }
        if(diff < min_diff) {
            min_diff = diff;
            current_index = index;
        }
    }
    QueryResult query_result;
    query_result.row_num_ = 1;
    query_result.result_distances_.push_back(min_diff);
    query_result.result_ids_.push_back(uids_[current_index]);
    // query_result.data_chunk_ = nullptr;
    result = std::move(query_result);
    return Status::OK();
}

}  // namespace milvus::engine










