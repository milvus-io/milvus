#include <dog_segment/SegmentNaive.h>

#include <algorithm>
#include <numeric>
#include <thread>

namespace milvus::dog_segment {
int
TestABI() {
    return 42;
}

std::unique_ptr<SegmentBase>
CreateSegment(SchemaPtr schema, IndexMetaPtr remote_index_meta) {
    auto segment = std::make_unique<SegmentNaive>(schema, remote_index_meta);
    return segment;
}

SegmentNaive::Record::Record(const Schema& schema) : uids_(1), timestamps_(1) {
    for (auto& field : schema) {
        if (field.is_vector()) {
            assert(field.get_data_type() == DataType::VECTOR_FLOAT);
            entity_vec_.emplace_back(std::make_shared<ConcurrentVector<float>>(field.get_dim()));
        } else {
            assert(field.get_data_type() == DataType::INT32);
            entity_vec_.emplace_back(std::make_shared<ConcurrentVector<int32_t, true>>());
        }
    }
}

int64_t
SegmentNaive::PreInsert(int64_t size) {
    auto reserved_begin = record_.reserved.fetch_add(size);
    return reserved_begin;
}

int64_t
SegmentNaive::PreDelete(int64_t size) {
    throw std::runtime_error("unimplemented");
}

Status
SegmentNaive::Insert(int64_t reserved_begin, int64_t size, const int64_t* uids_raw, const Timestamp* timestamps_raw,
                     const DogDataChunk& entities_raw) {
    assert(entities_raw.count == size);
    assert(entities_raw.sizeof_per_row == schema_->get_total_sizeof());
    auto raw_data = reinterpret_cast<const char*>(entities_raw.raw_data);
    //    std::vector<char> entities(raw_data, raw_data + size * len_per_row);

    auto len_per_row = entities_raw.sizeof_per_row;
    std::vector<std::tuple<Timestamp, idx_t, int64_t>> ordering;
    ordering.resize(size);
    // #pragma omp parallel for
    for (int i = 0; i < size; ++i) {
        ordering[i] = std::make_tuple(timestamps_raw[i], uids_raw[i], i);
    }
    std::sort(ordering.begin(), ordering.end());
    auto sizeof_infos = schema_->get_sizeof_infos();
    std::vector<int> offset_infos(schema_->size() + 1, 0);
    std::partial_sum(sizeof_infos.begin(), sizeof_infos.end(), offset_infos.begin() + 1);
    std::vector<std::vector<char>> entities(schema_->size());

    for (int fid = 0; fid < schema_->size(); ++fid) {
        auto len = sizeof_infos[fid];
        entities[fid].resize(len * size);
    }

    std::vector<idx_t> uids(size);
    std::vector<Timestamp> timestamps(size);
    // #pragma omp parallel for
    for (int index = 0; index < size; ++index) {
        auto [t, uid, order_index] = ordering[index];
        timestamps[index] = t;
        uids[index] = uid;
        for (int fid = 0; fid < schema_->size(); ++fid) {
            auto len = sizeof_infos[fid];
            auto offset = offset_infos[fid];
            auto src = raw_data + offset + order_index * len_per_row;
            auto dst = entities[fid].data() + index * len;
            memcpy(dst, src, len);
        }
    }

    record_.timestamps_.set_data(reserved_begin, timestamps.data(), size);
    record_.uids_.set_data(reserved_begin, uids.data(), size);
    for (int fid = 0; fid < schema_->size(); ++fid) {
        record_.entity_vec_[fid]->set_data_raw(reserved_begin, entities[fid].data(), size);
    }

    record_.ack_responder_.AddSegment(reserved_begin, size);
    return Status::OK();

    //    std::thread go(executor, std::move(uids), std::move(timestamps), std::move(entities));
    //    go.detach();
    //    const auto& schema = *schema_;
    //    auto record_ptr = GetMutableRecord();
    //    assert(record_ptr);
    //    auto& record = *record_ptr;
    //    auto data_chunk = ColumnBasedDataChunk::from(row_values, schema);
    //
    //    // TODO: use shared_lock for better concurrency
    //    std::lock_guard lck(mutex_);
    //    assert(state_ == SegmentState::Open);
    //    auto ack_id = ack_count_.load();
    //    record.uids_.grow_by(primary_keys, primary_keys + size);
    //    for (int64_t i = 0; i < size; ++i) {
    //        auto key = primary_keys[i];
    //        auto internal_index = i + ack_id;
    //        internal_indexes_[key] = internal_index;
    //    }
    //    record.timestamps_.grow_by(timestamps, timestamps + size);
    //    for (int fid = 0; fid < schema.size(); ++fid) {
    //        auto field = schema[fid];
    //        auto total_len = field.get_sizeof() * size / sizeof(float);
    //        auto source_vec = data_chunk.entity_vecs[fid];
    //        record.entity_vecs_[fid].grow_by(source_vec.data(), source_vec.data() + total_len);
    //    }
    //
    //    // finish insert
    //    ack_count_ += size;
    //    return Status::OK();
}

Status
SegmentNaive::Delete(int64_t reserved_offset, int64_t size, const int64_t* primary_keys, const Timestamp* timestamps) {
    throw std::runtime_error("unimplemented");
    //    for (int i = 0; i < size; ++i) {
    //        auto key = primary_keys[i];
    //        auto time = timestamps[i];
    //        delete_logs_.insert(std::make_pair(key, time));
    //    }
    //    return Status::OK();
}

// TODO: remove mock

Status
SegmentNaive::QueryImpl(const query::QueryPtr& query, Timestamp timestamp, QueryResult& result) {
    throw std::runtime_error("unimplemented");
    //    auto ack_count = ack_count_.load();
    //    assert(query == nullptr);
    //    assert(schema_->size() >= 1);
    //    const auto& field = schema_->operator[](0);
    //    assert(field.get_data_type() == DataType::VECTOR_FLOAT);
    //    assert(field.get_name() == "fakevec");
    //    auto dim = field.get_dim();
    //    // assume query vector is [0, 0, ..., 0]
    //    std::vector<float> query_vector(dim, 0);
    //    auto& target_vec = record.entity_vecs_[0];
    //    int current_index = -1;
    //    float min_diff = std::numeric_limits<float>::max();
    //    for (int index = 0; index < ack_count; ++index) {
    //        float diff = 0;
    //        int offset = index * dim;
    //        for (auto d = 0; d < dim; ++d) {
    //            auto v = target_vec[offset + d] - query_vector[d];
    //            diff += v * v;
    //        }
    //        if (diff < min_diff) {
    //            min_diff = diff;
    //            current_index = index;
    //        }
    //    }
    //    QueryResult query_result;
    //    query_result.row_num_ = 1;
    //    query_result.result_distances_.push_back(min_diff);
    //    query_result.result_ids_.push_back(record.uids_[current_index]);
    //    query_result.data_chunk_ = nullptr;
    //    result = std::move(query_result);
    //    return Status::OK();
}

Status
SegmentNaive::Query(const query::QueryPtr& query, Timestamp timestamp, QueryResult& result) {
    // TODO: enable delete
    // TODO: enable index
    auto& field = schema_->operator[](0);
    assert(field.get_name() == "fakevec");
    assert(field.get_data_type() == DataType::VECTOR_FLOAT);
    auto dim = field.get_dim();
    assert(query == nullptr);
    int64_t barrier = [&]
    {
        auto& vec = record_.timestamps_;
        int64_t beg = 0;
        int64_t end = record_.ack_responder_.GetAck();
        while (beg < end) {
            auto mid = (beg + end) / 2;
            if (vec[mid] < timestamp) {
                end = mid + 1;
            } else {
                beg = mid;
            }

        }
        return beg;
    }();
    // search until barriers
    // TODO: optimize
    auto vec_ptr = std::static_pointer_cast<ConcurrentVector<float>>(record_.entity_vec_[0]);
    for(int64_t i = 0; i < barrier; ++i) {
//        auto element =
        throw std::runtime_error("unimplemented");
    }

    return Status::OK();
    // find end of binary
    //    throw std::runtime_error("unimplemented");
    //    auto record_ptr = GetMutableRecord();
    //    if (record_ptr) {
    //        return QueryImpl(*record_ptr, query, timestamp, result);
    //    } else {
    //        assert(ready_immutable_);
    //        return QueryImpl(*record_immutable_, query, timestamp, result);
    //    }
}

Status
SegmentNaive::Close() {
    state_ = SegmentState::Closed;
    return Status::OK();
    //    auto src_record = GetMutableRecord();
    //    assert(src_record);
    //
    //    auto dst_record = std::make_shared<ImmutableRecord>(schema_->size());
    //
    //    auto data_move = [](auto& dst_vec, const auto& src_vec) {
    //        assert(dst_vec.size() == 0);
    //        dst_vec.insert(dst_vec.begin(), src_vec.begin(), src_vec.end());
    //    };
    //    data_move(dst_record->uids_, src_record->uids_);
    //    data_move(dst_record->timestamps_, src_record->uids_);
    //
    //    assert(src_record->entity_vecs_.size() == schema_->size());
    //    assert(dst_record->entity_vecs_.size() == schema_->size());
    //    for (int i = 0; i < schema_->size(); ++i) {
    //        data_move(dst_record->entity_vecs_[i], src_record->entity_vecs_[i]);
    //    }
    //    bool ready_old = false;
    //    record_immutable_ = dst_record;
    //    ready_immutable_.compare_exchange_strong(ready_old, true);
    //    if (ready_old) {
    //        throw std::logic_error("Close may be called twice, with potential race condition");
    //    }
    //    return Status::OK();
}

Status
SegmentNaive::BuildIndex() {
    throw std::runtime_error("unimplemented");
    //    assert(ready_immutable_);
    //    throw std::runtime_error("unimplemented");
}

}  // namespace milvus::dog_segment
