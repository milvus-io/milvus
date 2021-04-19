#include <segcore/SegmentNaive.h>
#include <random>
#include <algorithm>
#include <numeric>
#include <thread>
#include <queue>

#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include <knowhere/index/vector_index/VecIndexFactory.h>
#include <faiss/utils/distances.h>

namespace milvus::segcore {

int64_t
SegmentNaive::PreInsert(int64_t size) {
    auto reserved_begin = record_.reserved.fetch_add(size);
    return reserved_begin;
}

int64_t
SegmentNaive::PreDelete(int64_t size) {
    auto reserved_begin = deleted_record_.reserved.fetch_add(size);
    return reserved_begin;
}

auto
SegmentNaive::get_deleted_bitmap(int64_t del_barrier, Timestamp query_timestamp, int64_t insert_barrier, bool force)
    -> std::shared_ptr<DeletedRecord::TmpBitmap> {
    auto old = deleted_record_.get_lru_entry();

    if (!force || old->bitmap_ptr->count() == insert_barrier) {
        if (old->del_barrier == del_barrier) {
            return old;
        }
    }

    auto current = old->clone(insert_barrier);
    current->del_barrier = del_barrier;

    auto bitmap = current->bitmap_ptr;
    if (del_barrier < old->del_barrier) {
        for (auto del_index = del_barrier; del_index < old->del_barrier; ++del_index) {
            // get uid in delete logs
            auto uid = deleted_record_.uids_[del_index];
            // map uid to corrensponding offsets, select the max one, which should be the target
            // the max one should be closest to query_timestamp, so the delete log should refer to it
            int64_t the_offset = -1;
            auto [iter_b, iter_e] = uid2offset_.equal_range(uid);
            for (auto iter = iter_b; iter != iter_e; ++iter) {
                auto offset = iter->second;
                if (record_.timestamps_[offset] < query_timestamp) {
                    Assert(offset < insert_barrier);
                    the_offset = std::max(the_offset, offset);
                }
            }
            // if not found, skip
            if (the_offset == -1) {
                continue;
            }
            // otherwise, clear the flag
            bitmap->clear(the_offset);
        }
        return current;
    } else {
        for (auto del_index = old->del_barrier; del_index < del_barrier; ++del_index) {
            // get uid in delete logs
            auto uid = deleted_record_.uids_[del_index];
            // map uid to corrensponding offsets, select the max one, which should be the target
            // the max one should be closest to query_timestamp, so the delete log should refer to it
            int64_t the_offset = -1;
            auto [iter_b, iter_e] = uid2offset_.equal_range(uid);
            for (auto iter = iter_b; iter != iter_e; ++iter) {
                auto offset = iter->second;
                if (offset >= insert_barrier) {
                    continue;
                }
                if (record_.timestamps_[offset] < query_timestamp) {
                    Assert(offset < insert_barrier);
                    the_offset = std::max(the_offset, offset);
                }
            }

            // if not found, skip
            if (the_offset == -1) {
                continue;
            }

            // otherwise, set the flag
            bitmap->set(the_offset);
        }
        this->deleted_record_.insert_lru_entry(current);
    }
    return current;
}

Status
SegmentNaive::Insert(int64_t reserved_begin,
                     int64_t size,
                     const int64_t* uids_raw,
                     const Timestamp* timestamps_raw,
                     const RowBasedRawData& entities_raw) {
    Assert(entities_raw.count == size);
    if (entities_raw.sizeof_per_row != schema_->get_total_sizeof()) {
        std::string msg = "entity length = " + std::to_string(entities_raw.sizeof_per_row) +
                          ", schema length = " + std::to_string(schema_->get_total_sizeof());
        throw std::runtime_error(msg);
    }

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

    for (int i = 0; i < uids.size(); ++i) {
        auto uid = uids[i];
        // NOTE: this must be the last step, cannot be put above
        uid2offset_.insert(std::make_pair(uid, reserved_begin + i));
    }

    record_.ack_responder_.AddSegment(reserved_begin, reserved_begin + size);
    return Status::OK();

    //    std::thread go(executor, std::move(uids), std::move(timestamps), std::move(entities));
    //    go.detach();
    //    const auto& schema = *schema_;
    //    auto record_ptr = GetMutableRecord();
    //    Assert(record_ptr);
    //    auto& record = *record_ptr;
    //    auto data_chunk = ColumnBasedDataChunk::from(row_values, schema);
    //
    //    // TODO: use shared_lock for better concurrency
    //    std::lock_guard lck(mutex_);
    //    Assert(state_ == SegmentState::Open);
    //    auto ack_id = ack_count_.load();
    //    record.uids_.grow_by(row_ids, row_ids + size);
    //    for (int64_t i = 0; i < size; ++i) {
    //        auto key = row_ids[i];
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
SegmentNaive::Delete(int64_t reserved_begin, int64_t size, const int64_t* uids_raw, const Timestamp* timestamps_raw) {
    std::vector<std::tuple<Timestamp, idx_t>> ordering;
    ordering.resize(size);
    // #pragma omp parallel for
    for (int i = 0; i < size; ++i) {
        ordering[i] = std::make_tuple(timestamps_raw[i], uids_raw[i]);
    }
    std::sort(ordering.begin(), ordering.end());
    std::vector<idx_t> uids(size);
    std::vector<Timestamp> timestamps(size);
    // #pragma omp parallel for
    for (int index = 0; index < size; ++index) {
        auto [t, uid] = ordering[index];
        timestamps[index] = t;
        uids[index] = uid;
    }
    deleted_record_.timestamps_.set_data(reserved_begin, timestamps.data(), size);
    deleted_record_.uids_.set_data(reserved_begin, uids.data(), size);
    deleted_record_.ack_responder_.AddSegment(reserved_begin, reserved_begin + size);
    return Status::OK();
    //    for (int i = 0; i < size; ++i) {
    //        auto key = row_ids[i];
    //        auto time = timestamps[i];
    //        delete_logs_.insert(std::make_pair(key, time));
    //    }
    //    return Status::OK();
}

template <typename RecordType>
int64_t
get_barrier(const RecordType& record, Timestamp timestamp) {
    auto& vec = record.timestamps_;
    int64_t beg = 0;
    int64_t end = record.ack_responder_.GetAck();
    while (beg < end) {
        auto mid = (beg + end) / 2;
        if (vec[mid] < timestamp) {
            beg = mid + 1;
        } else {
            end = mid;
        }
    }
    return beg;
}

Status
SegmentNaive::QueryImpl(query::QueryPtr query_info, Timestamp timestamp, QueryResult& result) {
    auto ins_barrier = get_barrier(record_, timestamp);
    auto del_barrier = get_barrier(deleted_record_, timestamp);
    auto bitmap_holder = get_deleted_bitmap(del_barrier, timestamp, ins_barrier, true);
    Assert(bitmap_holder);
    Assert(bitmap_holder->bitmap_ptr->count() == ins_barrier);

    auto field_offset = schema_->get_offset(query_info->field_name);
    auto& field = schema_->operator[](query_info->field_name);

    Assert(field.get_data_type() == DataType::VECTOR_FLOAT);
    auto dim = field.get_dim();
    auto bitmap = bitmap_holder->bitmap_ptr;
    auto topK = query_info->topK;
    auto num_queries = query_info->num_queries;
    auto the_offset_opt = schema_->get_offset(query_info->field_name);
    Assert(the_offset_opt.has_value());
    Assert(the_offset_opt.value() < record_.entity_vec_.size());
    auto vec_ptr = std::static_pointer_cast<ConcurrentVector<float>>(record_.entity_vec_.at(the_offset_opt.value()));
    auto index_entry = index_meta_->lookup_by_field(query_info->field_name);
    auto conf = index_entry.config;

    conf[milvus::knowhere::meta::TOPK] = query_info->topK;
    {
        auto count = 0;
        for (int i = 0; i < bitmap->count(); ++i) {
            if (bitmap->test(i)) {
                ++count;
            }
        }
        std::cout << "fuck " << count << std::endl;
    }

    auto indexing = std::static_pointer_cast<knowhere::VecIndex>(indexings_[index_entry.index_name]);
    indexing->SetBlacklist(bitmap);
    auto ds = knowhere::GenDataset(query_info->num_queries, dim, query_info->query_raw_data.data());
    auto final = indexing->Query(ds, conf, bitmap);

    auto ids = final->Get<idx_t*>(knowhere::meta::IDS);
    auto distances = final->Get<float*>(knowhere::meta::DISTANCE);

    auto total_num = num_queries * topK;
    result.result_ids_.resize(total_num);
    result.result_distances_.resize(total_num);

    result.num_queries_ = num_queries;
    result.topK_ = topK;

    std::copy_n(ids, total_num, result.result_ids_.data());
    std::copy_n(distances, total_num, result.result_distances_.data());

    for (auto& id : result.result_ids_) {
        id = record_.uids_[id];
    }

    return Status::OK();
}

static void
merge_into(int64_t queries,
           int64_t topk,
           float* distances,
           int64_t* uids,
           const float* new_distances,
           const int64_t* new_uids) {
    for (int64_t qn = 0; qn < queries; ++qn) {
        auto base = qn * topk;
        auto src2_dis = distances + base;
        auto src2_uids = uids + base;

        auto src1_dis = new_distances + base;
        auto src1_uids = new_uids + base;

        std::vector<float> buf_dis(topk);
        std::vector<int64_t> buf_uids(topk);

        auto it1 = 0;
        auto it2 = 0;

        for (auto buf = 0; buf < topk; ++buf) {
            if (src1_dis[it1] <= src2_dis[it2]) {
                buf_dis[buf] = src1_dis[it1];
                buf_uids[buf] = src1_uids[it1];
                ++it1;
            } else {
                buf_dis[buf] = src2_dis[it2];
                buf_uids[buf] = src2_uids[it2];
                ++it2;
            }
        }
        std::copy_n(buf_dis.data(), topk, src2_dis);
        std::copy_n(buf_uids.data(), topk, src2_uids);
    }
}

Status
SegmentNaive::QueryBruteForceImpl(query::QueryPtr query_info, Timestamp timestamp, QueryResult& results) {
    auto ins_barrier = get_barrier(record_, timestamp);
    auto del_barrier = get_barrier(deleted_record_, timestamp);
    auto bitmap_holder = get_deleted_bitmap(del_barrier, timestamp, ins_barrier);
    Assert(bitmap_holder);

    auto& field = schema_->operator[](query_info->field_name);
    Assert(field.get_data_type() == DataType::VECTOR_FLOAT);
    auto dim = field.get_dim();
    auto bitmap = bitmap_holder->bitmap_ptr;
    auto topK = query_info->topK;
    auto num_queries = query_info->num_queries;
    auto total_count = topK * num_queries;
    // TODO: optimize

    auto the_offset_opt = schema_->get_offset(query_info->field_name);
    Assert(the_offset_opt.has_value());
    Assert(the_offset_opt.value() < record_.entity_vec_.size());
    auto vec_ptr = std::static_pointer_cast<ConcurrentVector<float>>(record_.entity_vec_.at(the_offset_opt.value()));

    std::vector<int64_t> final_uids(total_count);
    std::vector<float> final_dis(total_count, std::numeric_limits<float>::max());

    auto max_chunk = (ins_barrier + DefaultElementPerChunk - 1) / DefaultElementPerChunk;
    for (int chunk_id = 0; chunk_id < max_chunk; ++chunk_id) {
        std::vector<int64_t> buf_uids(total_count, -1);
        std::vector<float> buf_dis(total_count, std::numeric_limits<float>::max());

        faiss::float_maxheap_array_t buf = {(size_t)num_queries, (size_t)topK, buf_uids.data(), buf_dis.data()};

        auto src_data = vec_ptr->get_chunk(chunk_id).data();
        auto nsize =
            chunk_id != max_chunk - 1 ? DefaultElementPerChunk : ins_barrier - chunk_id * DefaultElementPerChunk;
        auto offset = chunk_id * DefaultElementPerChunk;

        faiss::knn_L2sqr(query_info->query_raw_data.data(), src_data, dim, num_queries, nsize, &buf, bitmap, offset);
        if (chunk_id == 0) {
            final_uids = buf_uids;
            final_dis = buf_dis;
        } else {
            merge_into(num_queries, topK, final_dis.data(), final_uids.data(), buf_dis.data(), buf_uids.data());
        }
    }

    for (auto& id : final_uids) {
        id = record_.uids_[id];
    }

    results.result_ids_ = std::move(final_uids);
    results.result_distances_ = std::move(final_dis);
    results.topK_ = topK;
    results.num_queries_ = num_queries;

    //    throw std::runtime_error("unimplemented");
    return Status::OK();
}

Status
SegmentNaive::QuerySlowImpl(query::QueryPtr query_info, Timestamp timestamp, QueryResult& result) {
    auto ins_barrier = get_barrier(record_, timestamp);
    auto del_barrier = get_barrier(deleted_record_, timestamp);
    auto bitmap_holder = get_deleted_bitmap(del_barrier, timestamp, ins_barrier);
    Assert(bitmap_holder);

    auto& field = schema_->operator[](query_info->field_name);
    Assert(field.get_data_type() == DataType::VECTOR_FLOAT);
    auto dim = field.get_dim();
    auto bitmap = bitmap_holder->bitmap_ptr;
    auto topK = query_info->topK;
    auto num_queries = query_info->num_queries;
    // TODO: optimize
    auto the_offset_opt = schema_->get_offset(query_info->field_name);
    Assert(the_offset_opt.has_value());
    Assert(the_offset_opt.value() < record_.entity_vec_.size());
    auto vec_ptr = std::static_pointer_cast<ConcurrentVector<float>>(record_.entity_vec_.at(the_offset_opt.value()));
    std::vector<std::priority_queue<std::pair<float, int>>> records(num_queries);

    auto get_L2_distance = [dim](const float* a, const float* b) {
        float L2_distance = 0;
        for (auto i = 0; i < dim; ++i) {
            auto d = a[i] - b[i];
            L2_distance += d * d;
        }
        return L2_distance;
    };

    for (int64_t i = 0; i < ins_barrier; ++i) {
        if (i < bitmap->count() && bitmap->test(i)) {
            continue;
        }
        auto element = vec_ptr->get_element(i);
        for (auto query_id = 0; query_id < num_queries; ++query_id) {
            auto query_blob = query_info->query_raw_data.data() + query_id * dim;
            auto dis = get_L2_distance(query_blob, element);
            auto& record = records[query_id];
            if (record.size() < topK) {
                record.emplace(dis, i);
            } else if (record.top().first > dis) {
                record.emplace(dis, i);
                record.pop();
            }
        }
    }

    result.num_queries_ = num_queries;
    result.topK_ = topK;
    auto row_num = topK * num_queries;

    result.result_ids_.resize(row_num);
    result.result_distances_.resize(row_num);

    for (int q_id = 0; q_id < num_queries; ++q_id) {
        // reverse
        for (int i = 0; i < topK; ++i) {
            auto dst_id = topK - 1 - i + q_id * topK;
            auto [dis, offset] = records[q_id].top();
            records[q_id].pop();
            result.result_ids_[dst_id] = record_.uids_[offset];
            result.result_distances_[dst_id] = dis;
        }
    }

    return Status::OK();
}

Status
SegmentNaive::QueryDeprecated(query::QueryPtr query_info, Timestamp timestamp, QueryResult& result) {
    // TODO: enable delete
    // TODO: enable index
    // TODO: remove mock
    if (query_info == nullptr) {
        query_info = std::make_shared<query::Query>();
        query_info->field_name = "fakevec";
        query_info->topK = 10;
        query_info->num_queries = 1;

        auto dim = schema_->operator[]("fakevec").get_dim();
        std::default_random_engine e(42);
        std::uniform_real_distribution<> dis(0.0, 1.0);
        query_info->query_raw_data.resize(query_info->num_queries * dim);
        for (auto& x : query_info->query_raw_data) {
            x = dis(e);
        }
    }

    if (index_ready_) {
        return QueryImpl(query_info, timestamp, result);
    } else {
        return QueryBruteForceImpl(query_info, timestamp, result);
    }
}

Status
SegmentNaive::Close() {
    if (this->record_.reserved != this->record_.ack_responder_.GetAck()) {
        std::runtime_error("insert not ready");
    }
    if (this->deleted_record_.reserved != this->record_.ack_responder_.GetAck()) {
        std::runtime_error("delete not ready");
    }
    state_ = SegmentState::Closed;
    return Status::OK();
}

template <typename Type>
knowhere::IndexPtr
SegmentNaive::BuildVecIndexImpl(const IndexMeta::Entry& entry) {
    auto offset_opt = schema_->get_offset(entry.field_name);
    Assert(offset_opt.has_value());
    auto offset = offset_opt.value();
    auto field = (*schema_)[offset];
    auto dim = field.get_dim();

    auto indexing = knowhere::VecIndexFactory::GetInstance().CreateVecIndex(entry.type, entry.mode);
    auto chunk_size = record_.uids_.chunk_size();

    auto& uids = record_.uids_;
    auto entities = record_.get_vec_entity<float>(offset);

    std::vector<knowhere::DatasetPtr> datasets;
    for (int chunk_id = 0; chunk_id < uids.chunk_size(); ++chunk_id) {
        auto entities_chunk = entities->get_chunk(chunk_id).data();
        int64_t count = chunk_id == uids.chunk_size() - 1 ? record_.reserved - chunk_id * DefaultElementPerChunk
                                                          : DefaultElementPerChunk;
        datasets.push_back(knowhere::GenDataset(count, dim, entities_chunk));
    }
    for (auto& ds : datasets) {
        indexing->Train(ds, entry.config);
    }
    for (auto& ds : datasets) {
        indexing->AddWithoutIds(ds, entry.config);
    }
    return indexing;
}

Status
SegmentNaive::BuildIndex(IndexMetaPtr remote_index_meta) {
    if (remote_index_meta == nullptr) {
        std::cout << "WARN: Null index ptr is detected, use default index" << std::endl;

        int dim = 0;
        std::string index_field_name;

        for (auto& field : schema_->get_fields()) {
            if (field.get_data_type() == DataType::VECTOR_FLOAT) {
                dim = field.get_dim();
                index_field_name = field.get_name();
            }
        }

        Assert(dim != 0);
        Assert(!index_field_name.empty());

        auto index_meta = std::make_shared<IndexMeta>(schema_);
        // TODO: this is merge of query conf and insert conf
        // TODO: should be splitted into multiple configs
        auto conf = milvus::knowhere::Config{
            {milvus::knowhere::meta::DIM, dim},         {milvus::knowhere::IndexParams::nlist, 100},
            {milvus::knowhere::IndexParams::nprobe, 4}, {milvus::knowhere::IndexParams::m, 4},
            {milvus::knowhere::IndexParams::nbits, 8},  {milvus::knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
            {milvus::knowhere::meta::DEVICEID, 0},
        };
        index_meta->AddEntry("fakeindex", index_field_name, knowhere::IndexEnum::INDEX_FAISS_IVFPQ,
                             knowhere::IndexMode::MODE_CPU, conf);
        remote_index_meta = index_meta;
    }

    if (record_.ack_responder_.GetAck() < 1024 * 4) {
        return Status(SERVER_BUILD_INDEX_ERROR, "too few elements");
    }

    index_meta_ = remote_index_meta;
    for (auto& [index_name, entry] : index_meta_->get_entries()) {
        Assert(entry.index_name == index_name);
        const auto& field = (*schema_)[entry.field_name];

        if (field.is_vector()) {
            Assert(field.get_data_type() == engine::DataType::VECTOR_FLOAT);
            auto index_ptr = BuildVecIndexImpl<float>(entry);
            indexings_[index_name] = index_ptr;
        } else {
            throw std::runtime_error("unimplemented");
        }
    }

    index_ready_ = true;
    return Status::OK();
}

int64_t
SegmentNaive::GetMemoryUsageInBytes() {
    int64_t total_bytes = 0;
    if (index_ready_) {
        auto& index_entries = index_meta_->get_entries();
        for (auto [index_name, entry] : index_entries) {
            Assert(schema_->operator[](entry.field_name).is_vector());
            auto vec_ptr = std::static_pointer_cast<knowhere::VecIndex>(indexings_[index_name]);
            total_bytes += vec_ptr->IndexSize();
        }
    }
    int64_t ins_n = (record_.reserved + DefaultElementPerChunk - 1) & ~(DefaultElementPerChunk - 1);
    total_bytes += ins_n * (schema_->get_total_sizeof() + 16 + 1);
    int64_t del_n = (deleted_record_.reserved + DefaultElementPerChunk - 1) & ~(DefaultElementPerChunk - 1);
    total_bytes += del_n * (16 * 2);
    return total_bytes;
}

}  // namespace milvus::segcore
