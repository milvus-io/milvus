#include <random>

#include <algorithm>
#include <numeric>
#include <thread>
#include <queue>

#include "segcore/SegmentNaive.h"
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include <knowhere/index/vector_index/VecIndexFactory.h>
#include <faiss/utils/distances.h>
#include "query/generated/ExecPlanNodeVisitor.h"
#include "segcore/SegmentSmallIndex.h"
#include "query/PlanNode.h"
#include "query/PlanImpl.h"
#include "segcore/Reduce.h"

namespace milvus::segcore {

int64_t
SegmentSmallIndex::PreInsert(int64_t size) {
    auto reserved_begin = record_.reserved.fetch_add(size);
    return reserved_begin;
}

int64_t
SegmentSmallIndex::PreDelete(int64_t size) {
    auto reserved_begin = deleted_record_.reserved.fetch_add(size);
    return reserved_begin;
}

auto
SegmentSmallIndex::get_deleted_bitmap(int64_t del_barrier,
                                      Timestamp query_timestamp,
                                      int64_t insert_barrier,
                                      bool force) -> std::shared_ptr<DeletedRecord::TmpBitmap> {
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
SegmentSmallIndex::Insert(int64_t reserved_begin,
                          int64_t size,
                          const int64_t* uids_raw,
                          const Timestamp* timestamps_raw,
                          const RowBasedRawData& entities_raw) {
    Assert(entities_raw.count == size);
    // step 1: check schema if valid
    if (entities_raw.sizeof_per_row != schema_->get_total_sizeof()) {
        std::string msg = "entity length = " + std::to_string(entities_raw.sizeof_per_row) +
                          ", schema length = " + std::to_string(schema_->get_total_sizeof());
        throw std::runtime_error(msg);
    }

    // step 2: sort timestamp
    auto raw_data = reinterpret_cast<const char*>(entities_raw.raw_data);
    auto len_per_row = entities_raw.sizeof_per_row;
    std::vector<std::tuple<Timestamp, idx_t, int64_t>> ordering;
    ordering.resize(size);
    // #pragma omp parallel for
    for (int i = 0; i < size; ++i) {
        ordering[i] = std::make_tuple(timestamps_raw[i], uids_raw[i], i);
    }
    std::sort(ordering.begin(), ordering.end());

    // step 3: and convert row-base data to column base accordingly
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

    // step 4: fill into Segment.ConcurrentVector
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
    // indexing_record_.UpdateResourceAck(record_.ack_responder_.GetAck() / DefaultElementPerChunk);
    return Status::OK();
}

Status
SegmentSmallIndex::Delete(int64_t reserved_begin,
                          int64_t size,
                          const int64_t* uids_raw,
                          const Timestamp* timestamps_raw) {
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
SegmentSmallIndex::QueryBruteForceImpl(const query::QueryInfo& info,
                                       const float* query_data,
                                       int64_t num_queries,
                                       Timestamp timestamp,
                                       QueryResult& results) {
    // step 1: binary search to find the barrier of the snapshot
    auto ins_barrier = get_barrier(record_, timestamp);
    auto del_barrier = get_barrier(deleted_record_, timestamp);
#if 0
    auto bitmap_holder = get_deleted_bitmap(del_barrier, timestamp, ins_barrier);
    Assert(bitmap_holder);
    auto bitmap = bitmap_holder->bitmap_ptr;
#endif

    // step 2.1: get meta
    // step 2.2: get which vector field to search
    auto vecfield_offset_opt = schema_->get_offset(info.field_id_);
    Assert(vecfield_offset_opt.has_value());
    auto vecfield_offset = vecfield_offset_opt.value();
    Assert(vecfield_offset < record_.entity_vec_.size());

    auto& field = schema_->operator[](vecfield_offset);
    auto vec_ptr = std::static_pointer_cast<ConcurrentVector<float>>(record_.entity_vec_.at(vecfield_offset));

    Assert(field.get_data_type() == DataType::VECTOR_FLOAT);
    auto dim = field.get_dim();
    auto topK = info.topK_;
    auto total_count = topK * num_queries;
    // TODO: optimize

    // step 3: small indexing search
    std::vector<int64_t> final_uids(total_count, -1);
    std::vector<float> final_dis(total_count, std::numeric_limits<float>::max());

    auto max_chunk = (ins_barrier + DefaultElementPerChunk - 1) / DefaultElementPerChunk;

    auto max_indexed_id = indexing_record_.get_finished_ack();
    const auto& indexing_entry = indexing_record_.get_indexing(vecfield_offset);
    auto search_conf = indexing_entry.get_search_conf(topK);

    for (int chunk_id = 0; chunk_id < max_indexed_id; ++chunk_id) {
        auto indexing = indexing_entry.get_indexing(chunk_id);
        auto src_data = vec_ptr->get_chunk(chunk_id).data();
        auto dataset = knowhere::GenDataset(num_queries, dim, src_data);
        auto ans = indexing->Query(dataset, search_conf, nullptr);
        auto dis = ans->Get<float*>(milvus::knowhere::meta::DISTANCE);
        auto uids = ans->Get<int64_t*>(milvus::knowhere::meta::IDS);
        merge_into(num_queries, topK, final_dis.data(), final_uids.data(), dis, uids);
    }

    // step 4: brute force search where small indexing is unavailable
    for (int chunk_id = max_indexed_id; chunk_id < max_chunk; ++chunk_id) {
        std::vector<int64_t> buf_uids(total_count, -1);
        std::vector<float> buf_dis(total_count, std::numeric_limits<float>::max());

        faiss::float_maxheap_array_t buf = {(size_t)num_queries, (size_t)topK, buf_uids.data(), buf_dis.data()};

        auto src_data = vec_ptr->get_chunk(chunk_id).data();
        auto nsize =
            chunk_id != max_chunk - 1 ? DefaultElementPerChunk : ins_barrier - chunk_id * DefaultElementPerChunk;
        faiss::knn_L2sqr(query_data, src_data, dim, num_queries, nsize, &buf);
        merge_into(num_queries, topK, final_dis.data(), final_uids.data(), buf_dis.data(), buf_uids.data());
    }

    // step 5: convert offset to uids
    for (auto& id : final_uids) {
        if (id == -1) {
            continue;
        }
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
SegmentSmallIndex::QueryDeprecated(query::QueryDeprecatedPtr query_info, Timestamp timestamp, QueryResult& result) {
    // TODO: enable delete
    // TODO: enable index
    // TODO: remove mock
    if (query_info == nullptr) {
        query_info = std::make_shared<query::QueryDeprecated>();
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
    int64_t inferred_dim = query_info->query_raw_data.size() / query_info->num_queries;
    // TODO
    query::QueryInfo info{
        query_info->topK,
        query_info->field_name,
        "L2",
        nlohmann::json{
            {"nprobe", 10},
        },
    };
    auto num_queries = query_info->num_queries;
    return QueryBruteForceImpl(info, query_info->query_raw_data.data(), num_queries, timestamp, result);
}

Status
SegmentSmallIndex::Close() {
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
SegmentSmallIndex::BuildVecIndexImpl(const IndexMeta::Entry& entry) {
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
SegmentSmallIndex::BuildIndex(IndexMetaPtr remote_index_meta) {
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
    // AssertInfo(false, "unimplemented");
    return Status::OK();
#if 0
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
#endif
    return Status::OK();
}

int64_t
SegmentSmallIndex::GetMemoryUsageInBytes() {
    int64_t total_bytes = 0;
#if 0
    if (index_ready_) {
        auto& index_entries = index_meta_->get_entries();
        for (auto [index_name, entry] : index_entries) {
            Assert(schema_->operator[](entry.field_name).is_vector());
            auto vec_ptr = std::static_pointer_cast<knowhere::VecIndex>(indexings_[index_name]);
            total_bytes += vec_ptr->IndexSize();
        }
    }
#endif
    int64_t ins_n = (record_.reserved + DefaultElementPerChunk - 1) & ~(DefaultElementPerChunk - 1);
    total_bytes += ins_n * (schema_->get_total_sizeof() + 16 + 1);
    int64_t del_n = (deleted_record_.reserved + DefaultElementPerChunk - 1) & ~(DefaultElementPerChunk - 1);
    total_bytes += del_n * (16 * 2);
    return total_bytes;
}

Status
SegmentSmallIndex::Search(const query::Plan* plan,
                          const query::PlaceholderGroup** placeholder_groups,
                          const Timestamp* timestamps,
                          int num_groups,
                          QueryResult& results) {
    Assert(num_groups == 1);
    query::ExecPlanNodeVisitor visitor(*this, timestamps[0], *placeholder_groups[0]);
    results = visitor.get_moved_result(*plan->plan_node_);
    return Status::OK();
}

}  // namespace milvus::segcore
