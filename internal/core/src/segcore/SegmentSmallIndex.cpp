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
#include "utils/tools.h"

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
            // map uid to corresponding offsets, select the max one, which should be the target
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
            // map uid to corresponding offsets, select the max one, which should be the target
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

Status
SegmentSmallIndex::Close() {
    if (this->record_.reserved != this->record_.ack_responder_.GetAck()) {
        PanicInfo("insert not ready");
    }
    if (this->deleted_record_.reserved != this->deleted_record_.ack_responder_.GetAck()) {
        PanicInfo("delete not ready");
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
        // TODO: should be split into multiple configs
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
    return Status::OK();
#endif
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
    int64_t ins_n = upper_align(record_.reserved, DefaultElementPerChunk);
    total_bytes += ins_n * (schema_->get_total_sizeof() + 16 + 1);
    int64_t del_n = upper_align(deleted_record_.reserved, DefaultElementPerChunk);
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
