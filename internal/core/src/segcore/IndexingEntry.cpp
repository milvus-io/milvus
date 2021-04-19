#include "IndexingEntry.h"
#include <thread>
#include <knowhere/index/vector_index/IndexIVF.h>
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>

namespace milvus::segcore {
void
IndexingEntry::BuildIndexRange(int64_t ack_beg, int64_t ack_end, const VectorBase* vec_base) {
    // TODO

    assert(field_meta_.get_data_type() == DataType::VECTOR_FLOAT);
    auto dim = field_meta_.get_dim();

    auto source = dynamic_cast<const ConcurrentVector<float>*>(vec_base);
    Assert(source);
    auto chunk_size = source->chunk_size();
    assert(ack_end <= chunk_size);
    auto conf = get_build_conf();
    data_.grow_to_at_least(ack_end);
    for (int chunk_id = ack_beg; chunk_id < ack_end; chunk_id++) {
        const auto& chunk = source->get_chunk(chunk_id);
        // build index for chunk
        // TODO
        auto indexing = std::make_unique<knowhere::IVF>();
        auto dataset = knowhere::GenDataset(DefaultElementPerChunk, dim, chunk.data());
        indexing->Train(dataset, conf);
        indexing->AddWithoutIds(dataset, conf);
        data_[chunk_id] = std::move(indexing);
    }
}

knowhere::Config
IndexingEntry::get_build_conf() const {
    return knowhere::Config{{knowhere::meta::DIM, field_meta_.get_dim()},
                            {knowhere::IndexParams::nlist, 100},
                            {knowhere::IndexParams::nprobe, 4},
                            {knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                            {knowhere::meta::DEVICEID, 0}};
}

knowhere::Config
IndexingEntry::get_search_conf(int top_K) const {
    return knowhere::Config{{knowhere::meta::DIM, field_meta_.get_dim()},
                            {knowhere::meta::TOPK, top_K},
                            {knowhere::IndexParams::nlist, 100},
                            {knowhere::IndexParams::nprobe, 4},
                            {knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                            {knowhere::meta::DEVICEID, 0}};
}

void
IndexingRecord::UpdateResourceAck(int64_t chunk_ack, const InsertRecord& record) {
    if (resource_ack_ >= chunk_ack) {
        return;
    }

    std::unique_lock lck(mutex_);
    int64_t old_ack = resource_ack_;
    if (old_ack >= chunk_ack) {
        return;
    }
    resource_ack_ = chunk_ack;
    lck.unlock();

    //    std::thread([this, old_ack, chunk_ack, &record] {
    for (auto& [field_offset, entry] : entries_) {
        auto vec_base = record.entity_vec_[field_offset].get();
        entry.BuildIndexRange(old_ack, chunk_ack, vec_base);
    }
    finished_ack_.AddSegment(old_ack, chunk_ack);
    //    }).detach();
}

}  // namespace milvus::segcore
