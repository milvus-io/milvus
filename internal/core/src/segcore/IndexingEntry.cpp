#include "IndexingEntry.h"
#include <thread>
#include <knowhere/index/vector_index/IndexIVF.h>
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>

namespace milvus::segcore {
void
VecIndexingEntry::BuildIndexRange(int64_t ack_beg, int64_t ack_end, const VectorBase* vec_base) {
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
VecIndexingEntry::get_build_conf() const {
    return knowhere::Config{{knowhere::meta::DIM, field_meta_.get_dim()},
                            {knowhere::IndexParams::nlist, 100},
                            {knowhere::IndexParams::nprobe, 4},
                            {knowhere::Metric::TYPE, milvus::knowhere::Metric::L2},
                            {knowhere::meta::DEVICEID, 0}};
}

knowhere::Config
VecIndexingEntry::get_search_conf(int top_K) const {
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
        entry->BuildIndexRange(old_ack, chunk_ack, vec_base);
    }
    finished_ack_.AddSegment(old_ack, chunk_ack);
    //    }).detach();
}

template <typename T>
void
ScalarIndexingEntry<T>::BuildIndexRange(int64_t ack_beg, int64_t ack_end, const VectorBase* vec_base) {
    auto dim = field_meta_.get_dim();

    auto source = dynamic_cast<const ConcurrentVector<T, true>*>(vec_base);
    Assert(source);
    auto chunk_size = source->chunk_size();
    assert(ack_end <= chunk_size);
    data_.grow_to_at_least(ack_end);
    for (int chunk_id = ack_beg; chunk_id < ack_end; chunk_id++) {
        const auto& chunk = source->get_chunk(chunk_id);
        // build index for chunk
        // TODO
        Assert(chunk.size() == DefaultElementPerChunk);
        auto indexing = std::make_unique<knowhere::scalar::StructuredIndexSort<T>>();
        indexing->Build(DefaultElementPerChunk, chunk.data());
        data_[chunk_id] = std::move(indexing);
    }
}

std::unique_ptr<IndexingEntry>
CreateIndex(const FieldMeta& field_meta) {
    if (field_meta.is_vector()) {
        return std::make_unique<VecIndexingEntry>(field_meta);
    }
    switch (field_meta.get_data_type()) {
        case DataType::INT8:
            return std::make_unique<ScalarIndexingEntry<int8_t>>(field_meta);
        case DataType::INT16:
            return std::make_unique<ScalarIndexingEntry<int16_t>>(field_meta);
        case DataType::INT32:
            return std::make_unique<ScalarIndexingEntry<int32_t>>(field_meta);
        case DataType::INT64:
            return std::make_unique<ScalarIndexingEntry<int64_t>>(field_meta);
        case DataType::FLOAT:
            return std::make_unique<ScalarIndexingEntry<float>>(field_meta);
        case DataType::DOUBLE:
            return std::make_unique<ScalarIndexingEntry<double>>(field_meta);
        default:
            PanicInfo("unsupported");
    }
}

}  // namespace milvus::segcore
