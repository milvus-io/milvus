#include "segcore/storagev1translator/InterimSealedIndexTranslator.h"
#include "index/VectorMemIndex.h"

namespace milvus::segcore::storagev1translator {

InterimSealedIndexTranslator::InterimSealedIndexTranslator(
    std::shared_ptr<ChunkedColumnInterface> vec_data,
    std::string segment_id,
    std::string field_id,
    knowhere::IndexType index_type,
    knowhere::MetricType metric_type,
    knowhere::Json build_config,
    int64_t dim,
    bool is_sparse)
    : vec_data_(vec_data),
      index_type_(index_type),
      metric_type_(metric_type),
      build_config_(build_config),
      dim_(dim),
      is_sparse_(is_sparse),
      index_key_(fmt::format("seg_{}_ii_{}", segment_id, field_id)),
      meta_(milvus::cachinglayer::StorageType::MEMORY) {
}

size_t
InterimSealedIndexTranslator::num_cells() const {
    return 1;
}

milvus::cachinglayer::cid_t
InterimSealedIndexTranslator::cell_id_of(
    milvus::cachinglayer::uid_t uid) const {
    return 0;
}

milvus::cachinglayer::ResourceUsage
InterimSealedIndexTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t cid) const {
    auto size = vec_data_->DataByteSize();
    return milvus::cachinglayer::ResourceUsage{static_cast<int64_t>(size), 0};
}

const std::string&
InterimSealedIndexTranslator::key() const {
    return index_key_;
}

std::vector<std::pair<milvus::cachinglayer::cid_t,
                      std::unique_ptr<milvus::index::IndexBase>>>
InterimSealedIndexTranslator::get_cells(
    const std::vector<milvus::cachinglayer::cid_t>& cids) {
    auto vec_index = std::make_unique<index::VectorMemIndex<float>>(
        index_type_,
        metric_type_,
        knowhere::Version::GetCurrentVersion().VersionNumber());
    auto num_chunk = vec_data_->num_chunks();
    for (int i = 0; i < num_chunk; ++i) {
        auto pw = vec_data_->GetChunk(i);
        auto chunk = pw.get();
        auto dataset = knowhere::GenDataSet(
            vec_data_->chunk_row_nums(i), dim_, chunk->Data());
        dataset->SetIsOwner(false);
        dataset->SetIsSparse(is_sparse_);

        if (i == 0) {
            vec_index->BuildWithDataset(dataset, build_config_);
        } else {
            vec_index->AddWithDataset(dataset, build_config_);
        }
    }
    std::vector<std::pair<cid_t, std::unique_ptr<milvus::index::IndexBase>>>
        result;
    result.emplace_back(std::make_pair(0, std::move(vec_index)));
    return result;
}

Meta*
InterimSealedIndexTranslator::meta() {
    return &meta_;
}

}  // namespace milvus::segcore::storagev1translator
