#include "segcore/storagev1translator/InterimSealedIndexTranslator.h"
#include "index/VectorMemIndex.h"
#include "segcore/Utils.h"

namespace milvus::segcore::storagev1translator {

InterimSealedIndexTranslator::InterimSealedIndexTranslator(
    std::shared_ptr<ChunkedColumnInterface> vec_data,
    std::string segment_id,
    std::string field_id,
    knowhere::IndexType index_type,
    knowhere::MetricType metric_type,
    knowhere::Json build_config,
    int64_t dim,
    bool is_sparse,
    DataType vec_data_type)
    : vec_data_(vec_data),
      index_type_(index_type),
      metric_type_(metric_type),
      build_config_(build_config),
      dim_(dim),
      is_sparse_(is_sparse),
      vec_data_type_(vec_data_type),
      index_key_(fmt::format("seg_{}_ii_{}", segment_id, field_id)),
      meta_(milvus::cachinglayer::StorageType::MEMORY,
            milvus::cachinglayer::CellIdMappingMode::ALWAYS_ZERO,
            milvus::segcore::getCacheWarmupPolicy(
                /* is_vector */ true,
                /* is_index */ true),
            /* support_eviction */ false) {
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
    // SCANN reference the raw data and has little meta, thus we ignore its size.
    if (index_type_ == knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR) {
        return milvus::cachinglayer::ResourceUsage{0, 0};
    }
    // IVF_FLAT_CC, SPARSE_WAND_CC and SPARSE_INVERTED_INDEX_CC basically has the same size as the
    // raw data.
    // TODO(tiered storage 1) cqy123456: provide a better estimation for IVF_SQ_CC once supported.
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
    std::unique_ptr<index::VectorIndex> vec_index = nullptr;
    if (!is_sparse_) {
        knowhere::ViewDataOp view_data = [field_raw_data_ptr =
                                              vec_data_](size_t id) {
            const void* data;
            int64_t data_id = id;
            field_raw_data_ptr->BulkValueAt(
                [&data, &data_id](const char* value, size_t i) {
                    data = static_cast<const void*>(value);
                },
                &data_id,
                1);
            return data;
        };

        if (vec_data_type_ == DataType::VECTOR_FLOAT) {
            vec_index = std::make_unique<index::VectorMemIndex<float>>(
                DataType::NONE,
                index_type_,
                metric_type_,
                knowhere::Version::GetCurrentVersion().VersionNumber(),
                view_data,
                false);
        } else if (vec_data_type_ == DataType::VECTOR_FLOAT16) {
            vec_index = std::make_unique<index::VectorMemIndex<knowhere::fp16>>(
                DataType::NONE,
                index_type_,
                metric_type_,
                knowhere::Version::GetCurrentVersion().VersionNumber(),
                view_data,
                false);
        } else if (vec_data_type_ == DataType::VECTOR_BFLOAT16) {
            vec_index = std::make_unique<index::VectorMemIndex<knowhere::bf16>>(
                DataType::NONE,
                index_type_,
                metric_type_,
                knowhere::Version::GetCurrentVersion().VersionNumber(),
                view_data,
                false);
        }
    } else {
        // sparse vector case
        vec_index = std::make_unique<index::VectorMemIndex<sparse_u32_f32>>(
            DataType::NONE,
            index_type_,
            metric_type_,
            knowhere::Version::GetCurrentVersion().VersionNumber(),
            false);
    }

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
