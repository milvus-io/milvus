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
            milvus::segcore::getCellDataType(
                /* is_vector */ true,
                /* is_index */ true),
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

std::pair<milvus::cachinglayer::ResourceUsage,
          milvus::cachinglayer::ResourceUsage>
InterimSealedIndexTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t cid) const {
    int64_t size = vec_data_->DataByteSize();
    int64_t row_count = vec_data_->NumRows();
    // TODO: hack, move these estimate logic to knowhere
    // ignore the size of centroids
    if (index_type_ == knowhere::IndexEnum::INDEX_FAISS_SCANN_DVR) {
        int64_t vec_size =
            int64_t(index::GetValueFromConfig<int>(
                        build_config_, knowhere::indexparam::SUB_DIM)
                        .value() /
                    8 * dim_);
        if (build_config_[knowhere::indexparam::REFINE_TYPE] ==
            knowhere::RefineType::UINT8_QUANT) {
            vec_size += dim_ * 1;
        } else if (build_config_[knowhere::indexparam::REFINE_TYPE] ==
                       knowhere::RefineType::FLOAT16_QUANT ||
                   build_config_[knowhere::indexparam::REFINE_TYPE] ==
                       knowhere::RefineType::BFLOAT16_QUANT) {
            vec_size += dim_ * 2;
        }  // else knowhere::RefineType::DATA_VIEW, no extra size
        return {{vec_size * row_count, 0},
                {static_cast<int64_t>(vec_size * row_count + size * 0.5), 0}};
    } else if (index_type_ == knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC) {
        // fp16/bf16 all use float32 to build index
        int64_t fp32_size = row_count * sizeof(float) * dim_;
        return {{fp32_size, 0},
                {static_cast<int64_t>(fp32_size + fp32_size * 0.5), 0}};
    } else {
        // SPARSE_WAND_CC and SPARSE_INVERTED_INDEX_CC basically has the same size as the
        // raw data.
        return {{size, 0}, {static_cast<int64_t>(size * 2.0), 0}};
    }
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
                nullptr,
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
        auto pw = vec_data_->GetChunk(nullptr, i);
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
