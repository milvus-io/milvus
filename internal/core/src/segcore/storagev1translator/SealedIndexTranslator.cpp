#include "segcore/storagev1translator/SealedIndexTranslator.h"
#include "index/IndexFactory.h"
#include "segcore/load_index_c.h"
#include "segcore/Utils.h"
#include <utility>

namespace milvus::segcore::storagev1translator {

SealedIndexTranslator::SealedIndexTranslator(
    milvus::index::CreateIndexInfo index_info,
    const milvus::segcore::LoadIndexInfo* load_index_info,
    milvus::tracer::TraceContext ctx,
    milvus::storage::FileManagerContext file_manager_context,
    Config config)
    : index_info_(std::move(index_info)),
      ctx_(ctx),
      file_manager_context_(std::move(file_manager_context)),
      config_(std::move(config)),
      index_key_(fmt::format("seg_{}_si_{}",
                             load_index_info->segment_id,
                             load_index_info->field_id)),
      index_load_info_({load_index_info->enable_mmap,
                        load_index_info->mmap_dir_path,
                        load_index_info->field_type,
                        load_index_info->element_type,
                        load_index_info->index_params,
                        load_index_info->index_size,
                        load_index_info->index_engine_version,
                        std::to_string(load_index_info->index_id),
                        std::to_string(load_index_info->segment_id),
                        std::to_string(load_index_info->field_id),
                        load_index_info->num_rows,
                        load_index_info->dim}),
      meta_(
          load_index_info->enable_mmap
              ? milvus::cachinglayer::StorageType::DISK
              : milvus::cachinglayer::StorageType::MEMORY,
          milvus::cachinglayer::CellIdMappingMode::ALWAYS_ZERO,
          milvus::segcore::getCellDataType(
              /* is_vector */ IsVectorDataType(load_index_info->field_type),
              /* is_index */ true),
          // if index data supports lazy load internally, we always use sync for index metadata
          // warmup policy will be used for index internally
          // currently only vector index is possible to support lazy load
          (IsVectorDataType(load_index_info->field_type) &&
           knowhere::IndexFactory::Instance().FeatureCheck(
               index_info_.index_type, knowhere::feature::LAZY_LOAD))
              ? CacheWarmupPolicy::CacheWarmupPolicy_Sync
              : milvus::segcore::getCacheWarmupPolicy(
                    /* is_vector */ IsVectorDataType(
                        load_index_info->field_type),
                    /* is_index */ true),
          /* support_eviction */
          // if index data supports lazy load internally, we don't need to support eviction for index metadata
          // currently only vector index is possible to support lazy load
          !(IsVectorDataType(load_index_info->field_type) &&
            knowhere::IndexFactory::Instance().FeatureCheck(
                index_info_.index_type, knowhere::feature::LAZY_LOAD))) {
}

size_t
SealedIndexTranslator::num_cells() const {
    return 1;
}

milvus::cachinglayer::cid_t
SealedIndexTranslator::cell_id_of(milvus::cachinglayer::uid_t uid) const {
    return 0;
}

std::pair<milvus::cachinglayer::ResourceUsage,
          milvus::cachinglayer::ResourceUsage>
SealedIndexTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t cid) const {
    LoadResourceRequest request =
        milvus::index::IndexFactory::GetInstance().IndexLoadResource(
            index_load_info_.field_type,
            index_load_info_.element_type,
            index_load_info_.index_engine_version,
            index_load_info_.index_size,
            index_load_info_.index_params,
            index_load_info_.enable_mmap,
            index_load_info_.num_rows,
            index_load_info_.dim);
    // this is an estimation, error could be up to 20%.
    return {milvus::cachinglayer::ResourceUsage(request.final_memory_cost,
                                                request.final_disk_cost),
            milvus::cachinglayer::ResourceUsage(request.max_memory_cost,
                                                request.max_disk_cost * 2)};
}

const std::string&
SealedIndexTranslator::key() const {
    return index_key_;
}

std::vector<std::pair<milvus::cachinglayer::cid_t,
                      std::unique_ptr<milvus::index::IndexBase>>>
SealedIndexTranslator::get_cells(const std::vector<cid_t>& cids) {
    std::unique_ptr<milvus::index::IndexBase> index =
        milvus::index::IndexFactory::GetInstance().CreateIndex(
            index_info_, file_manager_context_);
    LoadResourceRequest request =
        milvus::index::IndexFactory::GetInstance().IndexLoadResource(
            index_load_info_.field_type,
            index_load_info_.element_type,
            index_load_info_.index_engine_version,
            index_load_info_.index_size,
            index_load_info_.index_params,
            index_load_info_.enable_mmap,
            index_load_info_.num_rows,
            index_load_info_.dim);
    index->SetCellSize(milvus::cachinglayer::ResourceUsage(
        request.final_memory_cost, request.final_disk_cost));
    if (index_load_info_.enable_mmap && index->IsMmapSupported()) {
        AssertInfo(!index_load_info_.mmap_dir_path.empty(),
                   "mmap directory path is empty");
        auto filepath = std::filesystem::path(index_load_info_.mmap_dir_path) /
                        "index_files" / index_load_info_.index_id /
                        index_load_info_.segment_id / index_load_info_.field_id;

        config_[milvus::index::ENABLE_MMAP] = "true";
        config_[milvus::index::MMAP_FILE_PATH] = filepath.string();
    } else {
        config_[milvus::index::ENABLE_MMAP] = "false";
    }

    LOG_INFO("load index with configs: {}", config_.dump());
    index->Load(ctx_, config_);

    std::vector<std::pair<cid_t, std::unique_ptr<milvus::index::IndexBase>>>
        result;
    result.emplace_back(std::make_pair(0, std::move(index)));
    return result;
}

Meta*
SealedIndexTranslator::meta() {
    return &meta_;
}
}  // namespace milvus::segcore::storagev1translator
