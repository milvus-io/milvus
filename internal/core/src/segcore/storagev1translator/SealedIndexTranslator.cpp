#include "segcore/storagev1translator/SealedIndexTranslator.h"

#include <filesystem>
#include <utility>

#include "common/EasyAssert.h"
#include "common/common_type_c.h"
#include "common/resource_c.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "index/Index.h"
#include "index/IndexFactory.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "log/Log.h"
#include "nlohmann/json.hpp"
#include "segcore/Types.h"
#include "segcore/Utils.h"

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
                        load_index_info->dim,
                        load_index_info->index_files,
                        load_index_info->warmup_policy}),
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
                    load_index_info->warmup_policy,
                    /* is_vector */
                    IsVectorDataType(load_index_info->field_type),
                    /* is_index */ true),
          /* support_eviction */
          // if index data supports lazy load internally, we don't need to support eviction for index metadata
          // currently only vector index is possible to support lazy load
          !(IsVectorDataType(load_index_info->field_type) &&
            knowhere::IndexFactory::Instance().FeatureCheck(
                index_info_.index_type, knowhere::feature::LAZY_LOAD))) {
}

const LoadResourceRequest&
SealedIndexTranslator::EstimateLoadResource() const {
    std::call_once(load_resource_request_once_, [this]() {
        load_resource_request_ =
            milvus::index::IndexFactory::GetInstance().IndexLoadResource(
                index_load_info_.field_type,
                index_load_info_.element_type,
                index_load_info_.index_engine_version,
                index_load_info_.index_size,
                index_load_info_.index_params,
                index_load_info_.enable_mmap,
                index_load_info_.num_rows,
                index_load_info_.dim,
                index_load_info_.index_files,
                file_manager_context_);
    });
    return load_resource_request_;
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
SealedIndexTranslator::estimated_loading_usage(
    const std::vector<milvus::cachinglayer::cid_t>& cids) const {
    if (cids.empty()) {
        return {};
    }
    const auto& load_resource_request = EstimateLoadResource();
    // this is an estimation, error could be up to 20%.
    const auto final_usage = milvus::cachinglayer::ResourceUsage(
        load_resource_request.final_memory_cost,
        load_resource_request.final_disk_cost);
    const auto peak_usage = milvus::cachinglayer::ResourceUsage(
        load_resource_request.max_memory_cost,
        load_resource_request.max_disk_cost);
    LOG_INFO(
        "estimated index loading usage: index_id={}, segment_id={}, "
        "field_id={}, index_type={}, index_size={}, mmap={}, "
        "final_memory_bytes={}, final_disk_bytes={}, "
        "peak_memory_bytes={}, peak_disk_bytes={}",
        index_load_info_.index_id,
        index_load_info_.segment_id,
        index_load_info_.field_id,
        index_info_.index_type,
        index_load_info_.index_size,
        index_load_info_.enable_mmap,
        final_usage.memory_bytes,
        final_usage.file_bytes,
        peak_usage.memory_bytes,
        peak_usage.file_bytes);
    return {final_usage, peak_usage};
}

const std::string&
SealedIndexTranslator::key() const {
    return index_key_;
}

std::vector<std::pair<milvus::cachinglayer::cid_t,
                      std::unique_ptr<milvus::index::IndexBase>>>
SealedIndexTranslator::get_cells(milvus::OpContext* ctx,
                                 const std::vector<cid_t>& cids) {
    int64_t segment_id = std::stoll(index_load_info_.segment_id);
    const auto& load_resource_request = EstimateLoadResource();

    std::unique_ptr<milvus::index::IndexBase> index =
        milvus::index::IndexFactory::GetInstance().CreateIndex(
            index_info_, file_manager_context_);
    index->SetCellSize(milvus::cachinglayer::ResourceUsage(
        load_resource_request.final_memory_cost,
        load_resource_request.final_disk_cost));
    if (index_load_info_.enable_mmap && index->IsMmapSupported()) {
        AssertInfo(!index_load_info_.mmap_dir_path.empty(),
                   "mmap directory path is empty");
        auto filepath = std::filesystem::path(index_load_info_.mmap_dir_path) /
                        "index_files" / index_load_info_.index_id /
                        index_load_info_.segment_id /
                        index_load_info_.field_id / "index";
        auto embedding_list_meta_path =
            std::filesystem::path(index_load_info_.mmap_dir_path) /
            "index_files" / index_load_info_.index_id /
            index_load_info_.segment_id / index_load_info_.field_id /
            index::EMB_LIST_META_FILE_NAME;
        config_[milvus::index::ENABLE_MMAP] = "true";
        config_[milvus::index::MMAP_FILE_PATH] = filepath.string();
        config_[milvus::index::EMB_LIST_META_PATH] =
            embedding_list_meta_path.string();
    } else {
        config_[milvus::index::ENABLE_MMAP] = "false";
    }

    // Check for cancellation before loading index data
    CheckCancellation(ctx, segment_id, "LoadIndex");

    // Check scalar index engine version for V3 routing
    auto scalar_version =
        milvus::index::GetValueFromConfig<int32_t>(
            config_, milvus::index::SCALAR_INDEX_ENGINE_VERSION)
            .value_or(1);
    if (scalar_version >= 3 && !IsVectorDataType(index_info_.field_type)) {
        config_[milvus::index::COLLECTION_ID] =
            file_manager_context_.fieldDataMeta.collection_id;
        LOG_INFO("load V3 scalar index with configs: {}", config_.dump());
        index->LoadUnified(config_);
    } else {
        LOG_INFO("load index with configs: {}", config_.dump());
        index->Load(ctx_, config_);
    }

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
