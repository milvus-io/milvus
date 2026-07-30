#include "segcore/storagev1translator/SealedIndexTranslator.h"

#include <filesystem>
#include <optional>
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
#include "segcore/CacheMetricAttribution.h"
#include "segcore/Types.h"
#include "segcore/Utils.h"
#include "segcore/memory_planner.h"
#include "storage/LoadOverheadController.h"
#include "storage/ThreadPools.h"

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
                        load_index_info->warmup_policy,
                        load_index_info->load_resource_request}),
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
                index_info_.index_type, knowhere::feature::LAZY_LOAD)),
          std::nullopt,
          milvus::segcore::MetricAttributionFromShard(load_index_info->shard)) {
    const auto load_spec = milvus::index::IndexLoadSpec{
        .field_type = index_load_info_.field_type,
        .element_type = index_load_info_.element_type,
        .index_version =
            static_cast<IndexVersion>(index_load_info_.index_engine_version),
        .index_size_in_bytes =
            static_cast<uint64_t>(index_load_info_.index_size),
        .index_params = index_load_info_.index_params,
        .mmap_enable = index_load_info_.enable_mmap,
        .num_rows = index_load_info_.num_rows,
        .dim = index_load_info_.dim,
    };
    auto& index_factory = milvus::index::IndexFactory::GetInstance();
    if (IsVectorDataType(index_load_info_.field_type)) {
        load_resource_request_ =
            index_factory.EstimateIndexLoadResource(load_spec);
    } else {
        auto inspection = index_factory.InspectScalarIndexFiles(
            load_spec,
            milvus::index::IndexFileContext{
                .index_files = index_load_info_.index_files,
                .file_manager_context = file_manager_context_,
            });
        auto plan = index_factory.PlanScalarIndexLoad(load_spec, inspection);
        load_resource_request_ = plan.request;

        auto scalar_version =
            milvus::index::GetValueFromConfig<int32_t>(
                config_, milvus::index::SCALAR_INDEX_ENGINE_VERSION)
                .value_or(1);
        if (scalar_version >= 3) {
            AssertInfo(inspection.stream_load_info.has_value(),
                       "missing stream load info for packed scalar V3 index");
            if (plan.shared_memory_runtime_unit_bytes.has_value()) {
                auto max_runtime_unit = *plan.shared_memory_runtime_unit_bytes;
                auto memory_group =
                    milvus::storage::LoadMemoryOverheadController::GetInstance()
                        .GetOrCreate(
                            milvus::ThreadPools::GetLoadExecutorWorkers());
                meta_.loading_overhead_config =
                    milvus::cachinglayer::LoadingOverheadConfig{
                        milvus::cachinglayer::LoadingOverheadGroupBinding{
                            std::move(memory_group), max_runtime_unit},
                        // FIXME: Bind scalar V3 file overhead to the
                        // executor-backed file group after every file-backed
                        // load path writes through positioned tasks on the
                        // HIGH/LOW load executors. Some paths still use
                        // FileWriter or its independent worker pool, so binding
                        // them now would under-reserve concurrent disk overhead.
                        std::nullopt};
            }
        }
    }
    if (index_load_info_.load_resource_request.has_value()) {
        load_resource_request_ = *index_load_info_.load_resource_request;
    }
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
    // this is an estimation, error could be up to 20%.
    // Preserve the historical 2x disk safety margin for temporary file growth
    // during writes. final_disk_cost is already counted as loaded resource, so
    // the file overhead is the remainder of 2 * max_disk_cost.
    return {milvus::cachinglayer::ResourceUsage(
                load_resource_request_.final_memory_cost,
                load_resource_request_.final_disk_cost),
            milvus::cachinglayer::ResourceUsage(
                load_resource_request_.max_memory_cost -
                    load_resource_request_.final_memory_cost,
                load_resource_request_.max_disk_cost * 2 -
                    load_resource_request_.final_disk_cost)};
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

    std::unique_ptr<milvus::index::IndexBase> index =
        milvus::index::IndexFactory::GetInstance().CreateIndex(
            index_info_, file_manager_context_);
    index->SetCellSize(milvus::cachinglayer::ResourceUsage(
        load_resource_request_.final_memory_cost,
        load_resource_request_.final_disk_cost));
    if (index_load_info_.enable_mmap && index->IsMmapSupported()) {
        AssertInfo(!index_load_info_.mmap_dir_path.empty(),
                   "mmap directory path is empty");
        auto base_path = std::filesystem::path(index_load_info_.mmap_dir_path) /
                         "index_files" / index_load_info_.index_id /
                         index_load_info_.segment_id /
                         index_load_info_.field_id;
        config_[milvus::index::ENABLE_MMAP] = "true";
        config_[milvus::index::MMAP_FILE_PATH] = (base_path / "index").string();
        config_[milvus::index::EMB_LIST_META_PATH] =
            (base_path / index::EMB_LIST_META_FILE_NAME).string();
        config_[milvus::index::EMB_LIST_RAW_INDEX_PATH] =
            (base_path / index::EMB_LIST_RAW_INDEX_FILE_NAME).string();
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
        index->LoadUnified(config_, ctx);
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
