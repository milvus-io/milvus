#include "segcore/storagev1translator/SealedIndexTranslator.h"

#include <filesystem>
#include <limits>
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
#include "storage/EntryStreamUtils.h"

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
    // TODO: Recompute scalar V3 stream estimates and the registered loading
    // overhead upper bound when refreshable load-pool sizes grow. CacheSlot
    // snapshots both at translator construction and reuses them on reload.
    std::optional<milvus::storage::EntryStreamLoadInfo> stream_load_info;
    load_resource_request_ = EstimateLoadResource(&stream_load_info);

    auto scalar_version =
        milvus::index::GetValueFromConfig<int32_t>(
            config_, milvus::index::SCALAR_INDEX_ENGINE_VERSION)
            .value_or(1);
    if (scalar_version >= 3 && !IsVectorDataType(index_load_info_.field_type)) {
        AssertInfo(stream_load_info.has_value(),
                   "missing stream load info for packed scalar V3 index");
        auto budget_capacity = static_cast<int64_t>(
            milvus::storage::TransientMemoryBudget::GetLoadTransientBudget()
                .CapacityBytes());
        auto encrypted_stream = stream_load_info->encrypted;

        // The packed-file footer records exact encrypted slice sizes. They
        // give encrypted loading two independent ceilings:
        //   1. the sum of cipher_len + 2 * plain_len across all slices;
        //   2. the largest such task multiplied by load-pool concurrency.
        // A non-zero process-wide budget adds a third ceiling, but is floored
        // at one task because oversized tasks are admitted exclusively.
        auto exact_encrypted_upper_bound = [&]() {
            auto total_transient_bytes = static_cast<int64_t>(std::min(
                stream_load_info->total_transient_bytes,
                static_cast<size_t>(std::numeric_limits<int64_t>::max())));
            auto task_transient_bytes = static_cast<int64_t>(std::min(
                stream_load_info->max_task_transient_bytes,
                static_cast<size_t>(std::numeric_limits<int64_t>::max())));
            auto pool_upper_bound =
                milvus::segcore::LoadTransientPoolUpperBound(
                    stream_load_info->max_task_transient_bytes);
            auto budget_upper_bound =
                budget_capacity == 0
                    ? pool_upper_bound
                    : std::min(std::max(budget_capacity, task_transient_bytes),
                               pool_upper_bound);
            return std::min(total_transient_bytes, budget_upper_bound);
        };

        // Plaintext tasks have a fixed per-task bound: the read buffer plus
        // the aligned-write copy. A configured budget bounds aggregate
        // in-flight bytes but still permits one oversized task; budget=0 is
        // unlimited, so pool concurrency supplies the aggregate bound.
        auto plaintext_task_overhead =
            milvus::storage::PlainEntryFileStreamTaskTransientBytes();
        auto memory_upper_bound =
            encrypted_stream ? exact_encrypted_upper_bound()
            : budget_capacity != 0
                ? std::max<int64_t>(budget_capacity, plaintext_task_overhead)
                : milvus::segcore::LoadTransientPoolUpperBound(
                      plaintext_task_overhead);
        meta_.loading_overhead = milvus::cachinglayer::LoadingOverheadConfig{
            milvus::cachinglayer::LoadingOverheadDimensionConfig{
                memory_upper_bound,
                milvus::segcore::kLoadTransientOverheadGroup},
            std::nullopt};
    }
}

LoadResourceRequest
SealedIndexTranslator::EstimateLoadResource(
    std::optional<milvus::storage::EntryStreamLoadInfo>* stream_load_info)
    const {
    auto estimated =
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
            file_manager_context_,
            stream_load_info);
    if (index_load_info_.load_resource_request.has_value()) {
        return *index_load_info_.load_resource_request;
    }
    return estimated;
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
