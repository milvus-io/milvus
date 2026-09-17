#include "segcore/storagev1translator/SealedIndexTranslator.h"

#include <filesystem>
#include <limits>
#include <optional>
#include <utility>

#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/Utils.h"
#include "common/common_type_c.h"
#include "common/resource_c.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "index/Families.h"
#include "index/IndexTypeAdapter.h"
#include "index/LoadResource.h"
#include "index/Meta.h"
#include "index/ParamUtils.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/json/JsonProjectedIndexLoad.h"
#include "log/Log.h"
#include "nlohmann/json.hpp"
#include "segcore/CacheMetricAttribution.h"
#include "segcore/Types.h"
#include "segcore/Utils.h"
#include "segcore/memory_planner.h"
#include "storage/EntryStreamUtils.h"
#include "storage/LoadOverheadController.h"
#include "storage/ThreadPools.h"
#include "storage/artifact/FileSource.h"
#include "storage/artifact/LoadOptions.h"

namespace milvus::segcore::storagev1translator {

namespace {

int64_t
PolicyBytes(size_t bytes) {
    return static_cast<int64_t>(std::min(
        bytes, static_cast<size_t>(std::numeric_limits<int64_t>::max())));
}

IndexType
ReadIndexType(const Config& config) {
    const auto index_type =
        index::GetValueFromConfig<std::string>(config, index::INDEX_TYPE);
    AssertInfo(index_type.has_value() && !index_type->empty(),
               "index type is empty");
    return *index_type;
}

bool
UsesV1DiskLayout(const index::IndexFamily& family) {
    return family == index::families::kVectorDisk ||
           family == index::families::kInverted ||
           family == index::families::kNgram ||
           family == index::families::kRTree ||
           family == index::families::kJsonFlat ||
           family == index::families::kHybrid;
}

std::unique_ptr<storage::FileSource>
MakeRemoteSource(const index::IndexFamily& requested_family,
                 bool packed_v3,
                 const storage::FileManagerContext& context,
                 const std::vector<std::string>& remote_paths,
                 const storage::LoadOptions& options) {
    if (packed_v3) {
        return std::make_unique<storage::V3PackedSource>(
            context,
            remote_paths,
            options,
            storage::ArtifactStoragePath::Index);
    }
    const auto layout = UsesV1DiskLayout(requested_family)
                            ? storage::V1SourceLayout::DiskFiles
                            : storage::V1SourceLayout::MemoryEntries;
    return std::make_unique<storage::V1RemoteSource>(
        context,
        remote_paths,
        options,
        storage::ArtifactStoragePath::Index,
        layout);
}

}  // namespace

SealedIndexTranslator::SealedIndexTranslator(
    const milvus::segcore::LoadIndexInfo* load_index_info,
    milvus::tracer::TraceContext ctx,
    milvus::storage::FileManagerContext file_manager_context,
    Config config)
    : index_type_(ReadIndexType(config)),
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
      lazy_load_(IsVectorDataType(load_index_info->field_type) &&
                 knowhere::IndexFactory::Instance().FeatureCheck(
                     index_type_, knowhere::feature::LAZY_LOAD)),
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
          lazy_load_
              ? CacheWarmupPolicy::CacheWarmupPolicy_Sync
              : milvus::segcore::getCacheWarmupPolicy(
                    load_index_info->warmup_policy,
                    /* is_vector */
                    IsVectorDataType(load_index_info->field_type),
                    /* is_index */ true),
          /* support_eviction */
          // if index data supports lazy load internally, we don't need to support eviction for index metadata
          // currently only vector index is possible to support lazy load
          !lazy_load_,
          std::nullopt,
          milvus::segcore::MetricAttributionFromShard(load_index_info->shard)) {
    config_[milvus::index::INDEX_FILES] = index_load_info_.index_files;
    config_[DIM_KEY] = index_load_info_.dim;
    config_[INDEX_NUM_ROWS_KEY] = index_load_info_.num_rows;
    const auto nested = milvus::index::ReadNestedConfigParam(
                            config_, "sealed index translator")
                            .value_or(false);
    auto adapted = milvus::index::AdaptIndexType({
        .index_type = index_type_,
        .field_type = index_load_info_.field_type,
        .element_type = index_load_info_.element_type,
        .index_engine_version =
            static_cast<IndexVersion>(index_load_info_.index_engine_version),
        .params = std::move(config_),
        .is_nested = nested,
        .is_text_match = false,
    });
    const auto value_type = adapted.value_type;
    source_family_ = adapted.family;
    config_ = std::move(adapted.params);

    const auto scalar_version =
        milvus::index::GetValueFromConfig<int32_t>(
            config_, milvus::index::SCALAR_INDEX_ENGINE_VERSION)
            .value_or(1);
    const bool packed_v3 = scalar_version >= 3 &&
                           !IsVectorDataType(index_load_info_.field_type);
    storage::LoadOptions metadata_options;
    metadata_options.params = config_;
    metadata_options.estimated_bytes = index_load_info_.index_size;
    auto source = MakeRemoteSource(adapted.family,
                                   packed_v3,
                                   file_manager_context_,
                                   index_load_info_.index_files,
                                   metadata_options);
    auto resolved_family =
        milvus::index::ResolveLoadFamily(adapted.family, *source, config_);
    const auto loader =
        milvus::index::LoaderRegistry::Instance().Lookup(resolved_family);
    AssertInfo(static_cast<bool>(loader),
               "no index loader is registered for family {}",
               resolved_family);
    if (adapted.family != milvus::index::families::kJsonFlat) {
        config_ = milvus::index::AnnotateJsonProjectionCompleteness(
            std::move(config_), *source);
    }
    SetReaderContract(std::move(resolved_family),
                      value_type,
                      loader.derive_caps(config_));

    std::optional<milvus::storage::EntryStreamLoadInfo> stream_load_info;
    bool use_shared_memory_overhead_group = false;
    load_resource_request_ = EstimateLoadResource(
        &stream_load_info, &use_shared_memory_overhead_group);

    if (scalar_version >= 3 && !IsVectorDataType(index_load_info_.field_type)) {
        AssertInfo(stream_load_info.has_value(),
                   "missing stream load info for packed scalar V3 index");
        if (use_shared_memory_overhead_group) {
            const auto max_task_overhead =
                stream_load_info->encrypted
                    ? stream_load_info->max_task_transient_bytes
                    : milvus::SaturatingMultiply(
                          milvus::storage::MaxEntryStreamTaskBytes(),
                          milvus::storage::kFileStreamBufferMultiplier);
            auto memory_group =
                milvus::storage::LoadMemoryOverheadController::GetInstance()
                    .GetOrCreate(milvus::ThreadPools::GetLoadExecutorWorkers());
            meta_.loading_overhead_config =
                milvus::cachinglayer::LoadingOverheadConfig{
                    milvus::cachinglayer::LoadingOverheadGroupBinding{
                        std::move(memory_group),
                        PolicyBytes(max_task_overhead)},
                    // FIXME: Bind scalar V3 file overhead to the executor-backed
                    // file group after every file-backed load path writes through
                    // positioned tasks on the HIGH/LOW load executors. Some paths
                    // still use FileWriter or its independent worker pool, so
                    // binding them now would under-reserve concurrent disk
                    // overhead.
                    std::nullopt};
        }
    }
}

LoadResourceRequest
SealedIndexTranslator::EstimateLoadResource(
    std::optional<milvus::storage::EntryStreamLoadInfo>* stream_load_info,
    bool* use_shared_memory_overhead_group) const {
    auto estimated =
        milvus::index::IndexLoadResource(
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
            stream_load_info,
            use_shared_memory_overhead_group);
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
                      std::unique_ptr<milvus::index::IIndexReaderBase>>>
SealedIndexTranslator::get_cells(milvus::OpContext* ctx,
                                 const std::vector<cid_t>& cids) {
    int64_t segment_id = std::stoll(index_load_info_.segment_id);

    storage::LoadOptions options;
    options.enable_mmap = index_load_info_.enable_mmap;
    options.estimated_bytes = index_load_info_.index_size;
    options.op_ctx = ctx;
    options.warmup = ToStorageWarmup(milvus::segcore::getCacheWarmupPolicy(
        index_load_info_.warmup_policy,
        IsVectorDataType(index_load_info_.field_type),
        /* is_index */ true));
    if (index_load_info_.enable_mmap) {
        AssertInfo(!index_load_info_.mmap_dir_path.empty(),
                   "mmap directory path is empty");
        auto base_path = std::filesystem::path(index_load_info_.mmap_dir_path) /
                         "index_files" / index_load_info_.index_id /
                         index_load_info_.segment_id /
                         index_load_info_.field_id;
        options.mmap_dir_path = (base_path / "index").string();
        config_[milvus::index::ENABLE_MMAP] = true;
        config_[milvus::index::MMAP_FILE_PATH] = options.mmap_dir_path;
        config_[milvus::index::EMB_LIST_META_PATH] =
            (base_path / index::EMB_LIST_META_FILE_NAME).string();
        config_[milvus::index::EMB_LIST_RAW_INDEX_PATH] =
            (base_path / index::EMB_LIST_RAW_INDEX_FILE_NAME).string();
    } else {
        config_[milvus::index::ENABLE_MMAP] = false;
    }
    options.params = config_;

    // Check for cancellation before loading index data
    CheckCancellation(ctx, segment_id, "LoadIndex");

    const auto scalar_version =
        milvus::index::GetValueFromConfig<int32_t>(
            config_, milvus::index::SCALAR_INDEX_ENGINE_VERSION)
            .value_or(1);
    const bool packed_v3 = scalar_version >= 3 &&
                           !IsVectorDataType(index_load_info_.field_type);
    auto source = MakeRemoteSource(source_family_,
                                   packed_v3,
                                   file_manager_context_,
                                   index_load_info_.index_files,
                                   options);
    const auto loader =
        milvus::index::LoaderRegistry::Instance().Lookup(Family());
    AssertInfo(static_cast<bool>(loader),
               "no index loader is registered for family {}",
               Family());
    LOG_INFO("load index family {} with configs: {}",
             Family(),
             config_.dump());
    auto reader = loader.open(*source, options);
    AssertInfo(reader != nullptr,
               "index loader for family {} returned a null reader",
               Family());

    std::vector<
        std::pair<cid_t, std::unique_ptr<milvus::index::IIndexReaderBase>>>
        result;
    result.emplace_back(std::make_pair(0, std::move(reader)));
    return result;
}

milvus::cachinglayer::Meta*
SealedIndexTranslator::meta() {
    return &meta_;
}

}  // namespace milvus::segcore::storagev1translator
