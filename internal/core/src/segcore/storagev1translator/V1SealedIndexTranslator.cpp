#include "segcore/storagev1translator/V1SealedIndexTranslator.h"
#include "index/IndexFactory.h"
#include "segcore/load_index_c.h"
#include "segcore/Utils.h"
#include <utility>
#include "storage/RemoteChunkManagerSingleton.h"

namespace milvus::segcore::storagev1translator {

V1SealedIndexTranslator::V1SealedIndexTranslator(
    LoadIndexInfo* load_index_info, knowhere::BinarySet* binary_set)
    : index_load_info_({
          load_index_info->enable_mmap,
          load_index_info->mmap_dir_path,
          load_index_info->field_type,
          load_index_info->index_params,
          load_index_info->index_files,
          load_index_info->index_size,
          load_index_info->index_engine_version,
          load_index_info->index_id,
          load_index_info->collection_id,
          load_index_info->partition_id,
          load_index_info->segment_id,
          load_index_info->field_id,
          load_index_info->index_build_id,
          load_index_info->index_version,
      }),
      binary_set_(binary_set),
      key_(fmt::format("seg_{}_si_{}",
                       load_index_info->segment_id,
                       load_index_info->field_id)),
      meta_(load_index_info->enable_mmap
                ? milvus::cachinglayer::StorageType::DISK
                : milvus::cachinglayer::StorageType::MEMORY,
            milvus::segcore::getCacheWarmupPolicy(
                IsVectorDataType(load_index_info->field_type),
                /* is_index */ true),
            /* support_eviction */ false) {
}

size_t
V1SealedIndexTranslator::num_cells() const {
    return 1;
}

milvus::cachinglayer::cid_t
V1SealedIndexTranslator::cell_id_of(milvus::cachinglayer::uid_t uid) const {
    return 0;
}

milvus::cachinglayer::ResourceUsage
V1SealedIndexTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t cid) const {
    return {0, 0};
}

const std::string&
V1SealedIndexTranslator::key() const {
    return key_;
}

std::vector<std::pair<milvus::cachinglayer::cid_t,
                      std::unique_ptr<milvus::index::IndexBase>>>
V1SealedIndexTranslator::get_cells(
    const std::vector<milvus::cachinglayer::cid_t>& cids) {
    std::vector<std::pair<cid_t, std::unique_ptr<milvus::index::IndexBase>>>
        result;
    if (milvus::IsVectorDataType(index_load_info_.field_type)) {
        result.emplace_back(std::make_pair(0, LoadVecIndex()));
    } else {
        result.emplace_back(std::make_pair(0, LoadScalarIndex()));
    }
    return result;
}

milvus::cachinglayer::Meta*
V1SealedIndexTranslator::meta() {
    return &meta_;
}

std::unique_ptr<milvus::index::IndexBase>
V1SealedIndexTranslator::LoadVecIndex() {
    try {
        auto& index_params = index_load_info_.index_params;

        milvus::index::CreateIndexInfo index_info;
        index_info.field_type = index_load_info_.field_type;
        index_info.index_engine_version = index_load_info_.index_engine_version;

        // get index type
        AssertInfo(index_params.find("index_type") != index_params.end(),
                   "index type is empty");
        index_info.index_type = index_params.at("index_type");

        // get metric type
        AssertInfo(index_params.find("metric_type") != index_params.end(),
                   "metric type is empty");
        index_info.metric_type = index_params.at("metric_type");

        // init file manager
        milvus::storage::FieldDataMeta field_meta{
            index_load_info_.collection_id,
            index_load_info_.partition_id,
            index_load_info_.segment_id,
            index_load_info_.field_id};
        milvus::storage::IndexMeta index_meta{index_load_info_.segment_id,
                                              index_load_info_.field_id,
                                              index_load_info_.index_build_id,
                                              index_load_info_.index_version};
        auto remote_chunk_manager =
            milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                .GetRemoteChunkManager();

        auto config = milvus::index::ParseConfigFromIndexParams(
            index_load_info_.index_params);
        config["index_files"] = index_load_info_.index_files;

        milvus::storage::FileManagerContext fileManagerContext(
            field_meta, index_meta, remote_chunk_manager);
        fileManagerContext.set_for_loading_index(true);

        auto index = milvus::index::IndexFactory::GetInstance().CreateIndex(
            index_info, fileManagerContext);
        index->SetCellSize(index_load_info_.index_size);
        index->Load(*binary_set_, config);
        return index;
    } catch (std::exception& e) {
        throw std::runtime_error(e.what());
    }
}

std::unique_ptr<milvus::index::IndexBase>
V1SealedIndexTranslator::LoadScalarIndex() {
    try {
        auto field_type = index_load_info_.field_type;
        auto& index_params = index_load_info_.index_params;
        bool find_index_type =
            index_params.count("index_type") > 0 ? true : false;
        AssertInfo(find_index_type == true,
                   "Can't find index type in index_params");

        milvus::index::CreateIndexInfo index_info;
        index_info.field_type = milvus::DataType(field_type);
        index_info.index_type = index_params["index_type"];

        auto config = milvus::index::ParseConfigFromIndexParams(
            index_load_info_.index_params);

        // Config should have value for milvus::index::SCALAR_INDEX_ENGINE_VERSION for production calling chain.
        // Use value_or(1) for unit test without setting this value
        index_info.scalar_index_engine_version =
            milvus::index::GetValueFromConfig<int32_t>(
                config, milvus::index::SCALAR_INDEX_ENGINE_VERSION)
                .value_or(1);

        index_info.tantivy_index_version =
            index_info.scalar_index_engine_version <= 1
                ? milvus::index::TANTIVY_INDEX_MINIMUM_VERSION
                : milvus::index::TANTIVY_INDEX_LATEST_VERSION;

        auto index = milvus::index::IndexFactory::GetInstance().CreateIndex(
            index_info, milvus::storage::FileManagerContext());
        index->SetCellSize(index_load_info_.index_size);
        index->Load(*binary_set_);
        return index;
    } catch (std::exception& e) {
        throw std::runtime_error(e.what());
    }
}

}  // namespace milvus::segcore::storagev1translator
