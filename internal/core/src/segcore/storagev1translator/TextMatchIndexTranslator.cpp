// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "segcore/storagev1translator/TextMatchIndexTranslator.h"
#include "index/IndexLoaderFactory.h"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <utility>

#include "common/EasyAssert.h"
#include "common/ScopedTimer.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/PackedIndexLoad.h"
#include "index/LegacyIndexLoad.h"
#include "index/LoadResource.h"
#include "segcore/storagev2translator/StorageV2Config.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "log/Log.h"
#include "segcore/CacheMetricAttribution.h"
#include "segcore/Utils.h"
#include "storage/artifact/FileSource.h"
#include "storage/artifact/LoadOptions.h"

namespace milvus::segcore::storagev1translator {
namespace {

bool
IsPackedV3(const std::vector<std::string>& paths) {
    return paths.size() == 1 && std::filesystem::path(paths.front())
                                    .filename()
                                    .string()
                                    .ends_with(".v3");
}

std::unique_ptr<storage::FileSource>
MakeTextSource(const storage::FileManagerContext& context,
               const std::vector<std::string>& paths,
               const storage::LoadOptions& options) {
    return std::make_unique<storage::V1RemoteSource>(
        context,
        paths,
        options,
        storage::ArtifactStoragePath::TextLog,
        storage::V1SourceLayout::DiskFiles);
}

}  // namespace

namespace {

int64_t
EstimateValidityBitmapBytes(int64_t num_rows) {
    constexpr int64_t kWordBytes = sizeof(uint64_t);
    constexpr int64_t kBitsPerWord = kWordBytes * 8;
    if (num_rows <= 0) {
        return 0;
    }
    return ((num_rows - 1) / kBitsPerWord + 1) * kWordBytes;
}

}  // namespace

TextMatchIndexTranslator::TextMatchIndexTranslator(
    TextMatchIndexLoadInfo load_info,
    milvus::storage::FileManagerContext file_manager_context,
    milvus::Config config)
    : load_info_(std::move(load_info)),
      file_manager_context_(std::move(file_manager_context)),
      config_(std::move(config)),
      key_(fmt::format(
          "seg_{}_textindex_{}", load_info_.segment_id, load_info_.field_id)),
      meta_(load_info_.enable_mmap ? milvus::cachinglayer::StorageType::DISK
                                   : milvus::cachinglayer::StorageType::MEMORY,
            milvus::cachinglayer::CellIdMappingMode::ALWAYS_ZERO,
            milvus::segcore::getCellDataType(/* is_vector */ false,
                                             /* is_index */ true),
            milvus::segcore::getCacheWarmupPolicy(load_info_.warmup_policy,
                                                  /* is_vector */ false,
                                                  /* is_index */ true),
            /* support_eviction */ true,
            std::nullopt,
            milvus::segcore::MetricAttributionFromShard(load_info_.shard)) {
    milvus::index::IndexFamily family = milvus::index::families::kText;
    const auto value_type = static_cast<DataType>(
        file_manager_context_.fieldDataMeta.field_schema.data_type());
    AssertInfo(IsStringDataType(value_type),
               "text index requires a string field type");
    config_["field_type"] = static_cast<int32_t>(value_type);
    config_["value_type"] = static_cast<int32_t>(value_type);
    config_["nested"] = false;
    config_["is_nested"] = false;
    config_["analyzer_name"] = "milvus_tokenizer";
    config_["analyzer_params"] = load_info_.analyzer_params;
    config_[milvus::index::ENABLE_MMAP] = load_info_.enable_mmap;
    const auto loader =
        milvus::index::LoaderRegistry::Instance().Lookup(family);
    AssertInfo(static_cast<bool>(loader),
               "no index loader is registered for family {}",
               family);
    SetReaderContract(
        std::move(family), value_type, loader.derive_caps(config_));
    const auto files = index::GetValueFromConfig<std::vector<std::string>>(
        config_, index::INDEX_FILES);
    if (files && files->size() == 1 && files->front().ends_with(".v3")) {
        file_manager_context_.use_async_load =
            file_manager_context_.use_async_load.value_or(
                storagev2translator::StorageV2AsyncLoadEnabled());
        auto resources = index::ScalarIndexFileLoadResource(
            DataType::VARCHAR,
            load_info_.index_size,
            {{index::INDEX_TYPE, index::INVERTED_INDEX_TYPE},
             {index::SCALAR_INDEX_ENGINE_VERSION, "3"}},
            load_info_.enable_mmap,
            load_info_.num_rows,
            *files,
            file_manager_context_,
            /*is_index_file=*/false);
        packed_load_resource_request_ = resources.request;
        meta_.loading_overhead_config = std::move(resources.overhead);
    }
}

size_t
TextMatchIndexTranslator::num_cells() const {
    return 1;
}

milvus::cachinglayer::cid_t
TextMatchIndexTranslator::cell_id_of(milvus::cachinglayer::uid_t) const {
    return 0;
}

std::pair<milvus::cachinglayer::ResourceUsage,
          milvus::cachinglayer::ResourceUsage>
TextMatchIndexTranslator::estimated_byte_size_of_cell(
    milvus::cachinglayer::cid_t) const {
    if (packed_load_resource_request_) {
        const auto& request = *packed_load_resource_request_;
        return {{request.final_memory_cost, request.final_disk_cost},
                {request.max_memory_cost - request.final_memory_cost,
                 request.max_disk_cost - request.final_disk_cost}};
    }
    // ignore the cid checking, because there is only one cell
    auto bitmap_bytes = EstimateValidityBitmapBytes(load_info_.num_rows);
    if (load_info_.enable_mmap) {
        return {{bitmap_bytes, load_info_.index_size},
                {load_info_.index_size, 0}};
    } else {
        // The reason the maximum disk usage is not zero is that the text match index
        // is first written to the disk, then loaded into memory. Only after that are
        // the disk files deleted.
        return {{load_info_.index_size + bitmap_bytes, 0},
                {0, load_info_.index_size}};
    }
}

int64_t
TextMatchIndexTranslator::cells_storage_bytes(
    const std::vector<milvus::cachinglayer::cid_t>&) const {
    // ignore the cids checking, because there is only one cell
    constexpr int64_t MIN_STORAGE_BYTES = 1 * 1024 * 1024;
    return std::max(load_info_.index_size, MIN_STORAGE_BYTES);
}

const std::string&
TextMatchIndexTranslator::key() const {
    return key_;
}

std::vector<std::pair<milvus::cachinglayer::cid_t,
                      std::unique_ptr<milvus::index::IIndexReaderBase>>>
TextMatchIndexTranslator::get_cells(
    milvus::OpContext* ctx,
    const std::vector<milvus::cachinglayer::cid_t>& cids) {
    // Check for cancellation before loading text match index
    CheckCancellation(
        ctx, load_info_.segment_id, "TextMatchIndexTranslator::get_cells()");

    const auto files =
        milvus::index::GetValueFromConfig<std::vector<std::string>>(
            config_, milvus::index::INDEX_FILES);
    AssertInfo(files.has_value() && !files->empty(),
               "text index file paths are empty");

    storage::LoadOptions options;
    options.enable_mmap = load_info_.enable_mmap;
    options.estimated_bytes = load_info_.index_size;
    options.params = config_;
    options.op_ctx = ctx;
    options.warmup = ToStorageWarmup(
        milvus::segcore::getCacheWarmupPolicy(load_info_.warmup_policy,
                                              /* is_vector */ false,
                                              /* is_index */ true));
    const auto loader =
        milvus::index::LoaderRegistry::Instance().Lookup(Family());
    AssertInfo(static_cast<bool>(loader),
               "no index loader is registered for family {}",
               Family());

    std::unique_ptr<milvus::index::IIndexReaderBase> reader;
    {
        milvus::ScopedTimer timer(
            "text_match_index_load",
            [](double /*us*/) {
                // no specific metric defined for text match index load yet
            },
            milvus::ScopedTimer::LogLevel::Info);
        if (IsPackedV3(*files)) {
            reader = index::LoadIndex(
                loader,
                {index::IndexFiles{file_manager_context_,
                                   {files->front()},
                                   index::PackedIndexFile{false}},
                 options});
        } else {
            auto source =
                MakeTextSource(file_manager_context_, *files, options);
            reader = index::LoadIndex(
                loader,
                {index::OpenedIndexInput{index::LegacyIndexSource{
                     std::shared_ptr<storage::FileSource>(std::move(source)),
                     false}},
                 options});
        }
    }
    AssertInfo(reader != nullptr, "text loader returned a null reader");

    LOG_INFO("load text match index success for field:{} of segment:{}",
             load_info_.field_id,
             load_info_.segment_id);

    std::vector<std::pair<milvus::cachinglayer::cid_t,
                          std::unique_ptr<milvus::index::IIndexReaderBase>>>
        result;
    result.emplace_back(std::make_pair(0, std::move(reader)));
    return result;
}

milvus::cachinglayer::Meta*
TextMatchIndexTranslator::meta() {
    return &meta_;
}

}  // namespace milvus::segcore::storagev1translator
