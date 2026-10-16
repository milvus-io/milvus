// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "Constants.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/ValidityView.h"
#include "index/Families.h"
#include "index/IndexTypeAdapter.h"
#include "index/Meta.h"
#include "index/ParamUtils.h"
#include "index/contracts/Registry.h"
#include "index/contracts/build/ScalarBuildInput.h"
#include "index/contracts/build/VectorBuildInput.h"
#include "index/contracts/query/IVectorReader.h"
#include "index/scalar/ScalarIndexUtils.h"
#include "index/vector/VectorMemBuilder.h"
#include "index/vector/VectorTypeUtils.h"
#include "indexbuilder/BuildSession.h"
#include "segcore/Types.h"
#include "storage/Util.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus {

inline void
SetTestIndexMetadata(segcore::LoadIndexInfo& info,
                     const index::IIndexReaderBase& reader,
                     const std::string& family) {
    info.index_family = family;
    info.index_value_type = reader.ValueType();
    info.index_caps = reader.Caps();
    if (info.field_type == DataType::NONE) {
        info.field_type = reader.ValueType();
    }
    if (const auto* vector =
            dynamic_cast<const index::IVectorReader*>(&reader)) {
        info.dim = vector->Dim();
    }
}

namespace test_index_detail {

inline index::IIndexReaderBasePtr
LoadPublished(const std::string& family,
              const Config& params,
              storage::FileManagerContext context,
              const storage::ArtifactStats& stats,
              storage::Generation generation = storage::Generation::V1V2) {
    std::vector<std::string> paths;
    paths.reserve(stats.Files().size());
    for (const auto& file : stats.Files()) {
        paths.push_back(file.file_name);
    }
    context.set_for_loading_index(true);
    context.use_async_load = false;
    storage::LoadOptions options;
    options.params = params;
    options.mmap_dir_path = TestRemotePath;
    const auto loader = index::LoaderRegistry::Instance().Lookup(family);
    AssertInfo(static_cast<bool>(loader), "test index family has no loader");
    if (generation == storage::Generation::V3) {
        return loader.Load(
            {index::IndexFiles{std::move(context),
                               std::move(paths),
                               index::PackedIndexStorageConfig{}},
             std::move(options)});
    }
    return loader.Load(
        {index::IndexFiles{std::move(context),
                           std::move(paths),
                           index::LegacyIndexStorageConfig{
                               family == index::families::kVectorDisk ||
                                       family == index::families::kInverted ||
                                       family == index::families::kNgram ||
                                       family == index::families::kText ||
                                       family == index::families::kJsonFlat
                                   ? storage::V1SourceLayout::DiskFiles
                                   : storage::V1SourceLayout::MemoryEntries}},
         std::move(options)});
}

// Consumer fixtures open persisted artifacts through the same loader boundary
// as a segment. In particular, vector Load performs knowhere preparation that
// consuming the just-built artifact would bypass.
inline index::IIndexReaderBasePtr
PersistAndLoad(storage::ArtifactPtr artifact,
               const std::string& family,
               const Config& params) {
    auto directory = storage::LocalDirectory::CreateOwned(
        TestRemotePath, "consumer_index_XXXXXX", "consumer index test");
    storage::StorageConfig config;
    config.storage_type = "local";
    config.root_path = directory->Path();
    storage::FileManagerContext context(storage::FieldDataMeta{1, 2, 3, 100},
                                        storage::IndexMeta{3, 100, 1000, 1},
                                        storage::CreateChunkManager(config),
                                        storage::InitArrowFileSystem(config));
    storage::V1DiskSink sink(context);
    artifact->Serialize(sink);
    const auto stats = sink.Finish();
    auto reader = LoadPublished(family, params, context, stats);
    sink.ReleaseLocalStaging();
    return reader;
}

template <typename T>
index::IIndexReaderBasePtr
BuildScalar(const std::string& family,
            int64_t count,
            const T* values,
            const bool* valid_data,
            Config params) {
    if (!params.contains("field_type")) {
        params["field_type"] = static_cast<int>(index::CppDataType<T>());
    }
    if (!params.contains("value_type")) {
        params["value_type"] = static_cast<int>(index::CppDataType<T>());
    }
    if (!params.contains(index::FIELD_ID)) {
        params[index::FIELD_ID] = 100;
    }
    if (!params.contains("local_dir")) {
        params["local_dir"] = TestRemotePath;
    }
    if (!params.contains("nested")) {
        params["nested"] = false;
    }
    params["nullable"] = valid_data != nullptr;
    params["num_rows"] = count;
    auto builder =
        index::BuilderRegistry<index::ScalarBuildInput<T>>::Instance().Create(
            family, params);
    AssertInfo(builder != nullptr, "test scalar family/input has no builder");
    const index::ScalarBuildBatch<T> batch{
        {values, static_cast<size_t>(count)},
        ValidityView::FromExpanded(valid_data)};
    const index::ScalarBuildInput<T> input{{&batch, 1}};
    auto artifact = std::move(*builder).Build(input);
    return PersistAndLoad(std::move(artifact), family, params);
}

}  // namespace test_index_detail

template <typename T>
inline index::IIndexReaderBasePtr
BuildTestScalarIndex(const std::string& family,
                     int64_t count,
                     const T* values,
                     const bool* valid_data = nullptr,
                     Config params = Config::object()) {
    if constexpr (std::is_same_v<T, std::string>) {
        std::vector<std::string_view> views(values, values + count);
        return test_index_detail::BuildScalar(
            family, count, views.data(), valid_data, std::move(params));
    } else {
        return test_index_detail::BuildScalar(
            family, count, values, valid_data, std::move(params));
    }
}

template <typename T>
inline index::IIndexReaderBasePtr
BuildTestVectorIndex(
    int64_t count,
    int64_t dim,
    const typename index::VectorBuildInput<T>::value_type* values,
    const std::string& index_type,
    const std::string& metric,
    Config params = Config::object(),
    bool use_knowhere_build_pool = true) {
    constexpr auto physical_type = index::PhysicalVectorDataType<T>();
    const auto field_type =
        index::ReadDataTypeParam(params, "field_type").value_or(physical_type);
    const auto element_type = index::ReadDataTypeParam(params, "element_type")
                                  .value_or(DataType::NONE);
    params[index::METRIC_TYPE] = metric;
    params[DIM_KEY] = dim;
    auto adapted = index::AdaptIndexType(
        {.index_type = index_type,
         .field_type = field_type,
         .element_type = element_type,
         .index_engine_version =
             knowhere::Version::GetCurrentVersion().VersionNumber(),
         .params = std::move(params)});
    adapted.params[INDEX_NUM_ROWS_KEY] = count;
    AssertInfo(adapted.family == index::families::kVectorMem,
               "disk vector fixtures require source-backed BuildSession");
    const auto size =
        std::is_same_v<T, sparse_u32_f32>
            ? count
            : (std::is_same_v<T, bin1> ? count * dim / 8 : count * dim);
    const index::VectorBuildInput<T> input{
        .physical_values = {values, static_cast<size_t>(size)},
        .logical_rows = count,
        .physical_rows = count,
        .dim = dim};
    index::VectorMemBuilder<T> builder(
        element_type,
        index_type,
        metric,
        knowhere::Version::GetCurrentVersion().VersionNumber(),
        dim,
        adapted.params,
        use_knowhere_build_pool);
    auto artifact = std::move(builder).Build(input);
    return test_index_detail::PersistAndLoad(
        std::move(artifact), adapted.family, adapted.params);
}

// The caller owns source files and their storage root for the reader lifetime.
inline index::IIndexReaderBasePtr
BuildTestIndexFromSource(indexbuilder::BuildRequest request,
                         storage::FileManagerContext context) {
    indexbuilder::BuildSession session(request, context);
    session.BuildFromSource();
    return test_index_detail::LoadPublished(request.family,
                                            request.params,
                                            std::move(context),
                                            session.Publish(),
                                            request.output.generation);
}

}  // namespace milvus
