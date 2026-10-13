// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/FieldData.h"
#include "exec/expression/ExprIndexIntegrationTestUtils.h"
#include "index/contracts/build/VectorBuildInput.h"
#include "index/contracts/query/IVectorReader.h"
#include "index/vector/VectorMemBuilder.h"
#include "knowhere/version.h"
#include "storage/Util.h"
#include "storage/artifact/FileSink.h"
#include "storage/artifact/LocalDirectory.h"
#include "test_utils/Constants.h"

namespace milvus::test::consumer {

using OpenedIndex = expr_index::OpenedIndex;

template <typename T>
inline OpenedIndex
BuildScalarReader(FieldId field_id,
                  DataType field_type,
                  const std::string& index_type,
                  int64_t rows,
                  const T* values,
                  const bool* validity = nullptr,
                  Config params = Config::object()) {
    auto field =
        std::make_shared<FieldData<T>>(field_type, validity != nullptr);
    FieldDataBase& output = *field;
    if (validity != nullptr) {
        std::vector<uint8_t> packed((rows + 7) / 8, 0);
        for (int64_t row = 0; row < rows; ++row) {
            if (validity[row]) {
                packed[row / 8] |= uint8_t{1} << (row % 8);
            }
        }
        output.FillFieldData(values, packed.data(), rows, 0);
    } else {
        output.FillFieldData(values, rows);
    }
    return expr_index::BuildIndex(
        field_id, field_type, index_type, {field}, std::move(params));
}

// Consumer vector fixtures retain a real serialization/load boundary. In
// particular, IVF direct maps are prepared by loading, not merely by building.
template <typename T>
inline OpenedIndex
BuildVectorReader(DataType field_type,
                  const std::string& index_type,
                  const std::string& metric,
                  int64_t dim,
                  int64_t rows,
                  const typename index::VectorBuildInput<T>::value_type* values,
                  Config params = Config::object(),
                  bool use_build_pool = true,
                  ValidityView parent_validity = {},
                  int64_t physical_rows = -1) {
    if (physical_rows < 0) {
        physical_rows = rows;
    }
    params[index::METRIC_TYPE] = metric;
    params[DIM_KEY] = dim;
    params[INDEX_NUM_ROWS_KEY] = rows;
    params[NUM_ROWS_KEY] = rows;
    params["nullable"] = static_cast<bool>(parent_validity);
    if (!params.contains(knowhere::indexparam::NLIST)) {
        params[knowhere::indexparam::NLIST] = 1024;
    }
    const auto version = knowhere::Version::GetCurrentVersion().VersionNumber();
    auto adapted = index::AdaptIndexType({.index_type = index_type,
                                          .field_type = field_type,
                                          .index_engine_version = version,
                                          .params = std::move(params)});
    std::unique_ptr<index::IArtifactBuilder<index::VectorBuildInput<T>>>
        builder;
    if (use_build_pool) {
        builder = index::BuilderRegistry<index::VectorBuildInput<T>>::Instance()
                      .Create(adapted.family, adapted.params);
    } else {
        // The public registry uses the default pool policy. This fixture also
        // retains the existing explicit no-build-pool consumer scenario.
        AssertInfo(adapted.family == index::families::kVectorMem,
                   "resident consumer fixture needs a memory vector family");
        builder = std::make_unique<index::VectorMemBuilder<T>>(DataType::NONE,
                                                               index_type,
                                                               metric,
                                                               version,
                                                               dim,
                                                               adapted.params,
                                                               false);
    }
    AssertInfo(builder != nullptr,
               "consumer vector fixture has no resident-input builder");
    size_t count;
    if constexpr (std::is_same_v<T, sparse_u32_f32>) {
        count = physical_rows;
    } else {
        count = static_cast<size_t>(physical_rows) *
                (field_type == DataType::VECTOR_BINARY ? dim / 8 : dim);
    }
    const index::VectorBuildInput<T> input{
        .physical_values = {values, count},
        .logical_rows = rows,
        .physical_rows = physical_rows,
        .dim = dim,
        .parent_validity = parent_validity,
    };
    auto artifact = std::move(*builder).Build(input);

    auto directory = storage::LocalDirectory::CreateOwned(
        TestRemotePath, "src-consumer-vector-XXXXXX", "consumer vector files");
    storage::StorageConfig config;
    config.storage_type = "local";
    config.root_path = directory->Path();
    const auto id = static_cast<int64_t>(expr_index::NextFixtureId()) + 100000;
    storage::FileManagerContext context(storage::FieldDataMeta{1, 2, id, 100},
                                        storage::IndexMeta{id, 100, id, 1},
                                        storage::CreateChunkManager(config),
                                        storage::InitArrowFileSystem(config));
    storage::V1DiskSink sink(context);
    artifact->Serialize(sink);
    const auto stats = sink.Finish();
    std::vector<std::string> files;
    files.reserve(stats.Files().size());
    for (const auto& file : stats.Files()) {
        files.push_back(file.file_name);
    }
    storage::LoadOptions options;
    options.params = adapted.params;
    options.estimated_bytes = stats.MemSize();
    context.set_for_loading_index(true);
    const auto loader =
        index::LoaderRegistry::Instance().Lookup(adapted.family);
    AssertInfo(static_cast<bool>(loader),
               "consumer vector fixture has no loader");
    const auto caps = loader.derive_caps(adapted.params);
    auto reader =
        loader.Load({index::IndexFiles{std::move(context),
                                       std::move(files),
                                       index::LegacyIndexStorageConfig{}},
                     std::move(options)});
    sink.ReleaseLocalStaging();
    return {std::move(reader), adapted.family, std::move(adapted.params), caps};
}

inline std::shared_ptr<cachinglayer::CacheSlot<index::IIndexReaderBase>>
CreateReaderCache(index::IIndexReaderBasePtr reader,
                  OpContext** observed = nullptr) {
    std::unique_ptr<cachinglayer::Translator<index::IIndexReaderBase>>
        translator = std::make_unique<expr_index::ReaderTranslator>(
            std::move(reader), observed);
    return cachinglayer::Manager::GetInstance().CreateCacheSlot(
        std::move(translator));
}

inline segcore::LoadIndexInfo
MakeLoadIndexInfo(OpenedIndex opened,
                  DataType field_type,
                  int64_t field_id = 100,
                  DataType element_type = DataType::NONE) {
    segcore::LoadIndexInfo info{};
    info.field_id = field_id;
    info.field_type = field_type;
    info.element_type = element_type;
    info.index_id = expr_index::NextFixtureId();
    info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    info.index_family = opened.family;
    info.index_value_type = opened.reader->ValueType();
    info.index_caps = opened.caps;
    // Nullable vector readers count physical vectors; segment metadata must
    // retain the logical parent-row count from the build input.
    info.num_rows =
        opened.params.value(INDEX_NUM_ROWS_KEY, opened.reader->Count());
    info.index_size = opened.reader->MemoryUsage();
    if (const auto* vector =
            dynamic_cast<const index::IVectorReader*>(opened.reader.get())) {
        info.dim = vector->Dim();
    }
    for (const auto& [key, value] : opened.params.items()) {
        info.index_params.emplace(
            key, value.is_string() ? value.get<std::string>() : value.dump());
    }
    info.load_resource_request = LoadResourceRequest{};
    info.cache_index = CreateReaderCache(std::move(opened.reader));
    return info;
}

}  // namespace milvus::test::consumer
