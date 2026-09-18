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

#include "index/scalar/inverted/InvertedIndexLoader.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <optional>
#include <set>
#include <string_view>
#include <unistd.h>
#include <utility>
#include <vector>

#include "index/ParamUtils.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "index/scalar/ScalarIndexUtils.h"
#include "storage/artifact/FileSourceUtils.h"
#include "storage/artifact/LocalDirectory.h"
#include "storage/artifact/LocalFileUtils.h"
#include "common/Slice.h"
#include "index/Families.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/inverted/InvertedIndexArtifact.h"
#include "index/scalar/inverted/InvertedIndexReader.h"
#include "index/scalar/json/JsonProjectedIndexLoad.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

using inverted_params::IsSupportedType;

struct RuntimeParams {
    DataType field_type{DataType::NONE};
    DataType value_type{DataType::NONE};
    bool nested{false};
};

RuntimeParams
ParseRuntimeParams(const Config& params) {
    RuntimeParams result;
    result.field_type =
        ReadDataTypeParam(params, "field_type").value_or(DataType::NONE);
    const auto element_type =
        ReadCompatibleScalarArrayElementType(params, "inverted");
    const auto configured =
        ReadDataTypeParam(params, "value_type").value_or(DataType::NONE);
    result.nested = ReadRequiredNestedParam(params, "inverted loader");

    if (result.field_type == DataType::ARRAY) {
        if (configured != DataType::NONE && configured != DataType::ARRAY &&
            element_type != DataType::NONE &&
            !ScalarValueTypesMatch(configured, element_type)) {
            ThrowInfo(DataTypeInvalid,
                      "inverted ARRAY value_type {} conflicts with element "
                      "type {}",
                      static_cast<int>(configured),
                      static_cast<int>(element_type));
        }
        result.value_type =
            element_type != DataType::NONE
                ? element_type
                : (configured != DataType::ARRAY ? configured : DataType::NONE);
    } else if (result.field_type == DataType::JSON) {
        if (result.nested) {
            ThrowInfo(DataTypeInvalid,
                      "nested inverted input requires ARRAY field_type");
        }
        result.value_type = configured;
    } else {
        if (result.nested) {
            ThrowInfo(DataTypeInvalid,
                      "nested inverted input requires ARRAY field_type");
        }
        if (element_type != DataType::NONE) {
            ThrowInfo(DataTypeInvalid,
                      "scalar inverted field conflicts with element type {}",
                      static_cast<int>(element_type));
        }
        result.value_type =
            configured != DataType::NONE ? configured : result.field_type;
        if (result.field_type != DataType::NONE &&
            !ScalarValueTypesMatch(result.field_type, result.value_type)) {
            ThrowInfo(DataTypeInvalid,
                      "inverted field type {} conflicts with value type {}",
                      static_cast<int>(result.field_type),
                      static_cast<int>(result.value_type));
        }
    }
    if (!IsSupportedType(result.value_type)) {
        ThrowInfo(DataTypeInvalid,
                  "unsupported inverted value type {}",
                  static_cast<int>(result.value_type));
    }
    return result;
}

struct EffectiveLoadOptions {
    bool mmap{false};
    std::string directory_parent;
};

EffectiveLoadOptions
ResolveLoadOptions(const storage::LoadOptions& opts) {
    EffectiveLoadOptions result;
    result.mmap = opts.enable_mmap || GetValueFromConfigOrFallback<bool>(
                                               opts.params, ENABLE_MMAP, false);
    result.directory_parent = opts.mmap_dir_path;
    if (result.directory_parent.empty() && opts.params.is_object() &&
        opts.params.contains(MMAP_FILE_PATH)) {
        try {
            result.directory_parent =
                opts.params.at(MMAP_FILE_PATH).get<std::string>();
        } catch (const nlohmann::json::exception& error) {
            ThrowInfo(DataTypeInvalid,
                      "inverted mmap path must be a string: {}",
                      error.what());
        }
    }
    AssertInfo(result.directory_parent.find('\0') == std::string::npos,
               "inverted load options contain an invalid staging path");
    return result;
}

struct PersistedEntries {
    std::vector<std::string> engine_files;
    bool has_null{false};
};

PersistedEntries
ReadPersistedEntries(storage::FileSource& source) {
    PersistedEntries result;
    if (source.Gen() == storage::Generation::V3) {
        result.engine_files =
            storage::ReadRequiredMeta<std::vector<std::string>>(
                source, FILE_NAMES, "inverted V3");
        result.has_null = storage::ReadRequiredMeta<bool>(
            source, HAS_NULL, "inverted V3");
        std::set<std::string> unique;
        for (const auto& name : result.engine_files) {
            storage::ValidateArtifactEntryName(name, "inverted index");
            if (name == INDEX_NULL_OFFSET || name == INDEX_TYPE ||
                name == INDEX_NON_EXIST_OFFSET_FILE_NAME ||
                !unique.insert(name).second) {
                ThrowInfo(DataFormatBroken,
                          "invalid or duplicate inverted engine entry {}",
                          name);
            }
            if (!source.HasEntry(name)) {
                ThrowInfo(DataFormatBroken,
                          "inverted engine entry {} is missing",
                          name);
            }
        }
        if (source.HasEntry(INDEX_NULL_OFFSET) != result.has_null) {
            ThrowInfo(DataFormatBroken,
                      "inverted V3 has_null metadata disagrees with sidecar");
        }
    } else {
        result.has_null = source.HasEntry(INDEX_NULL_OFFSET);
        for (const auto& name : source.EntryNames()) {
            storage::ValidateArtifactEntryName(name, "inverted index");
            if (name != INDEX_NULL_OFFSET && name != INDEX_TYPE &&
                name != INDEX_NON_EXIST_OFFSET_FILE_NAME &&
                name != INDEX_FILE_SLICE_META) {
                result.engine_files.push_back(name);
            }
        }
    }
    if (result.engine_files.empty()) {
        ThrowInfo(DataFormatBroken, "inverted artifact has no engine files");
    }
    return result;
}

using storage::LocalEntryGuard;

using storage::FileDescriptorGuard;

std::shared_ptr<const std::vector<size_t>>
ReadNullOffsets(storage::FileSource& source,
                bool has_null,
                const std::string& staging_parent,
                size_t count,
                bool nested) {
    if (!has_null) {
        return std::make_shared<const std::vector<size_t>>();
    }

    std::optional<size_t> expected_bytes;
    if (source.Gen() == storage::Generation::V3) {
        const auto declared = source.EntrySize(INDEX_NULL_OFFSET);
        if (declared < 0 || static_cast<uint64_t>(declared) >
                                std::numeric_limits<size_t>::max()) {
            ThrowInfo(DataFormatBroken,
                      "invalid inverted null-offset byte size {}",
                      declared);
        }
        expected_bytes = static_cast<size_t>(declared);
    }

    // The sidecar uses a separate owned child so it can never be enumerated
    // as a Tantivy file by the publishable engine directory.
    auto staging = CreateInvertedIndexDirectory(staging_parent);
    auto path =
        (std::filesystem::path(staging->Path()) / INDEX_NULL_OFFSET).string();
    LocalEntryGuard local(std::move(path));
    source.ReadEntryToLocalFile(INDEX_NULL_OFFSET, local.Path());
    const auto bytes = storage::LocalFileSize(
        local.Path(), "failed to determine inverted entry size for");
    if (expected_bytes.has_value() && bytes != *expected_bytes) {
        ThrowInfo(DataFormatBroken,
                  "inverted null-offset size changed from {} to {}",
                  *expected_bytes,
                  bytes);
    }
    if (bytes == 0 || bytes % sizeof(size_t) != 0) {
        ThrowInfo(DataFormatBroken,
                  "invalid inverted null-offset byte size {}",
                  bytes);
    }
    const auto offset_count = bytes / sizeof(size_t);
    if (!nested && offset_count > count) {
        ThrowInfo(DataFormatBroken,
                  "inverted null-offset count {} exceeds row count {}",
                  offset_count,
                  count);
    }

    std::vector<size_t> result(offset_count);
    const auto fd = ::open(local.Path().c_str(), O_RDONLY | O_CLOEXEC);
    if (fd == -1) {
        ThrowInfo(FileOpenFailed,
                  "failed to open inverted staging file {}: {}",
                  local.Path(),
                  std::strerror(errno));
    }
    FileDescriptorGuard descriptor(fd);
    storage::ReadAll(descriptor.Get(),
                     result.data(),
                     bytes,
                     local.Path(),
                     "inverted staging file");
    descriptor.CloseChecked(local.Path(), "inverted staging file");
    local.RemoveChecked("inverted staging file");
    return std::make_shared<const std::vector<size_t>>(std::move(result));
}

size_t
MaterializedBytes(const std::vector<std::string>& paths,
                  bool ram_payload_only) {
    size_t total = 0;
    for (const auto& path : paths) {
        // Tantivy's MmapDirectory::convert_to_ram_directory copies every
        // regular file except *.lock. Keep heap payload accounting aligned
        // with that exact ownership boundary; mmap readers retain every staged
        // file and therefore do not skip locks.
        if (ram_payload_only && std::string_view(path).ends_with(".lock")) {
            continue;
        }
        std::error_code error;
        const auto bytes = std::filesystem::file_size(path, error);
        if (error) {
            ThrowInfo(FileReadFailed,
                      "failed to inspect inverted entry {}: {}",
                      path,
                      error.message());
        }
        if (bytes > std::numeric_limits<size_t>::max() - total) {
            ThrowInfo(DataFormatBroken,
                      "inverted artifact byte size overflows size_t");
        }
        total += static_cast<size_t>(bytes);
    }
    return total;
}

struct InvertedLoadState {
    // Declaration order makes the engine release before its backing directory.
    std::shared_ptr<storage::LocalDirectory> directory;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine;
    std::shared_ptr<const std::vector<size_t>> null_offsets;
    RuntimeParams params;
    bool mmap{false};
    size_t engine_bytes{0};
};

InvertedLoadState
LoadState(storage::FileSource& source,
          const storage::LoadOptions& opts,
          RuntimeParams params) {
    const auto effective = ResolveLoadOptions(opts);
    const auto entries = ReadPersistedEntries(source);

    // Keep local owners intact until the state has acquired its own shared
    // references. On every exception the engine is released before directory.
    auto directory =
        CreateInvertedIndexDirectory(effective.directory_parent);
    const auto paths =
        source.ReadEntriesToLocalDir(entries.engine_files, directory->Path());
    if (paths.size() != entries.engine_files.size()) {
        ThrowInfo(DataFormatBroken,
                  "inverted source materialized {} of {} engine entries",
                  paths.size(),
                  entries.engine_files.size());
    }
    for (size_t i = 0; i < paths.size(); ++i) {
        if (std::filesystem::path(paths[i]).filename().string() !=
            entries.engine_files[i]) {
            ThrowInfo(DataFormatBroken,
                      "inverted source materialized entry {} as {}",
                      entries.engine_files[i],
                      paths[i]);
        }
    }
    const auto engine_bytes = MaterializedBytes(paths, !effective.mmap);
    if (!tantivy_index_exist(directory->Path().c_str())) {
        ThrowInfo(DataFormatBroken,
                  "materialized inverted artifact is not a Tantivy index");
    }
    auto engine = std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
        directory->Path().c_str(), effective.mmap, SetBitsetSealed);
    auto null_offsets = ReadNullOffsets(source,
                                        entries.has_null,
                                        effective.directory_parent,
                                        static_cast<size_t>(engine->count()),
                                        params.nested);

    InvertedLoadState result;
    result.directory = directory;
    result.engine = engine;
    result.null_offsets = std::move(null_offsets);
    result.params = std::move(params);
    result.mmap = effective.mmap;
    result.engine_bytes = engine_bytes;
    return result;
}

std::unique_ptr<IIndexReaderBase>
MakeReader(const InvertedLoadState& state) {
    AssertInfo(state.directory != nullptr,
               "inverted load state requires a directory owner");
    AssertInfo(state.engine != nullptr,
               "inverted load state requires an engine");
    AssertInfo(state.null_offsets != nullptr,
               "inverted load state requires immutable null offsets");
    const auto engine_path_bytes = state.directory->PathHeapBytes();
    const auto make = [&]<typename T>() -> std::unique_ptr<IIndexReaderBase> {
        return std::make_unique<InvertedIndexReader<T>>(
            state.mmap ? state.directory : nullptr,
            state.engine,
            state.null_offsets,
            state.params.value_type,
            state.params.nested,
            state.mmap,
            state.engine_bytes,
            engine_path_bytes);
    };
    switch (state.params.value_type) {
        case DataType::BOOL:
            return make.template operator()<bool>();
        case DataType::INT8:
            return make.template operator()<int8_t>();
        case DataType::INT16:
            return make.template operator()<int16_t>();
        case DataType::INT32:
            return make.template operator()<int32_t>();
        case DataType::INT64:
        case DataType::TIMESTAMPTZ:
            return make.template operator()<int64_t>();
        case DataType::FLOAT:
            return make.template operator()<float>();
        case DataType::DOUBLE:
            return make.template operator()<double>();
        case DataType::STRING:
        case DataType::VARCHAR:
        case DataType::TEXT:
            return make.template operator()<std::string_view>();
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported inverted value type {}",
                      static_cast<int>(state.params.value_type));
    }
}

}  // namespace

ReaderCaps
InvertedIndexLoader::DeriveCaps(const Config& index_meta) {
    const auto params = ParseRuntimeParams(index_meta);
    return DeriveJsonProjectedCaps(
        families::kInverted,
        index_meta,
        ReaderCaps{
            .predicate = true,
            .pattern_match = IsStringDataType(params.value_type),
            .nested = params.nested,
            .exact = !params.nested,
        });
}

IIndexReaderBasePtr
InvertedIndexLoader::Open(storage::FileSource& source,
                          const storage::LoadOptions& opts) {
    auto projection =
        PrepareJsonProjectedOpen(families::kInverted, source, opts);
    auto state = LoadState(source, opts, ParseRuntimeParams(opts.params));
    auto reader = MakeReader(state);
    return FinishJsonProjectedOpen(
        std::move(projection), source, std::move(reader));
}

namespace {

const bool kInvertedLoaderRegistered = [] {
    LoaderRegistry::Instance().Register<InvertedIndexLoader>();
    return true;
}();

}  // namespace

}  // namespace milvus::index
