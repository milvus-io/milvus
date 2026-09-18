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

#include "index/scalar/json/JsonFlatIndexLoader.h"

#include <algorithm>
#include <cerrno>
#include <filesystem>
#include <fcntl.h>
#include <cstring>
#include <limits>
#include <optional>
#include <set>
#include <stdexcept>
#include <string_view>
#include <unistd.h>
#include <utility>
#include <vector>

#include "index/ParamUtils.h"
#include "index/scalar/json/JsonFlatIndexParams.h"
#include "common/EasyAssert.h"
#include "storage/artifact/FileSourceUtils.h"
#include "storage/artifact/LocalDirectory.h"
#include "storage/artifact/LocalFileUtils.h"
#include "common/JsonUtils.h"
#include "common/Slice.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/json/JsonFlatIndexArtifact.h"
#include "index/scalar/json/JsonFlatIndexReader.h"
#include "nlohmann/json.hpp"
#include "tantivy-wrapper.h"

namespace milvus::index {
namespace {

using json_flat_params::ParseInteger;
using json_flat_params::RequireDataTypeParam;
using json_flat_params::ParseString;
using json_flat_params::ValidateRowDomain;

constexpr std::string_view kJsonPathParam = "json_path";
constexpr std::string_view kJsonCastTypeParam = "json_cast_type";

std::string
ParseJsonPath(const Config& params) {
    const bool has_json_path = params.contains(kJsonPathParam);
    const bool has_nested_path = params.contains("nested_path");
    const auto json_path = ParseString(params, kJsonPathParam);
    const auto nested_path = ParseString(params, "nested_path");
    if (has_json_path && has_nested_path && json_path != nested_path) {
        ThrowInfo(DataTypeInvalid,
                  "JSON flat json_path and nested_path disagree");
    }
    auto result = has_json_path ? json_path : nested_path;
    if (result.find('\0') != std::string::npos) {
        ThrowInfo(DataTypeInvalid,
                  "JSON flat root path contains an embedded NUL");
    }
    try {
        (void)parse_json_pointer(result);
    } catch (const std::invalid_argument& error) {
        ThrowInfo(DataTypeInvalid,
                  "invalid JSON flat root path {}: {}",
                  result,
                  error.what());
    }
    return result;
}

void
ValidateTantivyVersion(const Config& params) {
    const auto scalar_version =
        ParseInteger(params, SCALAR_INDEX_ENGINE_VERSION, 1, false, "loader");
    const auto explicit_version =
        ParseInteger(params, TANTIVY_INDEX_VERSION, 0, false, "loader");
    if (scalar_version < 0 || explicit_version < 0 ||
        explicit_version > std::numeric_limits<uint32_t>::max()) {
        ThrowInfo(DataTypeInvalid,
                  "invalid JSON flat engine versions scalar={} tantivy={}",
                  scalar_version,
                  explicit_version);
    }
    if (explicit_version != 0 &&
        explicit_version != TANTIVY_INDEX_MINIMUM_VERSION &&
        explicit_version != TANTIVY_INDEX_LATEST_VERSION) {
        ThrowInfo(DataTypeInvalid,
                  "unsupported JSON flat Tantivy version {}",
                  explicit_version);
    }
}

struct RuntimeParams {
    std::string nested_path;
};

RuntimeParams
ParseRuntimeParams(const Config& params) {
    if (!params.is_object()) {
        ThrowInfo(DataTypeInvalid,
                  "JSON flat load parameters must be an object");
    }
    ValidateRowDomain(params, "loader");
    if (RequireDataTypeParam(params, "field_type", "loader") !=
            DataType::JSON ||
        RequireDataTypeParam(params, "value_type", "loader") !=
            DataType::JSON) {
        ThrowInfo(DataTypeInvalid,
                  "JSON flat loader requires JSON field_type and value_type");
    }
    if (ParseString(params, kJsonCastTypeParam) != "JSON") {
        ThrowInfo(DataTypeInvalid,
                  "JSON flat loader requires JSON json_cast_type");
    }
    for (const auto key : {std::string_view("element_type"),
                           std::string_view("array_element_type")}) {
        if (params.contains(key) &&
            RequireDataTypeParam(params, key, "loader") != DataType::NONE) {
            ThrowInfo(
                DataTypeInvalid, "JSON flat loader does not accept {}", key);
        }
    }
    ValidateTantivyVersion(params);
    return RuntimeParams{.nested_path = ParseJsonPath(params)};
}

struct EffectiveLoadOptions {
    bool mmap{false};
    std::string directory_parent;
};

EffectiveLoadOptions
ResolveLoadOptions(const storage::LoadOptions& opts) {
    EffectiveLoadOptions result;
    result.mmap = opts.enable_mmap;
    result.mmap = result.mmap || GetValueFromConfigOrFallback<bool>(
                                     opts.params, ENABLE_MMAP, false);
    result.directory_parent = opts.mmap_dir_path;
    if (result.directory_parent.empty() && opts.params.is_object() &&
        opts.params.contains(MMAP_FILE_PATH)) {
        result.directory_parent = ParseString(opts.params, MMAP_FILE_PATH);
        if (result.directory_parent.find('\0') != std::string::npos) {
            ThrowInfo(DataTypeInvalid,
                      "JSON flat mmap directory contains an embedded NUL");
        }
    } else {
        AssertInfo(result.directory_parent.find('\0') == std::string::npos,
                   "JSON flat load options contain an invalid staging path");
    }
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
                source, FILE_NAMES, "JSON flat V3");
        result.has_null = storage::ReadRequiredMeta<bool>(
            source, HAS_NULL, "JSON flat V3");
        std::set<std::string> expected;
        for (const auto& name : result.engine_files) {
            storage::ValidateArtifactEntryName(name, "JSON flat index");
            if (name == INDEX_NULL_OFFSET || name == INDEX_TYPE ||
                !expected.insert(name).second) {
                ThrowInfo(DataFormatBroken,
                          "invalid or duplicate JSON flat engine entry {}",
                          name);
            }
            if (!source.HasEntry(name)) {
                ThrowInfo(DataFormatBroken,
                          "JSON flat engine entry {} is missing",
                          name);
            }
        }
        if (source.HasEntry(INDEX_NULL_OFFSET) != result.has_null) {
            ThrowInfo(DataFormatBroken,
                      "JSON flat V3 has_null metadata disagrees with sidecar");
        }
        if (result.has_null) {
            expected.emplace(INDEX_NULL_OFFSET);
        }
        std::set<std::string> actual;
        for (const auto& name : source.EntryNames()) {
            storage::ValidateArtifactEntryName(name, "JSON flat index");
            if (!actual.insert(name).second) {
                ThrowInfo(DataFormatBroken,
                          "duplicate JSON flat artifact entry {}",
                          name);
            }
        }
        if (actual != expected) {
            ThrowInfo(DataFormatBroken,
                      "JSON flat V3 entries disagree with file_names "
                      "metadata");
        }
    } else {
        result.has_null = source.HasEntry(INDEX_NULL_OFFSET);
        std::set<std::string> unique;
        for (const auto& name : source.EntryNames()) {
            storage::ValidateArtifactEntryName(name, "JSON flat index");
            if (name == INDEX_NULL_OFFSET || name == INDEX_TYPE ||
                name == INDEX_FILE_SLICE_META) {
                continue;
            }
            if (!unique.insert(name).second) {
                ThrowInfo(DataFormatBroken,
                          "duplicate JSON flat engine entry {}",
                          name);
            }
            result.engine_files.push_back(name);
        }
    }
    if (result.engine_files.empty()) {
        ThrowInfo(DataFormatBroken, "JSON flat artifact has no engine files");
    }
    return result;
}

using storage::LocalEntryGuard;

using storage::FileDescriptorGuard;

std::shared_ptr<const std::vector<size_t>>
ReadNullOffsets(storage::FileSource& source,
                bool has_null,
                const std::string& staging_parent,
                size_t count) {
    if (!has_null) {
        return std::make_shared<const std::vector<size_t>>();
    }

    std::optional<size_t> expected_bytes;
    if (source.Gen() == storage::Generation::V3) {
        const auto declared = source.EntrySize(INDEX_NULL_OFFSET);
        if (declared < 0 || static_cast<uint64_t>(declared) >
                                std::numeric_limits<size_t>::max()) {
            ThrowInfo(DataFormatBroken,
                      "invalid JSON flat null-offset byte size {}",
                      declared);
        }
        expected_bytes = static_cast<size_t>(declared);
    }

    auto staging = CreateJsonFlatIndexDirectory(staging_parent);
    auto path = (std::filesystem::path(staging->Path()) /
                 std::string(INDEX_NULL_OFFSET))
                    .string();
    LocalEntryGuard local(std::move(path));
    source.ReadEntryToLocalFile(INDEX_NULL_OFFSET, local.Path());
    const auto bytes = storage::LocalFileSize(
        local.Path(), "failed to determine JSON flat entry size for");
    if (expected_bytes.has_value() && bytes != *expected_bytes) {
        ThrowInfo(DataFormatBroken,
                  "JSON flat null-offset size changed from {} to {}",
                  *expected_bytes,
                  bytes);
    }

    if (bytes == 0 || bytes % sizeof(size_t) != 0 ||
        bytes / sizeof(size_t) > count) {
        ThrowInfo(DataFormatBroken,
                  "invalid JSON flat null-offset byte size {} for count {}",
                  bytes,
                  count);
    }

    auto result = std::make_shared<std::vector<size_t>>(bytes / sizeof(size_t));
    const auto fd = ::open(local.Path().c_str(), O_RDONLY | O_CLOEXEC);
    if (fd == -1) {
        ThrowInfo(FileOpenFailed,
                  "failed to open JSON flat staging file {}: {}",
                  local.Path(),
                  std::strerror(errno));
    }
    FileDescriptorGuard descriptor(fd);
    storage::ReadAll(descriptor.Get(),
                     result->data(),
                     bytes,
                     local.Path(),
                     "JSON flat staging file");
    descriptor.CloseChecked(local.Path(), "JSON flat staging file");
    local.RemoveChecked("JSON flat staging file");
    return result;
}

size_t
MaterializedBytes(const std::vector<std::string>& paths) {
    size_t total = 0;
    for (const auto& path : paths) {
        std::error_code error;
        const auto bytes = std::filesystem::file_size(path, error);
        if (error) {
            ThrowInfo(FileReadFailed,
                      "failed to inspect JSON flat entry {}: {}",
                      path,
                      error.message());
        }
        if (bytes > std::numeric_limits<size_t>::max() - total) {
            ThrowInfo(DataFormatBroken,
                      "JSON flat artifact byte size overflows size_t");
        }
        total += static_cast<size_t>(bytes);
    }
    return total;
}

size_t
RamPayloadBytes(milvus::tantivy::TantivyIndexWrapper& engine) {
    const auto bytes = engine.index_size_bytes();
    if (bytes > std::numeric_limits<size_t>::max()) {
        ThrowInfo(DataFormatBroken,
                  "RAM JSON flat payload exceeds size_t domain");
    }
    return static_cast<size_t>(bytes);
}

std::shared_ptr<const JsonFlatIndexReaderState>
LoadState(storage::FileSource& source, const storage::LoadOptions& opts) {
    const auto params = ParseRuntimeParams(opts.params);
    const auto effective = ResolveLoadOptions(opts);
    const auto entries = ReadPersistedEntries(source);

    auto directory =
        CreateJsonFlatIndexDirectory(effective.directory_parent);
    const auto paths =
        source.ReadEntriesToLocalDir(entries.engine_files, directory->Path());
    if (paths.size() != entries.engine_files.size()) {
        ThrowInfo(DataFormatBroken,
                  "JSON flat source materialized {} of {} engine entries",
                  paths.size(),
                  entries.engine_files.size());
    }
    for (size_t i = 0; i < paths.size(); ++i) {
        if (std::filesystem::path(paths[i]).filename().string() !=
            entries.engine_files[i]) {
            ThrowInfo(DataFormatBroken,
                      "JSON flat source materialized entry {} as {}",
                      entries.engine_files[i],
                      paths[i]);
        }
    }
    const auto mapped_bytes =
        effective.mmap ? MaterializedBytes(paths) : size_t{0};
    if (!tantivy_index_exist(directory->Path().c_str())) {
        ThrowInfo(DataFormatBroken,
                  "materialized JSON flat artifact is not a Tantivy index");
    }

    // Keep the directory and engine local until state construction succeeds.
    // RAM engines copy their directory and the resulting state does not retain
    // the staging directory; mmap engines receive the owner explicitly.
    auto engine = std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
        directory->Path().c_str(), effective.mmap, SetBitsetSealed);
    const auto count = static_cast<size_t>(engine->count());
    auto null_offsets = ReadNullOffsets(
        source, entries.has_null, effective.directory_parent, count);
    const auto engine_bytes =
        effective.mmap ? mapped_bytes : RamPayloadBytes(*engine);
    return JsonFlatIndexReaderState::Create(
        effective.mmap ? directory : nullptr,
        engine,
        params.nested_path,
        std::move(null_offsets),
        effective.mmap,
        engine_bytes,
        directory->PathHeapBytes());
}

}  // namespace

ReaderCaps
JsonFlatIndexLoader::DeriveCaps(const Config& index_meta) {
    static_cast<void>(ParseRuntimeParams(index_meta));
    return ReaderCaps{.json_paths = true, .exact = true};
}

IIndexReaderBasePtr
JsonFlatIndexLoader::Open(storage::FileSource& source,
                          const storage::LoadOptions& opts) {
    return std::make_unique<JsonFlatIndexReader>(LoadState(source, opts));
}

namespace {

const bool kJsonFlatLoaderRegistered = [] {
    LoaderRegistry::Instance().Register<JsonFlatIndexLoader>();
    return true;
}();

}  // namespace

}  // namespace milvus::index
