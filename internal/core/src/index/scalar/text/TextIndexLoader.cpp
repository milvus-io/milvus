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

#include "index/scalar/text/TextIndexLoader.h"

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
#include "common/EasyAssert.h"
#include "storage/artifact/FileSourceUtils.h"
#include "storage/artifact/LocalDirectory.h"
#include "storage/artifact/LocalFileUtils.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/contracts/Registry.h"
#include "index/scalar/text/TextIndexArtifact.h"
#include "index/scalar/text/TextIndexReader.h"
#include "nlohmann/json.hpp"
#include "tantivy-wrapper.h"

namespace milvus::index {
namespace {

bool
ReadRequiredRowDomain(const Config& params) {
    if (ReadRequiredNestedParam(params, "text loader")) {
        ThrowInfo(DataTypeInvalid,
                  "text indexes support only the row coordinate domain");
    }
    return false;
}

struct RuntimeParams {
    DataType value_type{DataType::NONE};
    std::string analyzer_name;
    std::string analyzer_params;
    std::string analyzer_extra_info;
};

RuntimeParams
ParseRuntimeParams(const Config& params) {
    if (!params.is_object()) {
        ThrowInfo(DataTypeInvalid, "text load parameters must be an object");
    }
    static_cast<void>(ReadRequiredRowDomain(params));
    const auto field_type =
        ReadDataTypeParam(params, "field_type").value_or(DataType::NONE);
    const auto value_type =
        ReadDataTypeParam(params, "value_type").value_or(DataType::NONE);
    if (field_type != DataType::NONE && value_type != DataType::NONE &&
        field_type != value_type &&
        !(IsStringDataType(field_type) && IsStringDataType(value_type))) {
        ThrowInfo(DataTypeInvalid,
                  "text field_type {} disagrees with value_type {}",
                  static_cast<int>(field_type),
                  static_cast<int>(value_type));
    }
    RuntimeParams result;
    result.value_type = field_type != DataType::NONE ? field_type : value_type;
    if (!IsStringDataType(result.value_type)) {
        ThrowInfo(DataTypeInvalid,
                  "text loader requires STRING, VARCHAR, or TEXT value_type");
    }
    result.analyzer_name =
        ReadStringParam(params, "analyzer_name", "milvus_tokenizer", "text");
    result.analyzer_params =
        ReadStringParam(params, "analyzer_params", "{}", "text");
    result.analyzer_extra_info =
        ReadStringParam(params, "analyzer_extra_info", {}, "text");
    if (result.analyzer_name.empty()) {
        ThrowInfo(DataTypeInvalid, "text loader requires an analyzer name");
    }
    return result;
}

struct EffectiveLoadOptions {
    bool file_backed{false};
    std::string directory_parent;
};

EffectiveLoadOptions
ResolveLoadOptions(const storage::LoadOptions& opts) {
    EffectiveLoadOptions result;
    result.file_backed = opts.enable_mmap;
    result.file_backed =
        result.file_backed || GetValueFromConfigOrFallback<bool>(
                                  opts.params, ENABLE_MMAP, false);
    result.directory_parent = opts.mmap_dir_path;
    if (result.directory_parent.empty() && opts.params.is_object() &&
        opts.params.contains(MMAP_FILE_PATH)) {
        result.directory_parent =
            ReadStringParam(opts.params, MMAP_FILE_PATH, {}, "text");
    }
    AssertInfo(result.directory_parent.find('\0') == std::string::npos,
               "text load options contain an invalid staging path");
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
                source, FILE_NAMES, "text V3");
        result.has_null =
            storage::ReadRequiredMeta<bool>(source, HAS_NULL, "text V3");
        std::set<std::string> unique;
        for (const auto& name : result.engine_files) {
            storage::ValidateArtifactEntryName(name, "text index");
            if (name == INDEX_NULL_OFFSET || !unique.insert(name).second) {
                ThrowInfo(DataFormatBroken,
                          "invalid or duplicate text engine entry {}",
                          name);
            }
            if (!source.HasEntry(name)) {
                ThrowInfo(
                    DataFormatBroken, "text engine entry {} is missing", name);
            }
        }
        if (source.HasEntry(INDEX_NULL_OFFSET) != result.has_null) {
            ThrowInfo(DataFormatBroken,
                      "text V3 has_null metadata disagrees with sidecar");
        }
        auto expected = unique;
        if (result.has_null) {
            expected.emplace(INDEX_NULL_OFFSET);
        }
        std::set<std::string> actual;
        for (const auto& name : source.EntryNames()) {
            storage::ValidateArtifactEntryName(name, "text index");
            if (!actual.insert(name).second) {
                ThrowInfo(
                    DataFormatBroken, "duplicate text artifact entry {}", name);
            }
        }
        if (actual != expected) {
            ThrowInfo(DataFormatBroken,
                      "text V3 entries disagree with file_names metadata");
        }
    } else {
        result.has_null = source.HasEntry(INDEX_NULL_OFFSET);
        std::set<std::string> unique;
        for (const auto& name : source.EntryNames()) {
            storage::ValidateArtifactEntryName(name, "text index");
            if (name == INDEX_NULL_OFFSET) {
                continue;
            }
            if (!unique.insert(name).second) {
                ThrowInfo(
                    DataFormatBroken, "duplicate text engine entry {}", name);
            }
            result.engine_files.push_back(name);
        }
    }
    if (result.engine_files.empty()) {
        ThrowInfo(DataFormatBroken, "text artifact has no engine files");
    }
    return result;
}

using storage::LocalEntryGuard;

using storage::FileDescriptorGuard;

void
ValidateNullOffsets(const std::vector<size_t>& offsets, size_t count) {
    size_t previous = 0;
    bool first = true;
    for (const auto offset : offsets) {
        if ((!first && offset <= previous) || offset >= count) {
            ThrowInfo(DataFormatBroken,
                      "invalid text null offset {} for count {}",
                      offset,
                      count);
        }
        previous = offset;
        first = false;
    }
}

std::shared_ptr<const std::vector<size_t>>
ReadNullOffsets(storage::FileSource& source,
                bool has_null,
                const std::string& staging_parent,
                size_t count) {
    if (!has_null) {
        return std::make_shared<const std::vector<size_t>>();
    }

    // Keep the sidecar outside the Tantivy directory so it can never enter the
    // engine inventory enumerated by TextIndexArtifact::Serialize.
    auto directory = CreateTextIndexDirectory(staging_parent, "null");
    auto path =
        (std::filesystem::path(directory->Path()) / INDEX_NULL_OFFSET).string();
    LocalEntryGuard local(std::move(path));
    source.ReadEntryToLocalFile(INDEX_NULL_OFFSET, local.Path());

    std::error_code error;
    const auto observed = std::filesystem::file_size(local.Path(), error);
    if (error || observed > std::numeric_limits<size_t>::max()) {
        ThrowInfo(FileReadFailed,
                  "failed to determine text NULL sidecar size {}: {}",
                  local.Path(),
                  error.message());
    }
    const auto bytes = static_cast<size_t>(observed);
    if (bytes == 0 || bytes % sizeof(size_t) != 0) {
        ThrowInfo(
            DataFormatBroken, "invalid text null-offset byte size {}", bytes);
    }
    const auto offset_count = bytes / sizeof(size_t);
    if (offset_count > count) {
        ThrowInfo(DataFormatBroken,
                  "text null-offset count {} exceeds row count {}",
                  offset_count,
                  count);
    }

    auto result = std::make_shared<std::vector<size_t>>(offset_count);
    const auto fd = ::open(local.Path().c_str(), O_RDONLY | O_CLOEXEC);
    if (fd == -1) {
        ThrowInfo(FileOpenFailed,
                  "failed to open text NULL staging file {}: {}",
                  local.Path(),
                  std::strerror(errno));
    }
    FileDescriptorGuard descriptor(fd);
    storage::ReadAll(descriptor.Get(),
                     result->data(),
                     bytes,
                     local.Path(),
                     "text NULL staging file");
    descriptor.CloseChecked(local.Path(), "text NULL staging file");
    ValidateNullOffsets(*result, count);
    local.RemoveChecked("text NULL staging file");
    return result;
}

struct TextLoadState {
    // Declaration order makes the engine release before the final directory
    // owner on every success and exception path.
    std::shared_ptr<storage::LocalDirectory> directory;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine;
    std::shared_ptr<const std::vector<size_t>> null_offsets;
    RuntimeParams params;
    int64_t count{0};
    bool reader_file_backed{false};
    size_t payload_bytes{0};
};

TextLoadState
LoadState(storage::FileSource& source,
          const storage::LoadOptions& opts,
          RuntimeParams params) {
    const auto effective = ResolveLoadOptions(opts);
    const auto entries = ReadPersistedEntries(source);

    // Keep these local owners intact until TextLoadState has acquired all
    // shared references. The engine is declared after its backing directory
    // and therefore releases first if any later operation throws.
    auto directory =
        CreateTextIndexDirectory(effective.directory_parent, "loaded");
    const auto paths =
        source.ReadEntriesToLocalDir(entries.engine_files, directory->Path());
    if (paths.size() != entries.engine_files.size()) {
        ThrowInfo(DataFormatBroken,
                  "text source materialized {} of {} engine entries",
                  paths.size(),
                  entries.engine_files.size());
    }
    for (size_t i = 0; i < paths.size(); ++i) {
        if (std::filesystem::path(paths[i]).filename().string() !=
            entries.engine_files[i]) {
            ThrowInfo(DataFormatBroken,
                      "text source materialized entry {} as {}",
                      entries.engine_files[i],
                      paths[i]);
        }
    }
    if (!tantivy_index_exist(directory->Path().c_str())) {
        ThrowInfo(DataFormatBroken,
                  "materialized text artifact is not a Tantivy index");
    }

    auto engine = std::make_shared<milvus::tantivy::TantivyIndexWrapper>(
        directory->Path().c_str(), effective.file_backed, SetBitsetSealed);
    engine->set_analyzer_extra_info(params.analyzer_extra_info);
    engine->register_tokenizer(params.analyzer_name.c_str(),
                               params.analyzer_params.c_str());
    const auto count = static_cast<int64_t>(engine->count());
    auto null_offsets = ReadNullOffsets(source,
                                        entries.has_null,
                                        effective.directory_parent,
                                        static_cast<size_t>(count));
    const auto payload_bytes =
        effective.file_backed ? TextIndexDirectoryBytes(*directory)
                              : TextIndexRamPayloadBytes(*engine);

    TextLoadState result;
    result.directory = directory;
    result.engine = engine;
    result.null_offsets = std::move(null_offsets);
    result.params = std::move(params);
    result.count = count;
    result.reader_file_backed = effective.file_backed;
    result.payload_bytes = payload_bytes;
    return result;
}

std::unique_ptr<IIndexReaderBase>
MakeReader(const TextLoadState& state) {
    AssertInfo(state.directory != nullptr,
               "text load state requires a staging directory");
    AssertInfo(state.engine != nullptr,
               "text load state requires a reader engine");
    AssertInfo(state.null_offsets != nullptr,
               "text load state requires null offsets");
    return std::make_unique<TextIndexReader>(
        state.reader_file_backed ? state.directory : nullptr,
        state.engine,
        state.null_offsets,
        state.count,
        state.params.value_type,
        state.reader_file_backed,
        state.payload_bytes);
}

}  // namespace

ReaderCaps
TextIndexLoader::DeriveCaps(const Config& index_meta) {
    static_cast<void>(ParseRuntimeParams(index_meta));
    return ReaderCaps{.text_match = true};
}

IIndexReaderBasePtr
TextIndexLoader::Open(storage::FileSource& source,
                      const storage::LoadOptions& opts) {
    auto state = LoadState(source, opts, ParseRuntimeParams(opts.params));
    return MakeReader(state);
}

namespace {

const bool kTextLoaderRegistered = [] {
    LoaderRegistry::Instance().Register<TextIndexLoader>();
    return true;
}();

}  // namespace

}  // namespace milvus::index
