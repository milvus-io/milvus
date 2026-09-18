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

#include "index/scalar/spatial/RTreeIndexLoader.h"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cstring>
#include <cstdlib>
#include <filesystem>
#include <fcntl.h>
#include <limits>
#include <optional>
#include <set>
#include <string_view>
#include <unistd.h>
#include <utility>
#include <vector>

#include "index/ParamUtils.h"
#include "index/scalar/spatial/RTreeIndexParams.h"
#include "common/EasyAssert.h"
#include "storage/artifact/FileSourceUtils.h"
#include "storage/artifact/LocalDirectory.h"
#include "storage/artifact/LocalFileUtils.h"
#include "index/Meta.h"
#include "index/contracts/Registry.h"
#include "index/scalar/spatial/RTreeEngine.h"
#include "index/scalar/spatial/RTreeIndexArtifact.h"
#include "index/scalar/spatial/RTreeIndexReader.h"
#include "nlohmann/json.hpp"

namespace milvus::index {
namespace {

using spatial_params::ValidateGeometryParams;

constexpr std::string_view kArchiveSuffix = ".bgi";

bool
EndsWith(std::string_view value, std::string_view suffix) {
    return value.size() >= suffix.size() &&
           value.substr(value.size() - suffix.size()) == suffix;
}

int64_t
ParseRowCountValue(const nlohmann::json& value, std::string_view key) {
    if (value.is_number_unsigned()) {
        const auto count = value.get<uint64_t>();
        if (count <=
            static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return static_cast<int64_t>(count);
        }
    } else if (value.is_number_integer()) {
        const auto count = value.get<int64_t>();
        if (count >= 0) {
            return count;
        }
    } else if (value.is_string()) {
        const auto text = value.get<std::string>();
        int64_t count = -1;
        const auto [end, error] =
            std::from_chars(text.data(), text.data() + text.size(), count);
        if (error == std::errc() && end == text.data() + text.size() &&
            count >= 0) {
            return count;
        }
    }
    ThrowInfo(UnexpectedError,
              "normalized R-Tree parameter {} is not a non-negative int64",
              key);
}

int64_t
ReadRequiredRowCount(const Config& params) {
    std::optional<int64_t> result;
    std::string_view first_key;
    for (const auto key :
         {std::string_view("num_rows"), std::string_view("index_num_rows")}) {
        if (!params.contains(key)) {
            continue;
        }
        const auto count = ParseRowCountValue(params.at(key), key);
        if (result.has_value() && *result != count) {
            ThrowInfo(UnexpectedError,
                      "normalized R-Tree row counts {} and {} disagree",
                      first_key,
                      key);
        }
        if (!result.has_value()) {
            result = count;
            first_key = key;
        }
    }
    if (!result.has_value()) {
        ThrowInfo(UnexpectedError,
                  "R-Tree loader requires authoritative runtime num_rows");
    }
    return *result;
}

void
ValidateEntryNames(const std::vector<std::string>& names) {
    std::set<std::string> unique;
    size_t archives = 0;
    for (const auto& name : names) {
        if (name.empty() || name.find('\0') != std::string::npos ||
            std::filesystem::path(name).filename() != name || name == "." ||
            name == ".." || name == INDEX_NULL_OFFSET) {
            ThrowInfo(DataFormatBroken,
                      "invalid R-Tree artifact entry name {}",
                      name);
        }
        if (!unique.insert(name).second) {
            ThrowInfo(
                DataFormatBroken, "duplicate R-Tree artifact entry {}", name);
        }
        if (EndsWith(name, kArchiveSuffix)) {
            ++archives;
        }
    }
    if (archives != 1) {
        ThrowInfo(DataFormatBroken,
                  "R-Tree artifact must contain exactly one .bgi archive, got "
                  "{}",
                  archives);
    }
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
                source, FILE_NAMES, "R-Tree artifact");
        result.has_null = storage::ReadRequiredMeta<bool>(
            source, HAS_NULL, "R-Tree artifact");
        ValidateEntryNames(result.engine_files);

        std::set<std::string> expected(result.engine_files.begin(),
                                       result.engine_files.end());
        if (result.has_null) {
            expected.emplace(INDEX_NULL_OFFSET);
        }
        std::set<std::string> actual;
        for (const auto& name : source.EntryNames()) {
            if (name.empty() || name.find('\0') != std::string::npos ||
                std::filesystem::path(name).filename().string() != name ||
                name == "." || name == ".." || !actual.insert(name).second) {
                ThrowInfo(DataFormatBroken,
                          "invalid or duplicate R-Tree artifact entry {}",
                          name);
            }
        }
        if (actual != expected) {
            ThrowInfo(DataFormatBroken,
                      "R-Tree V3 entries disagree with file_names/has_null "
                      "metadata");
        }
    } else {
        std::set<std::string> unique;
        for (const auto& name : source.EntryNames()) {
            if (name.empty() || name.find('\0') != std::string::npos ||
                std::filesystem::path(name).filename().string() != name ||
                name == "." || name == ".." || !unique.insert(name).second) {
                ThrowInfo(DataFormatBroken,
                          "invalid or duplicate R-Tree artifact entry {}",
                          name);
            }
            if (name == INDEX_NULL_OFFSET) {
                result.has_null = true;
            } else {
                result.engine_files.push_back(name);
            }
        }
        ValidateEntryNames(result.engine_files);
    }
    return result;
}

std::string
FindBasePath(const std::vector<std::string>& local_paths) {
    for (const auto& path : local_paths) {
        if (EndsWith(path, kArchiveSuffix)) {
            return path.substr(0, path.size() - kArchiveSuffix.size());
        }
    }
    ThrowInfo(DataFormatBroken,
              "materialized R-Tree artifact has no .bgi archive");
}

using storage::LocalEntryGuard;

using storage::FileDescriptorGuard;

std::shared_ptr<const std::vector<size_t>>
ReadNullOffsets(storage::FileSource& source,
                bool has_null,
                int64_t total_num_rows,
                const std::string& staging_parent) {
    if (!has_null) {
        return std::make_shared<const std::vector<size_t>>();
    }
    if (!source.HasEntry(INDEX_NULL_OFFSET)) {
        ThrowInfo(DataFormatBroken,
                  "R-Tree artifact declares nulls but has no {} entry",
                  INDEX_NULL_OFFSET);
    }

    auto directory = CreateRTreeIndexDirectory(staging_parent, "null");
    auto path =
        (std::filesystem::path(directory->Path()) / INDEX_NULL_OFFSET).string();
    LocalEntryGuard local(std::move(path));
    source.ReadEntryToLocalFile(INDEX_NULL_OFFSET, local.Path());

    std::error_code error;
    const auto observed = std::filesystem::file_size(local.Path(), error);
    if (error || observed > std::numeric_limits<size_t>::max()) {
        ThrowInfo(FileReadFailed,
                  "failed to determine R-Tree NULL sidecar size {}: {}",
                  local.Path(),
                  error.message());
    }
    const auto bytes = static_cast<size_t>(observed);
    if (bytes == 0 || bytes % sizeof(size_t) != 0 ||
        bytes / sizeof(size_t) > static_cast<size_t>(total_num_rows)) {
        ThrowInfo(DataFormatBroken,
                  "R-Tree null-offset byte size {} is invalid for {} rows",
                  bytes,
                  total_num_rows);
    }

    auto offsets =
        std::make_shared<std::vector<size_t>>(bytes / sizeof(size_t));
    const auto fd = ::open(local.Path().c_str(), O_RDONLY | O_CLOEXEC);
    if (fd == -1) {
        ThrowInfo(FileOpenFailed,
                  "failed to open R-Tree NULL staging file {}: {}",
                  local.Path(),
                  std::strerror(errno));
    }
    FileDescriptorGuard descriptor(fd);
    storage::ReadAll(descriptor.Get(),
                     offsets->data(),
                     bytes,
                     local.Path(),
                     "R-Tree NULL staging file");
    descriptor.CloseChecked(local.Path(), "R-Tree NULL staging file");
    local.RemoveChecked("R-Tree NULL staging file");
    return offsets;
}

std::shared_ptr<const RTreeIndexState>
LoadState(storage::FileSource& source, const storage::LoadOptions& opts) {
    ValidateGeometryParams(opts.params);
    const auto total_num_rows = ReadRequiredRowCount(opts.params);
    AssertInfo(opts.mmap_dir_path.find('\0') == std::string::npos,
               "R-Tree load options contain an invalid staging path");
    auto entries = ReadPersistedEntries(source);

    auto directory =
        CreateRTreeIndexDirectory(opts.mmap_dir_path, "load");
    auto local_paths =
        source.ReadEntriesToLocalDir(entries.engine_files, directory->Path());
    if (local_paths.size() != entries.engine_files.size()) {
        ThrowInfo(DataFormatBroken,
                  "R-Tree source materialized {} of {} engine entries",
                  local_paths.size(),
                  entries.engine_files.size());
    }
    for (size_t i = 0; i < local_paths.size(); ++i) {
        if (std::filesystem::path(local_paths[i]).filename().string() !=
            entries.engine_files[i]) {
            ThrowInfo(DataFormatBroken,
                      "R-Tree source materialized entry {} as {}",
                      entries.engine_files[i],
                      local_paths[i]);
        }
    }

    auto engine = std::make_shared<RTreeQueryEngine>(FindBasePath(local_paths));
    engine->Load();
    auto null_offsets = ReadNullOffsets(
        source, entries.has_null, total_num_rows, opts.mmap_dir_path);
    return RTreeIndexState::Create(
        std::move(engine), std::move(null_offsets), total_num_rows);
}

}  // namespace

ReaderCaps
RTreeIndexLoader::DeriveCaps(const Config& index_meta) {
    ValidateGeometryParams(index_meta);
    return ReaderCaps{.spatial = true, .exact = false};
}

IIndexReaderBasePtr
RTreeIndexLoader::Open(storage::FileSource& source,
                       const storage::LoadOptions& opts) {
    // Boost's R-tree archive has no mmap view. `enable_mmap` is a preference;
    // preserve the baseline heap fallback and report the resulting heap bytes.
    return std::make_unique<RTreeIndexReader>(LoadState(source, opts));
}

namespace {

const bool kRTreeLoaderRegistered = [] {
    LoaderRegistry::Instance().Register<RTreeIndexLoader>();
    return true;
}();

}  // namespace

}  // namespace milvus::index
