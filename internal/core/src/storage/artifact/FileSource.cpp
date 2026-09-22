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

#include "storage/artifact/FileSource.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <set>
#include <unordered_map>
#include <utility>

#include <sys/stat.h>
#include <unistd.h>

#include "common/EasyAssert.h"
#include "common/OpContext.h"
#include "common/Slice.h"
#include "nlohmann/json.hpp"
#include "pb/common.pb.h"
#include "storage/DataCodec.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/EntryStreamUtils.h"
#include "storage/FileManager.h"
#include "storage/FileWriter.h"
#include "storage/LegacyIndexLoader.h"
#include "folly/coro/WithCancellation.h"
#include "storage/LocalFileIOPool.h"
#include "storage/MemFileManagerImpl.h"
#include "storage/ThreadPools.h"
#include "storage/artifact/DiskEngineFileHandle.h"

namespace milvus::storage {

namespace {

// Named per unit rather than plain BaseName: the source and its sibling
// translation unit both need this helper, and milvus_storage_artifact is a
// unity-build target, where two file-local BaseName definitions would merge
// into one translation unit and redefine each other.
std::string
SourceBaseName(const std::string& path) {
    return std::filesystem::path(path).filename().string();
}

std::optional<std::pair<std::string, int>>
SplitNumericSuffix(const std::string& name) {
    auto pos = name.find_last_of('_');
    if (pos == std::string::npos || pos + 1 == name.size()) {
        return std::nullopt;
    }
    int number = 0;
    for (size_t i = pos + 1; i < name.size(); ++i) {
        if (name[i] < '0' || name[i] > '9') {
            return std::nullopt;
        }
        const auto digit = name[i] - '0';
        if (number > (std::numeric_limits<int>::max() - digit) / 10) {
            ThrowInfo(DataFormatBroken,
                      "artifact slice number is out of range: {}",
                      name);
        }
        number = number * 10 + digit;
    }
    return std::make_pair(name.substr(0, pos), number);
}

milvus::proto::common::LoadPriority
LoadPriority(const LoadOptions& options) {
    if (options.op_ctx != nullptr &&
        options.op_ctx->runtime_load_priority.has_value() &&
        *options.op_ctx->runtime_load_priority != 0) {
        return milvus::proto::common::LoadPriority::LOW;
    }
    return milvus::proto::common::LoadPriority::HIGH;
}

ThreadPoolPriority
PoolPriority(const LoadOptions& options) {
    return LoadPriority(options) == milvus::proto::common::LoadPriority::HIGH
               ? ThreadPoolPriority::HIGH
               : ThreadPoolPriority::LOW;
}

folly::CancellationToken
CancellationToken(const LoadOptions& options) {
    return options.op_ctx == nullptr ? folly::CancellationToken()
                                     : options.op_ctx->cancellation_token;
}

uint64_t
ReadNonNegativeJsonInteger(const nlohmann::json& object,
                           const char* key,
                           std::string_view description) {
    if (!object.is_object() || !object.contains(key)) {
        ThrowInfo(DataFormatBroken,
                  "V1/V2 slice meta {} has no {}",
                  description,
                  key);
    }
    const auto& value = object.at(key);
    if (value.is_number_unsigned()) {
        return value.get<uint64_t>();
    }
    if (value.is_number_integer()) {
        const auto signed_value = value.get<int64_t>();
        if (signed_value >= 0) {
            return static_cast<uint64_t>(signed_value);
        }
    }
    ThrowInfo(DataFormatBroken,
              "V1/V2 slice meta {} has invalid non-negative integer {}",
              description,
              key);
}

bool
HasExactPathPrefix(std::string_view path, std::string_view prefix) {
    return path == prefix || (path.size() > prefix.size() &&
                              path.compare(0, prefix.size(), prefix) == 0 &&
                              path[prefix.size()] == '/');
}

void
CreateParentDirectories(const std::string& path) {
    auto parent = std::filesystem::path(path).parent_path();
    if (parent.empty()) {
        return;
    }
    std::error_code error;
    std::filesystem::create_directories(parent, error);
    if (error) {
        ThrowInfo(FileCreateFailed,
                  "failed to create local artifact directory {}: {}",
                  parent.string(),
                  error.message());
    }
}

std::string
CreateTemporaryPath(const std::string& target, std::string_view stem) {
    CreateParentDirectories(target);
    auto parent = std::filesystem::path(target).parent_path();
    if (parent.empty()) {
        parent = ".";
    }
    auto pattern = (parent / (std::string(stem) + "-XXXXXX")).string();
    std::vector<char> mutable_pattern(pattern.begin(), pattern.end());
    mutable_pattern.push_back('\0');
    auto fd = ::mkstemp(mutable_pattern.data());
    if (fd < 0) {
        ThrowInfo(FileCreateFailed,
                  "failed to create staged artifact file beside {}: {}",
                  target,
                  std::strerror(errno));
    }
    if (::close(fd) != 0) {
        auto error = errno;
        ::unlink(mutable_pattern.data());
        ThrowInfo(FileCreateFailed,
                  "failed to close staged artifact file beside {}: {}",
                  target,
                  std::strerror(error));
    }
    return mutable_pattern.data();
}

class StagedLocalFile {
 public:
    explicit StagedLocalFile(std::string target)
        : target_(std::move(target)),
          staging_(CreateTemporaryPath(target_, ".milvus-artifact")) {
    }

    StagedLocalFile(const StagedLocalFile&) = delete;
    StagedLocalFile&
    operator=(const StagedLocalFile&) = delete;

    StagedLocalFile(StagedLocalFile&& other) noexcept
        : target_(std::move(other.target_)),
          staging_(std::move(other.staging_)),
          owns_staging_(std::exchange(other.owns_staging_, false)) {
    }

    ~StagedLocalFile() {
        if (owns_staging_) {
            ::unlink(staging_.c_str());
        }
    }

    const std::string&
    Target() const {
        return target_;
    }

    const std::string&
    Staging() const {
        return staging_;
    }

    void
    Commit() {
        if (::rename(staging_.c_str(), target_.c_str()) != 0) {
            ThrowInfo(FileWriteFailed,
                      "failed to publish local artifact file {}: {}",
                      target_,
                      std::strerror(errno));
        }
        owns_staging_ = false;
    }

    void
    Disarm() {
        owns_staging_ = false;
    }

 private:
    std::string target_;
    std::string staging_;
    bool owns_staging_{true};
};

bool
PathExists(const std::string& path) {
    struct stat status;
    if (::lstat(path.c_str(), &status) == 0) {
        return true;
    }
    if (errno == ENOENT) {
        return false;
    }
    ThrowInfo(FileReadFailed,
              "failed to inspect local artifact destination {}: {}",
              path,
              std::strerror(errno));
}

std::string
CreateVacantBackupPath(const std::string& target) {
    auto backup = CreateTemporaryPath(target, ".milvus-artifact-backup");
    if (::unlink(backup.c_str()) != 0) {
        ThrowInfo(FileWriteFailed,
                  "failed to prepare artifact backup beside {}: {}",
                  target,
                  std::strerror(errno));
    }
    return backup;
}

// Publish a directory materialization as one operation. Destinations are
// caller-owned and must not have concurrent writers. Existing files are moved
// aside until every staged file is published; a failure restores them.
void
CommitAll(std::vector<StagedLocalFile>& files) {
    struct State {
        std::string backup;
        bool has_backup{false};
        bool published{false};
    };

    std::vector<State> states(files.size());
    try {
        for (size_t i = 0; i < files.size(); ++i) {
            auto& file = files[i];
            auto& state = states[i];
            if (PathExists(file.Target())) {
                state.backup = CreateVacantBackupPath(file.Target());
                if (::rename(file.Target().c_str(), state.backup.c_str()) !=
                    0) {
                    ThrowInfo(FileWriteFailed,
                              "failed to back up local artifact destination "
                              "{}: {}",
                              file.Target(),
                              std::strerror(errno));
                }
                state.has_backup = true;
            }
            if (::rename(file.Staging().c_str(), file.Target().c_str()) != 0) {
                ThrowInfo(FileWriteFailed,
                          "failed to publish local artifact file {}: {}",
                          file.Target(),
                          std::strerror(errno));
            }
            file.Disarm();
            state.published = true;
        }
    } catch (...) {
        std::string rollback_error;
        for (size_t i = files.size(); i > 0; --i) {
            auto& file = files[i - 1];
            auto& state = states[i - 1];
            if (state.has_backup) {
                if (::rename(state.backup.c_str(), file.Target().c_str()) !=
                        0 &&
                    rollback_error.empty()) {
                    rollback_error = "failed to restore " + file.Target() +
                                     " from " + state.backup + ": " +
                                     std::strerror(errno);
                }
            } else if (state.published &&
                       ::unlink(file.Target().c_str()) != 0 &&
                       errno != ENOENT && rollback_error.empty()) {
                rollback_error = "failed to roll back " + file.Target() + ": " +
                                 std::strerror(errno);
            }
        }
        if (!rollback_error.empty()) {
            ThrowInfo(FileWriteFailed, rollback_error);
        }
        throw;
    }

    // The complete target set is already published. Backup cleanup is
    // best-effort so a cleanup failure cannot turn a complete publication into
    // a reported failure with no safe rollback path.
    for (const auto& state : states) {
        if (state.has_backup) {
            ::unlink(state.backup.c_str());
        }
    }
}

void
CloseOutput(std::ofstream& output, const std::string& path) {
    output.flush();
    if (!output.good()) {
        ThrowInfo(FileWriteFailed,
                  "failed to write staged local artifact file {}",
                  path);
    }
    output.close();
    if (output.fail()) {
        ThrowInfo(FileWriteFailed,
                  "failed to close staged local artifact file {}",
                  path);
    }
}

int64_t
LocalFileSize(const std::string& path) {
    std::error_code error;
    auto size = std::filesystem::file_size(path, error);
    if (error) {
        ThrowInfo(FileReadFailed,
                  "failed to get local artifact file size {}: {}",
                  path,
                  error.message());
    }
    if (size > static_cast<uintmax_t>(std::numeric_limits<int64_t>::max())) {
        ThrowInfo(FileReadFailed,
                  "local artifact file is too large to account: {}",
                  path);
    }
    return static_cast<int64_t>(size);
}

void
AppendMemory(const uint8_t* data,
             size_t size,
             std::ofstream& output,
             const folly::CancellationToken& cancellation_token) {
    constexpr size_t kCopyBufferSize = 1024 * 1024;
    AssertInfo(size == 0 || data != nullptr,
               "null data for non-empty artifact entry");
    size_t offset = 0;
    while (offset < size) {
        ThrowIfCancelled(cancellation_token, "artifact file materialization");
        const auto chunk = std::min(kCopyBufferSize, size - offset);
        output.write(reinterpret_cast<const char*>(data + offset), chunk);
        if (!output.good()) {
            ThrowInfo(FileWriteFailed,
                      "failed to append staged artifact entry");
        }
        offset += chunk;
    }
    ThrowIfCancelled(cancellation_token, "artifact file materialization");
}

void
WriteBytes(const std::string& path,
           const uint8_t* data,
           size_t size,
           const folly::CancellationToken& cancellation_token = {}) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    if (!output.good()) {
        ThrowInfo(FileOpenFailed,
                  "failed to open staged local artifact file {}",
                  path);
    }
    AppendMemory(data, size, output, cancellation_token);
    CloseOutput(output, path);
}

void
AppendFile(const std::string& source,
           std::ofstream& output,
           const folly::CancellationToken& cancellation_token = {}) {
    std::ifstream input(source, std::ios::binary);
    if (!input.good()) {
        ThrowInfo(FileOpenFailed,
                  "failed to open staged local artifact source {}",
                  source);
    }
    std::vector<char> buffer(1024 * 1024);
    while (input) {
        ThrowIfCancelled(cancellation_token, "artifact file materialization");
        input.read(buffer.data(), buffer.size());
        auto read = input.gcount();
        if (read > 0) {
            output.write(buffer.data(), read);
        }
    }
    if (!input.eof()) {
        ThrowInfo(FileReadFailed,
                  "failed to read staged local artifact source {}",
                  source);
    }
    if (!output.good()) {
        ThrowInfo(FileWriteFailed,
                  "failed to append staged local artifact source {}",
                  source);
    }
    ThrowIfCancelled(cancellation_token, "artifact file materialization");
}

}  // namespace

std::shared_ptr<DiskEngineFileHandle>
FileSource::OpenDiskEngineFiles(DiskEngineFileMode,
                                const std::vector<std::string>&) const {
    ThrowInfo(Unsupported,
              "artifact source does not support disk engine files");
}

folly::coro::Task<int64_t>
FileSource::EntrySizeAsync(std::string_view name, bool use_async) {
    if (!use_async)
        co_return EntrySize(name);
    int64_t bytes = 0;
    co_await RunLocalFileIOAsync([&] { bytes = EntrySize(name); },
                                 proto::common::LoadPriority::HIGH);
    co_return bytes;
}

folly::coro::Task<std::vector<uint8_t>>
FileSource::ReadEntryAsync(std::string_view name, bool use_async) {
    if (!use_async)
        co_return ReadEntry(name);
    std::vector<uint8_t> bytes;
    co_await RunLocalFileIOAsync([&] { bytes = ReadEntry(name); },
                                 proto::common::LoadPriority::HIGH);
    co_return bytes;
}

folly::coro::Task<void>
FileSource::ReadEntryToLocalFileAsync(std::string_view name,
                                      const std::string& path,
                                      bool use_async) {
    if (!use_async) {
        ReadEntryToLocalFile(name, path);
        co_return;
    }
    co_await RunLocalFileIOAsync([&] { ReadEntryToLocalFile(name, path); },
                                 proto::common::LoadPriority::HIGH);
}

folly::coro::Task<void>
FileSource::ReadEntriesToLocalFileAsync(const std::vector<std::string>& names,
                                        const std::string& path,
                                        bool use_async) {
    if (!use_async) {
        ReadEntriesToLocalFile(names, path);
        co_return;
    }
    co_await RunLocalFileIOAsync([&] { ReadEntriesToLocalFile(names, path); },
                                 proto::common::LoadPriority::HIGH);
}

folly::coro::Task<std::vector<std::string>>
FileSource::ReadEntriesToLocalDirAsync(const std::vector<std::string>& names,
                                       const std::string& directory,
                                       bool use_async) {
    if (!use_async) {
        co_return ReadEntriesToLocalDir(names, directory);
    }
    std::vector<std::string> paths;
    co_await RunLocalFileIOAsync(
        [&] { paths = ReadEntriesToLocalDir(names, directory); },
        proto::common::LoadPriority::HIGH);
    co_return paths;
}

class V1RemoteSource::Impl {
 public:
    Impl(const FileManagerContext& context,
         std::vector<std::string> paths,
         LoadOptions options,
         ArtifactStoragePath storage_path,
         V1SourceLayout layout,
         bool defer_directory = false)
        : context(context),
          remote_paths(std::move(paths)),
          load_priority(LoadPriority(options)),
          cancellation_token(CancellationToken(options)),
          storage_path(storage_path),
          layout(layout),
          memory_manager(std::make_shared<MemFileManagerImpl>(context)),
          disk_manager(std::make_shared<DiskFileManagerImpl>(context)) {
        NormalizePaths();
        if (!defer_directory) {
            BuildDirectory();
        }
    }

    void
    NormalizePaths() {
        auto prefix = storage_path == ArtifactStoragePath::TextLog
                          ? memory_manager->GetRemoteTextLogPrefix()
                          : memory_manager->GetRemoteIndexObjectPrefix();
        normalized_paths.reserve(remote_paths.size());
        for (const auto& path : remote_paths) {
            if (storage_path == ArtifactStoragePath::TextLog) {
                normalized_paths.push_back(HasExactPathPrefix(path, prefix)
                                               ? path
                                               : prefix + "/" + path);
            } else {
                // Index ArtifactStats carries full remote paths. Retain the
                // basename-only compatibility accepted by the old load path.
                normalized_paths.push_back(path.find('/') == std::string::npos
                                               ? prefix + "/" + path
                                               : path);
            }
        }
    }

    void
    BuildDirectory(
        std::optional<std::vector<uint8_t>> downloaded_meta = std::nullopt) {
        std::map<std::string, std::string> by_basename;
        for (size_t i = 0; i < normalized_paths.size(); ++i) {
            auto name = SourceBaseName(normalized_paths[i]);
            auto [_, inserted] = by_basename.emplace(name, normalized_paths[i]);
            if (!inserted) {
                ThrowInfo(DataFormatBroken,
                          "duplicate remote artifact basename {}",
                          name);
            }
        }

        std::set<std::string> consumed;
        auto meta = by_basename.find(INDEX_FILE_SLICE_META);
        if (meta != by_basename.end()) {
            std::vector<uint8_t> encoded;
            if (downloaded_meta) {
                encoded = std::move(*downloaded_meta);
            } else {
                CheckCancelled("V1/V2 slice-meta download");
                auto codecs = memory_manager->LoadIndexToMemory({meta->second},
                                                                load_priority);
                CheckCancelled("V1/V2 slice-meta download");
                auto codec = codecs.find(INDEX_FILE_SLICE_META);
                if (codec == codecs.end() || codec->second == nullptr) {
                    ThrowInfo(FileReadFailed,
                              "failed to load V1/V2 slice meta");
                }
                const auto bytes = codec->second->PayloadSize();
                const auto* payload = codec->second->PayloadData();
                if (bytes != 0 && payload == nullptr) {
                    ThrowInfo(DataFormatBroken,
                              "V1/V2 slice meta has null payload");
                }
                if (bytes != 0) {
                    encoded.assign(payload, payload + bytes);
                }
            }
            auto size = encoded.size();
            const auto* data = encoded.data();
            if (size == 0) {
                ThrowInfo(DataFormatBroken, "V1/V2 slice meta is empty");
            }
            while (size > 0 && data[size - 1] == 0) {
                --size;
            }
            nlohmann::json slice_meta;
            try {
                slice_meta = nlohmann::json::parse(data, data + size);
            } catch (const nlohmann::json::parse_error& error) {
                ThrowInfo(DataFormatBroken,
                          "invalid V1/V2 slice meta JSON: {}",
                          error.what());
            }
            if (!slice_meta.is_object() || !slice_meta.contains(META) ||
                !slice_meta.at(META).is_array()) {
                ThrowInfo(DataFormatBroken,
                          "V1/V2 slice meta must contain a meta array");
            }
            consumed.insert(INDEX_FILE_SLICE_META);
            for (const auto& item : slice_meta.at(META)) {
                if (!item.is_object() || !item.contains(NAME) ||
                    !item.at(NAME).is_string()) {
                    ThrowInfo(DataFormatBroken,
                              "V1/V2 slice meta item has invalid name");
                }
                auto logical_name = item.at(NAME).get<std::string>();
                if (logical_name.empty()) {
                    ThrowInfo(DataFormatBroken,
                              "V1/V2 slice meta item has an empty name");
                }
                const auto slice_count =
                    ReadNonNegativeJsonInteger(item, SLICE_NUM, logical_name);
                const auto total_length =
                    ReadNonNegativeJsonInteger(item, TOTAL_LEN, logical_name);
                if (slice_count > by_basename.size()) {
                    ThrowInfo(DataFormatBroken,
                              "V1/V2 slice count {} for {} exceeds {} physical "
                              "files",
                              slice_count,
                              logical_name,
                              by_basename.size());
                }
                if (total_length > static_cast<uint64_t>(
                                       std::numeric_limits<int64_t>::max())) {
                    ThrowInfo(DataFormatBroken,
                              "V1/V2 total length {} for {} is out of range",
                              total_length,
                              logical_name);
                }
                if (!slice_meta_entries.insert(logical_name).second ||
                    !entry_sizes
                         .emplace(logical_name,
                                  static_cast<int64_t>(total_length))
                         .second) {
                    ThrowInfo(DataFormatBroken,
                              "duplicate V1/V2 slice meta entry {}",
                              logical_name);
                }
                auto [paths_it, inserted] = logical_entries.emplace(
                    logical_name, std::vector<std::string>{});
                if (!inserted) {
                    ThrowInfo(DataFormatBroken,
                              "duplicate V1/V2 logical entry {}",
                              logical_name);
                }
                auto& paths = paths_it->second;
                paths.reserve(static_cast<size_t>(slice_count));
                for (size_t i = 0; i < slice_count; ++i) {
                    auto physical = milvus::GenSlicedFileName(logical_name, i);
                    auto it = by_basename.find(physical);
                    if (it == by_basename.end()) {
                        ThrowInfo(DataFormatBroken,
                                  "missing artifact slice {}",
                                  physical);
                    }
                    paths.push_back(it->second);
                    if (!consumed.insert(physical).second) {
                        ThrowInfo(DataFormatBroken,
                                  "artifact slice {} is referenced more than "
                                  "once",
                                  physical);
                    }
                }
            }
        }

        std::map<std::string, std::vector<std::pair<int, std::string>>> grouped;
        for (const auto& [name, path] : by_basename) {
            if (consumed.count(name) != 0) {
                continue;
            }
            auto suffix = layout == V1SourceLayout::DiskFiles
                              ? SplitNumericSuffix(name)
                              : std::nullopt;
            if (suffix.has_value()) {
                grouped[suffix->first].emplace_back(suffix->second, path);
            } else {
                const auto inserted =
                    logical_entries
                        .emplace(name, std::vector<std::string>{path})
                        .second;
                if (!inserted) {
                    ThrowInfo(DataFormatBroken,
                              "physical artifact entry {} conflicts with "
                              "slice metadata",
                              name);
                }
            }
        }
        for (auto& [name, slices] : grouped) {
            std::sort(slices.begin(), slices.end());
            std::vector<std::string> paths;
            paths.reserve(slices.size());
            for (auto& [_, path] : slices) {
                paths.push_back(std::move(path));
            }
            if (!logical_entries.emplace(name, std::move(paths)).second) {
                ThrowInfo(DataFormatBroken,
                          "physical artifact entry {} conflicts with slice "
                          "metadata",
                          name);
            }
        }
    }

    const std::vector<std::string>&
    Paths(std::string_view name) const {
        auto it = logical_entries.find(std::string(name));
        if (it == logical_entries.end()) {
            // The persisted entry set does not contain an entry the loader
            // requires: the artifact is incomplete, not a Milvus bug (same
            // bucket as a lost index slice).
            ThrowInfo(DataFormatBroken, "artifact entry not found: {}", name);
        }
        return it->second;
    }

    folly::coro::Task<std::vector<LegacyIndexFile>>
    InspectSourcesAsync(const std::vector<std::string>& paths) {
        std::vector<LegacyIndexFile> files;
        files.reserve(paths.size());
        for (const auto& path : paths) {
            CheckCancelled("V1/V2 inspect");
            auto found = file_infos.find(path);
            if (found == file_infos.end()) {
                auto input = co_await OpenLegacyIndexInputAsync(
                    context.chunkManagerPtr, context.fs, path, load_priority);
                auto info = co_await InspectLegacyIndexFileAsync(
                    *input, load_priority, cancellation_token);
                found = file_infos.emplace(path, info).first;
            }
            files.push_back({path, found->second});
        }
        co_return files;
    }

    static size_t
    PayloadBytes(const std::vector<LegacyIndexFile>& files) {
        size_t total = 0;
        for (const auto& file : files) {
            if (file.info.payload_bytes >
                static_cast<size_t>(std::numeric_limits<int64_t>::max()) -
                    total) {
                ThrowInfo(DataFormatBroken,
                          "V1/V2 assembled payload exceeds int64 size");
            }
            total += file.info.payload_bytes;
        }
        return total;
    }

    folly::coro::Task<std::vector<uint8_t>>
    ReadPhysicalAsync(const std::vector<std::string>& paths,
                      std::string_view logical_name = {}) {
        const auto files = co_await InspectSourcesAsync(paths);
        const auto total = PayloadBytes(files);
        if (!logical_name.empty()) {
            RecordOrValidateEntrySize(logical_name,
                                      static_cast<int64_t>(total));
        }
        std::vector<uint8_t> bytes(total);
        const EntryTarget target =
            MemoryEntryTarget{nullptr, bytes.data(), bytes.size()};
        co_await StreamLegacyIndexFilesAsync(files,
                                             context.chunkManagerPtr,
                                             context.fs,
                                             target,
                                             load_priority,
                                             cancellation_token);
        co_return bytes;
    }

    folly::coro::Task<void>
    OpenDirectoryAsync() {
        std::optional<std::vector<uint8_t>> metadata;
        for (const auto& path : normalized_paths) {
            if (SourceBaseName(path) == INDEX_FILE_SLICE_META) {
                const std::vector<std::string> paths{path};
                metadata = co_await ReadPhysicalAsync(paths);
                break;
            }
        }
        BuildDirectory(std::move(metadata));
    }

    folly::coro::Task<void>
    WritePhysicalAsync(const std::vector<LegacyIndexFile>& files,
                       const std::string& path) {
        const auto bytes = PayloadBytes(files);
        // Logical entries must remain byte-for-byte concatenated for knowhere.
        // Use buffered positioned writes when an internal boundary is unaligned.
        std::optional<FileWriter::WriteMode> mode;
        size_t offset = 0;
        for (const auto& file : files) {
            if ((offset & FileWriter::ALIGNMENT_MASK) != 0) {
                mode = FileWriter::WriteMode::BUFFERED;
                break;
            }
            offset += file.info.payload_bytes;
        }
        auto file = std::make_shared<IndexFileTarget>(path, bytes, true, mode);
        const EntryTarget target = FileEntryTarget{file, 0, bytes};
        std::exception_ptr failure;
        try {
            co_await ReadLegacyIndexFilesAsync(files,
                                               context.chunkManagerPtr,
                                               context.fs,
                                               target,
                                               load_priority,
                                               cancellation_token);
            file->Commit();
        } catch (...) {
            failure = std::current_exception();
        }
        co_await RunLocalFileIOAsync([&] { file->Cleanup(); }, load_priority);
        if (failure) {
            std::rethrow_exception(failure);
        }
    }

    // All destinations are staged before publishing any of them. Both the
    // existing rollback helper and cleanup run on the local file executor.
    folly::coro::Task<void>
    MaterializeAsync(const std::vector<std::vector<std::string>>& entries,
                     const std::vector<std::string>& paths) {
        const auto token = folly::cancellation_token_merge(
            cancellation_token,
            co_await folly::coro::co_current_cancellation_token);
        ThrowIfCancelled(token, "V1/V2 artifact materialization");
        std::vector<StagedLocalFile> staged;
        staged.reserve(paths.size());
        std::exception_ptr failure;
        try {
            co_await RunLocalFileIOAsync(
                [&] {
                    for (const auto& path : paths) {
                        staged.emplace_back(path);
                    }
                },
                load_priority);
            for (size_t i = 0; i < paths.size(); ++i) {
                std::vector<LegacyIndexFile> files;
                for (const auto& name : entries[i]) {
                    auto sources = co_await InspectSourcesAsync(Paths(name));
                    RecordOrValidateEntrySize(
                        name, static_cast<int64_t>(PayloadBytes(sources)));
                    files.insert(files.end(),
                                 std::make_move_iterator(sources.begin()),
                                 std::make_move_iterator(sources.end()));
                }
                co_await WritePhysicalAsync(files, staged[i].Staging());
            }
            ThrowIfCancelled(token, "V1/V2 artifact publication");
            co_await RunLocalFileIOAsync(
                [&] {
                    ThrowIfCancelled(token, "V1/V2 artifact publication");
                    CommitAll(staged);
                },
                load_priority);
        } catch (...) {
            failure = std::current_exception();
        }
        co_await RunLocalFileIOAsync([&] { staged.clear(); }, load_priority);
        if (failure) {
            std::rethrow_exception(failure);
        }
    }

    // Keep the old synchronous download concurrency, but retain only one
    // bounded batch instead of accumulating codecs for the whole entry.
    template <typename Consume>
    size_t
    ReadPayload(std::string_view name, Consume consume) {
        const auto& paths = Paths(name);
        const auto batch_limit =
            std::max<size_t>(1,
                             DEFAULT_FIELD_MAX_MEMORY_LIMIT /
                                 std::max<int64_t>(1, FILE_SLICE_SIZE.load()));
        size_t total = 0;
        for (size_t begin = 0; begin < paths.size();) {
            const auto count = std::min(batch_limit, paths.size() - begin);
            const std::vector<std::string> batch(paths.begin() + begin,
                                                 paths.begin() + begin + count);
            CheckCancelled("V1/V2 artifact download");
            auto codecs =
                memory_manager->LoadIndexToMemory(batch, load_priority);
            CheckCancelled("V1/V2 artifact download");
            for (const auto& path : batch) {
                const auto found = codecs.find(SourceBaseName(path));
                if (found == codecs.end() || found->second == nullptr) {
                    ThrowInfo(FileReadFailed,
                              "loaded artifact slice {} is missing",
                              path);
                }
                const auto bytes = found->second->PayloadSize();
                const auto* data = found->second->PayloadData();
                if (bytes > static_cast<size_t>(
                                std::numeric_limits<int64_t>::max()) -
                                total ||
                    (bytes != 0 && data == nullptr)) {
                    ThrowInfo(DataFormatBroken,
                              "invalid artifact slice payload: {}",
                              path);
                }
                consume(data, bytes, total);
                total += bytes;
                codecs.erase(found);
            }
            begin += count;
        }
        RecordOrValidateEntrySize(name, static_cast<int64_t>(total));
        return total;
    }

    // Read only envelopes with synchronous ChunkManager range I/O; cache sizes,
    // never payloads, and introduce no dependency on an executor.
    int64_t
    Measure(std::string_view name) {
        auto known = entry_sizes.find(std::string(name));
        if (known != entry_sizes.end())
            return known->second;
        std::vector<LegacyIndexFile> files;
        files.reserve(Paths(name).size());
        for (const auto& path : Paths(name)) {
            CheckCancelled("V1/V2 inspect");
            auto found = file_infos.find(path);
            if (found == file_infos.end()) {
                auto info = InspectLegacyIndexFile(context.chunkManagerPtr,
                                                   path,
                                                   load_priority,
                                                   cancellation_token);
                found = file_infos.emplace(path, info).first;
            }
            files.push_back({path, found->second});
        }
        const auto bytes = static_cast<int64_t>(PayloadBytes(files));
        RecordOrValidateEntrySize(name, bytes);
        return bytes;
    }

    // Allocate one final entry, then release each decoded slice after copying.
    std::vector<uint8_t>
    Decode(std::string_view name) {
        std::vector<uint8_t> result(static_cast<size_t>(Measure(name)));
        ReadPayload(
            name, [&](const uint8_t* data, size_t bytes, size_t offset) {
                if (offset > result.size() || bytes > result.size() - offset) {
                    ThrowInfo(DataFormatBroken,
                              "artifact entry {} exceeds declared size",
                              name);
                }
                constexpr size_t kCopyBufferSize = 1024 * 1024;
                for (size_t copied = 0; copied < bytes;) {
                    CheckCancelled("V1/V2 artifact decode");
                    const auto chunk =
                        std::min(kCopyBufferSize, bytes - copied);
                    std::memcpy(
                        result.data() + offset + copied, data + copied, chunk);
                    copied += chunk;
                }
            });
        CheckCancelled("V1/V2 artifact decode");
        return result;
    }

    // Stream decoded physical slices into an already-open staged output.
    void
    Append(std::string_view name, std::ofstream& output) {
        ReadPayload(name, [&](const uint8_t* data, size_t bytes, size_t) {
            AppendMemory(data, bytes, output, cancellation_token);
        });
    }

    bool
    CanStreamToDisk(std::string_view name) const {
        if (layout != V1SourceLayout::DiskFiles &&
            slice_meta_entries.count(std::string(name)) == 0) {
            return false;
        }
        const auto& paths = Paths(name);
        return !paths.empty() &&
               std::all_of(paths.begin(), paths.end(), [](const auto& path) {
                   return SplitNumericSuffix(SourceBaseName(path)).has_value();
               });
    }

    void
    CheckCancelled(const std::string& operation) const {
        ThrowIfCancelled(cancellation_token, operation);
    }

    void
    RecordOrValidateEntrySize(std::string_view name,
                              int64_t observed,
                              std::string invalid_path = {}) const {
        const auto key = std::string(name);
        auto known = entry_sizes.find(key);
        if (known != entry_sizes.end() && known->second != observed) {
            staged_files.erase(key);
            if (!invalid_path.empty()) {
                std::error_code ignored;
                std::filesystem::remove(invalid_path, ignored);
            }
            ThrowInfo(DataFormatBroken,
                      "artifact entry size mismatch for {}: expected {}, got "
                      "{}",
                      name,
                      known->second,
                      observed);
        }
        entry_sizes.emplace(key, observed);
    }

    std::string
    CacheToDisk(std::string_view name) const {
        auto cached = staged_files.find(std::string(name));
        if (cached != staged_files.end()) {
            std::error_code error;
            if (std::filesystem::exists(cached->second, error) && !error) {
                RecordOrValidateEntrySize(
                    name, LocalFileSize(cached->second), cached->second);
                return cached->second;
            }
            if (error) {
                ThrowInfo(FileReadFailed,
                          "failed to inspect cached artifact entry {}: {}",
                          cached->second,
                          error.message());
            }
            staged_files.erase(cached);
        }

        const auto& paths = Paths(name);
        CheckCancelled("V1/V2 artifact disk cache");
        if (storage_path == ArtifactStoragePath::TextLog) {
            disk_manager->CacheTextLogToDisk(paths, load_priority);
        } else {
            disk_manager->CacheIndexToDisk(paths, load_priority);
        }
        CheckCancelled("V1/V2 artifact disk cache");
        for (const auto& local : disk_manager->GetLocalFilePaths()) {
            if (SourceBaseName(local) == name) {
                RecordOrValidateEntrySize(name, LocalFileSize(local), local);
                staged_files.emplace(std::string(name), local);
                return local;
            }
        }
        ThrowInfo(FileReadFailed, "cached artifact entry not found: {}", name);
    }

    FileManagerContext context;
    std::vector<std::string> remote_paths;
    std::vector<std::string> normalized_paths;
    milvus::proto::common::LoadPriority load_priority;
    folly::CancellationToken cancellation_token;
    ArtifactStoragePath storage_path;
    V1SourceLayout layout;
    std::shared_ptr<MemFileManagerImpl> memory_manager;
    std::shared_ptr<DiskFileManagerImpl> disk_manager;
    std::map<std::string, std::vector<std::string>> logical_entries;
    std::unordered_map<std::string, LegacyIndexFileInfo> file_infos;
    std::set<std::string> slice_meta_entries;
    mutable std::unordered_map<std::string, int64_t> entry_sizes;
    mutable std::unordered_map<std::string, std::string> staged_files;
};

void
V1RemoteSource::SetLoadContext(proto::common::LoadPriority priority,
                               folly::CancellationToken token) {
    impl_->load_priority = priority;
    impl_->cancellation_token = std::move(token);
}

V1RemoteSource::V1RemoteSource(const FileManagerContext& context,
                               std::vector<std::string> remote_paths,
                               LoadOptions options,
                               ArtifactStoragePath storage_path,
                               V1SourceLayout layout)
    : impl_(std::make_unique<Impl>(context,
                                   std::move(remote_paths),
                                   std::move(options),
                                   storage_path,
                                   layout)) {
}

V1RemoteSource::~V1RemoteSource() = default;

V1RemoteSource::V1RemoteSource(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {
}

folly::coro::Task<std::unique_ptr<V1RemoteSource>>
V1RemoteSource::OpenAsync(const FileManagerContext& context,
                          std::vector<std::string> paths,
                          LoadOptions options,
                          ArtifactStoragePath storage_path,
                          V1SourceLayout layout) {
    auto impl = std::make_unique<Impl>(context,
                                       std::move(paths),
                                       std::move(options),
                                       storage_path,
                                       layout,
                                       true);
    impl->cancellation_token = folly::cancellation_token_merge(
        impl->cancellation_token,
        co_await folly::coro::co_current_cancellation_token);
    impl->CheckCancelled("V1/V2 open source");
    co_await impl->OpenDirectoryAsync();
    impl->CheckCancelled("V1/V2 open source");
    co_return std::unique_ptr<V1RemoteSource>(
        new V1RemoteSource(std::move(impl)));
}

folly::coro::Task<int64_t>
V1RemoteSource::EntrySizeAsync(std::string_view name, bool use_async) {
    if (!use_async) {
        co_return EntrySize(name);
    }
    auto files = co_await impl_->InspectSourcesAsync(impl_->Paths(name));
    auto bytes = static_cast<int64_t>(Impl::PayloadBytes(files));
    impl_->RecordOrValidateEntrySize(name, bytes);
    co_return bytes;
}

folly::coro::Task<std::vector<uint8_t>>
V1RemoteSource::ReadEntryAsync(std::string_view name, bool use_async) {
    if (!use_async) {
        co_return ReadEntry(name);
    }
    co_return co_await impl_->ReadPhysicalAsync(impl_->Paths(name), name);
}

folly::coro::Task<void>
V1RemoteSource::ReadEntryToLocalFileAsync(std::string_view name,
                                          const std::string& path,
                                          bool use_async) {
    if (!use_async) {
        ReadEntryToLocalFile(name, path);
        co_return;
    }
    const std::vector<std::string> names{std::string(name)};
    co_await ReadEntriesToLocalFileAsync(names, path);
}

folly::coro::Task<void>
V1RemoteSource::ReadEntriesToLocalFileAsync(
    const std::vector<std::string>& names,
    const std::string& path,
    bool use_async) {
    if (!use_async) {
        ReadEntriesToLocalFile(names, path);
        co_return;
    }
    const std::vector<std::vector<std::string>> entries{names};
    const std::vector<std::string> paths{path};
    co_await impl_->MaterializeAsync(entries, paths);
}

folly::coro::Task<std::vector<std::string>>
V1RemoteSource::ReadEntriesToLocalDirAsync(
    const std::vector<std::string>& names,
    const std::string& directory,
    bool use_async) {
    if (!use_async) {
        co_return ReadEntriesToLocalDir(names, directory);
    }
    std::set<std::string> targets;
    std::vector<std::string> paths;
    std::vector<std::vector<std::string>> entries;
    paths.reserve(names.size());
    entries.reserve(names.size());
    for (const auto& name : names) {
        auto path =
            (std::filesystem::path(directory) / SourceBaseName(name)).string();
        AssertInfo(targets.insert(path).second,
                   "artifact entries collide at local path {}",
                   path);
        paths.push_back(std::move(path));
        entries.push_back({name});
    }
    co_await impl_->MaterializeAsync(entries, paths);
    co_return paths;
}

folly::coro::Task<V1RemoteSource::LoadBytes>
V1RemoteSource::InspectLoadBytesAsync(const std::vector<std::string>& names) {
    LoadBytes result;
    for (const auto& name : names) {
        const auto files =
            co_await impl_->InspectSourcesAsync(impl_->Paths(name));
        for (const auto& file : files) {
            result.payload = milvus::SaturatingAdd(
                result.payload, uint64_t{file.info.payload_bytes});
            result.transient = milvus::SaturatingAdd(
                result.transient, uint64_t{file.info.TotalTransientBytes()});
        }
    }
    // Directory maps and slice names survive across payload reads. Bound the
    // parsed representation from its encoded metadata and owned path strings.
    for (const auto& path : impl_->normalized_paths) {
        result.directory = milvus::SaturatingAdd(
            result.directory,
            milvus::SaturatingMultiply(
                uint64_t{path.size() + sizeof(std::string)}, uint64_t{8}));
        if (SourceBaseName(path) == INDEX_FILE_SLICE_META) {
            const auto found = impl_->file_infos.find(path);
            if (found != impl_->file_infos.end()) {
                result.directory = milvus::SaturatingAdd(
                    result.directory,
                    milvus::SaturatingMultiply(
                        uint64_t{found->second.payload_bytes}, uint64_t{32}));
            }
        }
    }
    co_return result;
}

Generation
V1RemoteSource::Gen() const {
    return Generation::V1V2;
}

std::vector<std::string>
V1RemoteSource::EntryNames() const {
    std::vector<std::string> names;
    names.reserve(impl_->logical_entries.size());
    for (const auto& [name, _] : impl_->logical_entries) {
        names.push_back(name);
    }
    return names;
}

bool
V1RemoteSource::HasEntry(std::string_view name) const {
    return impl_->logical_entries.count(std::string(name)) != 0;
}

int64_t
V1RemoteSource::EntrySize(std::string_view name) const {
    impl_->CheckCancelled("V1/V2 artifact size lookup");
    auto it = impl_->entry_sizes.find(std::string(name));
    if (it != impl_->entry_sizes.end()) {
        return it->second;
    }
    return impl_->Measure(name);
}

std::vector<uint8_t>
V1RemoteSource::ReadEntry(std::string_view name) {
    impl_->CheckCancelled("V1/V2 artifact read");
    return impl_->Decode(name);
}

void
V1RemoteSource::ReadEntryToLocalFile(std::string_view name,
                                     const std::string& local_path) {
    StagedLocalFile staged(local_path);
    if (impl_->CanStreamToDisk(name)) {
        auto cached = impl_->CacheToDisk(name);
        std::ofstream output(staged.Staging(),
                             std::ios::binary | std::ios::trunc);
        if (!output.good()) {
            ThrowInfo(FileOpenFailed,
                      "failed to open staged local artifact file {}",
                      staged.Staging());
        }
        AppendFile(cached, output, impl_->cancellation_token);
        CloseOutput(output, staged.Staging());
        impl_->CheckCancelled("V1/V2 artifact file publication");
        staged.Commit();
        return;
    }
    std::ofstream output(staged.Staging(), std::ios::binary | std::ios::trunc);
    if (!output.good()) {
        ThrowInfo(FileOpenFailed,
                  "failed to open staged local artifact file {}",
                  staged.Staging());
    }
    impl_->Append(name, output);
    CloseOutput(output, staged.Staging());
    impl_->CheckCancelled("V1/V2 artifact file publication");
    staged.Commit();
}

void
V1RemoteSource::ReadEntriesToLocalFile(const std::vector<std::string>& names,
                                       const std::string& local_path) {
    StagedLocalFile staged(local_path);
    std::ofstream output(staged.Staging(), std::ios::binary | std::ios::trunc);
    if (!output.good()) {
        ThrowInfo(FileOpenFailed,
                  "failed to open staged local artifact file {}",
                  staged.Staging());
    }
    for (const auto& name : names) {
        if (impl_->CanStreamToDisk(name)) {
            AppendFile(
                impl_->CacheToDisk(name), output, impl_->cancellation_token);
        } else {
            impl_->Append(name, output);
        }
    }
    CloseOutput(output, staged.Staging());
    impl_->CheckCancelled("V1/V2 artifact file publication");
    staged.Commit();
}

std::vector<std::string>
V1RemoteSource::ReadEntriesToLocalDir(const std::vector<std::string>& names,
                                      const std::string& local_dir) {
    CreateParentDirectories(
        (std::filesystem::path(local_dir) / ".artifact-target").string());
    std::vector<std::string> paths;
    std::vector<StagedLocalFile> staged;
    std::set<std::string> targets;
    paths.reserve(names.size());
    staged.reserve(names.size());
    for (const auto& name : names) {
        auto path =
            (std::filesystem::path(local_dir) / SourceBaseName(name)).string();
        AssertInfo(targets.insert(path).second,
                   "artifact entries collide at local path {}",
                   path);
        paths.push_back(path);
        staged.emplace_back(path);
        if (impl_->CanStreamToDisk(name)) {
            auto cached = impl_->CacheToDisk(name);
            std::ofstream output(staged.back().Staging(),
                                 std::ios::binary | std::ios::trunc);
            if (!output.good()) {
                ThrowInfo(FileOpenFailed,
                          "failed to open staged local artifact file {}",
                          staged.back().Staging());
            }
            AppendFile(cached, output, impl_->cancellation_token);
            CloseOutput(output, staged.back().Staging());
        } else {
            std::ofstream output(staged.back().Staging(),
                                 std::ios::binary | std::ios::trunc);
            if (!output.good()) {
                ThrowInfo(FileOpenFailed,
                          "failed to open staged local artifact file {}",
                          staged.back().Staging());
            }
            impl_->Append(name, output);
            CloseOutput(output, staged.back().Staging());
        }
    }
    impl_->CheckCancelled("V1/V2 artifact directory publication");
    CommitAll(staged);
    return paths;
}

std::shared_ptr<DiskEngineFileHandle>
V1RemoteSource::OpenDiskEngineFiles(
    DiskEngineFileMode mode,
    const std::vector<std::string>& engine_entry_names) const {
    if (impl_->storage_path != ArtifactStoragePath::Index ||
        impl_->layout != V1SourceLayout::DiskFiles) {
        ThrowInfo(Unsupported,
                  "artifact source does not support disk engine files");
    }
    return std::make_shared<DiskEngineFileHandle>(
        impl_->context, mode, impl_->remote_paths, engine_entry_names);
}

}  // namespace milvus::storage
