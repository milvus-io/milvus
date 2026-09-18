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
#include "storage/IndexEntryReader.h"
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

void
AppendBytes(const std::vector<uint8_t>& bytes,
            std::ofstream& output,
            const folly::CancellationToken& cancellation_token = {}) {
    AppendMemory(bytes.data(), bytes.size(), output, cancellation_token);
}

}  // namespace

std::shared_ptr<DiskEngineFileHandle>
FileSource::OpenDiskEngineFiles(
    DiskEngineFileMode,
    const std::vector<std::string>&) const {
    ThrowInfo(Unsupported,
              "artifact source does not support disk engine files");
}

class V1RemoteSource::Impl {
 public:
    Impl(const FileManagerContext& context,
         std::vector<std::string> paths,
         LoadOptions options,
         ArtifactStoragePath storage_path,
         V1SourceLayout layout)
        : context(context),
          remote_paths(std::move(paths)),
          load_priority(LoadPriority(options)),
          cancellation_token(CancellationToken(options)),
          storage_path(storage_path),
          layout(layout),
          memory_manager(std::make_shared<MemFileManagerImpl>(context)),
          disk_manager(std::make_shared<DiskFileManagerImpl>(context)) {
        NormalizePaths();
        BuildDirectory();
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
    BuildDirectory() {
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
            CheckCancelled("V1/V2 slice-meta download");
            auto codecs = memory_manager->LoadIndexToMemory({meta->second},
                                                            load_priority);
            CheckCancelled("V1/V2 slice-meta download");
            auto codec = codecs.find(INDEX_FILE_SLICE_META);
            if (codec == codecs.end() || codec->second == nullptr) {
                ThrowInfo(FileReadFailed, "failed to load V1/V2 slice meta");
            }
            auto size = codec->second->PayloadSize();
            const auto* data = codec->second->PayloadData();
            if (size != 0 && data == nullptr) {
                ThrowInfo(DataFormatBroken,
                          "V1/V2 slice meta has null payload");
            }
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

    std::vector<uint8_t>
    Decode(std::string_view name) const {
        const auto& paths = Paths(name);
        CheckCancelled("V1/V2 artifact download");
        auto codecs = memory_manager->LoadIndexToMemory(paths, load_priority);
        CheckCancelled("V1/V2 artifact download");
        size_t total = 0;
        for (const auto& path : paths) {
            auto it = codecs.find(SourceBaseName(path));
            if (it == codecs.end() || it->second == nullptr) {
                ThrowInfo(FileReadFailed,
                          "loaded artifact slice {} is missing",
                          path);
            }
            const auto payload_size = it->second->PayloadSize();
            if (payload_size >
                static_cast<size_t>(std::numeric_limits<int64_t>::max()) -
                    total) {
                ThrowInfo(DataFormatBroken,
                          "V1/V2 artifact entry {} is too large",
                          name);
            }
            total += payload_size;
        }
        std::vector<uint8_t> result(total);
        size_t offset = 0;
        constexpr size_t kCopyBufferSize = 1024 * 1024;
        for (const auto& path : paths) {
            const auto& codec = codecs.at(SourceBaseName(path));
            const auto payload_size = codec->PayloadSize();
            const auto* payload = codec->PayloadData();
            if (payload_size != 0 && payload == nullptr) {
                ThrowInfo(DataFormatBroken,
                          "V1/V2 artifact slice {} has null payload",
                          path);
            }
            size_t payload_offset = 0;
            while (payload_offset < payload_size) {
                CheckCancelled("V1/V2 artifact decode");
                const auto chunk =
                    std::min(kCopyBufferSize, payload_size - payload_offset);
                std::memcpy(result.data() + offset + payload_offset,
                            payload + payload_offset,
                            chunk);
                payload_offset += chunk;
            }
            offset += payload_size;
        }
        CheckCancelled("V1/V2 artifact decode");
        RecordOrValidateEntrySize(name, static_cast<int64_t>(total));
        return result;
    }

    std::vector<uint8_t>
    Take(std::string_view name) const {
        auto cached = entry_cache.find(std::string(name));
        if (cached == entry_cache.end()) {
            return Decode(name);
        }
        auto bytes = std::move(cached->second);
        entry_cache.erase(cached);
        return bytes;
    }

    int64_t
    Measure(std::string_view name) const {
        auto known = entry_sizes.find(std::string(name));
        if (known != entry_sizes.end()) {
            return known->second;
        }
        auto bytes = Decode(name);
        auto size = static_cast<int64_t>(bytes.size());
        entry_cache.emplace(std::string(name), std::move(bytes));
        return size;
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
    std::set<std::string> slice_meta_entries;
    mutable std::unordered_map<std::string, int64_t> entry_sizes;
    mutable std::unordered_map<std::string, std::vector<uint8_t>> entry_cache;
    mutable std::unordered_map<std::string, std::string> staged_files;
};

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
    if (impl_->CanStreamToDisk(name)) {
        auto cached = impl_->CacheToDisk(name);
        return LocalFileSize(cached);
    }
    return impl_->Measure(name);
}

std::vector<uint8_t>
V1RemoteSource::ReadEntry(std::string_view name) {
    impl_->CheckCancelled("V1/V2 artifact read");
    return impl_->Take(name);
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
    auto bytes = impl_->Take(name);
    WriteBytes(staged.Staging(),
               bytes.data(),
               bytes.size(),
               impl_->cancellation_token);
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
            AppendBytes(impl_->Take(name), output, impl_->cancellation_token);
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
            auto bytes = impl_->Take(name);
            WriteBytes(staged.back().Staging(),
                       bytes.data(),
                       bytes.size(),
                       impl_->cancellation_token);
        }
    }
    impl_->CheckCancelled("V1/V2 artifact directory publication");
    CommitAll(staged);
    return paths;
}

std::optional<nlohmann::json>
V1RemoteSource::GetMeta(std::string_view) const {
    return std::nullopt;
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
    return std::make_shared<DiskEngineFileHandle>(impl_->context,
                                                  mode,
                                                  impl_->remote_paths,
                                                  engine_entry_names);
}

class V3PackedSource::Impl {
 public:
    Impl(const FileManagerContext& context,
         std::vector<std::string> remote_paths,
         LoadOptions options,
         ArtifactStoragePath storage_path)
        : options(std::move(options)),
          manager(std::make_shared<MemFileManagerImpl>(context)) {
        AssertInfo(remote_paths.size() == 1,
                   "V3 artifact requires exactly one packed file");
        auto input = manager->OpenInputStream(
            remote_paths.front(),
            storage_path == ArtifactStoragePath::Index);
        if (input == nullptr) {
            ThrowInfo(FileOpenFailed, "failed to open V3 artifact input");
        }
        reader = IndexEntryReader::Open(input,
                                        input->Size(),
                                        context.fieldDataMeta.collection_id,
                                        PoolPriority(this->options),
                                        CancellationToken(this->options));
        if (reader == nullptr) {
            ThrowInfo(FileOpenFailed, "failed to create V3 artifact reader");
        }
    }

    LoadOptions options;
    std::shared_ptr<MemFileManagerImpl> manager;
    std::unique_ptr<IndexEntryReader> reader;
};

V3PackedSource::V3PackedSource(const FileManagerContext& context,
                               std::vector<std::string> remote_paths,
                               LoadOptions options,
                               ArtifactStoragePath storage_path)
    : impl_(std::make_unique<Impl>(
          context, std::move(remote_paths), std::move(options), storage_path)) {
}

V3PackedSource::~V3PackedSource() = default;

Generation
V3PackedSource::Gen() const {
    return Generation::V3;
}

std::vector<std::string>
V3PackedSource::EntryNames() const {
    auto names = impl_->reader->GetEntryNames();
    std::erase(names, std::string(MILVUS_V3_META_ENTRY_NAME));
    return names;
}

bool
V3PackedSource::HasEntry(std::string_view name) const {
    return impl_->reader->HasEntry(std::string(name));
}

int64_t
V3PackedSource::EntrySize(std::string_view name) const {
    return static_cast<int64_t>(impl_->reader->GetEntrySize(std::string(name)));
}

std::vector<uint8_t>
V3PackedSource::ReadEntry(std::string_view name) {
    return impl_->reader->ReadEntry(std::string(name)).data;
}

void
V3PackedSource::ReadEntryToLocalFile(std::string_view name,
                                     const std::string& local_path) {
    StagedLocalFile staged(local_path);
    impl_->reader->ReadEntryStreamToFile(
        std::string(name),
        staged.Staging(),
        io::GetPriorityFromLoadPriority(LoadPriority(impl_->options)));
    if (LocalFileSize(staged.Staging()) != EntrySize(name)) {
        // The streamed entry does not match the size the packed directory
        // declares: the artifact bytes or its directory are inconsistent, the
        // same bucket master uses for a short index slice.
        ThrowInfo(DataFormatBroken,
                  "materialized V3 artifact entry size mismatch: {}",
                  name);
    }
    staged.Commit();
}

void
V3PackedSource::ReadEntriesToLocalFile(const std::vector<std::string>& names,
                                       const std::string& local_path) {
    StagedLocalFile staged(local_path);
    std::ofstream output(staged.Staging(), std::ios::binary | std::ios::trunc);
    if (!output.good()) {
        ThrowInfo(FileOpenFailed,
                  "failed to open staged local artifact file {}",
                  staged.Staging());
    }
    for (const auto& name : names) {
        impl_->reader->ReadEntryStream(
            name, [&output](const uint8_t* data, size_t size) {
                output.write(reinterpret_cast<const char*>(data), size);
                if (!output.good()) {
                    ThrowInfo(FileWriteFailed,
                              "failed to append staged V3 artifact entry");
                }
            });
    }
    CloseOutput(output, staged.Staging());
    staged.Commit();
}

std::vector<std::string>
V3PackedSource::ReadEntriesToLocalDir(const std::vector<std::string>& names,
                                      const std::string& local_dir) {
    CreateParentDirectories(
        (std::filesystem::path(local_dir) / ".artifact-target").string());
    std::vector<std::pair<std::string, std::string>> pairs;
    std::vector<std::string> paths;
    std::vector<StagedLocalFile> staged;
    std::set<std::string> targets;
    pairs.reserve(names.size());
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
        pairs.emplace_back(name, staged.back().Staging());
    }
    impl_->reader->ReadEntriesStreamToFiles(
        pairs, io::GetPriorityFromLoadPriority(LoadPriority(impl_->options)));
    for (size_t i = 0; i < names.size(); ++i) {
        if (LocalFileSize(staged[i].Staging()) != EntrySize(names[i])) {
            ThrowInfo(DataFormatBroken,
                      "materialized V3 artifact entry size mismatch: {}",
                      names[i]);
        }
    }
    CommitAll(staged);
    return paths;
}

std::optional<nlohmann::json>
V3PackedSource::GetMeta(std::string_view key) const {
    if (!impl_->reader->HasMeta(std::string(key))) {
        return std::nullopt;
    }
    return impl_->reader->GetMeta<nlohmann::json>(std::string(key));
}

}  // namespace milvus::storage
