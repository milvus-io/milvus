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

#include "index/test_utils/TestArtifactIO.h"

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

#include "common/EasyAssert.h"
#include "storage/artifact/ArtifactStats.h"

namespace milvus::index::test {
namespace {

std::vector<uint8_t>
ReadLocalFile(const std::string& path) {
    std::ifstream input(path, std::ios::binary | std::ios::ate);
    if (!input) {
        throw std::logic_error("cannot open artifact input file " + path);
    }
    const auto end = input.tellg();
    if (end < 0 ||
        static_cast<uint64_t>(end) >
            static_cast<uint64_t>(std::numeric_limits<size_t>::max())) {
        throw std::logic_error("invalid artifact input size for " + path);
    }
    std::vector<uint8_t> bytes(static_cast<size_t>(end));
    input.seekg(0);
    if (!bytes.empty()) {
        input.read(reinterpret_cast<char*>(bytes.data()),
                   static_cast<std::streamsize>(bytes.size()));
        if (!input) {
            throw std::logic_error("cannot read artifact input file " + path);
        }
    }
    return bytes;
}

std::string
CreateTemporaryPath(const std::string& target, std::string_view stem) {
    const auto parent = std::filesystem::path(target).parent_path();
    if (!parent.empty()) {
        std::filesystem::create_directories(parent);
    }
    const auto directory = parent.empty() ? std::filesystem::path(".") : parent;
    auto pattern = (directory / (std::string(stem) + "-XXXXXX")).string();
    std::vector<char> mutable_pattern(pattern.begin(), pattern.end());
    mutable_pattern.push_back('\0');
    const auto descriptor = ::mkstemp(mutable_pattern.data());
    if (descriptor < 0) {
        throw std::logic_error("cannot create staged file beside " + target);
    }
    if (::close(descriptor) != 0) {
        const auto error = errno;
        ::unlink(mutable_pattern.data());
        throw std::logic_error("cannot close staged file beside " + target +
                               ": " + std::strerror(error));
    }
    return mutable_pattern.data();
}

class StagedLocalFile {
 public:
    explicit StagedLocalFile(std::string target)
        : target_(std::move(target)),
          staging_(CreateTemporaryPath(target_, ".milvus-index-test")) {
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
            throw std::logic_error("cannot publish artifact file " + target_);
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
    throw std::logic_error("cannot inspect artifact destination " + path);
}

std::string
CreateVacantBackupPath(const std::string& target) {
    auto backup = CreateTemporaryPath(target, ".milvus-index-test-backup");
    if (::unlink(backup.c_str()) != 0) {
        throw std::logic_error("cannot prepare artifact backup beside " +
                               target);
    }
    return backup;
}

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
                    throw std::logic_error(
                        "cannot back up artifact destination " + file.Target());
                }
                state.has_backup = true;
            }
            if (::rename(file.Staging().c_str(), file.Target().c_str()) != 0) {
                throw std::logic_error("cannot publish artifact file " +
                                       file.Target());
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
                    rollback_error =
                        "cannot restore artifact destination " + file.Target();
                }
            } else if (state.published &&
                       ::unlink(file.Target().c_str()) != 0 &&
                       errno != ENOENT && rollback_error.empty()) {
                rollback_error =
                    "cannot roll back artifact destination " + file.Target();
            }
        }
        if (!rollback_error.empty()) {
            throw std::logic_error(rollback_error);
        }
        throw;
    }

    for (const auto& state : states) {
        if (state.has_backup) {
            ::unlink(state.backup.c_str());
        }
    }
}

void
WriteBytes(const std::string& path, const std::vector<uint8_t>& bytes) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    if (!output) {
        throw std::logic_error("cannot open artifact output file " + path);
    }
    if (!bytes.empty()) {
        output.write(reinterpret_cast<const char*>(bytes.data()),
                     static_cast<std::streamsize>(bytes.size()));
    }
    if (!output) {
        throw std::logic_error("cannot write artifact output file " + path);
    }
    output.flush();
    if (!output) {
        throw std::logic_error("cannot flush artifact output file " + path);
    }
    output.close();
    if (output.fail()) {
        throw std::logic_error("cannot close artifact output file " + path);
    }
}

void
WriteLocalFile(const std::string& path, const std::vector<uint8_t>& bytes) {
    StagedLocalFile staged(path);
    WriteBytes(staged.Staging(), bytes);
    staged.Commit();
}

}  // namespace

TestArtifactSink::TestArtifactSink(TestArtifactData& artifact,
                                   storage::Generation generation)
    : artifact_(artifact), generation_(generation) {
}

storage::Generation
TestArtifactSink::Gen() const {
    return generation_;
}

void
TestArtifactSink::WriteEntry(std::string_view name,
                             const void* data,
                             size_t size) {
    if (name.empty() || (data == nullptr && size != 0)) {
        throw std::logic_error("invalid in-memory artifact entry");
    }
    std::vector<uint8_t> bytes(size);
    if (size != 0) {
        const auto* begin = static_cast<const uint8_t*>(data);
        std::copy_n(begin, size, bytes.begin());
    }
    if (!artifact_.entries.emplace(std::string(name), std::move(bytes))
             .second) {
        throw std::logic_error(std::string(name) +
                               ": duplicate artifact entry");
    }
}

void
TestArtifactSink::WriteEntryFromLocalFile(std::string_view name,
                                          const std::string& local_path) {
    auto bytes = ReadLocalFile(local_path);
    WriteEntry(name, bytes.data(), bytes.size());
}

void
TestArtifactSink::WriteRawEntryFromLocalFile(std::string_view name,
                                             const std::string& local_path) {
    if (generation_ == storage::Generation::V1V2) {
        ThrowInfo(Unsupported,
                  "V1/V2 test artifacts cannot publish a raw file entry");
    }
    WriteEntryFromLocalFile(name, local_path);
}

void
TestArtifactSink::PutMeta(std::string_view key, const nlohmann::json& value) {
    if (generation_ == storage::Generation::V1V2) {
        ThrowInfo(Unsupported,
                  "V1/V2 test artifacts store metadata as named entries");
    }
    if (key.empty() ||
        !artifact_.metadata.emplace(std::string(key), value).second) {
        throw std::logic_error(std::string(key) +
                               ": invalid or duplicate artifact metadata");
    }
}

storage::ArtifactStats
TestArtifactSink::Finish() {
    int64_t total = 0;
    std::vector<storage::SerializedFileInfo> files;
    files.reserve(artifact_.entries.size());
    for (const auto& [name, bytes] : artifact_.entries) {
        if (bytes.size() >
            static_cast<size_t>(std::numeric_limits<int64_t>::max() - total)) {
            throw std::logic_error("in-memory artifact size overflows");
        }
        const auto size = static_cast<int64_t>(bytes.size());
        total += size;
        files.emplace_back(name, size);
    }
    if (generation_ == storage::Generation::V1V2) {
        return {total, {}};
    }
    return {total, std::move(files)};
}

void
TestArtifactSink::ReleaseLocalStaging() {
}

TestArtifactSource::TestArtifactSource(const TestArtifactData& artifact,
                                       storage::Generation generation)
    : artifact_(artifact), generation_(generation) {
}

storage::Generation
TestArtifactSource::Gen() const {
    return generation_;
}

std::vector<std::string>
TestArtifactSource::EntryNames() const {
    std::vector<std::string> names;
    names.reserve(artifact_.entries.size());
    for (const auto& [name, unused] : artifact_.entries) {
        static_cast<void>(unused);
        names.push_back(name);
    }
    return names;
}

bool
TestArtifactSource::HasEntry(std::string_view name) const {
    return artifact_.entries.contains(std::string(name));
}

int64_t
TestArtifactSource::EntrySize(std::string_view name) const {
    const auto& bytes = Entry(name);
    if (bytes.size() >
        static_cast<size_t>(std::numeric_limits<int64_t>::max())) {
        throw std::logic_error(std::string(name) +
                               ": artifact entry is too large");
    }
    return static_cast<int64_t>(bytes.size());
}

std::vector<uint8_t>
TestArtifactSource::ReadEntry(std::string_view name) {
    return Entry(name);
}

void
TestArtifactSource::ReadEntryToLocalFile(std::string_view name,
                                         const std::string& local_path) {
    WriteLocalFile(local_path, Entry(name));
}

void
TestArtifactSource::ReadEntriesToLocalFile(
    const std::vector<std::string>& names, const std::string& local_path) {
    std::vector<uint8_t> combined;
    size_t total = 0;
    for (const auto& name : names) {
        const auto& bytes = Entry(name);
        if (bytes.size() > std::numeric_limits<size_t>::max() - total) {
            throw std::logic_error("combined artifact size overflows");
        }
        total += bytes.size();
    }
    combined.reserve(total);
    for (const auto& name : names) {
        const auto& bytes = Entry(name);
        combined.insert(combined.end(), bytes.begin(), bytes.end());
    }
    WriteLocalFile(local_path, combined);
}

std::vector<std::string>
TestArtifactSource::ReadEntriesToLocalDir(const std::vector<std::string>& names,
                                          const std::string& local_dir) {
    std::vector<std::string> paths;
    std::vector<const std::vector<uint8_t>*> entries;
    paths.reserve(names.size());
    entries.reserve(names.size());
    for (const auto& name : names) {
        const auto filename = std::filesystem::path(name).filename().string();
        if (filename != name || filename.empty()) {
            throw std::logic_error(name + ": invalid artifact entry name");
        }
        auto path = (std::filesystem::path(local_dir) / filename).string();
        if (std::find(paths.begin(), paths.end(), path) != paths.end()) {
            throw std::logic_error(name + ": colliding artifact entry");
        }
        paths.push_back(std::move(path));
        entries.push_back(&Entry(name));
    }

    std::vector<StagedLocalFile> staged;
    staged.reserve(paths.size());
    for (size_t i = 0; i < paths.size(); ++i) {
        staged.emplace_back(paths[i]);
        WriteBytes(staged.back().Staging(), *entries[i]);
    }
    CommitAll(staged);
    return paths;
}

std::optional<nlohmann::json>
TestArtifactSource::GetMeta(std::string_view key) const {
    if (generation_ == storage::Generation::V1V2) {
        return std::nullopt;
    }
    const auto it = artifact_.metadata.find(std::string(key));
    if (it == artifact_.metadata.end()) {
        return std::nullopt;
    }
    return it->second;
}

const std::vector<uint8_t>&
TestArtifactSource::Entry(std::string_view name) const {
    const auto it = artifact_.entries.find(std::string(name));
    if (it == artifact_.entries.end()) {
        ThrowInfo(DataFormatBroken, "missing artifact entry {}", name);
    }
    return it->second;
}

}  // namespace milvus::index::test
