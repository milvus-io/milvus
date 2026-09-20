// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
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
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "nlohmann/json.hpp"
#include "storage/artifact/FileSink.h"
#include "storage/artifact/LocalFileUtils.h"
#include "storage/artifact/FileSource.h"

namespace milvus::index::test {

struct TestArtifactData {
    std::map<std::string, std::vector<uint8_t>> entries;
    std::map<std::string, nlohmann::json> metadata;
};

// Capture the direct V3 writer contract for existing format/loader tests.
class TestArtifactWriter final : public storage::IndexEntryWriter {
 public:
    explicit TestArtifactWriter(TestArtifactData& artifact)
        : artifact_(artifact) {
    }

    void
    WriteEntry(const std::string& name,
               const void* data,
               size_t size) override {
        CheckDuplicateName(name);
        auto& bytes = artifact_.entries[name];
        bytes.resize(size);
        if (size != 0) {
            std::memcpy(bytes.data(), data, size);
        }
        size_ += size;
    }

    void
    WriteEntry(const std::string& name, int fd, size_t size) override {
        CheckDuplicateName(name);
        auto& bytes = artifact_.entries[name];
        bytes.resize(size);
        storage::ReadAll(fd, bytes.data(), size, name, "test artifact");
        size_ += size;
    }

    void
    Finish() override {
        for (const auto& [key, value] : meta_json_.items()) {
            artifact_.metadata.emplace(key, value);
        }
    }

    size_t
    GetTotalBytesWritten() const override {
        return size_;
    }

 private:
    TestArtifactData& artifact_;
    size_t size_{0};
};

class TestArtifactSink final : public storage::FileSink {
 public:
    explicit TestArtifactSink(
        TestArtifactData& artifact,
        storage::Generation generation = storage::Generation::V3);

    storage::Generation
    Gen() const override;
    void
    WriteEntry(std::string_view name, const void* data, size_t size) override;
    void
    WriteEntryFromLocalFile(std::string_view name,
                            const std::string& local_path) override;
    void
    WriteRawEntryFromLocalFile(std::string_view name,
                               const std::string& local_path) override;
    storage::ArtifactStats
    Finish() override;
    void
    ReleaseLocalStaging() override;

 private:
    TestArtifactData& artifact_;
    storage::Generation generation_;
};

class TestArtifactSource final : public storage::FileSource {
 public:
    explicit TestArtifactSource(
        const TestArtifactData& artifact,
        storage::Generation generation = storage::Generation::V3);

    storage::Generation
    Gen() const override;
    std::vector<std::string>
    EntryNames() const override;
    bool
    HasEntry(std::string_view name) const override;
    int64_t
    EntrySize(std::string_view name) const override;
    std::vector<uint8_t>
    ReadEntry(std::string_view name) override;
    void
    ReadEntryToLocalFile(std::string_view name,
                         const std::string& local_path) override;
    void
    ReadEntriesToLocalFile(const std::vector<std::string>& names,
                           const std::string& local_path) override;
    std::vector<std::string>
    ReadEntriesToLocalDir(const std::vector<std::string>& names,
                          const std::string& local_dir) override;

 private:
    const std::vector<uint8_t>&
    Entry(std::string_view name) const;

    const TestArtifactData& artifact_;
    storage::Generation generation_;
};

}  // namespace milvus::index::test
