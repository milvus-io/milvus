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

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "nlohmann/json_fwd.hpp"
#include "storage/artifact/FileSink.h"
#include "storage/artifact/LoadOptions.h"

// Injected read side of artifact persistence. Resolve logical entries to bytes
// or local files, hiding transport and slice assembly from loaders. Supports
// materialized buffers, streamed local files, and file-backed loading without
// requiring a segment or query context.
namespace milvus::storage {

struct FileManagerContext;
class DiskEngineFileHandle;
enum class DiskEngineFileMode;

// V1/V2 has two physical conventions. Memory-entry artifacts use SLICE_META to
// identify slices. Directory artifacts name every
// physical slice `<basename>_<N>` without SLICE_META. The caller knows the
// family and must select the convention; guessing from a numeric suffix would
// reinterpret legitimate entry names.
enum class V1SourceLayout {
    MemoryEntries,
    DiskFiles,
};

class FileSource {
 public:
    virtual ~FileSource() = default;

    virtual Generation
    Gen() const = 0;

    virtual std::vector<std::string>
    EntryNames() const = 0;

    virtual bool
    HasEntry(std::string_view name) const = 0;

    // Returns a known logical length when the transport carries one. On legacy
    // layouts without length metadata this may materialize exactly this entry
    // as load-time I/O. It is not a metadata-only caps/admission API.
    virtual int64_t
    EntrySize(std::string_view name) const = 0;

    // Fully materialize one entry in memory.
    virtual std::vector<uint8_t>
    ReadEntry(std::string_view name) = 0;

    // Materialize one entry as a local file without a full in-memory copy.
    // This is the DiskANN path. Publication uses a checked same-directory
    // staging file and atomic replacement, so failure preserves an existing
    // destination.
    virtual void
    ReadEntryToLocalFile(std::string_view name,
                         const std::string& local_path) = 0;

    // Materialize several logical entries into one file in exactly the order
    // supplied. This is the vector mmap concatenation contract.
    virtual void
    ReadEntriesToLocalFile(const std::vector<std::string>& names,
                           const std::string& local_path) = 0;

    // Materialize a set of entries into a local directory, returning their
    // local paths in the order requested. This is the mmap path and the tantivy
    // directory path. Destinations must have no concurrent writers; all entries
    // are staged before publication and existing files are restored if the
    // publication fails.
    virtual std::vector<std::string>
    ReadEntriesToLocalDir(const std::vector<std::string>& names,
                          const std::string& local_dir) = 0;

    // Metadata written by FileSink::PutMeta, available without payload reads.
    // Load planning uses it for format selection and pre-pin capabilities.
    virtual std::optional<nlohmann::json>
    GetMeta(std::string_view key) const = 0;

    // Opens the storage backing required by a disk-vector engine. Sources that
    // do not represent V1/V2 index directory artifacts reject this capability.
    virtual std::shared_ptr<DiskEngineFileHandle>
    OpenDiskEngineFiles(
        DiskEngineFileMode mode,
        const std::vector<std::string>& engine_entry_names) const;
};

class V1RemoteSource final : public FileSource {
 public:
    V1RemoteSource(
        const FileManagerContext& context,
        std::vector<std::string> remote_paths,
        LoadOptions options = {},
        ArtifactStoragePath storage_path = ArtifactStoragePath::Index,
        V1SourceLayout layout = V1SourceLayout::MemoryEntries);
    ~V1RemoteSource() override;

    Generation
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
    std::optional<nlohmann::json>
    GetMeta(std::string_view key) const override;
    std::shared_ptr<DiskEngineFileHandle>
    OpenDiskEngineFiles(
        DiskEngineFileMode mode,
        const std::vector<std::string>& engine_entry_names) const override;

 private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

class V3PackedSource final : public FileSource {
 public:
    V3PackedSource(
        const FileManagerContext& context,
        std::vector<std::string> remote_paths,
        LoadOptions options = {},
        ArtifactStoragePath storage_path = ArtifactStoragePath::Index);
    ~V3PackedSource() override;

    Generation
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
    std::optional<nlohmann::json>
    GetMeta(std::string_view key) const override;

 private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace milvus::storage
