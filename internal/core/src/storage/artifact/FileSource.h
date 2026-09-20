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

#include "folly/coro/Task.h"
#include "folly/CancellationToken.h"
#include "pb/common.pb.h"
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

    // Return the logical payload length. Remote legacy sources inspect object
    // envelopes when slice metadata omits it; they never cache decoded payloads
    // for a size query. This may perform I/O, so it is not a caps-only API.
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

    // Async transports suspend during remote I/O and admission. The default
    // implementation supports local/test sources; remote sources override it.
    // use_async=false preserves synchronous transport for shared loader bodies.
    // Calls borrow their arguments until the returned task has completed.
    virtual folly::coro::Task<int64_t>
    EntrySizeAsync(std::string_view name, bool use_async = true);
    virtual folly::coro::Task<std::vector<uint8_t>>
    ReadEntryAsync(std::string_view name, bool use_async = true);
    virtual folly::coro::Task<void>
    ReadEntryToLocalFileAsync(std::string_view name,
                              const std::string& path,
                              bool use_async = true);
    virtual folly::coro::Task<void>
    ReadEntriesToLocalFileAsync(const std::vector<std::string>& names,
                                const std::string& path,
                                bool use_async = true);
    virtual folly::coro::Task<std::vector<std::string>>
    ReadEntriesToLocalDirAsync(const std::vector<std::string>& names,
                               const std::string& directory,
                               bool use_async = true);

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
    static folly::coro::Task<std::unique_ptr<V1RemoteSource>>
    OpenAsync(const FileManagerContext& context,
              std::vector<std::string> remote_paths,
              LoadOptions options = {},
              ArtifactStoragePath storage_path = ArtifactStoragePath::Index,
              V1SourceLayout layout = V1SourceLayout::MemoryEntries);

    // Set per-load state only between fully drained sequential operations.
    void
    SetLoadContext(proto::common::LoadPriority priority,
                   folly::CancellationToken token);

    folly::coro::Task<int64_t>
    EntrySizeAsync(std::string_view name, bool use_async = true) override;
    folly::coro::Task<std::vector<uint8_t>>
    ReadEntryAsync(std::string_view name, bool use_async = true) override;
    folly::coro::Task<void>
    ReadEntryToLocalFileAsync(std::string_view name,
                              const std::string& path,
                              bool use_async = true) override;
    folly::coro::Task<void>
    ReadEntriesToLocalFileAsync(const std::vector<std::string>& names,
                                const std::string& path,
                                bool use_async = true) override;
    folly::coro::Task<std::vector<std::string>>
    ReadEntriesToLocalDirAsync(const std::vector<std::string>& names,
                               const std::string& directory,
                               bool use_async = true) override;

    struct LoadBytes {
        uint64_t payload{0};
        uint64_t transient{0};
        uint64_t directory{0};
    };

    // Inspect only the requested logical entries. Never fetch engine payloads;
    // callers exclude native remote-stream entries from admission estimates.
    folly::coro::Task<LoadBytes>
    InspectLoadBytesAsync(const std::vector<std::string>& names);

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
    std::shared_ptr<DiskEngineFileHandle>
    OpenDiskEngineFiles(
        DiskEngineFileMode mode,
        const std::vector<std::string>& engine_entry_names) const override;

 private:
    class Impl;
    explicit V1RemoteSource(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> impl_;
};

}  // namespace milvus::storage
