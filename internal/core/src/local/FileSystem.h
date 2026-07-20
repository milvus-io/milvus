// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <vector>

#include "local/Path.h"
#include "local/io/File.h"
#include "local/io/MappedRegion.h"

namespace milvus::local {

class ManagedSubtree;

struct WriteOptions {
    bool create{false};
    bool truncate{false};
    bool create_parent{false};
    bool direct_io{false};
};

struct MapOptions {
    uint64_t offset{0};
    size_t length{0};
    bool populate{false};
};

class FileSystem final {
 public:
    // FileSystem is a copyable capability for one immutable rooted subtree.
    // Copies may be used concurrently.
    static FileSystem
    Open(std::filesystem::path absolute_root);

    FileSystem
    Subtree(const Path& path) const;

    std::shared_ptr<ManagedSubtree>
    ManageSubtree(const Path& path) const;

    bool
    Exists(const Path& path) const;

    uint64_t
    FileSize(const Path& path) const;

    uint64_t
    UsedSize() const;

    std::vector<Path>
    List(const Path& directory, bool recursive) const;

    void
    CreateDirectories(const Path& path) const;

    void
    RemoveFile(const Path& path) const;

    void
    RemoveAll(const Path& path) const;

    void
    Clear() const;

    void
    Rename(const Path& from, const Path& to) const;

    io::RandomAccessFile
    OpenForRead(const Path& path) const;

    io::WritableFile
    OpenForWrite(const Path& path, const WriteOptions& options) const;

    io::MappedRegion
    OpenMappedRegion(const Path& path, const MapOptions& options) const;

    std::filesystem::path
    ResolveNativePath(const Path& path) const;

    std::filesystem::path
    NativeRoot() const;

    Path
    PathFromNativePath(std::filesystem::path native_path) const;

 private:
    struct RootState;

    FileSystem(std::shared_ptr<const RootState> root,
               std::filesystem::path prefix);

    std::filesystem::path
    ScopedRoot() const;

    std::filesystem::path
    CheckedNativePath(const Path& path) const;

    std::shared_ptr<const RootState> root_;
    std::filesystem::path prefix_;
};

}  // namespace milvus::local
