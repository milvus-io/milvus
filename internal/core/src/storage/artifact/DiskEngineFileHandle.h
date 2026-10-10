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

#include <memory>
#include <string>
#include <vector>

#include "filemanager/FileManager.h"

namespace milvus::storage {

struct FileManagerContext;

enum class DiskEngineFileMode {
    LocalFiles,
    RemoteStreams,
};

// Owns the complete storage backing passed to a disk-vector engine. The
// remote-stream mode restricts raw reads to the engine artifact inventory and
// preserves the first storage exception when a native API reduces it to a
// status value.
class DiskEngineFileHandle final {
 public:
    DiskEngineFileHandle(
        const FileManagerContext& context,
        DiskEngineFileMode mode,
        const std::vector<std::string>& remote_paths,
        const std::vector<std::string>& engine_entry_names);
    ~DiskEngineFileHandle();

    std::shared_ptr<milvus::FileManager>
    Manager() const;

    const std::string&
    LocalPrefix() const;

    void
    RethrowFirstFailure() const;

 private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace milvus::storage
