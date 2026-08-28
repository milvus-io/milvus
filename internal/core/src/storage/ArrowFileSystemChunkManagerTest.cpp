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

// Offline unit tests for the remote-only routing/fallback logic of
// ArrowFileSystemChunkManager. The chunk manager itself is remote only (it
// routes Read/Write through the milvus-storage Arrow FileSystem and delegates
// the control plane to a legacy remote chunk manager), so its CRUD path needs a
// live object store and is covered by ArrowFileSystemChunkManagerRemoteTest
// against a real MinIO. What is testable without a network is exactly the part
// that decides *whether* the arrow-backed manager is built at all: Make()'s
// nullptr fallbacks and CreateChunkManager routing under the master switch.

#include <gtest/gtest.h>

#include <string>

#include "storage/ArrowFileSystemChunkManager.h"
#include "storage/Types.h"
#include "storage/Util.h"

using namespace milvus::storage;

namespace {

StorageConfig
LocalStorageConfig(const std::string& root_path) {
    StorageConfig config;
    config.storage_type = "local";
    config.root_path = root_path;
    config.bucket_name = "";
    return config;
}

}  // namespace

// Make() returns nullptr -- signalling CreateChunkManager to fall back to the
// legacy chunk managers -- for every config the arrow-backed manager does not
// own, and it does so WITHOUT any network: it returns before it would build the
// remote control-plane delegate (whose constructor PreChecks the endpoint).
// The two reasons to fall back:
//   - a local config: remote only, local disk IO stays on LocalChunkManager;
//   - a remote config milvus-storage has no arrow producer for (unknown / empty
//     / gcpnative cloud provider, or one its filesystem layer rejects).
// A *supported* remote provider instead builds the arrow manager plus its
// legacy delegate, whose PreCheck needs a live object store -- covered by
// ArrowFileSystemChunkManagerRemoteTest.
TEST(ArrowFileSystemChunkManagerSwitch, MakeFallsBackWithoutNetwork) {
    // local: never arrow-backed.
    EXPECT_EQ(ArrowFileSystemChunkManager::Make(LocalStorageConfig("/tmp")),
              nullptr);

    StorageConfig config;
    config.storage_type = "remote";
    config.address = "localhost:9000";
    config.bucket_name = "a-bucket";

    // gcpnative: milvus-storage has no arrow producer -> nullptr -> legacy.
    config.cloud_provider = "gcpnative";
    EXPECT_EQ(ArrowFileSystemChunkManager::Make(config), nullptr);

    // empty provider (e.g. self-hosted MinIO with no explicit provider):
    // milvus-storage rejects it (CreateArrowFileSystem -> Status::Invalid), so
    // Make() falls back rather than throwing a hard error at the caller.
    config.cloud_provider = "";
    EXPECT_EQ(ArrowFileSystemChunkManager::Make(config), nullptr);
}

// The remote-only invariant: a local storage type routes to LocalChunkManager
// whether the master switch is off OR on -- local disk IO is never rerouted
// through the arrow filesystem. (Positive remote routing to
// ArrowFileSystemChunkManager builds a legacy delegate whose constructor runs a
// network PreCheck -- the same reason RemoteChunkManagerTest.cpp is excluded
// from this binary -- so it is covered by ArrowFileSystemChunkManagerRemoteTest,
// not here.)
TEST(ArrowFileSystemChunkManagerSwitch, LocalNeverReroutedBySwitch) {
    // The suite-level master switch (MILVUS_USE_ARROW_FS_CHUNK_MANAGER in
    // init_gtest) may have flipped the flag already; restore whatever was
    // set when done.
    const bool prev = UseArrowFileSystemChunkManager();

    SetUseArrowFileSystemChunkManager(false);
    {
        auto cm = CreateChunkManager(LocalStorageConfig("/tmp"));
        EXPECT_EQ(cm->GetName(), "LocalChunkManager");
    }

    SetUseArrowFileSystemChunkManager(true);
    {
        auto cm = CreateChunkManager(LocalStorageConfig("/tmp"));
        EXPECT_EQ(cm->GetName(), "LocalChunkManager");
    }

    SetUseArrowFileSystemChunkManager(prev);
    ASSERT_EQ(UseArrowFileSystemChunkManager(), prev);
}
