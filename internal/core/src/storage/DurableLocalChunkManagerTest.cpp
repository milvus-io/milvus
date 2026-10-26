// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <unistd.h>
#include <milvus-storage/common/fiu_local.h>

#include "storage/LocalChunkManager.h"

namespace milvus::storage {
namespace {

class DurableLocalChunkManagerTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        char pattern[] = "/tmp/milvus-durable-chunks-XXXXXX";
        auto root = ::mkdtemp(pattern);
        ASSERT_NE(root, nullptr);
        root_ = root;
        FIU_INIT();
    }
    void
    TearDown() override {
        std::filesystem::remove_all(root_);
    }
    std::string
    Read(const std::string& path) {
        std::ifstream input(path);
        return {std::istreambuf_iterator<char>(input),
                std::istreambuf_iterator<char>()};
    }
    std::string root_;
};

TEST_F(DurableLocalChunkManagerTest, PersistentObjectsAndPositionalWrites) {
    DurableLocalChunkManager cm(root_);
    auto path = root_ + "/new/parents/index-slice";
    std::string data = "abcdef";
    cm.Write(path, data.data(), data.size());
    ASSERT_EQ(Read(path), data);
    std::string replacement = "XX";
    cm.Write(path, 2, replacement.data(), replacement.size());
    ASSERT_EQ(Read(path), "abXXef");
    cm.Write(path, replacement.data(), replacement.size());
    ASSERT_EQ(Read(path), "XX");
}

#ifdef BUILD_WITH_FIU
TEST_F(DurableLocalChunkManagerTest, SyncFailureCannotAcknowledgeIndexSlice) {
    DurableLocalChunkManager cm(root_);
    auto path = root_ + "/index-slice";
    std::string old = "old";
    cm.Write(path, old.data(), old.size());
    milvus_storage::ScopedFiuFault fault(FIUKEY_LOCAL_FILE_SYNC_FAIL, false);
    std::string replacement = "replacement";
    ASSERT_ANY_THROW(cm.Write(path, replacement.data(), replacement.size()));
    // A failed write is not acknowledged; incomplete output may remain.
    ASSERT_EQ(Read(path), replacement);
}

TEST_F(DurableLocalChunkManagerTest,
       ReconstructibleCacheDoesNotSynchronizeEachChunk) {
    LocalChunkManager cache(root_);
    auto path = root_ + "/cache";
    milvus_storage::ScopedFiuFault fault(FIUKEY_LOCAL_FILE_SYNC_FAIL, false);
    std::string data = "cache";
    ASSERT_NO_THROW(cache.Write(path, data.data(), data.size()));
    ASSERT_NO_THROW(cache.Write(path, 1, data.data(), data.size()));
    ASSERT_EQ(Read(path), "ccache");
}
#endif

}  // namespace
}  // namespace milvus::storage
