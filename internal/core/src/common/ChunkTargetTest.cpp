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

#include <cerrno>
#include <cstring>
#include <memory>
#include <type_traits>
#include <sys/mman.h>
#include <unistd.h>

#include "common/Chunk.h"
#include "common/ChunkTarget.h"
#include "gtest/gtest.h"

namespace milvus {
namespace {

int
MappingStatus(char* data, size_t size) {
#ifdef __APPLE__
    char residency;
#else
    unsigned char residency;
#endif
    return mincore(data, size, &residency);
}

TEST(MemChunkTarget, DiscardedTargetUnmapsItsBuffer) {
    const auto page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    char* data = nullptr;
    {
        MemChunkTarget target(page_size, false);
        data = target.release();
        ASSERT_EQ(MappingStatus(data, page_size), 0);
    }
    errno = 0;
    const auto result = MappingStatus(data, page_size);
    const auto error = errno;
    EXPECT_EQ(result, -1);
    EXPECT_EQ(error, ENOMEM);
    EXPECT_FALSE(std::is_copy_constructible_v<MemChunkTarget>);
}

TEST(MemChunkTarget, GuardKeepsTransferredBufferAlive) {
    const auto page_size = static_cast<size_t>(sysconf(_SC_PAGESIZE));
    const char value[] = "chunk payload";
    char* data = nullptr;
    std::shared_ptr<ChunkMmapGuard> guard;
    {
        MemChunkTarget target(page_size, false);
        target.write(value, sizeof(value));
        data = target.release();
        guard = std::make_shared<ChunkMmapGuard>(data, page_size, "");
        target.TransferOwnership();
    }
    ASSERT_EQ(MappingStatus(data, page_size), 0);
    EXPECT_EQ(std::memcmp(data, value, sizeof(value)), 0);

    guard.reset();
    errno = 0;
    const auto result = MappingStatus(data, page_size);
    const auto error = errno;
    EXPECT_EQ(result, -1);
    EXPECT_EQ(error, ENOMEM);
}

}  // namespace
}  // namespace milvus
