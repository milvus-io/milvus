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
#include <sys/stat.h>

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

TEST_F(DurableLocalChunkManagerTest, PersistentObjectsAreWrittenWhole) {
    DurableLocalChunkManager cm(root_);
    auto path = root_ + "/new/parents/index-slice";
    std::string data = "abcdef";
    cm.Write(path, data.data(), data.size());
    ASSERT_EQ(Read(path), data);
    std::string replacement = "XX";
    cm.Write(path, replacement.data(), replacement.size());
    ASSERT_EQ(Read(path), "XX");
}

TEST_F(DurableLocalChunkManagerTest, SearchOnlyAncestorAndPhysicalRoot) {
    if (::geteuid() == 0)
        GTEST_SKIP() << "root bypasses directory permissions";
    auto ancestor = root_ + "/search";
    auto root = ancestor + "/data";
    std::filesystem::create_directories(root + "/actual/child");
    std::filesystem::create_directory_symlink(root + "/actual/child",
                                              root + "/link");
    ASSERT_EQ(::chmod(ancestor.c_str(), 0111), 0);
    struct Restore {
        std::string path;
        ~Restore() {
            ::chmod(path.c_str(), 0700);
        }
    } restore{ancestor};
    DurableLocalChunkManager cm(root);
    auto path = root + "/link/../index-slice";
    std::string data = "data";
    ASSERT_NO_THROW(cm.Write(path, data.data(), data.size()));
    ASSERT_EQ(Read(root + "/actual/index-slice"), data);
}

}  // namespace
}  // namespace milvus::storage
