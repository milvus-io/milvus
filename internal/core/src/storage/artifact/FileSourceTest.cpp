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

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <string>
#include <vector>

#include <sys/stat.h>
#include <unistd.h>

#include "storage/LocalChunkManager.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/Util.h"
#include "storage/artifact/FileSink.h"
#include "storage/artifact/FileSource.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::storage::test {
namespace {

class CountingSourceChunkManager final : public LocalChunkManager {
 public:
    using LocalChunkManager::LocalChunkManager;

    uint64_t
    Read(const std::string& path, void* data, uint64_t bytes) override {
        whole_reads.fetch_add(1);
        return LocalChunkManager::Read(path, data, bytes);
    }

    std::atomic<size_t> whole_reads{0};
};

class V1FileSourceTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        original_slice_size_ = FILE_SLICE_SIZE.exchange(3);
        root_ = LocalDirectory::CreateOwned(
            std::filesystem::temp_directory_path().string(),
            "milvus-file-source-XXXXXX",
            "V1 source test");
        manager_ = std::make_shared<CountingSourceChunkManager>(root_->Path() +
                                                                "/objects/");
        StorageConfig config;
        config.storage_type = "local";
        config.root_path = manager_->GetRootPath();
        static std::atomic<int64_t> build_id{910051};
        const auto id = build_id.fetch_add(1);
        context_ = FileManagerContext(
            {1, 2, 3, 100},
            {3, 100, id, 1, "file_source", "field", DataType::INT64, 1, false},
            manager_,
            InitArrowFileSystem(config));
        const auto local =
            LocalChunkManagerSingleton::GetInstance().GetChunkManager();
        auto prefix = std::filesystem::path(
            GenIndexPathPrefix(local, id, 1, 3, 100, false));
        if (prefix.filename().empty()) {
            prefix = prefix.parent_path();
        }
        cache_parent_ = prefix.parent_path();
        cache_leaf_ = prefix.filename().string() + "_";
        V1DiskSink sink(context_);
        for (const auto& [name, bytes] : entries_) {
            sink.WriteEntry(name,
                            reinterpret_cast<const uint8_t*>(bytes.data()),
                            bytes.size());
        }
        const auto stats = sink.Finish();
        for (const auto& file : stats.Files()) {
            remote_paths_.push_back(file.file_name);
        }
        sink.ReleaseLocalStaging();
    }

    void
    TearDown() override {
        FILE_SLICE_SIZE.store(original_slice_size_);
    }

    std::unique_ptr<V1RemoteSource>
    Source() const {
        return std::make_unique<V1RemoteSource>(context_,
                                                remote_paths_,
                                                LoadOptions{},
                                                ArtifactStorageNamespace::Index,
                                                V1SourceLayout::MemoryEntries);
    }

    static std::string
    ReadFile(const std::string& path) {
        std::ifstream input(path, std::ios::binary);
        return {std::istreambuf_iterator<char>(input), {}};
    }

    std::map<std::string, std::string>
    CachedFiles() const {
        std::map<std::string, std::string> result;
        if (!std::filesystem::exists(cache_parent_)) {
            return result;
        }
        for (const auto& directory :
             std::filesystem::directory_iterator(cache_parent_)) {
            if (!directory.path().filename().string().starts_with(
                    cache_leaf_)) {
                continue;
            }
            for (const auto& [name, bytes] : entries_) {
                const auto path = directory.path() / name;
                if (std::filesystem::is_regular_file(path)) {
                    result.emplace(name, path.string());
                }
            }
        }
        return result;
    }

    void
    PrimeCache(V1RemoteSource& source) const {
        source.ReadEntriesToLocalFile({"first", "second"},
                                      root_->Path() + "/concatenated");
        EXPECT_EQ(ReadFile(root_->Path() + "/concatenated"),
                  entries_.at("first") + entries_.at("second"));
    }

    int64_t original_slice_size_;
    std::shared_ptr<LocalDirectory> root_;
    std::shared_ptr<CountingSourceChunkManager> manager_;
    FileManagerContext context_;
    std::vector<std::string> remote_paths_;
    std::filesystem::path cache_parent_;
    std::string cache_leaf_;
    const std::map<std::string, std::string> entries_ = {{"first", "abcdefgh"},
                                                         {"second", "12345"}};
};

TEST_F(V1FileSourceTest, MovesCachedEntriesIntoDirectoryAndRedownloads) {
    auto source = Source();
    PrimeCache(*source);
    const auto cached = CachedFiles();
    ASSERT_EQ(cached.size(), entries_.size());
    auto target = LocalDirectory::CreateOwned(
        cache_parent_.string(), "source-target-XXXXXX", "source target test");
    std::map<std::string, ino_t> inodes;
    for (const auto& [name, path] : cached) {
        struct stat info;
        ASSERT_EQ(::stat(path.c_str(), &info), 0);
        inodes.emplace(name, info.st_ino);
    }
    const auto paths =
        source->ReadEntriesToLocalDir({"first", "second"}, target->Path());
    ASSERT_EQ(paths.size(), entries_.size());
    for (const auto& [name, bytes] : entries_) {
        const auto path = target->Path() + "/" + name;
        EXPECT_EQ(ReadFile(path), bytes);
        EXPECT_FALSE(std::filesystem::exists(cached.at(name)));
        struct stat info;
        ASSERT_EQ(::stat(path.c_str(), &info), 0);
        EXPECT_EQ(info.st_ino, inodes.at(name));
    }
    EXPECT_TRUE(CachedFiles().empty());
    const auto before = manager_->whole_reads.load();
    const auto second_target = root_->Path() + "/second-target";
    source->ReadEntriesToLocalDir({"first", "second"}, second_target);
    EXPECT_GT(manager_->whole_reads.load(), before);
    EXPECT_TRUE(CachedFiles().empty());
    source.reset();
    for (const auto& [name, bytes] : entries_) {
        EXPECT_EQ(ReadFile(target->Path() + "/" + name), bytes);
        EXPECT_EQ(ReadFile(second_target + "/" + name), bytes);
    }
}

TEST_F(V1FileSourceTest, SingleEntryFileMaterializationTransfersCache) {
    auto source = Source();
    PrimeCache(*source);
    const auto cached = CachedFiles();
    ASSERT_EQ(cached.size(), entries_.size());
    const auto first = root_->Path() + "/single-entry";
    source->ReadEntryToLocalFile("first", first);
    EXPECT_EQ(ReadFile(first), entries_.at("first"));
    EXPECT_FALSE(std::filesystem::exists(cached.at("first")));
    const auto second = root_->Path() + "/single-entry-list";
    source->ReadEntriesToLocalFile({"second"}, second);
    EXPECT_EQ(ReadFile(second), entries_.at("second"));
    EXPECT_FALSE(std::filesystem::exists(cached.at("second")));
    EXPECT_TRUE(CachedFiles().empty());
    const auto before = manager_->whole_reads.load();
    source->ReadEntryToLocalFile("first", first);
    EXPECT_GT(manager_->whole_reads.load(), before);
    EXPECT_EQ(ReadFile(first), entries_.at("first"));
}

TEST_F(V1FileSourceTest, CrossFilesystemMoveCopiesAndRemovesCache) {
    auto source = Source();
    PrimeCache(*source);
    const auto cached = CachedFiles();
    ASSERT_EQ(cached.size(), entries_.size());
    struct stat source_info;
    struct stat target_info;
    ASSERT_EQ(::stat(cached.begin()->second.c_str(), &source_info), 0);
    if (::stat("/dev/shm", &target_info) != 0 ||
        source_info.st_dev == target_info.st_dev ||
        ::access("/dev/shm", W_OK) != 0) {
        GTEST_SKIP() << "requires a writable second filesystem";
    }
    auto target = LocalDirectory::CreateOwned(
        "/dev/shm", "milvus-source-target-XXXXXX", "cross-filesystem test");
    source->ReadEntriesToLocalDir({"first", "second"}, target->Path());
    for (const auto& [name, bytes] : entries_) {
        EXPECT_EQ(ReadFile(target->Path() + "/" + name), bytes);
        EXPECT_FALSE(std::filesystem::exists(cached.at(name)));
    }
    EXPECT_TRUE(CachedFiles().empty());
}

}  // namespace
}  // namespace milvus::storage::test
