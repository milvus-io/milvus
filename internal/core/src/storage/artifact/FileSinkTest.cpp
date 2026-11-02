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

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <fstream>
#include <filesystem>
#include <iterator>
#include <memory>
#include <set>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "common/Common.h"
#include "common/Slice.h"
#include "storage/ChunkManager.h"
#include "storage/FileManager.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "storage/artifact/FileSink.h"
#include "storage/artifact/FileSource.h"
#include "storage/artifact/LocalDirectory.h"
#include "storage/artifact/LocalFileUtils.h"

namespace milvus::storage::test {

namespace {

class BorrowedFileWriter final : public IndexEntryWriter {
 public:
    bool fail{false};
    int descriptor{-1};
    std::string bytes;

    void
    WriteEntry(const std::string&, const void*, size_t) override {
        ADD_FAILURE()
            << "file must be streamed through the descriptor overload";
    }
    void
    WriteEntry(const std::string& name, int fd, size_t size) override {
        descriptor = fd;
        if (fail) {
            ThrowInfo(FileWriteFailed, "injected write failure");
        }
        bytes.resize(size);
        ReadAll(fd, bytes.data(), size, name, "borrowed artifact test");
    }
    void
    Finish() override {
    }
    size_t
    GetTotalBytesWritten() const override {
        return bytes.size();
    }
};

TEST(V3ArtifactWriterTest, BorrowedFileSurvivesSuccessAndFailure) {
    const auto directory = LocalDirectory::CreateOwned(
        std::filesystem::temp_directory_path().string(),
        "milvus-v3-writer-test-XXXXXX",
        "V3 writer test");
    const auto path = directory->Path() + "/payload";
    {
        std::ofstream file(path, std::ios::binary);
        file << "payload";
        ASSERT_TRUE(file.good());
    }
    BorrowedFileWriter writer;
    WriteEntryFromLocalFile(writer, "payload", path);
    EXPECT_EQ(writer.bytes, "payload");
    EXPECT_EQ(::fcntl(writer.descriptor, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
    writer.fail = true;
    EXPECT_THROW(WriteEntryFromLocalFile(writer, "payload", path),
                 SegcoreError);
    EXPECT_EQ(::fcntl(writer.descriptor, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
    EXPECT_EQ(std::filesystem::file_size(path), 7);
}

class FileSliceSizeGuard {
 public:
    explicit FileSliceSizeGuard(int64_t value)
        : original_(FILE_SLICE_SIZE.load()) {
        FILE_SLICE_SIZE.store(value);
    }

    ~FileSliceSizeGuard() {
        FILE_SLICE_SIZE.store(original_);
    }

 private:
    int64_t original_;
};

class RemoteFileGuard {
 public:
    RemoteFileGuard(ChunkManagerPtr manager, std::vector<std::string> paths)
        : manager_(std::move(manager)), paths_(std::move(paths)) {
    }

    ~RemoteFileGuard() {
        for (const auto& path : paths_) {
            try {
                if (manager_->Exist(path)) {
                    manager_->Remove(path);
                }
            } catch (...) {
            }
        }
    }

 private:
    ChunkManagerPtr manager_;
    std::vector<std::string> paths_;
};

TEST(V1ArtifactIOTest, RoundTripsSlicedEntryThroughProductionTransport) {
    FileSliceSizeGuard slice_size(3);
    auto remote_root = LocalDirectory::CreateOwned(
        std::filesystem::temp_directory_path().string(),
        "milvus-filesink-test-XXXXXX",
        "V1 artifact IO test");
    StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = remote_root->Path() + "/";
    auto chunk_manager = CreateChunkManager(storage_config);
    auto filesystem = InitArrowFileSystem(storage_config);
    FieldDataMeta field_meta = {1, 2, 3, 100};
    IndexMeta index_meta = {
        3, 100, 1000, 1, "artifact_io", "field", DataType::INT64, 1, false};
    FileManagerContext context(
        field_meta, index_meta, chunk_manager, std::move(filesystem));

    V1DiskSink sink(context);
    const std::array<uint8_t, 8> payload = {
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h'};
    sink.WriteEntry("payload", payload.data(), payload.size());
    const auto stats = sink.Finish();

    std::vector<std::string> remote_paths;
    std::set<std::string> physical_names;
    remote_paths.reserve(stats.Files().size());
    for (const auto& file : stats.Files()) {
        remote_paths.push_back(file.file_name);
        physical_names.insert(
            std::filesystem::path(file.file_name).filename().string());
    }
    RemoteFileGuard remote_files(chunk_manager, remote_paths);

    EXPECT_EQ(
        physical_names,
        (std::set<std::string>{
            INDEX_FILE_SLICE_META, "payload_0", "payload_1", "payload_2"}));

    V1RemoteSource source(context,
                          remote_paths,
                          {},
                          ArtifactStorageNamespace::Index,
                          V1SourceLayout::MemoryEntries);
    EXPECT_EQ(source.EntryNames(), (std::vector<std::string>{"payload"}));
    EXPECT_EQ(source.EntrySize("payload"), payload.size());
    EXPECT_EQ(source.ReadEntry("payload"),
              (std::vector<uint8_t>(payload.begin(), payload.end())));

    sink.ReleaseLocalStaging();
}

class V1BorrowedFileTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        const auto local =
            LocalChunkManagerSingleton::GetInstance().GetChunkManager();
        root_ = LocalDirectory::CreateOwned(local->GetRootPath(),
                                            "milvus-borrowed-file-XXXXXX",
                                            "V1 borrowed file test");
        StorageConfig config;
        config.storage_type = "local";
        config.root_path = root_->Path() + "/remote";
        context_ =
            std::make_unique<FileManagerContext>(FieldDataMeta{1, 2, 3, 100},
                                                 IndexMeta{3,
                                                           100,
                                                           740074,
                                                           1,
                                                           "borrowed_file",
                                                           "field",
                                                           DataType::INT64,
                                                           1,
                                                           false},
                                                 CreateChunkManager(config),
                                                 InitArrowFileSystem(config));
    }

    std::filesystem::path
    StagedPath(const std::string& name) const {
        const auto local =
            LocalChunkManagerSingleton::GetInstance().GetChunkManager();
        auto prefix = GenIndexPathPrefix(local, 740074, 1, 3, 100, false);
        while (!prefix.empty() && prefix.back() == '/') {
            prefix.pop_back();
        }
        const auto base = std::filesystem::path(prefix);
        for (const auto& entry :
             std::filesystem::directory_iterator(base.parent_path())) {
            const auto path = entry.path() / "artifact_sink" / name;
            if (entry.path().filename().string().starts_with(
                    base.filename().string() + "_") &&
                std::filesystem::exists(path)) {
                return path;
            }
        }
        return {};
    }

    std::shared_ptr<LocalDirectory> root_;
    std::unique_ptr<FileManagerContext> context_;
};

TEST_F(V1BorrowedFileTest, SameFilesystemLinksAndPreservesBorrowedSource) {
    FileSliceSizeGuard slice_size(3);
    const auto source_path = root_->Path() + "/borrowed";
    const auto name = std::filesystem::path(root_->Path()).filename().string();
    const std::string payload("payload\0abc", 11);
    {
        std::ofstream file(source_path, std::ios::binary);
        file.write(payload.data(), payload.size());
        ASSERT_TRUE(file.good());
    }
    std::vector<std::string> remote_paths;
    {
        V1DiskSink sink(*context_);
        sink.WriteEntryFromLocalFile(name, source_path);
        EXPECT_EQ(std::filesystem::hard_link_count(source_path), 2);
        const auto stats = sink.Finish();
        for (const auto& file : stats.Files()) {
            remote_paths.push_back(file.file_name);
        }
    }
    EXPECT_EQ(std::filesystem::hard_link_count(source_path), 1);
    std::ifstream file(source_path, std::ios::binary);
    EXPECT_EQ(std::string(std::istreambuf_iterator<char>(file), {}), payload);
    V1RemoteSource source(*context_,
                          remote_paths,
                          {},
                          ArtifactStorageNamespace::Index,
                          V1SourceLayout::DiskFiles);
    EXPECT_EQ(source.ReadEntry(name),
              (std::vector<uint8_t>(payload.begin(), payload.end())));
}

TEST_F(V1BorrowedFileTest, LinkFailuresPreserveDiagnosticsAndExistingEntry) {
    {
        V1DiskSink sink(*context_);
        try {
            sink.WriteEntryFromLocalFile("missing", root_->Path() + "/missing");
            FAIL() << "a missing source must fail link and copy";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), FileWriteFailed);
            const auto missing =
                std::make_error_code(std::errc::no_such_file_or_directory)
                    .message();
            const std::string message(error.what());
            EXPECT_NE(message.find("hard link: " + missing), std::string::npos);
            EXPECT_NE(message.find("copy: " + missing), std::string::npos);
        }
        EXPECT_THROW(sink.Finish(), SegcoreError);
    }
    const auto source_path = root_->Path() + "/borrowed";
    const auto name = std::filesystem::path(root_->Path()).filename().string();
    {
        std::ofstream file(source_path);
        file << "source";
        ASSERT_TRUE(file.good());
    }
    V1DiskSink sink(*context_);
    sink.WriteEntryFromLocalFile(name + "_seed", source_path);
    const auto seed = StagedPath(name + "_seed");
    ASSERT_FALSE(seed.empty());
    const auto conflict = seed.parent_path() / name;
    {
        std::ofstream file(conflict);
        file << "existing";
        ASSERT_TRUE(file.good());
    }
    try {
        sink.WriteEntryFromLocalFile(name, source_path);
        FAIL() << "an existing entry must reject staging";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), FileWriteFailed);
        EXPECT_EQ(std::string(error.what()).find("; copy:"), std::string::npos);
    }
    std::ifstream file(conflict);
    EXPECT_EQ(std::string(std::istreambuf_iterator<char>(file), {}),
              "existing");
}

}  // namespace
}  // namespace milvus::storage::test
