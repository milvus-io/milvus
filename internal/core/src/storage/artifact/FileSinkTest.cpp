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

#include <sys/stat.h>
#include <unistd.h>

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

// Internal hook defined only in FileSink.cpp; no production header exposes it.
std::error_code
ExchangeV1SinkHardLinkError(std::error_code error);

namespace {

class HardLinkErrorGuard {
 public:
    explicit HardLinkErrorGuard(std::error_code error)
        : original_(ExchangeV1SinkHardLinkError(error)) {
    }

    ~HardLinkErrorGuard() {
        ExchangeV1SinkHardLinkError(original_);
    }

 private:
    std::error_code original_;
};

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

class V1BorrowedFileTest
    : public ::testing::TestWithParam<ArtifactStorageNamespace> {
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
        FieldDataMeta field_meta = {1, 2, 3, 100};
        IndexMeta index_meta = {3,
                                100,
                                740074,
                                1,
                                "borrowed_file",
                                "field",
                                DataType::INT64,
                                1,
                                false};
        context_ =
            std::make_unique<FileManagerContext>(field_meta,
                                                 index_meta,
                                                 CreateChunkManager(config),
                                                 InitArrowFileSystem(config));
    }

    std::filesystem::path
    StagedPath(const std::string& name) const {
        const auto local =
            LocalChunkManagerSingleton::GetInstance().GetChunkManager();
        auto prefix =
            GetParam() == ArtifactStorageNamespace::Index
                ? GenIndexPathPrefix(local, 740074, 1, 3, 100, false)
                : GenTextIndexPathPrefix(local, 740074, 1, 3, 100, false);
        while (!prefix.empty() && prefix.back() == '/') {
            prefix.pop_back();
        }
        const auto base = std::filesystem::path(prefix);
        const auto generation_prefix = base.filename().string() + "_";
        for (const auto& entry :
             std::filesystem::directory_iterator(base.parent_path())) {
            if (entry.path().filename().string().starts_with(
                    generation_prefix)) {
                const auto path = entry.path() / "artifact_sink" / name;
                if (std::filesystem::exists(path)) {
                    return path;
                }
            }
        }
        return {};
    }

    void
    ExpectUpload(const std::string& source_parent,
                 bool expect_hard_link,
                 bool release_explicitly,
                 std::errc forced_link_error = std::errc{}) {
        FileSliceSizeGuard slice_size(3);
        HardLinkErrorGuard link_error(std::make_error_code(forced_link_error));
        const auto borrowed = LocalDirectory::CreateOwned(
            source_parent, "milvus-source-XXXXXX", "borrowed source test");
        const auto name =
            std::filesystem::path(borrowed->Path()).filename().string() +
            ".bin";
        const auto source_path = borrowed->Path() + "/" + name;
        const std::string payload("payload\0abc", 11);
        {
            std::ofstream file(source_path, std::ios::binary);
            file.write(payload.data(), payload.size());
            ASSERT_TRUE(file.good());
        }
        std::vector<std::string> remote_paths;
        std::filesystem::path staged_path;
        {
            V1DiskSink sink(*context_, GetParam());
            sink.WriteEntryFromLocalFile(name, source_path);
            staged_path = StagedPath(name);
            ASSERT_FALSE(staged_path.empty());
            struct stat source_stat {};
            struct stat staged_stat {};
            ASSERT_EQ(::stat(source_path.c_str(), &source_stat), 0);
            ASSERT_EQ(::stat(staged_path.c_str(), &staged_stat), 0);
            if (expect_hard_link) {
                EXPECT_EQ(source_stat.st_dev, staged_stat.st_dev);
                EXPECT_EQ(source_stat.st_ino, staged_stat.st_ino);
                EXPECT_EQ(source_stat.st_nlink, 2);
            } else {
                if (forced_link_error != std::errc{}) {
                    EXPECT_EQ(source_stat.st_dev, staged_stat.st_dev);
                    EXPECT_NE(source_stat.st_ino, staged_stat.st_ino);
                } else {
                    EXPECT_NE(source_stat.st_dev, staged_stat.st_dev);
                }
                EXPECT_EQ(source_stat.st_nlink, 1);
                EXPECT_EQ(staged_stat.st_nlink, 1);
            }
            const auto stats = sink.Finish();
            for (const auto& file : stats.Files()) {
                remote_paths.push_back(file.file_name);
            }
            ASSERT_EQ(remote_paths.size(), 4);
            if (release_explicitly) {
                sink.ReleaseLocalStaging();
                sink.ReleaseLocalStaging();
                EXPECT_FALSE(std::filesystem::exists(staged_path));
                EXPECT_TRUE(std::filesystem::exists(source_path));
            }
        }
        // The sink's DiskFileManager is gone before checking the borrowed path
        // and reopening the actual sliced remote objects.
        EXPECT_FALSE(std::filesystem::exists(staged_path));
        ASSERT_TRUE(std::filesystem::exists(source_path));
        EXPECT_EQ(std::filesystem::hard_link_count(source_path), 1);
        std::ifstream file(source_path, std::ios::binary);
        EXPECT_EQ(std::string(std::istreambuf_iterator<char>(file), {}),
                  payload);
        V1RemoteSource source(
            *context_, remote_paths, {}, GetParam(), V1SourceLayout::DiskFiles);
        EXPECT_EQ(source.ReadEntry(name),
                  (std::vector<uint8_t>(payload.begin(), payload.end())));
    }

    std::shared_ptr<LocalDirectory> root_;
    std::unique_ptr<FileManagerContext> context_;
};

TEST_P(V1BorrowedFileTest, SameFilesystemLinksAndPreservesBorrowedSource) {
    for (const bool release_explicitly : {false, true}) {
        SCOPED_TRACE(release_explicitly);
        ExpectUpload(root_->Path(), true, release_explicitly);
    }
}

TEST_P(V1BorrowedFileTest, CrossFilesystemCopiesAndPreservesBorrowedSource) {
    struct stat local_stat {};
    struct stat shm_stat {};
    ASSERT_EQ(::stat(root_->Path().c_str(), &local_stat), 0);
    if (::stat("/dev/shm", &shm_stat) != 0 ||
        ::access("/dev/shm", W_OK | X_OK) != 0 ||
        local_stat.st_dev == shm_stat.st_dev) {
        GTEST_SKIP()
            << "/dev/shm is unavailable or shares the staging filesystem";
    }
    ExpectUpload("/dev/shm", false, true);
}

TEST_P(V1BorrowedFileTest,
       NonCrossDeviceLinkFailuresCopyAndPreserveBorrowedSource) {
    for (const auto error : {std::errc::operation_not_permitted,
                             std::errc::operation_not_supported,
                             std::errc::too_many_links}) {
        SCOPED_TRACE(std::make_error_code(error).message());
        ExpectUpload(root_->Path(), false, true, error);
    }
}

TEST_P(V1BorrowedFileTest, LinkAndCopyFailuresReportBothErrors) {
    const auto link_error =
        std::make_error_code(std::errc::operation_not_permitted);
    const auto copy_error =
        std::make_error_code(std::errc::no_such_file_or_directory);
    HardLinkErrorGuard injected(link_error);
    V1DiskSink sink(*context_, GetParam());
    try {
        sink.WriteEntryFromLocalFile("payload", root_->Path() + "/missing");
        FAIL() << "copying a missing borrowed source must fail";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), FileWriteFailed);
        const std::string message(error.what());
        EXPECT_NE(message.find("hard link: " + link_error.message()),
                  std::string::npos);
        EXPECT_NE(message.find("copy: " + copy_error.message()),
                  std::string::npos);
    }
    EXPECT_THROW(sink.Finish(), SegcoreError);
}

TEST_P(V1BorrowedFileTest, ExistingStagingEntryFailsWithoutOverwriting) {
    const auto source_path = root_->Path() + "/borrowed";
    const auto name = std::filesystem::path(root_->Path()).filename().string();
    {
        std::ofstream file(source_path, std::ios::binary);
        file << "source";
        ASSERT_TRUE(file.good());
    }
    std::filesystem::path conflict;
    {
        V1DiskSink sink(*context_, GetParam());
        sink.WriteEntryFromLocalFile(name + "_seed", source_path);
        const auto seed = StagedPath(name + "_seed");
        ASSERT_FALSE(seed.empty());
        conflict = seed.parent_path() / name;
        {
            std::ofstream file(conflict, std::ios::binary);
            file << "existing";
            ASSERT_TRUE(file.good());
        }
        try {
            sink.WriteEntryFromLocalFile(name, source_path);
            FAIL() << "an existing staging entry must reject the hard link";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), FileWriteFailed);
            EXPECT_EQ(std::string(error.what()).find("; copy:"),
                      std::string::npos);
        }
        std::ifstream file(conflict, std::ios::binary);
        EXPECT_EQ(std::string(std::istreambuf_iterator<char>(file), {}),
                  "existing");
        EXPECT_THROW(sink.Finish(), SegcoreError);
    }
    EXPECT_FALSE(std::filesystem::exists(conflict));
    EXPECT_TRUE(std::filesystem::exists(source_path));
    EXPECT_EQ(std::filesystem::hard_link_count(source_path), 1);
}

INSTANTIATE_TEST_SUITE_P(IndexAndTextLog,
                         V1BorrowedFileTest,
                         ::testing::Values(ArtifactStorageNamespace::Index,
                                           ArtifactStorageNamespace::TextLog),
                         [](const auto& info) {
                             return info.param ==
                                            ArtifactStorageNamespace::Index
                                        ? "Index"
                                        : "TextLog";
                         });

}  // namespace
}  // namespace milvus::storage::test
