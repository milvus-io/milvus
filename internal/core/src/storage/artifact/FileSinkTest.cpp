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
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/Common.h"
#include "common/Slice.h"
#include "storage/ChunkManager.h"
#include "storage/FileManager.h"
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
                          ArtifactStoragePath::Index,
                          V1SourceLayout::MemoryEntries);
    EXPECT_EQ(source.EntryNames(), (std::vector<std::string>{"payload"}));
    EXPECT_EQ(source.EntrySize("payload"), payload.size());
    EXPECT_EQ(source.ReadEntry("payload"),
              (std::vector<uint8_t>(payload.begin(), payload.end())));

    sink.ReleaseLocalStaging();
}

}  // namespace
}  // namespace milvus::storage::test
