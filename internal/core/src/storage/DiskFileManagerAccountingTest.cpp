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

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "indexbuilder/IndexBuildCapiAdapter.h"
#include "pb/cgo_msg.pb.h"
#include "storage/DiskFileManagerImpl.h"
#include "storage/Util.h"
#include "storage/artifact/FileSink.h"
#include "test_utils/Constants.h"

using namespace milvus;
using namespace milvus::storage;

class DiskFileManagerAccountingTest : public testing::Test {
 protected:
    void
    SetUp() override {
        StorageConfig config;
        config.storage_type = "local";
        config.root_path = TestRemotePath + "streamed_index_accounting";
        cm_ = CreateChunkManager(config);
        // Local object keys are complete paths, so the filesystem is rooted at /.
        auto fs_config = config;
        fs_config.root_path = "/";
        auto fs = InitArrowFileSystem(fs_config);
        FieldDataMeta field_meta = {1, 2, 3, 100};
        IndexMeta index_meta = {3, 100, 1000, 1, "index"};
        context_ = FileManagerContext(field_meta, index_meta, cm_, fs);
        file_manager_ = std::make_shared<DiskFileManagerImpl>(context_);
    }

    void
    TearDown() override {
        file_manager_.reset();
        std::filesystem::remove_all(cm_->GetRootPath());
    }

    ChunkManagerPtr cm_;
    FileManagerContext context_;
    std::shared_ptr<DiskFileManagerImpl> file_manager_;
};

TEST_F(DiskFileManagerAccountingTest, RepeatedMetadataReplacesFileSize) {
    EXPECT_EQ(file_manager_->GetAddedTotalFileSize(), 0);
    EXPECT_TRUE(file_manager_->GetRemotePathsToFileSize().empty());

    // Repeat, grow, shrink, empty, and then restore the same remote object.
    for (size_t size : {1024, 1024, 4096, 256, 0, 512}) {
        SCOPED_TRACE(size);
        FileMeta meta;
        meta.file_path = "local/index";
        meta.file_size = size;
        ASSERT_TRUE(file_manager_->AddFileMeta(meta));
        EXPECT_EQ(file_manager_->GetAddedTotalFileSize(), size);
        const auto& files = file_manager_->GetRemotePathsToFileSize();
        ASSERT_EQ(files.size(), 1);
        EXPECT_EQ(files.at(file_manager_->GetRemoteIndexPrefix() + "/index"),
                  size);
    }

    // Different local directories with the same basename address one object.
    FileMeta meta;
    meta.file_path = "another/local/index";
    meta.file_size = 128;
    ASSERT_TRUE(file_manager_->AddFileMeta(meta));
    EXPECT_EQ(file_manager_->GetAddedTotalFileSize(), 128);
    const auto& files = file_manager_->GetRemotePathsToFileSize();
    ASSERT_EQ(files.size(), 1);
    EXPECT_EQ(files.at(file_manager_->GetRemoteIndexPrefix() + "/index"), 128);
}

TEST_F(DiskFileManagerAccountingTest, MetadataAggregatesDistinctFiles) {
    const auto prefix = file_manager_->GetRemoteIndexPrefix() + "/";
    const std::vector<std::pair<std::string, size_t>> registrations = {
        {"empty", 0}, {"first", 1024}, {"second", 4096}, {"first", 256}};
    const std::vector<size_t> totals = {0, 1024, 5120, 4352};
    std::map<std::string, int64_t> expected_files;
    for (size_t i = 0; i < registrations.size(); ++i) {
        SCOPED_TRACE(i);
        FileMeta meta;
        meta.file_path = "local/" + registrations[i].first;
        meta.file_size = registrations[i].second;
        ASSERT_TRUE(file_manager_->AddFileMeta(meta));
        expected_files[prefix + registrations[i].first] = meta.file_size;
        EXPECT_EQ(file_manager_->GetRemotePathsToFileSize(), expected_files);
        EXPECT_EQ(file_manager_->GetAddedTotalFileSize(), totals[i]);
    }
}

TEST_F(DiskFileManagerAccountingTest, StreamedFileSizesReachSerializedStats) {
    V1DiskSink empty_sink(context_);
    const auto empty_stats = empty_sink.Finish();
    EXPECT_EQ(empty_stats.MemSize(), 0);
    EXPECT_TRUE(empty_stats.Files().empty());

    const auto source_dir =
        std::filesystem::path(cm_->GetRootPath()) / "borrowed";
    std::filesystem::create_directories(source_dir);
    const std::vector<std::pair<std::string, size_t>> entries = {
        {"empty", 0}, {"first", 1024}, {"second", 4096}};
    for (const auto storage_namespace :
         {ArtifactStorageNamespace::Index, ArtifactStorageNamespace::TextLog}) {
        SCOPED_TRACE(static_cast<int>(storage_namespace));
        V1DiskSink sink(context_, storage_namespace);
        const auto prefix = storage_namespace == ArtifactStorageNamespace::Index
                                ? file_manager_->GetRemoteIndexPrefix()
                                : file_manager_->GetRemoteTextLogPrefix();
        std::map<std::string, int64_t> expected_files;
        for (const auto& [name, size] : entries) {
            const auto path = (source_dir / name).string();
            {
                std::ofstream file(path, std::ios::binary);
                const std::vector<char> bytes(size, 42);
                if (!bytes.empty()) {
                    file.write(bytes.data(), bytes.size());
                }
                ASSERT_TRUE(file.good());
            }
            sink.WriteRawEntryFromLocalFile(name, path);
            EXPECT_EQ(std::filesystem::file_size(path), size);
            EXPECT_EQ(cm_->Size(prefix + "/" + name), size);
            const auto published_name =
                storage_namespace == ArtifactStorageNamespace::Index
                    ? prefix + "/" + name
                    : name;
            expected_files[published_name] = size;
        }

        const auto stats = sink.Finish();
        EXPECT_EQ(stats.MemSize(), 5120);
        const auto projected = indexbuilder::AdaptArtifactStats(stats);
        std::string serialized;
        ASSERT_TRUE(projected.SerializeToString(&serialized));
        milvus::proto::cgo::IndexStats decoded;
        ASSERT_TRUE(decoded.ParseFromString(serialized));
        EXPECT_EQ(decoded.mem_size(), 5120);
        ASSERT_EQ(decoded.serialized_index_infos_size(), expected_files.size());
        std::map<std::string, int64_t> decoded_files;
        for (const auto& info : decoded.serialized_index_infos()) {
            decoded_files.emplace(info.file_name(), info.file_size());
        }
        EXPECT_EQ(decoded_files, expected_files);
    }
}
