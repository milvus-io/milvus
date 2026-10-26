// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>

#include "arrow/buffer.h"
#include "index/vector/VectorDiskBuildFileManager.h"

namespace milvus::index::test {

TEST(VectorDiskBuildFileManagerTest, DefaultOutputUsesLocalFile) {
    auto files = storage::LocalDirectory::CreateOwned(
        "/tmp", "vector_local_output_XXXXXX", "vector output test");
    auto manager = std::make_shared<VectorDiskBuildFileManager>(files);
    const auto path = files->Path() + "/nested/index";
    // Exercise the default through the shared interface used by the engine.
    std::shared_ptr<milvus::FileManager> base = manager;
    auto stream = base->OpenOutputStream(path);
    EXPECT_EQ(stream->Write("payload", 7), 7);
    ASSERT_TRUE(manager->AddFileMeta({path, 7}));
    EXPECT_ANY_THROW(manager->LocalFiles());
    stream->Close();
    EXPECT_NO_THROW(stream->Close());
    EXPECT_EQ(stream->Tell(), 7);
    EXPECT_TRUE(manager->TakeOutput().empty());
    const auto output = manager->LocalFiles();
    ASSERT_EQ(output.size(), 1);
    EXPECT_EQ(output[0].path, path);
    EXPECT_EQ(output[0].size, 7);
    EXPECT_EQ(output[0].transport, VectorDiskFileTransport::RawUnsliced);
    std::ifstream input(path);
    std::string contents;
    input >> contents;
    EXPECT_EQ(contents, "payload");
    manager->ResetOutput();
    EXPECT_TRUE(std::filesystem::exists(path));
}

TEST(VectorDiskBuildFileManagerTest, SizeHintDoesNotEnableMemoryOutput) {
    auto files = storage::LocalDirectory::CreateOwned(
        "/tmp", "vector_output_modes_XXXXXX", "vector output modes test");
    auto manager = std::make_shared<VectorDiskBuildFileManager>(files);
    auto local = manager->OpenOutputStream(files->Path() + "/local",
                                           {.expected_size = 7});
    auto memory = manager->OpenOutputStream(files->Path() + "/memory",
                                            {.use_memory = true});
    local->Write("local", 5);
    memory->Write("memory", 6);
    local->Close();
    memory->Close();
    ASSERT_EQ(manager->LocalFiles().size(), 1);
    EXPECT_EQ(manager->LocalFiles()[0].size, 5);
    const auto buffers = manager->TakeOutput();
    ASSERT_EQ(buffers.size(), 1);
    EXPECT_EQ(buffers.at(files->Path() + "/memory")->ToString(), "memory");
    EXPECT_FALSE(std::filesystem::exists(files->Path() + "/memory"));
}

TEST(VectorDiskBuildFileManagerTest,
     IncompleteLocalOutputIsRemovedBeforeRetry) {
    auto files = storage::LocalDirectory::CreateOwned(
        "/tmp", "vector_local_retry_XXXXXX", "vector local retry test");
    auto manager = std::make_shared<VectorDiskBuildFileManager>(files);
    const auto path = files->Path() + "/index";
    {
        auto stream = manager->OpenOutputStream(path);
        stream->Write("pay", 3);
        ASSERT_TRUE(manager->AddFileMeta({path, 3}));
        stream->Write("load", 4);
        EXPECT_ANY_THROW(stream->Close());
        EXPECT_ANY_THROW(manager->RethrowFirstFailure());
    }
    EXPECT_FALSE(std::filesystem::exists(path));
    EXPECT_TRUE(manager->LocalFiles().empty());
    manager->ResetOutput();
    EXPECT_NO_THROW(manager->RethrowFirstFailure());
    auto stream = manager->OpenOutputStream(path);
    stream->Write("payload", 7);
    stream->Close();
    // A completed serialization can also be replaced on an upload retry.
    stream = manager->OpenOutputStream(path);
    stream->Write("retry", 5);
    stream->Close();
    ASSERT_EQ(manager->LocalFiles().size(), 1);
    EXPECT_EQ(manager->LocalFiles()[0].size, 5);
}

TEST(VectorDiskBuildFileManagerTest,
     BufferedOutputPreservesClosedStreamAndMetadata) {
    auto files = storage::LocalDirectory::CreateOwned(
        "/tmp", "vector_output_XXXXXX", "vector output test");
    auto manager = std::make_shared<VectorDiskBuildFileManager>(files);
    const auto path = files->Path() + "/index";
    auto stream = manager->OpenOutputStream(
        path, {.expected_size = 7, .use_memory = true});
    EXPECT_EQ(stream->Write("pay", 3), 3);
    EXPECT_EQ(stream->Tell(), 3);
    EXPECT_EQ(stream->Write("load", 4), 4);
    stream->Close();
    EXPECT_NO_THROW(stream->Close());
    EXPECT_EQ(stream->Tell(), 7);
    EXPECT_TRUE(manager->AddFileMeta({path, 7}));
    EXPECT_TRUE(manager->LocalFiles().empty());
    EXPECT_FALSE(std::filesystem::exists(path));

    auto output = manager->TakeOutput();
    ASSERT_EQ(output.size(), 1);
    EXPECT_EQ(output.at(path)->ToString(), "payload");
    EXPECT_TRUE(manager->TakeOutput().empty());
    manager->ResetOutput();
    EXPECT_EQ(output.at(path)->ToString(), "payload");
}

TEST(VectorDiskBuildFileManagerTest,
     PreparesBufferedOutputBeforeProducerWritesAndRetainsFailures) {
    auto files = storage::LocalDirectory::CreateOwned(
        "/tmp", "vector_prepare_output_XXXXXX", "prepare output test");
    auto manager = std::make_shared<VectorDiskBuildFileManager>(files);
    const auto path = files->Path() + "/index";
    size_t preparations = 0;
    manager->SetOutputPreparer([&](const std::string& name, size_t size) {
        EXPECT_EQ(name, path);
        EXPECT_EQ(size, 7);
        // Called outside the manager lock, before returning to the producer.
        EXPECT_TRUE(manager->LocalFiles().empty());
        ++preparations;
    });
    auto stream = manager->OpenOutputStream(
        path, {.expected_size = 7, .use_memory = true});
    EXPECT_EQ(preparations, 1);
    EXPECT_EQ(stream->Tell(), 0);
    stream->Write("payload", 7);
    stream->Close();
    manager->ResetOutput();

    manager->SetOutputPreparer([](const std::string&, size_t) {
        throw std::runtime_error("preparation failed");
    });
    EXPECT_ANY_THROW(manager->OpenOutputStream(
        path, {.expected_size = 7, .use_memory = true}));
    EXPECT_THROW(manager->RethrowFirstFailure(), std::runtime_error);
    manager->SetOutputPreparer({});
    manager->ResetOutput();
    EXPECT_NO_THROW(manager->OpenOutputStream(
        path, {.expected_size = 7, .use_memory = true}));
}

TEST(VectorDiskBuildFileManagerTest, FailedOutputCanBeDiscardedBeforeRetry) {
    auto files = storage::LocalDirectory::CreateOwned(
        "/tmp", "vector_output_retry_XXXXXX", "vector output retry test");
    auto manager = std::make_shared<VectorDiskBuildFileManager>(files);
    const auto path = files->Path() + "/index";
    auto stream = manager->OpenOutputStream(
        path, {.expected_size = 7, .use_memory = true});
    stream->Write("pay", 3);
    EXPECT_FALSE(manager->AddFileMeta({path, 7}));
    EXPECT_ANY_THROW(manager->RethrowFirstFailure());
    manager->ResetOutput();
    EXPECT_NO_THROW(manager->RethrowFirstFailure());

    stream = manager->OpenOutputStream(
        path, {.expected_size = 7, .use_memory = true});
    stream->Write("payload", 7);
    stream->Close();
    ASSERT_TRUE(manager->AddFileMeta({path, 7}));
    auto output = manager->TakeOutput();
    ASSERT_EQ(output.size(), 1);
    EXPECT_EQ(output.at(path)->ToString(), "payload");
}

TEST(VectorDiskBuildFileManagerTest,
     BufferedOutputRetainsWriteFailureWithoutOwningManager) {
    auto files = storage::LocalDirectory::CreateOwned(
        "/tmp", "vector_output_error_XXXXXX", "vector output error test");
    auto manager = std::make_shared<VectorDiskBuildFileManager>(files);
    auto stream = manager->OpenOutputStream(files->Path() + "/index",
                                            {.use_memory = true});
    try {
        stream->Write(-1, 1);
        FAIL() << "invalid source descriptor must fail";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), FileReadFailed);
        try {
            manager->RethrowFirstFailure();
            FAIL() << "the manager must retain the stream failure";
        } catch (const SegcoreError& retained) {
            EXPECT_EQ(retained.get_error_code(), error.get_error_code());
            EXPECT_STREQ(retained.what(), error.what());
        }
    }
    manager->ResetOutput();
    stream = manager->OpenOutputStream(files->Path() + "/retry",
                                       {.use_memory = true});
    stream->Close();
    EXPECT_ANY_THROW(stream->Write("x", 1));
    EXPECT_ANY_THROW(manager->RethrowFirstFailure());
    std::weak_ptr<VectorDiskBuildFileManager> owner = manager;
    manager.reset();
    EXPECT_TRUE(owner.expired());
}

}  // namespace milvus::index::test
