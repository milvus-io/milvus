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
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <future>
#include <optional>
#include <string>

#include "common/EasyAssert.h"
#include "local/FileSystem.h"
#include "local/ManagedSubtree.h"

namespace milvus::local {
namespace {

namespace fs = std::filesystem;

class ManagedSubtreeTest : public testing::Test {
 protected:
    void
    SetUp() override {
        root_ = fs::temp_directory_path() / "milvus_managed_subtree" /
                testing::UnitTest::GetInstance()->current_test_info()->name();
        fs::remove_all(root_);
        files_.emplace(FileSystem::Open(root_));
    }

    void
    TearDown() override {
        files_.reset();
        fs::remove_all(root_);
        fs::remove_all(OutsidePath());
    }

    fs::path
    OutsidePath() const {
        return root_.parent_path() / (root_.filename().string() + "_outside");
    }

    fs::path root_;
    std::optional<FileSystem> files_;
};

TEST_F(ManagedSubtreeTest, DefersRemovalUntilTheFinalWriterExits) {
    auto directory = files_->ManageSubtree(Path("artifact"));
    directory->Files().CreateDirectories(Path("work"));
    auto first = directory->AcquireWriter();
    std::optional<ManagedSubtree::WriteLease> second(
        directory->AcquireWriter());

    directory->RemoveWhenIdle();
    EXPECT_TRUE(fs::exists(root_ / "artifact"));

    try {
        static_cast<void>(directory->AcquireWriter());
        FAIL() << "expected a closing subtree to reject new writers";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FileWriteFailed);
    }

    first = std::move(*second);
    second.reset();
    EXPECT_TRUE(fs::exists(root_ / "artifact"));

    first = {};
    EXPECT_FALSE(fs::exists(root_ / "artifact"));

    directory->RemoveWhenIdle();
    directory->RemoveAndWait();
}

TEST_F(ManagedSubtreeTest, RemoveAndWaitBlocksForActiveWriters) {
    auto directory = files_->ManageSubtree(Path("artifact"));
    directory->Files().CreateDirectories(Path("work"));
    std::optional<ManagedSubtree::WriteLease> writer(
        directory->AcquireWriter());

    auto removal = std::async(std::launch::async,
                              [directory]() { directory->RemoveAndWait(); });
    EXPECT_EQ(removal.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);

    writer.reset();
    EXPECT_NO_THROW(removal.get());
    EXPECT_FALSE(fs::exists(root_ / "artifact"));
}

TEST_F(ManagedSubtreeTest, IndependentSubtreesDoNotShareLifecycleState) {
    auto first = files_->ManageSubtree(Path("first"));
    auto second = files_->ManageSubtree(Path("second"));
    first->Files().CreateDirectories(Path("work"));
    second->Files().CreateDirectories(Path("work"));
    auto second_writer = second->AcquireWriter();

    first->RemoveAndWait();

    EXPECT_FALSE(fs::exists(root_ / "first"));
    EXPECT_TRUE(fs::exists(root_ / "second"));
    EXPECT_NO_THROW(second->AcquireWriter());
}

TEST_F(ManagedSubtreeTest, RemoveAndWaitPropagatesCleanupFailure) {
    auto directory = files_->ManageSubtree(Path("artifact"));
    directory->Files().CreateDirectories(Path("work"));

    auto outside = OutsidePath();
    fs::create_directories(outside);
    fs::remove_all(root_ / "artifact");
    fs::create_directory_symlink(outside, root_ / "artifact");

    try {
        directory->RemoveAndWait();
        FAIL() << "expected subtree cleanup to report the symlink escape";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FileOpenFailed);
        EXPECT_NE(std::string(error.what()).find("artifact"),
                  std::string::npos);
        EXPECT_NE(std::string(error.what()).find("escapes"), std::string::npos);
    }

    EXPECT_TRUE(fs::exists(outside));
    EXPECT_THROW(directory->AcquireWriter(), SegcoreError);
}

TEST_F(ManagedSubtreeTest, ExplicitlyRetriesFailedDeferredCleanup) {
    const auto directory = files_->ManageSubtree(Path("artifact"));
    const auto outside = OutsidePath();
    fs::create_directories(outside);
    fs::create_directory_symlink(outside, root_ / "artifact");
    std::optional<ManagedSubtree::WriteLease> writer(
        directory->AcquireWriter());

    directory->RemoveWhenIdle();
    writer.reset();  // The final writer encounters the cleanup failure.
    EXPECT_THROW(directory->AcquireWriter(), SegcoreError);
    EXPECT_TRUE(fs::exists(outside));

    fs::remove(root_ / "artifact");
    fs::create_directories(root_ / "artifact/work");
    directory->RemoveWhenIdle();  // noexcept cleanup does not retry failures.
    EXPECT_TRUE(fs::exists(root_ / "artifact/work"));

    EXPECT_NO_THROW(directory->RemoveAndWait());
    EXPECT_FALSE(fs::exists(root_ / "artifact"));
    EXPECT_TRUE(fs::exists(outside));
    EXPECT_THROW(directory->AcquireWriter(), SegcoreError);
    EXPECT_NO_THROW(directory->RemoveAndWait());
}

TEST_F(ManagedSubtreeTest, SharedOwnerReportsCleanupFailureToAllWaiters) {
    const auto directory = files_->ManageSubtree(Path("artifact"));
    const auto outside = OutsidePath();
    fs::create_directories(outside);
    fs::create_directory_symlink(outside, root_ / "artifact");
    std::optional<ManagedSubtree::WriteLease> writer(
        directory->AcquireWriter());

    auto remove = [directory]() { directory->RemoveAndWait(); };
    auto first = std::async(std::launch::async, remove);
    auto second = std::async(std::launch::async, remove);
    EXPECT_EQ(first.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);
    EXPECT_EQ(second.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);
    writer.reset();

    for (auto* waiter : {&first, &second}) {
        try {
            waiter->get();
            FAIL() << "expected cleanup failure for each waiter";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), ErrorCode::FileOpenFailed);
            EXPECT_NE(std::string(error.what()).find("artifact"),
                      std::string::npos);
        }
    }
    fs::remove(root_ / "artifact");
    fs::create_directories(root_ / "artifact/work");
    EXPECT_NO_THROW(directory->RemoveAndWait());
    EXPECT_FALSE(fs::exists(root_ / "artifact"));
}

TEST_F(ManagedSubtreeTest, RetriesAfterDirectoryRemovalPermissionFailure) {
    if (geteuid() == 0) {
        GTEST_SKIP() << "root bypasses directory permissions";
    }
    const auto directory = files_->ManageSubtree(Path("artifact"));
    directory->Files().CreateDirectories(Path("work/remaining"));
    const auto blocked = root_ / "artifact/work";
    fs::permissions(blocked, fs::perms::none);
    struct RestorePermissions {
        fs::path path;
        ~RestorePermissions() {
            std::error_code error;
            fs::permissions(path, fs::perms::owner_all, error);
        }
    } restore{blocked};

    // The target directory passes path validation; remove_all itself fails.
    for (int attempt = 0; attempt < 2; ++attempt) {
        try {
            directory->RemoveAndWait();
            FAIL() << "expected directory removal to report permission denial";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(), ErrorCode::FileWriteFailed);
            EXPECT_NE(std::string(error.what()).find("remove subtree"),
                      std::string::npos);
            EXPECT_NE(std::string(error.what()).find("artifact"),
                      std::string::npos);
        }
        EXPECT_THROW(directory->AcquireWriter(), SegcoreError);
    }

    fs::permissions(blocked, fs::perms::owner_all);
    EXPECT_TRUE(fs::exists(blocked / "remaining"));
    EXPECT_NO_THROW(directory->RemoveAndWait());
    EXPECT_FALSE(fs::exists(root_ / "artifact"));

    // A completed owner must never delete a later generation at the same path.
    fs::create_directories(root_ / "artifact/new-generation");
    directory->RemoveWhenIdle();
    EXPECT_NO_THROW(directory->RemoveAndWait());
    EXPECT_TRUE(fs::exists(root_ / "artifact/new-generation"));
}

}  // namespace
}  // namespace milvus::local
