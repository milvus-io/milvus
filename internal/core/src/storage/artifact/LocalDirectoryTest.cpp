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

#include <cerrno>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include "index/test_utils/AssertHelpers.h"
#include "storage/artifact/LocalDirectory.h"
#include "storage/artifact/LocalFileUtils.h"

namespace milvus::storage::test {
namespace {

using milvus::index::test::ExpectSegcoreError;

std::shared_ptr<LocalDirectory>
MakeTestRoot() {
    return LocalDirectory::CreateOwned(std::filesystem::temp_directory_path(),
                                       "milvus-local-directory-test-XXXXXX",
                                       "LocalDirectory test");
}

void
WriteFile(const std::filesystem::path& path, std::string_view value = "x") {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output.good());
    output.write(value.data(), static_cast<std::streamsize>(value.size()));
    output.close();
    ASSERT_FALSE(output.fail());
}

TEST(LocalDirectoryTest, EmptyParentIsRejected) {
    ExpectSegcoreError(ErrorCode::UnexpectedError, [] {
        static_cast<void>(LocalDirectory::Pattern("", "child-XXXXXX", "test"));
    });
}

TEST(LocalDirectoryTest, PatternCreatesOnlyItsConfiguredParent) {
    auto root = MakeTestRoot();
    const auto parent = std::filesystem::path(root->Path()) / "nested" / "root";

    const auto pattern =
        LocalDirectory::Pattern(parent.string(), "child-XXXXXX", "test");

    EXPECT_TRUE(std::filesystem::is_directory(parent));
    EXPECT_EQ(pattern, (parent / "child-XXXXXX").string());
}

TEST(LocalDirectoryTest, OwnedDirectoriesAreUniqueAndPreserveParent) {
    auto root = MakeTestRoot();
    const auto parent = root->Path();
    auto first =
        LocalDirectory::CreateOwned(parent, "child-XXXXXX", "first child");
    auto second =
        LocalDirectory::CreateOwned(parent, "child-XXXXXX", "second child");

    EXPECT_NE(first->Path(), second->Path());
    EXPECT_TRUE(std::filesystem::is_directory(first->Path()));
    EXPECT_TRUE(std::filesystem::is_directory(second->Path()));
    first.reset();
    second.reset();
    EXPECT_TRUE(std::filesystem::is_directory(parent));
}

TEST(LocalDirectoryTest, LastSharedOwnerRemovesDirectoryTree) {
    auto root = MakeTestRoot();
    auto owner = LocalDirectory::CreateOwned(
        root->Path(), "owned-XXXXXX", "shared ownership");
    const auto owned_path = owner->Path();
    WriteFile(std::filesystem::path(owned_path) / "nested" / "entry");
    auto second_owner = owner;

    owner.reset();
    EXPECT_TRUE(std::filesystem::exists(owned_path));
    second_owner.reset();

    EXPECT_FALSE(std::filesystem::exists(owned_path));
    EXPECT_TRUE(std::filesystem::exists(root->Path()));
}

TEST(LocalDirectoryTest, UnarmedInstanceDoesNotRemoveAnExistingPath) {
    auto root = MakeTestRoot();
    const auto existing = std::filesystem::path(root->Path()) / "existing";
    WriteFile(existing / "entry");

    {
        LocalDirectory unarmed(existing.string());
        EXPECT_EQ(unarmed.Path(), existing.string());
    }

    EXPECT_TRUE(std::filesystem::exists(existing / "entry"));
}

TEST(LocalDirectoryTest, OwnsOnlyResolvedDescendants) {
    auto root = MakeTestRoot();
    auto owner = LocalDirectory::CreateOwned(
        root->Path(), "owned-XXXXXX", "ownership boundaries");
    const auto owned = std::filesystem::path(owner->Path());
    const auto child = owned / "child";
    const auto deep_missing = child / "missing" / "entry";
    std::filesystem::create_directories(child);
    const auto sibling =
        std::filesystem::path(owner->Path() + std::string("-sibling"));
    std::filesystem::create_directories(sibling);

    EXPECT_TRUE(owner->Owns(child.string()));
    EXPECT_TRUE(owner->Owns(deep_missing.string()));
    EXPECT_FALSE(owner->Owns(owner->Path()));
    EXPECT_FALSE(owner->Owns(root->Path()));
    EXPECT_FALSE(owner->Owns(sibling.string()));
    EXPECT_FALSE(owner->Owns(""));

    const auto outside = std::filesystem::path(root->Path()) / "outside";
    std::filesystem::create_directories(outside);
    std::error_code error;
    std::filesystem::create_directory_symlink(outside, owned / "escape", error);
    ASSERT_FALSE(error) << error.message();
    EXPECT_FALSE(owner->Owns((owned / "escape" / "entry").string()));
}

TEST(LocalDirectoryTest, HeapAccountingIncludesObjectAndOwnedPath) {
    auto owner = MakeTestRoot();

    EXPECT_EQ(owner->HeapBytes(),
              sizeof(LocalDirectory) + owner->PathHeapBytes());
    EXPECT_GE(owner->HeapBytes(), sizeof(LocalDirectory));
}

TEST(LocalEntryGuardTest, DestructorAndCheckedRemovalDeleteEntry) {
    auto root = MakeTestRoot();
    const auto first = std::filesystem::path(root->Path()) / "first";
    const auto second = std::filesystem::path(root->Path()) / "second";
    WriteFile(first);
    WriteFile(second);

    {
        LocalEntryGuard guard(first.string());
        EXPECT_EQ(guard.Path(), first.string());
    }
    EXPECT_FALSE(std::filesystem::exists(first));

    LocalEntryGuard checked(second.string());
    checked.RemoveChecked("test entry");
    EXPECT_FALSE(std::filesystem::exists(second));
    EXPECT_TRUE(checked.Path().empty());
}

TEST(LocalEntryGuardTest,
     MoveTransfersOwnershipAndAssignmentRemovesPriorEntry) {
    auto root = MakeTestRoot();
    const auto first = std::filesystem::path(root->Path()) / "first";
    const auto second = std::filesystem::path(root->Path()) / "second";
    WriteFile(first);
    WriteFile(second);

    {
        LocalEntryGuard original(first.string());
        LocalEntryGuard moved(std::move(original));
        EXPECT_TRUE(original.Path().empty());
        EXPECT_EQ(moved.Path(), first.string());

        LocalEntryGuard destination(second.string());
        destination = std::move(moved);
        EXPECT_FALSE(std::filesystem::exists(second));
        EXPECT_TRUE(moved.Path().empty());
        EXPECT_EQ(destination.Path(), first.string());
    }

    EXPECT_FALSE(std::filesystem::exists(first));
}

TEST(LocalEntryGuardTest, ReleaseDisarmsCleanup) {
    auto root = MakeTestRoot();
    const auto entry = std::filesystem::path(root->Path()) / "entry";
    WriteFile(entry);

    std::string released;
    {
        LocalEntryGuard guard(entry.string());
        released = guard.Release();
        EXPECT_TRUE(guard.Path().empty());
    }

    EXPECT_EQ(released, entry.string());
    EXPECT_TRUE(std::filesystem::exists(entry));
}

TEST(MappedRegionGuardTest, MoveAndReleaseTransferMappingOwnership) {
    constexpr size_t size = 4096;
    auto* first_mapping = ::mmap(nullptr,
                                 size,
                                 PROT_READ | PROT_WRITE,
                                 MAP_PRIVATE | MAP_ANONYMOUS,
                                 -1,
                                 0);
    ASSERT_NE(first_mapping, MAP_FAILED);
    auto* second_mapping = ::mmap(nullptr,
                                  size,
                                  PROT_READ | PROT_WRITE,
                                  MAP_PRIVATE | MAP_ANONYMOUS,
                                  -1,
                                  0);
    ASSERT_NE(second_mapping, MAP_FAILED);
    auto* first_data = static_cast<char*>(first_mapping);
    auto* second_data = static_cast<char*>(second_mapping);

    MappedRegionGuard first(first_data, size);
    MappedRegionGuard moved(std::move(first));
    EXPECT_EQ(first.Data(), nullptr);
    EXPECT_EQ(first.Size(), 0);
    EXPECT_EQ(moved.Data(), first_data);

    MappedRegionGuard destination(second_data, size);
    destination = std::move(moved);
    EXPECT_EQ(moved.Data(), nullptr);
    EXPECT_EQ(destination.Data(), first_data);

    unsigned char residency = 0;
    errno = 0;
    EXPECT_EQ(::mincore(second_data, size, &residency), -1);
    EXPECT_EQ(errno, ENOMEM);

    auto* released = destination.Release();
    EXPECT_EQ(released, first_data);
    EXPECT_EQ(destination.Data(), nullptr);
    EXPECT_EQ(destination.Size(), 0);
    EXPECT_EQ(::munmap(released, size), 0);
}

TEST(FileDescriptorGuardTest, CloseCheckedClosesDescriptorExactlyOnce) {
    auto root = MakeTestRoot();
    const auto path = std::filesystem::path(root->Path()) / "entry";
    const auto descriptor =
        ::open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600);
    ASSERT_GE(descriptor, 0);

    FileDescriptorGuard guard(descriptor);
    EXPECT_EQ(guard.Get(), descriptor);
    guard.CloseChecked(path.string(), "test descriptor");

    EXPECT_EQ(guard.Get(), -1);
    errno = 0;
    EXPECT_EQ(::fcntl(descriptor, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
}

}  // namespace
}  // namespace milvus::storage::test
