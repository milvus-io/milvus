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
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <span>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <filesystem>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>

#include "common/EasyAssert.h"
#include "local/FileSystem.h"

namespace milvus::local {
namespace {

namespace fs = std::filesystem;

static_assert(std::is_copy_constructible_v<FileSystem>);
static_assert(std::is_copy_assignable_v<FileSystem>);
static_assert(!std::is_copy_constructible_v<FileHandle>);
static_assert(std::is_move_constructible_v<FileHandle>);
static_assert(!std::is_copy_constructible_v<io::MappedRegion>);

template <typename T>
concept HasOpenForRead = requires { &T::OpenForRead; };

template <typename T>
concept HasOpenForWrite = requires { &T::OpenForWrite; };

static_assert(!HasOpenForRead<FileSystem>);
static_assert(!HasOpenForWrite<FileSystem>);

class CurrentPathGuard {
 public:
    CurrentPathGuard() : original_(fs::current_path()) {
    }

    CurrentPathGuard(const CurrentPathGuard&) = delete;
    CurrentPathGuard&
    operator=(const CurrentPathGuard&) = delete;

    ~CurrentPathGuard() {
        std::error_code error;
        fs::current_path(original_, error);
    }

 private:
    fs::path original_;
};

class LocalFileSystemTest : public testing::Test {
 protected:
    void
    SetUp() override {
        root_ = fs::temp_directory_path() / "milvus_rooted_local_filesystem" /
                testing::UnitTest::GetInstance()->current_test_info()->name();
        fs::remove_all(root_);
        files_.emplace(FileSystem::Open(root_));
    }

    void
    TearDown() override {
        files_.reset();
        fs::remove_all(root_);
    }

    fs::path root_;
    std::optional<FileSystem> files_;
};

TEST(PathTest, ValidatesAndNormalizesRelativePaths) {
    EXPECT_EQ(Path("index/./field//data").String(), "index/field/data");

    EXPECT_THROW(Path(""), SegcoreError);
    EXPECT_THROW(Path("."), SegcoreError);
    EXPECT_THROW(Path("/tmp/outside"), SegcoreError);
    EXPECT_THROW(Path("../outside"), SegcoreError);
    EXPECT_THROW(Path("index/../../outside"), SegcoreError);
    EXPECT_THROW(Path(std::string("index\0outside", 13)), SegcoreError);
    EXPECT_THROW(FileSystem::Open("relative/root"), SegcoreError);
}

TEST_F(LocalFileSystemTest, CopiesAndSubtreesKeepIndependentRoots) {
    auto other_root = root_.parent_path() / (root_.filename().string() + "_2");
    fs::remove_all(other_root);
    auto other = FileSystem::Open(other_root);

    auto local_chunk = files_->Subtree(Path("local_chunk"));
    auto copied = local_chunk;
    auto sibling = files_->Subtree(Path("expr_cache"));

    copied.CreateDirectories(Path("indexes"));
    sibling.CreateDirectories(Path("entries"));
    other.CreateDirectories(Path("indexes"));

    EXPECT_TRUE(fs::is_directory(root_ / "local_chunk/indexes"));
    EXPECT_TRUE(fs::is_directory(root_ / "expr_cache/entries"));
    EXPECT_TRUE(fs::is_directory(other_root / "indexes"));
    EXPECT_THROW(local_chunk.ResolveNativePath(Path("../expr_cache")),
                 SegcoreError);

    fs::remove_all(other_root);
}

TEST_F(LocalFileSystemTest, OperationsDoNotDependOnCurrentWorkingDirectory) {
    CurrentPathGuard guard;
    auto subtree = files_->Subtree(Path("local_chunk"));

    fs::current_path(fs::temp_directory_path());
    subtree.CreateDirectories(Path("indexes"));

    EXPECT_TRUE(fs::is_directory(root_ / "local_chunk/indexes"));
    EXPECT_EQ(subtree.ResolveNativePath(Path("indexes/data")),
              (fs::weakly_canonical(root_) / "local_chunk/indexes/data")
                  .lexically_normal());
}

TEST_F(LocalFileSystemTest, RejectsSymlinkEscapes) {
    auto outside =
        root_.parent_path() / (root_.filename().string() + "_outside");
    fs::remove_all(outside);
    fs::create_directories(outside);
    {
        auto outside_files = FileSystem::Open(fs::weakly_canonical(outside));
        auto output = outside_files.Open(
            Path("data"),
            OpenOptions{
                .mode = OpenMode::ReadWrite, .create = true, .truncate = true});
        constexpr std::array<std::byte, 1> data = {std::byte{42}};
        ASSERT_EQ(write(output.Get(), data.data(), data.size()), data.size());
    }
    fs::create_directory_symlink(outside, root_ / "escape");

    EXPECT_THROW(files_->Exists(Path("escape/data")), SegcoreError);
    EXPECT_THROW(files_->Subtree(Path("escape")).Exists(Path("data")),
                 SegcoreError);

    fs::remove_all(outside);
}

TEST_F(LocalFileSystemTest, OpensFileHandleWithExplicitOptions) {
    auto handle = files_->Open(Path("nested/data"),
                               OpenOptions{.mode = OpenMode::ReadWrite,
                                           .create = true,
                                           .truncate = true,
                                           .create_parent = true});

    EXPECT_NE(handle.Get(), -1);
    EXPECT_EQ(handle.DebugPath(), root_ / "nested/data");
    EXPECT_FALSE(handle.DirectIOEnabled());
    EXPECT_TRUE(fs::exists(root_ / "nested/data"));
}

TEST_F(LocalFileSystemTest, FileHandleOwnsAndTransfersTheDescriptor) {
    int closed_fd = -1;
    {
        auto handle = files_->Open(
            Path("close_on_destroy"),
            OpenOptions{.mode = OpenMode::ReadWrite, .create = true});
        closed_fd = handle.Get();
    }
    errno = 0;
    EXPECT_EQ(fcntl(closed_fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);

    auto handle =
        files_->Open(Path("release"),
                     OpenOptions{.mode = OpenMode::ReadWrite, .create = true});
    auto moved = std::move(handle);
    EXPECT_EQ(handle.Get(), -1);
    auto released_fd = moved.Release();
    EXPECT_EQ(moved.Get(), -1);
    EXPECT_NE(fcntl(released_fd, F_GETFD), -1);
    EXPECT_EQ(close(released_fd), 0);
}

TEST_F(LocalFileSystemTest, AdoptsAnExistingDescriptor) {
    auto path = root_ / "adopt";
    auto fd = open(path.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
    ASSERT_NE(fd, -1);

    {
        auto handle = FileHandle::Adopt(fd, path, false);
        EXPECT_EQ(handle.Get(), fd);
        EXPECT_EQ(handle.DebugPath(), path);
        EXPECT_FALSE(handle.DirectIOEnabled());
    }

    errno = 0;
    EXPECT_EQ(fcntl(fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
}

TEST_F(LocalFileSystemTest, RejectsWriteSideEffectsForReadOnlyHandle) {
    try {
        static_cast<void>(files_->Open(
            Path("invalid"),
            OpenOptions{.mode = OpenMode::ReadOnly, .create = true}));
        FAIL() << "expected read-only create options to fail";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::UnexpectedError);
    }
}

TEST_F(LocalFileSystemTest, UsesFileHandleWithConsumerOwnedOperations) {
    constexpr std::array<std::byte, 8> expected = {
        std::byte{1},
        std::byte{2},
        std::byte{3},
        std::byte{4},
        std::byte{5},
        std::byte{6},
        std::byte{7},
        std::byte{8},
    };
    auto path = Path("index/field/data");

    {
        auto output = files_->Open(path,
                                   OpenOptions{.mode = OpenMode::ReadWrite,
                                               .create = true,
                                               .truncate = true,
                                               .create_parent = true});
        auto first = std::span(expected).first<3>();
        ASSERT_EQ(write(output.Get(), first.data(), first.size()),
                  first.size());
        auto rest = std::span(expected).subspan<3>();
        ASSERT_EQ(pwrite(output.Get(), rest.data(), rest.size(), 3),
                  rest.size());
        ASSERT_EQ(fsync(output.Get()), 0);

        struct stat status {};
        ASSERT_EQ(fstat(output.Get(), &status), 0);
        EXPECT_EQ(status.st_size, expected.size());
        ASSERT_EQ(ftruncate(output.Get(), 6), 0);
        ASSERT_EQ(fstat(output.Get(), &status), 0);
        EXPECT_EQ(status.st_size, 6);
        ASSERT_EQ(ftruncate(output.Get(), expected.size()), 0);
        auto tail = std::span(expected).last<2>();
        ASSERT_EQ(pwrite(output.Get(), tail.data(), tail.size(), 6),
                  tail.size());
    }

    auto input = files_->Open(path, OpenOptions{.mode = OpenMode::ReadOnly});
    std::array<std::byte, 8> actual{};
    EXPECT_EQ(pread(input.Get(), actual.data(), actual.size(), 0),
              actual.size());
    EXPECT_EQ(actual, expected);
    struct stat status {};
    ASSERT_EQ(fstat(input.Get(), &status), 0);
    EXPECT_EQ(status.st_size, expected.size());
    EXPECT_EQ(pread(input.Get(), actual.data(), actual.size(), expected.size()),
              0);
}

TEST_F(LocalFileSystemTest, ManagesDirectoriesFilesAndRenameWithinRoot) {
    constexpr std::array<std::byte, 1> data = {std::byte{42}};
    auto original = Path("nested/a/data");
    auto renamed = Path("nested/b/data");

    auto output = files_->Open(original,
                               OpenOptions{.mode = OpenMode::ReadWrite,
                                           .create = true,
                                           .truncate = true,
                                           .create_parent = true});
    ASSERT_EQ(write(output.Get(), data.data(), data.size()), data.size());

    EXPECT_TRUE(files_->Exists(original));
    EXPECT_EQ(files_->FileSize(original), data.size());
    files_->CreateDirectories(Path("nested/b"));
    files_->Rename(original, renamed);
    EXPECT_FALSE(files_->Exists(original));
    EXPECT_TRUE(files_->Exists(renamed));

    auto listed = files_->List(Path("nested"), true);
    ASSERT_EQ(listed.size(), 1);
    EXPECT_EQ(listed.front().String(), "nested/b/data");

    files_->RemoveFile(renamed);
    files_->RemoveAll(Path("nested"));
    EXPECT_FALSE(files_->Exists(Path("nested")));
}

TEST_F(LocalFileSystemTest, MapsUnalignedRangesAndOwnsTheMapping) {
    constexpr std::array<std::byte, 12> data = {
        std::byte{0},
        std::byte{1},
        std::byte{2},
        std::byte{3},
        std::byte{4},
        std::byte{5},
        std::byte{6},
        std::byte{7},
        std::byte{8},
        std::byte{9},
        std::byte{10},
        std::byte{11},
    };
    auto path = Path("mmap/data");
    {
        auto output = files_->Open(path,
                                   OpenOptions{.mode = OpenMode::ReadWrite,
                                               .create = true,
                                               .truncate = true,
                                               .create_parent = true});
        ASSERT_EQ(write(output.Get(), data.data(), data.size()), data.size());
    }

    auto region = files_->OpenMappedRegion(
        path, MapOptions{.offset = 3, .length = 7, .populate = false});
    ASSERT_EQ(region.Data().size(), 7);
    EXPECT_TRUE(std::equal(
        region.Data().begin(), region.Data().end(), data.begin() + 3));

    auto moved = std::move(region);
    EXPECT_TRUE(region.Data().empty());
    ASSERT_EQ(moved.Data().size(), 7);
    EXPECT_EQ(moved.Data().front(), std::byte{3});
    EXPECT_EQ(moved.Data().back(), std::byte{9});

    auto populated = files_->OpenMappedRegion(
        path, MapOptions{.offset = 1, .length = 2, .populate = true});
    EXPECT_EQ(populated.Data()[0], std::byte{1});
    EXPECT_EQ(populated.Data()[1], std::byte{2});

    auto tail = files_->OpenMappedRegion(path, MapOptions{.offset = 10});
    ASSERT_EQ(tail.Data().size(), 2);
    EXPECT_EQ(tail.Data()[0], std::byte{10});
    EXPECT_EQ(tail.Data()[1], std::byte{11});
}

TEST_F(LocalFileSystemTest, PreservesFileAndMappingErrorCategories) {
    try {
        static_cast<void>(files_->Open(
            Path("missing"), OpenOptions{.mode = OpenMode::ReadOnly}));
        FAIL() << "expected opening a missing file to fail";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FileOpenFailed);
    }

    auto path = Path("mmap/empty");
    auto output = files_->Open(path,
                               OpenOptions{.mode = OpenMode::ReadWrite,
                                           .create = true,
                                           .truncate = true,
                                           .create_parent = true});
    ASSERT_EQ(ftruncate(output.Get(), 4), 0);

    try {
        static_cast<void>(files_->OpenMappedRegion(
            path, MapOptions{.offset = 3, .length = 2}));
        FAIL() << "expected an out-of-range mapping to fail";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::InvalidParameter);
    }
}

TEST_F(LocalFileSystemTest, ConvertsNativePathsOnlyWithinRoot) {
    EXPECT_EQ(Path("nested/").String(), "nested");
    auto native = files_->ResolveNativePath(Path("nested/file"));
    EXPECT_EQ(files_->PathFromNativePath(native).String(), "nested/file");

    EXPECT_THROW(static_cast<void>(files_->PathFromNativePath(
                     root_.parent_path() / "outside")),
                 SegcoreError);
    EXPECT_THROW(static_cast<void>(files_->PathFromNativePath("relative")),
                 SegcoreError);
}

}  // namespace
}  // namespace milvus::local
