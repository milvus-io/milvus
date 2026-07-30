// Copyright (C) 2019-2025 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <gtest/gtest.h>
#include <stdint.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <future>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "common/ChunkTarget.h"
#include "common/EasyAssert.h"
#include "folly/ScopeGuard.h"
#include "folly/futures/Promise.h"
#include "folly/system/ThreadName.h"
#include "gtest/gtest.h"
#include "storage/FileWriter.h"
#include "storage/LocalFileIOPool.h"
#include "test_utils/Constants.h"

using namespace milvus;
using namespace milvus::storage;

namespace milvus::storage {

class FileWriterTestAccessor {
 public:
    static bool
    ShouldFdatasyncOnFinish(const FileWriter& writer) {
        return writer.fdatasync_on_finish_ && !writer.use_direct_io_;
    }

    static void
    SetSyncFileDataHook(FileWriter& writer, std::function<void()> hook) {
        writer.sync_file_data_hook_for_test_ = std::move(hook);
    }

    static void
    InvalidateFd(FileWriter& writer) {
        close(writer.fd_);
        writer.fd_ = -1;
    }
};

}  // namespace milvus::storage

class FileWriterTest : public testing::Test {
 protected:
    void
    SetUp() override {
        test_dir_ = std::filesystem::path(TestLocalPath) / "file_writer_test";
        std::filesystem::create_directories(test_dir_);
    }

    void
    TearDown() override {
        LocalFileIOPool::GetInstance().Configure(0);
        std::filesystem::remove_all(test_dir_);
        // Reset rate limiter to disabled ratios to avoid test interference
        auto& limiter = milvus::storage::io::WriteRateLimiter::GetInstance();
        limiter.Configure(/*refill_period_us*/ 100000,
                          /*avg_bps*/ 8192 * 10,
                          /*max_burst_bps*/ 8192 * 40,
                          /*high*/ -1,
                          /*middle*/ -1,
                          /*low*/ -1);
    }

    std::filesystem::path test_dir_;
    const size_t kBufferSize = 4096;  // 4KB buffer size
};

namespace {

std::string
ReadFile(const std::string& filename) {
    std::ifstream file(filename, std::ios::binary);
    return {std::istreambuf_iterator<char>(file),
            std::istreambuf_iterator<char>()};
}

}  // namespace

// Test basic file writing functionality with buffered IO
TEST_F(FileWriterTest, BasicWriteWithBufferedIO) {
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);

    std::string filename = (test_dir_ / "basic_write.txt").string();
    FileWriter writer(filename);

    std::string test_data = "Hello, World!";
    writer.Write(test_data.data(), test_data.size());
    writer.Finish();

    // Verify file contents
    std::ifstream file(filename, std::ios::binary);
    std::string content((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());
    EXPECT_EQ(content, test_data);
}

TEST_F(FileWriterTest, FinishWithFdatasyncWriteback) {
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);

    std::string filename = (test_dir_ / "fdatasync_writeback.txt").string();
    FileWriter writer(filename);
    writer.SetFdatasyncOnFinish();
    EXPECT_TRUE(FileWriterTestAccessor::ShouldFdatasyncOnFinish(writer));

    std::string test_data(kBufferSize + 17, 'x');
    writer.Write(test_data.data(), test_data.size());
    writer.Finish();

    EXPECT_EQ(ReadFile(filename), test_data);
}

TEST_F(FileWriterTest, FinishSkipsFdatasyncWritebackWithDirectIO) {
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(kBufferSize);

    std::string filename =
        (test_dir_ / "direct_io_skips_fdatasync.txt").string();
    FileWriter writer(filename);
    writer.SetFdatasyncOnFinish();
    EXPECT_FALSE(FileWriterTestAccessor::ShouldFdatasyncOnFinish(writer));

    std::string test_data(kBufferSize + 17, 'x');
    writer.Write(test_data.data(), test_data.size());
    writer.Finish();

    EXPECT_EQ(ReadFile(filename), test_data);
}

TEST_F(FileWriterTest, FinishKeepsFdatasyncUnderWritePermit) {
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);
    LocalFileIOPool::GetInstance().Configure(1);

    for (bool has_tail_flush : {false, true}) {
        SCOPED_TRACE(has_tail_flush ? "with tail flush" : "without tail flush");

        auto first_filename =
            (test_dir_ /
             (has_tail_flush ? "sync_with_tail" : "sync_without_tail"))
                .string();
        FileWriter first_writer(first_filename);
        first_writer.SetFdatasyncOnFinish();
        std::string first_data(
            has_tail_flush ? kBufferSize + 1 : kBufferSize * 2, 'x');
        first_writer.Write(first_data.data(), first_data.size());

        std::promise<void> sync_started;
        auto sync_started_future = sync_started.get_future();
        std::promise<void> release_sync;
        auto release_sync_future = release_sync.get_future().share();
        FileWriterTestAccessor::SetSyncFileDataHook(
            first_writer, [&sync_started, release_sync_future]() {
                sync_started.set_value();
                release_sync_future.wait();
            });

        auto first_finish = std::async(std::launch::async, [&first_writer]() {
            return first_writer.Finish();
        });
        sync_started_future.wait();

        auto second_filename =
            (test_dir_ /
             (has_tail_flush ? "second_with_tail" : "second_without_tail"))
                .string();
        FileWriter second_writer(second_filename);
        std::string second_data(kBufferSize, 'y');
        second_writer.Write(second_data.data(), second_data.size());
        auto second_finish = std::async(std::launch::async, [&second_writer]() {
            return second_writer.Finish();
        });

        EXPECT_EQ(second_finish.wait_for(std::chrono::seconds(1)),
                  std::future_status::timeout);

        release_sync.set_value();
        EXPECT_EQ(first_finish.get(), first_data.size());
        EXPECT_EQ(second_finish.get(), second_data.size());
    }

    LocalFileIOPool::GetInstance().Configure(0);
}

TEST_F(FileWriterTest, FinishPropagatesSyncFailureWithWriteLimit) {
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);
    LocalFileIOPool::GetInstance().Configure(1);

    std::string filename =
        (test_dir_ / "write_limit_sync_failure.txt").string();
    FileWriter writer(filename);
    writer.SetFdatasyncOnFinish();
    std::string test_data(kBufferSize * 2, 'x');
    writer.Write(test_data.data(), test_data.size());
    FileWriterTestAccessor::SetSyncFileDataHook(
        writer, []() { throw std::runtime_error("injected sync failure"); });

    EXPECT_THROW(writer.Finish(), std::runtime_error);

    LocalFileIOPool::GetInstance().Configure(0);
}

TEST_F(FileWriterTest, WritePropagatesFailureWithWriteLimit) {
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);
    FileWriter::SetBufferSize(kBufferSize);
    LocalFileIOPool::GetInstance().Configure(1);

    std::string filename =
        (test_dir_ / "write_limit_write_failure.txt").string();
    FileWriter writer(filename);
    FileWriterTestAccessor::InvalidateFd(writer);
    std::string test_data(kBufferSize * 2, 'x');

    EXPECT_THROW(writer.Write(test_data.data(), test_data.size()),
                 std::runtime_error);

    LocalFileIOPool::GetInstance().Configure(0);
}

TEST_F(FileWriterTest, MmapChunkTargetWithWriteback) {
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);

    std::string filename = (test_dir_ / "mmap_chunk_target").string();
    MmapChunkTarget target(filename,
                           /*populate=*/false,
                           kBufferSize,
                           io::Priority::LOW,
                           MmapChunkWritebackMode::FdatasyncOnFinish);

    std::string test_data = "mmap writeback";
    target.write(test_data.data(), test_data.size());
    auto* data = target.release();
    ASSERT_NE(data, nullptr);
    EXPECT_EQ(std::string(data, test_data.size()), test_data);
    target.TransferOwnership();

    EXPECT_EQ(munmap(data, kBufferSize), 0);
    EXPECT_EQ(unlink(filename.c_str()), 0);
}

TEST_F(FileWriterTest, MmapChunkTargetCleansUpFileWhenReleaseFails) {
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);

    std::string filename = (test_dir_ / "mmap_release_failure").string();
    {
        MmapChunkTarget target(filename,
                               /*populate=*/false,
                               /*cap=*/0,
                               io::Priority::LOW,
                               MmapChunkWritebackMode::FdatasyncOnFinish);
        ASSERT_TRUE(std::filesystem::exists(filename));
        EXPECT_ANY_THROW(target.release());
    }

    EXPECT_FALSE(std::filesystem::exists(filename));
}

// Test basic file writing functionality with direct IO
TEST_F(FileWriterTest, BasicWriteWithDirectIO) {
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(kBufferSize);

    std::string filename = (test_dir_ / "basic_write.txt").string();
    FileWriter writer(filename);

    std::string test_data = "Hello, World!";
    writer.Write(test_data.data(), test_data.size());
    writer.Finish();

    // Verify file contents
    std::ifstream file(filename, std::ios::binary);
    std::string content((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());
    EXPECT_EQ(content, test_data);
}

// Test writing data with size exactly equal to buffer size
TEST_F(FileWriterTest, ExactBufferSizeWriteWithDirectIO) {
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(kBufferSize);

    std::string filename = (test_dir_ / "exact_buffer.txt").string();
    FileWriter writer(filename);

    std::vector<char> exact_buffer_data(kBufferSize);
    std::generate(
        exact_buffer_data.begin(), exact_buffer_data.end(), std::rand);

    writer.Write(exact_buffer_data.data(), exact_buffer_data.size());
    writer.Finish();

    std::ifstream file(filename, std::ios::binary);
    std::vector<char> read_data((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());
    EXPECT_EQ(read_data, exact_buffer_data);
}

// Test writing data size with multiple of buffer size
TEST_F(FileWriterTest, MultipleOfBufferSizeWriteWithDirectIO) {
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(kBufferSize);

    std::string filename = (test_dir_ / "multiple_of_buffer_size.txt").string();
    FileWriter writer(filename);

    std::vector<char> data(kBufferSize * 5);
    std::generate(data.begin(), data.end(), std::rand);
    writer.Write(data.data(), data.size());
    writer.Finish();

    // Verify file contents
    std::ifstream file(filename, std::ios::binary);
    std::vector<char> content((std::istreambuf_iterator<char>(file)),
                              std::istreambuf_iterator<char>());
    EXPECT_EQ(content, std::vector<char>(data.begin(), data.end()));
}

// Test writing data size with unaligned to buffer size
TEST_F(FileWriterTest, UnalignedToBufferSizeWriteWithDirectIO) {
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(kBufferSize);

    std::string filename =
        (test_dir_ / "unaligned_to_buffer_size.txt").string();
    FileWriter writer(filename);

    std::vector<char> large_data(kBufferSize * 2 + 17);
    std::generate(large_data.begin(), large_data.end(), std::rand);
    writer.Write(large_data.data(), large_data.size());
    writer.Finish();

    // Verify file contents
    std::ifstream file(filename, std::ios::binary);
    std::vector<char> content((std::istreambuf_iterator<char>(file)),
                              std::istreambuf_iterator<char>());
    EXPECT_EQ(content, std::vector<char>(large_data.begin(), large_data.end()));
}

// Test writing data with direct IO without finishing
TEST_F(FileWriterTest, WriteWithoutFinishWithDirectIO) {
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(kBufferSize);

    std::vector<char> data(kBufferSize * 2 + 10);
    std::generate(data.begin(), data.end(), std::rand);
    std::string filename = (test_dir_ / "write_without_finish.txt").string();
    {
        FileWriter writer(filename);
        writer.Write(data.data(), data.size());
    }

    std::ifstream file(filename, std::ios::binary);
    std::vector<char> content((std::istreambuf_iterator<char>(file)),
                              std::istreambuf_iterator<char>());

    EXPECT_NE(content.size(), data.size());
    EXPECT_EQ(content.size(), kBufferSize * 2);
    EXPECT_NE(content, std::vector<char>(data.begin(), data.end()));
    EXPECT_EQ(content,
              std::vector<char>(data.begin(), data.begin() + kBufferSize * 2));
}

// Test writing data with size slightly less than buffer size
TEST_F(FileWriterTest, SlightlyLessThanBufferSizeWriteWithDirectIO) {
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(kBufferSize);

    std::string filename = (test_dir_ / "slightly_less.txt").string();
    FileWriter writer(filename);

    std::vector<char> data(kBufferSize - 1);
    std::generate(data.begin(), data.end(), std::rand);

    writer.Write(data.data(), data.size());
    writer.Finish();

    std::ifstream file(filename, std::ios::binary);
    std::vector<char> read_data((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());
    EXPECT_EQ(read_data, data);
}

// Test writing data with multiple small chunks with direct IO
TEST_F(FileWriterTest, MultipleSmallChunksWriteWithBufferedIO) {
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);

    std::string filename =
        (test_dir_ / "multiple_small_chunks_buffered.txt").string();
    FileWriter writer(filename);

    const int num_chunks = 100;
    const size_t chunk_size = 10;  // 10 bytes per chunk
    std::vector<std::vector<char>> chunks(num_chunks,
                                          std::vector<char>(chunk_size));

    for (auto& chunk : chunks) {
        std::generate(chunk.begin(), chunk.end(), std::rand);
        writer.Write(chunk.data(), chunk.size());
    }
    writer.Finish();

    std::ifstream file(filename, std::ios::binary);
    std::vector<char> read_data((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());

    std::vector<char> expected_data;
    for (const auto& chunk : chunks) {
        expected_data.insert(expected_data.end(), chunk.begin(), chunk.end());
    }
    EXPECT_EQ(read_data, expected_data);
}

// Test writing data with multiple small chunks with direct IO
TEST_F(FileWriterTest, MultipleSmallChunksWriteWithDirectIO) {
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(kBufferSize);

    std::string filename =
        (test_dir_ / "multiple_small_chunks_direct.txt").string();
    FileWriter writer(filename);

    const int num_chunks = 100;
    const size_t chunk_size = 10;  // 10 bytes per chunk
    std::vector<std::vector<char>> chunks(num_chunks,
                                          std::vector<char>(chunk_size));

    for (auto& chunk : chunks) {
        std::generate(chunk.begin(), chunk.end(), std::rand);
        writer.Write(chunk.data(), chunk.size());
    }
    writer.Finish();

    std::ifstream file(filename, std::ios::binary);
    std::vector<char> read_data((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());

    std::vector<char> expected_data;
    for (const auto& chunk : chunks) {
        expected_data.insert(expected_data.end(), chunk.begin(), chunk.end());
    }
    EXPECT_EQ(read_data, expected_data);
}

// Test writing memory address aligned data
TEST_F(FileWriterTest, MemoryAddressAlignedDataWriteWithDirectIO) {
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(kBufferSize);

    std::string filename = (test_dir_ / "aligned_write.txt").string();
    FileWriter writer(filename);

    // Create 4KB aligned data using aligned_alloc
    void* aligned_data = std::aligned_alloc(4096, kBufferSize);
    ASSERT_NE(aligned_data, nullptr);
    std::generate(static_cast<char*>(aligned_data),
                  static_cast<char*>(aligned_data) + kBufferSize,
                  std::rand);

    writer.Write(aligned_data, kBufferSize);
    writer.Finish();

    // Verify file contents
    std::ifstream file(filename, std::ios::binary);
    std::vector<char> read_data((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());
    EXPECT_EQ(
        read_data,
        std::vector<char>(static_cast<char*>(aligned_data),
                          static_cast<char*>(aligned_data) + kBufferSize));

    // Clean up aligned memory
    free(aligned_data);
}

// Test writing empty data
TEST_F(FileWriterTest, EmptyDataWriteWithDirectIO) {
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(kBufferSize);

    std::string filename = (test_dir_ / "empty_write.txt").string();
    FileWriter writer(filename);

    writer.Write(nullptr, 0);
    writer.Finish();

    // Verify file is empty
    std::ifstream file(filename, std::ios::binary);
    EXPECT_EQ(file.tellg(), 0);
}

// Test concurrent writes to different files
TEST_F(FileWriterTest, ConcurrentWritesWithDirectIO) {
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(kBufferSize);

    const int num_threads = 4;
    std::vector<std::thread> threads;
    std::vector<std::string> filenames;

    filenames.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i) {
        filenames.emplace_back(
            (test_dir_ / ("concurrent_" + std::to_string(i) + ".txt"))
                .string());
    }

    threads.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            FileWriter writer(filenames[i]);
            std::string test_data = "Thread " + std::to_string(i) + " data";
            writer.Write(test_data.data(), test_data.size());
            writer.Finish();
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Verify all files
    for (int i = 0; i < num_threads; ++i) {
        std::ifstream file(filenames[i], std::ios::binary);
        std::string content((std::istreambuf_iterator<char>(file)),
                            std::istreambuf_iterator<char>());
        EXPECT_EQ(content, "Thread " + std::to_string(i) + " data");
    }
}

// Test error handling for invalid file path
TEST_F(FileWriterTest, InvalidFilePathWithDirectIO) {
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(kBufferSize);

    std::string invalid_path = "/invalid/path/file.txt";
    EXPECT_THROW(FileWriter writer(invalid_path), std::runtime_error);
}

// Test writing to a file that already exists
TEST_F(FileWriterTest, ExistingFileWithDirectIO) {
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(kBufferSize);

    std::string filename = (test_dir_ / "existing.txt").string();

    // Create initial file
    {
        FileWriter writer(filename);
        std::string initial_data = "Initial data";
        writer.Write(initial_data.data(), initial_data.size());
        writer.Finish();
    }

    // Write to the same file again
    FileWriter writer(filename);
    std::string new_data = "New data";
    writer.Write(new_data.data(), new_data.size());
    writer.Finish();

    // Verify file contains new data
    std::ifstream file(filename, std::ios::binary);
    std::string content((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());
    EXPECT_EQ(content, new_data);
}

TEST_F(FileWriterTest, PositionedWriterBufferedWritesOutOfOrder) {
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);

    std::string filename =
        (test_dir_ / "positioned_buffered_out_of_order.txt").string();
    std::vector<char> data(kBufferSize * 3 + 17);
    std::generate(data.begin(), data.end(), std::rand);

    {
        PositionedFileWriter writer(filename, data.size());
        writer.WriteAt(kBufferSize * 2, data.data() + kBufferSize * 2, 17);
        writer.WriteAt(0, data.data(), kBufferSize);
        writer.WriteAt(kBufferSize, data.data() + kBufferSize, kBufferSize);
        writer.WriteAt(kBufferSize * 2 + 17,
                       data.data() + kBufferSize * 2 + 17,
                       kBufferSize);
        writer.Finish();
    }

    std::ifstream file(filename, std::ios::binary);
    std::vector<char> read_data((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());
    EXPECT_EQ(read_data, data);
}

TEST_F(FileWriterTest, PositionedWriterDirectIOHandlesUnalignedInputAndTail) {
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(kBufferSize);

    std::string filename =
        (test_dir_ / "positioned_direct_unaligned_tail.txt").string();
    std::vector<char> storage(kBufferSize * 2 + 18);
    std::generate(storage.begin(), storage.end(), std::rand);
    auto* data = storage.data() + 1;
    const size_t data_size = kBufferSize * 2 + 17;

    {
        PositionedFileWriter writer(filename, data_size);
        writer.WriteAt(kBufferSize, data + kBufferSize, kBufferSize);
        writer.WriteAt(0, data, kBufferSize);
        writer.WriteAt(kBufferSize * 2, data + kBufferSize * 2, 17);
        writer.Finish();
    }

    std::ifstream file(filename, std::ios::binary);
    std::vector<char> read_data((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());
    ASSERT_EQ(read_data.size(), data_size);
    EXPECT_EQ(read_data, std::vector<char>(data, data + data_size));
}

TEST_F(FileWriterTest, PositionedWriterDirectIORejectsUnalignedOffset) {
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(kBufferSize);

    std::string filename =
        (test_dir_ / "positioned_direct_unaligned_offset.txt").string();
    std::vector<char> data(kBufferSize);

    PositionedFileWriter writer(filename, data.size());
    EXPECT_THROW(writer.WriteAt(1, data.data(), data.size()),
                 std::runtime_error);
}

TEST_F(FileWriterTest, PositionedWriterDirectIORejectsMiddleUnalignedWrite) {
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(kBufferSize);

    std::string filename =
        (test_dir_ / "positioned_direct_middle_tail.txt").string();
    std::vector<char> data(kBufferSize * 2 + 17);

    PositionedFileWriter writer(filename, data.size());
    EXPECT_THROW(writer.WriteAt(kBufferSize, data.data(), 17),
                 std::runtime_error);
}

TEST_F(FileWriterTest, PositionedWriterKeepsFileOpenAfterWriteAtError) {
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(kBufferSize);

    std::string filename =
        (test_dir_ / "positioned_direct_error_recovery.txt").string();
    std::vector<char> data(kBufferSize * 2);
    std::generate(data.begin(), data.end(), std::rand);

    {
        PositionedFileWriter writer(filename, data.size());
        EXPECT_THROW(writer.WriteAt(kBufferSize, data.data(), 17),
                     std::runtime_error);
        writer.WriteAt(0, data.data(), kBufferSize);
        writer.WriteAt(kBufferSize, data.data() + kBufferSize, kBufferSize);
        writer.Finish();
    }

    std::ifstream file(filename, std::ios::binary);
    std::vector<char> read_data((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());
    ASSERT_EQ(read_data.size(), data.size());
    EXPECT_EQ(read_data, data);
}

TEST_F(FileWriterTest, PositionedWriterDirectIOSupportsConcurrentWrites) {
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(kBufferSize);

    std::string filename =
        (test_dir_ / "positioned_direct_concurrent.txt").string();
    std::vector<char> data(kBufferSize * 8);
    std::generate(data.begin(), data.end(), std::rand);

    {
        PositionedFileWriter writer(filename, data.size());
        std::vector<std::thread> threads;
        for (size_t i = 0; i < 8; ++i) {
            threads.emplace_back([&, i]() {
                writer.WriteAt(i * kBufferSize,
                               data.data() + i * kBufferSize,
                               kBufferSize);
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
        writer.Finish();
    }

    std::ifstream file(filename, std::ios::binary);
    std::vector<char> read_data((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());
    EXPECT_EQ(read_data, data);
}

// Test rate limiter basic behavior: alignment and refill period
TEST_F(FileWriterTest, RateLimiterAlignmentAndPeriods) {
    using milvus::storage::io::Priority;
    using milvus::storage::io::WriteRateLimiter;

    // Configure: 100ms period, 8KB per period avg, 32KB burst, ratios enabled
    auto& limiter = WriteRateLimiter::GetInstance();
    limiter.Configure(/*refill_period_us*/ 100000,
                      /*avg_bps*/ 8192 * 10,        // 8KB per 100ms
                      /*max_burst_bps*/ 8192 * 40,  // 32KB burst
                      /*high*/ 1,
                      /*middle*/ 1,
                      /*low*/ 1);

    // Wait one period to accumulate credits
    std::this_thread::sleep_for(std::chrono::milliseconds(120));

    // Request 8KB with 4KB alignment → expect a multiple of 4KB, <= 8KB
    size_t allowed = limiter.Acquire(/*bytes*/ 8192,
                                     /*alignment*/ 4096,
                                     /*priority*/ Priority::MIDDLE);
    EXPECT_GT(allowed, 0u);
    EXPECT_LE(allowed, static_cast<size_t>(8192));
    EXPECT_EQ(allowed % 4096, 0u);
}

// Test that buffered IO path writes correct data under throttling (no overlap)
TEST_F(FileWriterTest, FileWriterBufferedRateLimitedWriteCorrectness) {
    using milvus::storage::io::Priority;
    using milvus::storage::io::WriteRateLimiter;

    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);

    // Configure limiter to force multiple internal chunks
    auto& limiter = WriteRateLimiter::GetInstance();
    limiter.Configure(/*refill_period_us*/ 50000,   // 50ms
                      /*avg_bps*/ 4096 * 20,        // 4KB per 50ms
                      /*max_burst_bps*/ 4096 * 80,  // 16KB burst
                      /*high*/ 1,
                      /*middle*/ 1,
                      /*low*/ 1);

    // Prepare data larger than a few chunks
    const size_t total_size = 12 * 4096;
    std::vector<char> data(total_size);
    std::generate(data.begin(), data.end(), std::rand);

    std::string filename = (test_dir_ / "buffered_rate_limited.txt").string();
    {
        FileWriter writer(filename, Priority::MIDDLE);
        writer.Write(data.data(), data.size());
        writer.Finish();
    }

    // Verify file contents match exactly
    std::ifstream file(filename, std::ios::binary);
    std::vector<char> read_data((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());
    EXPECT_EQ(read_data.size(), data.size());
    EXPECT_EQ(read_data, data);
}

// Test that priority ratio impacts allowance (HIGH > MIDDLE)
TEST_F(FileWriterTest, RateLimiterPriorityRatioEffect) {
    using milvus::storage::io::Priority;
    using milvus::storage::io::WriteRateLimiter;

    auto& limiter = WriteRateLimiter::GetInstance();
    // 100ms period, 8KB per period, 32KB burst
    limiter.Configure(/*refill_period_us*/ 100000,
                      /*avg_bps*/ 8192 * 10,
                      /*max_burst_bps*/ 8192 * 40,
                      /*high*/ 2,
                      /*middle*/ 1,
                      /*low*/ 1);

    // Accumulate two periods of credits
    std::this_thread::sleep_for(std::chrono::milliseconds(220));

    // Request with same bytes and alignment; HIGH should allow more than MIDDLE
    size_t req = 8 * 4096;  // divisible by 4KB
    size_t mid = limiter.Acquire(req, 4096, Priority::MIDDLE);

    // Reset time/credits by waiting again for comparable conditions
    std::this_thread::sleep_for(std::chrono::milliseconds(220));
    size_t hig = limiter.Acquire(req, 4096, Priority::HIGH);

    EXPECT_GT(hig, mid);
    EXPECT_EQ(mid % 4096, 0u);
    EXPECT_EQ(hig % 4096, 0u);
}

// Test config FileWriterConfig with very small buffer size
TEST_F(FileWriterTest, SmallBufferSizeWriteWithDirectIO) {
    const size_t small_buffer_size = 64;  // 64 bytes
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(small_buffer_size);
    auto real_buffer_size = FileWriter::GetBufferSize();
    EXPECT_EQ(real_buffer_size, kBufferSize);
}

// Test config FileWriterConfig with unaligned buffer size
TEST_F(FileWriterTest, UnalignedBufferSizeWriteWithDirectIO) {
    const size_t unaligned_buffer_size = kBufferSize + 1;  // Not aligned to 4KB
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(unaligned_buffer_size);
    auto real_buffer_size = FileWriter::GetBufferSize();
    EXPECT_EQ(real_buffer_size, 2 * kBufferSize);
}

// Test config FileWriterConfig with zero buffer size
TEST_F(FileWriterTest, ZeroBufferSizeWriteWithDirectIO) {
    const size_t zero_buffer_size = 0;
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(zero_buffer_size);
    auto real_buffer_size = FileWriter::GetBufferSize();
    EXPECT_EQ(real_buffer_size, kBufferSize);
}

// Test config FileWriterConfig with very large buffer size
TEST_F(FileWriterTest, LargeBufferSizeWriteWithDirectIO) {
    const size_t large_buffer_size = 1024 * 1024;  // 1MB
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(large_buffer_size);

    std::string filename = (test_dir_ / "large_buffer.txt").string();
    FileWriter writer(filename);

    std::vector<char> data(2 * large_buffer_size + 1);
    std::generate(data.begin(), data.end(), std::rand);
    writer.Write(data.data(), data.size());
    writer.Finish();

    std::ifstream file(filename, std::ios::binary);
    std::vector<char> read_data((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());
    EXPECT_EQ(read_data, std::vector<char>(data.begin(), data.end()));
}

// Tese config FileWriterConfig with unknown mode
TEST_F(FileWriterTest, UnknownModeWriteWithDirectIO) {
    uint8_t mode = 2;
    EXPECT_NO_THROW({
        FileWriter::SetMode(static_cast<FileWriter::WriteMode>(mode));
        FileWriter::SetBufferSize(kBufferSize);
    });
}

TEST_F(FileWriterTest, HalfAlignedDataWriteWithDirectIO) {
    const size_t aligned_buffer_size = 2 * kBufferSize;
    std::string filename = (test_dir_ / "half_aligned_buffer.txt").string();
    FileWriter writer(filename);

    char* aligned_buffer = static_cast<char*>(
        std::aligned_alloc(kBufferSize, aligned_buffer_size));
    ASSERT_NE(aligned_buffer, nullptr);

    const size_t first_half_size = kBufferSize / 2;
    const size_t rest_size = aligned_buffer_size - first_half_size;
    writer.Write(aligned_buffer, first_half_size);
    writer.Write(aligned_buffer + first_half_size, rest_size);
    writer.Finish();

    std::ifstream file(filename, std::ios::binary);
    std::vector<char> read_data((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());
    EXPECT_EQ(read_data,
              std::vector<char>(aligned_buffer,
                                aligned_buffer + aligned_buffer_size));

    free(aligned_buffer);
}

// Test writing data with alternating large and small chunks
TEST_F(FileWriterTest, AlternatingChunksWriteWithDirectIO) {
    std::string filename = (test_dir_ / "alternating_chunks.txt").string();
    FileWriter writer(filename);

    const int num_chunks = 10;
    std::vector<std::vector<char>> chunks;

    for (int i = 0; i < num_chunks; ++i) {
        size_t chunk_size = (i % 2 == 0) ? kBufferSize * 2 : 10;
        std::vector<char> chunk(chunk_size);
        std::generate(chunk.begin(), chunk.end(), std::rand);
        chunks.push_back(chunk);
        writer.Write(chunk.data(), chunk.size());
    }
    writer.Finish();

    std::ifstream file(filename, std::ios::binary);
    std::vector<char> read_data((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());

    std::vector<char> expected_data;
    for (const auto& chunk : chunks) {
        expected_data.insert(expected_data.end(), chunk.begin(), chunk.end());
    }
    EXPECT_EQ(read_data, expected_data);
}

// Test writing data with very large file size
TEST_F(FileWriterTest, VeryLargeFileWriteWithDirectIO) {
    std::string filename = (test_dir_ / "very_large_file.txt").string();
    FileWriter writer(filename);

    const size_t large_size = 100 * 1024 * 1024;  // 100MB
    const size_t alignment = 4096;                // 4KB alignment
    char* aligned_data =
        static_cast<char*>(std::aligned_alloc(alignment, large_size));
    ASSERT_NE(aligned_data, nullptr);
    std::generate(aligned_data, aligned_data + large_size, std::rand);

    writer.Write(aligned_data, large_size);
    writer.Finish();

    std::ifstream file(filename, std::ios::binary);
    std::vector<char> read_data((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());
    EXPECT_EQ(read_data.size(), large_size);
    EXPECT_EQ(std::memcmp(read_data.data(), aligned_data, large_size), 0);

    free(aligned_data);
}

// Test writing data with different buffer sizes in the same file
TEST_F(FileWriterTest, MixedBufferSizesWriteWithDirectIO) {
    std::string filename = (test_dir_ / "mixed_buffer_sizes.txt").string();
    FileWriter writer(filename);

    std::vector<size_t> chunk_sizes = {
        10,               // Very small
        kBufferSize - 1,  // Slightly less than buffer
        kBufferSize,      // Exact buffer size
        kBufferSize + 1,  // Slightly more than buffer
        kBufferSize * 2,  // Double buffer size
        kBufferSize * 10  // Much larger than buffer
    };

    std::vector<std::vector<char>> chunks;
    for (size_t size : chunk_sizes) {
        std::vector<char> chunk(size);
        std::generate(chunk.begin(), chunk.end(), std::rand);
        chunks.push_back(chunk);
        writer.Write(chunk.data(), chunk.size());
    }
    writer.Finish();

    std::ifstream file(filename, std::ios::binary);
    std::vector<char> read_data((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());

    std::vector<char> expected_data;
    for (const auto& chunk : chunks) {
        expected_data.insert(expected_data.end(), chunk.begin(), chunk.end());
    }
    EXPECT_EQ(read_data, expected_data);
}

// Test multi-threaded writing to different files
TEST_F(FileWriterTest, MultiThreadedWriteWithDirectIO) {
    const int num_threads = 4;
    const size_t data_size_per_thread = 50 * 1024 * 1024;  // 50MB per thread
    std::vector<std::thread> threads;
    std::vector<std::string> filenames;
    std::vector<std::vector<char>> test_data;

    // Prepare filenames and test data
    for (int i = 0; i < num_threads; ++i) {
        filenames.push_back(
            (test_dir_ / ("multi_thread_" + std::to_string(i) + ".txt"))
                .string());
        test_data.emplace_back(data_size_per_thread);
        std::generate(test_data[i].begin(), test_data[i].end(), std::rand);
    }

    // Launch threads
    threads.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            FileWriter writer(filenames[i]);
            writer.Write(test_data[i].data(), test_data[i].size());
            writer.Finish();
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify all files
    for (int i = 0; i < num_threads; ++i) {
        std::ifstream file(filenames[i], std::ios::binary);
        std::vector<char> read_data((std::istreambuf_iterator<char>(file)),
                                    std::istreambuf_iterator<char>());
        EXPECT_EQ(read_data.size(), data_size_per_thread);
        EXPECT_EQ(read_data, test_data[i]);
    }
}

TEST_F(FileWriterTest, WritesDoNotWaitForTheLocalFileWorkerPool) {
    auto& pool = LocalFileIOPool::GetInstance();
    pool.Configure(1);
    auto executor = pool.GetExecutor();
    ASSERT_TRUE(executor);
    auto blocker_started_promise = std::make_shared<std::promise<void>>();
    auto blocker_started = blocker_started_promise->get_future();
    auto release_blocker_promise = std::make_shared<std::promise<void>>();
    auto release_blocker = release_blocker_promise->get_future().share();
    executor->add([blocker_started_promise, release_blocker]() {
        blocker_started_promise->set_value();
        release_blocker.wait();
    });
    ASSERT_EQ(blocker_started.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);

    auto filename = (test_dir_ / "synchronous_writer.txt").string();
    auto buffer_size = kBufferSize;
    auto write = std::async(std::launch::async, [filename, buffer_size]() {
        FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);
        FileWriter::SetBufferSize(buffer_size);
        FileWriter writer(filename);
        std::vector<char> data(buffer_size + 1, 'x');
        writer.Write(data.data(), data.size());
        writer.Finish();
    });

    EXPECT_EQ(write.wait_for(std::chrono::milliseconds(100)),
              std::future_status::ready);
    release_blocker_promise->set_value();
    write.get();
}

TEST_F(FileWriterTest, LocalFileIOPoolUsesPriorityWorkerExecutor) {
    auto& pool = LocalFileIOPool::GetInstance();
    pool.Configure(1);
    auto executor = pool.GetExecutor();
    ASSERT_TRUE(executor);
    auto promise = std::make_shared<folly::Promise<std::string>>();
    auto future = promise->getSemiFuture();

    executor->add([promise]() {
        promise->setValue(folly::getCurrentThreadName().value_or(""));
    });

    auto thread_name = std::move(future).get();
    EXPECT_EQ(thread_name.rfind("MILVUS_LF_IO_", 0), 0);
    EXPECT_EQ(executor->getNumPriorities(), 2);
}

TEST_F(FileWriterTest, LocalFileIOWorkerCanUseFileWriter) {
    auto& pool = LocalFileIOPool::GetInstance();
    pool.Configure(1);
    auto executor = pool.GetExecutor();
    ASSERT_TRUE(executor);
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    auto filename = (test_dir_ / "worker_file_writer.txt").string();

    executor->add([promise, filename]() {
        try {
            FileWriter writer(filename);
            const char data = 'x';
            writer.Write(&data, sizeof(data));
            writer.Finish();
            promise->set_value();
        } catch (...) {
            promise->set_exception(std::current_exception());
        }
    });

    EXPECT_NO_THROW(future.get());
}

TEST_F(FileWriterTest, ConfiguredWriteLimitBlocksAdditionalWriters) {
    auto& pool = LocalFileIOPool::GetInstance();
    pool.Configure(1);
    auto first = pool.AcquireWritePermit();

    auto second = std::async(std::launch::async,
                             [&pool]() { return pool.AcquireWritePermit(); });
    EXPECT_EQ(second.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);

    first = {};
    EXPECT_EQ(second.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    auto second_permit = second.get();
    (void)second_permit;
}

TEST_F(FileWriterTest, FileWriterWaitsForConfiguredWritePermit) {
    auto& pool = LocalFileIOPool::GetInstance();
    pool.Configure(1);
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);
    FileWriter::SetBufferSize(kBufferSize);

    std::future<void> write;
    auto permit = pool.AcquireWritePermit();
    auto write_started_promise = std::make_shared<std::promise<void>>();
    auto write_started = write_started_promise->get_future();
    auto filename = (test_dir_ / "permit_limited_writer.txt").string();
    std::string data(kBufferSize + 1, 'x');
    write = std::async(std::launch::async,
                       [filename, data, write_started_promise]() {
                           FileWriter writer(filename);
                           write_started_promise->set_value();
                           writer.Write(data.data(), data.size());
                           writer.Finish();
                       });

    ASSERT_EQ(write_started.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_EQ(write.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);

    permit = {};
    EXPECT_EQ(write.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_NO_THROW(write.get());
    EXPECT_EQ(ReadFile(filename), data);
}

TEST_F(FileWriterTest, DisablingWriteLimitUnblocksWaitingWriters) {
    auto& pool = LocalFileIOPool::GetInstance();
    pool.Configure(1);
    auto first = pool.AcquireWritePermit();
    auto second = std::async(std::launch::async,
                             [&pool]() { return pool.AcquireWritePermit(); });
    ASSERT_EQ(second.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);

    pool.Configure(0);
    EXPECT_EQ(second.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    auto second_permit = second.get();
    (void)second_permit;
}

TEST_F(FileWriterTest, SameWorkerCountDoesNotReplaceActiveExecutor) {
    auto& pool = LocalFileIOPool::GetInstance();
    pool.Configure(1);
    auto executor = pool.GetExecutor();
    ASSERT_TRUE(executor);
    auto* original_executor = executor.get();
    auto task_started_promise = std::make_shared<std::promise<void>>();
    auto task_started = task_started_promise->get_future();
    auto release_task_promise = std::make_shared<std::promise<void>>();
    auto release_task = release_task_promise->get_future().share();
    bool task_released = false;
    auto release_guard = folly::makeGuard([&]() {
        if (!task_released) {
            release_task_promise->set_value();
        }
    });
    executor->add([task_started_promise, release_task]() {
        task_started_promise->set_value();
        release_task.wait();
    });
    ASSERT_EQ(task_started.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);

    auto configure =
        std::async(std::launch::async, [&pool]() { pool.Configure(1); });
    auto configure_status = configure.wait_for(std::chrono::seconds(2));
    if (configure_status != std::future_status::ready) {
        release_task_promise->set_value();
        task_released = true;
        release_guard.dismiss();
        configure.get();
        FAIL() << "same-size configuration waited for the active executor";
    }
    configure.get();
    EXPECT_EQ(pool.GetExecutor().get(), original_executor);

    release_task_promise->set_value();
    task_released = true;
    release_guard.dismiss();
}

TEST_F(FileWriterTest, ShrinkingLocalFileIOPoolDrainsQueuedTasks) {
    auto& pool = LocalFileIOPool::GetInstance();
    pool.Configure(2);
    auto executor = pool.GetExecutor();
    ASSERT_TRUE(executor);
    auto release_tasks_promise = std::make_shared<std::promise<void>>();
    auto release_tasks = release_tasks_promise->get_future().share();
    bool tasks_released = false;
    auto release_guard = folly::makeGuard([&]() {
        if (!tasks_released) {
            release_tasks_promise->set_value();
        }
    });
    auto first_started_promise = std::make_shared<std::promise<void>>();
    auto first_started = first_started_promise->get_future();
    auto second_started_promise = std::make_shared<std::promise<void>>();
    auto second_started = second_started_promise->get_future();
    executor->add([first_started_promise, release_tasks]() {
        first_started_promise->set_value();
        release_tasks.wait();
    });
    executor->add([second_started_promise, release_tasks]() {
        second_started_promise->set_value();
        release_tasks.wait();
    });
    ASSERT_EQ(first_started.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    ASSERT_EQ(second_started.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);

    auto completed = std::make_shared<std::atomic<size_t>>(0);
    std::vector<std::future<void>> queued;
    for (size_t i = 0; i < 4; ++i) {
        auto promise = std::make_shared<std::promise<void>>();
        queued.push_back(promise->get_future());
        executor->add([completed, promise]() {
            completed->fetch_add(1);
            promise->set_value();
        });
    }

    auto configure =
        std::async(std::launch::async, [&pool]() { pool.Configure(1); });
    EXPECT_EQ(configure.wait_for(std::chrono::milliseconds(50)),
              std::future_status::timeout);

    release_tasks_promise->set_value();
    tasks_released = true;
    release_guard.dismiss();
    configure.get();
    for (auto& future : queued) {
        EXPECT_NO_THROW(future.get());
    }
    EXPECT_EQ(completed->load(), queued.size());
}

TEST_F(FileWriterTest, ConfiguredLimitPreservesWriteErrors) {
    if (access("/dev/full", W_OK) != 0) {
        GTEST_SKIP() << "/dev/full is unavailable";
    }
    LocalFileIOPool::GetInstance().Configure(1);
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);
    FileWriter::SetBufferSize(kBufferSize);
    FileWriter writer("/dev/full");
    std::vector<char> data(kBufferSize + 1, 'x');

    try {
        writer.Write(data.data(), data.size());
        FAIL() << "expected write failure";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FileWriteFailed);
    }
}

TEST_F(FileWriterTest, ConfiguredLimitPreservesFinishErrors) {
    if (access("/dev/full", W_OK) != 0) {
        GTEST_SKIP() << "/dev/full is unavailable";
    }
    LocalFileIOPool::GetInstance().Configure(1);
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);
    FileWriter::SetBufferSize(kBufferSize);
    FileWriter writer("/dev/full");
    const char data = 'x';
    writer.Write(&data, sizeof(data));

    try {
        writer.Finish();
        FAIL() << "expected finish failure";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::FileWriteFailed);
    }
}

// Test configured write concurrency limit
TEST_F(FileWriterTest, ConfiguredWriteLimitWithBufferedIO) {
    // Configure a write concurrency limit of 2
    LocalFileIOPool::GetInstance().Configure(2);
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);

    std::string filename = (test_dir_ / "limited_buffered.txt").string();
    FileWriter writer(filename);

    // Write multiple chunks
    const int num_chunks = 10;
    const size_t chunk_size = 1024;
    std::vector<std::vector<char>> chunks(num_chunks,
                                          std::vector<char>(chunk_size));

    for (auto& chunk : chunks) {
        std::generate(chunk.begin(), chunk.end(), std::rand);
        writer.Write(chunk.data(), chunk.size());
    }
    writer.Finish();

    // Verify file contents
    std::ifstream file(filename, std::ios::binary);
    std::vector<char> read_data((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());

    std::vector<char> expected_data;
    for (const auto& chunk : chunks) {
        expected_data.insert(expected_data.end(), chunk.begin(), chunk.end());
    }
    EXPECT_EQ(read_data, expected_data);
}

// Test configured write concurrency limit with direct IO
TEST_F(FileWriterTest, ConfiguredWriteLimitWithDirectIO) {
    // Configure a write concurrency limit of 2
    LocalFileIOPool::GetInstance().Configure(2);
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(kBufferSize);

    std::string filename = (test_dir_ / "limited_direct.txt").string();
    FileWriter writer(filename);

    // Write multiple chunks asynchronously
    const int num_chunks = 10;
    const size_t chunk_size = kBufferSize;
    std::vector<std::vector<char>> chunks(num_chunks,
                                          std::vector<char>(chunk_size));

    for (auto& chunk : chunks) {
        std::generate(chunk.begin(), chunk.end(), std::rand);
        writer.Write(chunk.data(), chunk.size());
    }
    writer.Finish();

    // Verify file contents
    std::ifstream file(filename, std::ios::binary);
    std::vector<char> read_data((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());

    std::vector<char> expected_data;
    for (const auto& chunk : chunks) {
        expected_data.insert(expected_data.end(), chunk.begin(), chunk.end());
    }
    EXPECT_EQ(read_data, expected_data);
}

// Test concurrent writes with configured limit
TEST_F(FileWriterTest, ConcurrentWritesWithConfiguredLimit) {
    // Configure a write concurrency limit of 4
    LocalFileIOPool::GetInstance().Configure(4);
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);

    const int num_files = 8;
    std::vector<std::string> filenames;
    std::vector<std::vector<char>> test_data;

    // Prepare filenames and test data
    for (int i = 0; i < num_files; ++i) {
        filenames.push_back(
            (test_dir_ / ("concurrent_limited_" + std::to_string(i) + ".txt"))
                .string());
        test_data.emplace_back(1024 * 1024);  // 1MB per file
        std::generate(test_data[i].begin(), test_data[i].end(), std::rand);
    }

    // Write to all files concurrently
    std::vector<std::thread> threads;
    threads.reserve(num_files);
    for (int i = 0; i < num_files; ++i) {
        threads.emplace_back([&, i]() {
            FileWriter writer(filenames[i]);
            writer.Write(test_data[i].data(), test_data[i].size());
            writer.Finish();
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify all files
    for (int i = 0; i < num_files; ++i) {
        std::ifstream file(filenames[i], std::ios::binary);
        std::vector<char> read_data((std::istreambuf_iterator<char>(file)),
                                    std::istreambuf_iterator<char>());
        EXPECT_EQ(read_data, test_data[i]);
    }
}

// Test local file I/O configuration with invalid number of threads
TEST_F(FileWriterTest, InvalidLocalFileIOConfiguration) {
    // Test with zero threads
    EXPECT_NO_THROW(LocalFileIOPool::GetInstance().Configure(0));

    // Test with negative number of threads
    EXPECT_NO_THROW(LocalFileIOPool::GetInstance().Configure(-1));
}

// Test local file I/O configuration changes
TEST_F(FileWriterTest, LocalFileIOConfigurationChanges) {
    // Set initial executor
    LocalFileIOPool::GetInstance().Configure(2);

    // Change local file I/O configuration
    LocalFileIOPool::GetInstance().Configure(4);

    // Verify the change doesn't break functionality
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);

    std::string filename = (test_dir_ / "limit_change.txt").string();
    FileWriter writer(filename);

    std::string test_data = "Test data for executor change";
    writer.Write(test_data.data(), test_data.size());
    writer.Finish();

    // Verify file contents
    std::ifstream file(filename, std::ios::binary);
    std::string content((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());
    EXPECT_EQ(content, test_data);
}

// Test mixed buffered and direct IO with configured limit
TEST_F(FileWriterTest, MixedIOWithConfiguredLimit) {
    LocalFileIOPool::GetInstance().Configure(2);

    // Test buffered IO with configured limit
    {
        FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);
        std::string filename = (test_dir_ / "mixed_buffered.txt").string();
        FileWriter writer(filename);

        std::string test_data = "Buffered IO test data";
        writer.Write(test_data.data(), test_data.size());
        writer.Finish();

        std::ifstream file(filename, std::ios::binary);
        std::string content((std::istreambuf_iterator<char>(file)),
                            std::istreambuf_iterator<char>());
        EXPECT_EQ(content, test_data);
    }

    // Test direct IO with configured limit
    {
        FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
        FileWriter::SetBufferSize(kBufferSize);
        std::string filename = (test_dir_ / "mixed_direct.txt").string();
        FileWriter writer(filename);

        std::string test_data = "Direct IO test data";
        writer.Write(test_data.data(), test_data.size());
        writer.Finish();

        std::ifstream file(filename, std::ios::binary);
        std::string content((std::istreambuf_iterator<char>(file)),
                            std::istreambuf_iterator<char>());
        EXPECT_EQ(content, test_data);
    }
}

// Test large data writes with configured limit
TEST_F(FileWriterTest, LargeDataWritesWithConfiguredLimit) {
    LocalFileIOPool::GetInstance().Configure(2);
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);

    std::string filename = (test_dir_ / "large_data_limited.txt").string();
    FileWriter writer(filename);

    const size_t large_size = 10 * 1024 * 1024;  // 10MB
    std::vector<char> large_data(large_size);
    std::generate(large_data.begin(), large_data.end(), std::rand);

    writer.Write(large_data.data(), large_data.size());
    writer.Finish();

    std::ifstream file(filename, std::ios::binary);
    std::vector<char> read_data((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());
    EXPECT_EQ(read_data, large_data);
}

// Test executor with different buffer sizes
TEST_F(FileWriterTest, ConfiguredLimitWithDifferentBufferSizes) {
    LocalFileIOPool::GetInstance().Configure(2);
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);

    std::vector<size_t> buffer_sizes = {4096, 8192, 16384, 32768};

    for (size_t buffer_size : buffer_sizes) {
        FileWriter::SetBufferSize(buffer_size);

        std::string filename =
            (test_dir_ /
             ("buffer_size_" + std::to_string(buffer_size) + ".txt"))
                .string();
        FileWriter writer(filename);

        std::vector<char> test_data(buffer_size * 2);
        std::generate(test_data.begin(), test_data.end(), std::rand);

        writer.Write(test_data.data(), test_data.size());
        writer.Finish();

        std::ifstream file(filename, std::ios::binary);
        std::vector<char> read_data((std::istreambuf_iterator<char>(file)),
                                    std::istreambuf_iterator<char>());
        EXPECT_EQ(read_data, test_data);
    }
}

// Test error handling in configured-limit operations
TEST_F(FileWriterTest, ErrorHandlingWithConfiguredLimit) {
    LocalFileIOPool::GetInstance().Configure(2);
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);

    // Test with invalid file path with a configured limit
    std::string invalid_path = "/invalid/path/async_test.txt";

    // This should throw an exception even with a configured limit
    EXPECT_THROW(
        {
            FileWriter writer(invalid_path);
            std::string test_data = "Test data";
            writer.Write(test_data.data(), test_data.size());
            writer.Finish();
        },
        std::runtime_error);
}

// Test concurrent access to FileWriterConfig
TEST_F(FileWriterTest, ConcurrentAccessToFileWriterConfig) {
    const int num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;

    threads.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([i]() {
            // Each thread sets different local file I/O configurations
            LocalFileIOPool::GetInstance().Configure(i + 1);
            FileWriter::SetMode(i % 2 == 0 ? FileWriter::WriteMode::BUFFERED
                                           : FileWriter::WriteMode::DIRECT);
            FileWriter::SetBufferSize(4096 * (i + 1));
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify that the configuration is still valid
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);
    std::string filename = (test_dir_ / "concurrent_config_test.txt").string();

    FileWriter writer(filename);
    std::string test_data = "Concurrent config test";
    writer.Write(test_data.data(), test_data.size());
    writer.Finish();
}
// Test that changing FileWriterConfig during FileWriter operations doesn't affect existing instances
TEST_F(FileWriterTest, ConfigChangeDuringFileWriterOperations) {
    // Start with buffered mode
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);
    LocalFileIOPool::GetInstance().Configure(2);

    std::string filename1 = (test_dir_ / "config_change_test1.txt").string();
    std::string filename2 = (test_dir_ / "config_change_test2.txt").string();

    // Create first FileWriter
    FileWriter writer1(filename1);

    // Start writing some data with first writer
    std::string test_data1 = "First writer data";
    writer1.Write(test_data1.data(), test_data1.size());

    // Change configuration while first writer is still active
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(8192);
    LocalFileIOPool::GetInstance().Configure(4);

    // Create second FileWriter with new configuration
    FileWriter writer2(filename2);

    // Continue writing with both writers
    std::string test_data2 = "Second writer data";
    writer2.Write(test_data2.data(), test_data2.size());

    std::string more_data1 = "More data for first writer";
    writer1.Write(more_data1.data(), more_data1.size());

    // Finish both writers
    size_t size1 = writer1.Finish();
    size_t size2 = writer2.Finish();

    // Verify both files were written correctly
    EXPECT_EQ(size1, test_data1.size() + more_data1.size());
    EXPECT_EQ(size2, test_data2.size());

    // Read back and verify content
    std::ifstream file1(filename1);
    std::string content1((std::istreambuf_iterator<char>(file1)),
                         std::istreambuf_iterator<char>());
    EXPECT_EQ(content1, test_data1 + more_data1);

    std::ifstream file2(filename2);
    std::string content2((std::istreambuf_iterator<char>(file2)),
                         std::istreambuf_iterator<char>());
    EXPECT_EQ(content2, test_data2);
}
