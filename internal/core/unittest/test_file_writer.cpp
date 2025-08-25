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
#include <gmock/gmock.h>
#include <filesystem>
#include <fstream>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <thread>

#include "storage/FileWriter.h"

using namespace milvus;
using namespace milvus::storage;

class FileWriterTest : public testing::Test {
 protected:
    void
    SetUp() override {
        test_dir_ = std::filesystem::temp_directory_path() / "file_writer_test";
        std::filesystem::create_directories(test_dir_);
    }

    void
    TearDown() override {
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

// Test executor-based asynchronous writes
TEST_F(FileWriterTest, ExecutorBasedAsyncWrites) {
    // Set up executor with 2 threads
    FileWriteWorkerPool::GetInstance().Configure(2);
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);

    std::string filename = (test_dir_ / "executor_async.txt").string();
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

// Test executor-based asynchronous writes with direct IO
TEST_F(FileWriterTest, ExecutorBasedAsyncWritesWithDirectIO) {
    // Set up executor with 2 threads
    FileWriteWorkerPool::GetInstance().Configure(2);
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(kBufferSize);

    std::string filename = (test_dir_ / "executor_async_direct.txt").string();
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

// Test concurrent writes using executor
TEST_F(FileWriterTest, ConcurrentWritesWithExecutor) {
    // Set up executor with 4 threads
    FileWriteWorkerPool::GetInstance().Configure(4);
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);

    const int num_files = 8;
    std::vector<std::string> filenames;
    std::vector<std::vector<char>> test_data;

    // Prepare filenames and test data
    for (int i = 0; i < num_files; ++i) {
        filenames.push_back(
            (test_dir_ / ("concurrent_executor_" + std::to_string(i) + ".txt"))
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

// Test executor configuration with invalid number of threads
TEST_F(FileWriterTest, InvalidExecutorConfiguration) {
    // Test with zero threads
    EXPECT_NO_THROW(FileWriteWorkerPool::GetInstance().Configure(0));

    // Test with negative number of threads
    EXPECT_NO_THROW(FileWriteWorkerPool::GetInstance().Configure(-1));
}

// Test executor configuration changes
TEST_F(FileWriterTest, ExecutorConfigurationChanges) {
    // Set initial executor
    FileWriteWorkerPool::GetInstance().Configure(2);

    // Change executor configuration
    FileWriteWorkerPool::GetInstance().Configure(4);

    // Verify the change doesn't break functionality
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);

    std::string filename = (test_dir_ / "executor_change.txt").string();
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

// Test mixed buffered and direct IO with executor
TEST_F(FileWriterTest, MixedIOWithExecutor) {
    FileWriteWorkerPool::GetInstance().Configure(2);

    // Test buffered IO with executor
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

    // Test direct IO with executor
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

// Test large data writes with executor
TEST_F(FileWriterTest, LargeDataWritesWithExecutor) {
    FileWriteWorkerPool::GetInstance().Configure(2);
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);

    std::string filename = (test_dir_ / "large_data_executor.txt").string();
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
TEST_F(FileWriterTest, ExecutorWithDifferentBufferSizes) {
    FileWriteWorkerPool::GetInstance().Configure(2);
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

// Test error handling in async operations
TEST_F(FileWriterTest, ErrorHandlingInAsyncOperations) {
    FileWriteWorkerPool::GetInstance().Configure(2);
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);

    // Test with invalid file path in async context
    std::string invalid_path = "/invalid/path/async_test.txt";

    // This should throw an exception even in async context
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
            // Each thread sets different executor configurations
            FileWriteWorkerPool::GetInstance().Configure(i + 1);
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
    std::string filename =
        (std::filesystem::temp_directory_path() / "concurrent_config_test.txt")
            .string();

    FileWriter writer(filename);
    std::string test_data = "Concurrent config test";
    writer.Write(test_data.data(), test_data.size());
    writer.Finish();

    // Clean up
    std::filesystem::remove(filename);
}
// Test that changing FileWriterConfig during FileWriter operations doesn't affect existing instances
TEST_F(FileWriterTest, ConfigChangeDuringFileWriterOperations) {
    // Start with buffered mode
    FileWriter::SetMode(FileWriter::WriteMode::BUFFERED);
    FileWriteWorkerPool::GetInstance().Configure(2);

    std::string filename1 =
        (std::filesystem::temp_directory_path() / "config_change_test1.txt")
            .string();
    std::string filename2 =
        (std::filesystem::temp_directory_path() / "config_change_test2.txt")
            .string();

    // Create first FileWriter
    FileWriter writer1(filename1);

    // Start writing some data with first writer
    std::string test_data1 = "First writer data";
    writer1.Write(test_data1.data(), test_data1.size());

    // Change configuration while first writer is still active
    FileWriter::SetMode(FileWriter::WriteMode::DIRECT);
    FileWriter::SetBufferSize(8192);
    FileWriteWorkerPool::GetInstance().Configure(4);

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

    // Clean up
    std::filesystem::remove(filename1);
    std::filesystem::remove(filename2);
}
