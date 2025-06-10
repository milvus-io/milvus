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
#include <random>
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
    }

    std::filesystem::path test_dir_;
    const size_t kBufferSize = 4096;  // 4KB buffer size
};

// Test basic file writing functionality with buffered IO
TEST_F(FileWriterTest, BasicWriteWithBufferedIO) {
    FileWriterConfig::GetInstance().SetMode(
        FileWriterConfig::WriteMode::BUFFERED);

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
    FileWriterConfig::GetInstance().SetMode(
        FileWriterConfig::WriteMode::DIRECT);
    FileWriterConfig::GetInstance().SetBufferSize(kBufferSize);

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
    FileWriterConfig::GetInstance().SetMode(
        FileWriterConfig::WriteMode::DIRECT);
    FileWriterConfig::GetInstance().SetBufferSize(kBufferSize);

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
    FileWriterConfig::GetInstance().SetMode(
        FileWriterConfig::WriteMode::DIRECT);
    FileWriterConfig::GetInstance().SetBufferSize(kBufferSize);

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
    FileWriterConfig::GetInstance().SetMode(
        FileWriterConfig::WriteMode::DIRECT);
    FileWriterConfig::GetInstance().SetBufferSize(kBufferSize);

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

// Test writing data with size slightly less than buffer size
TEST_F(FileWriterTest, SlightlyLessThanBufferSizeWriteWithDirectIO) {
    FileWriterConfig::GetInstance().SetMode(
        FileWriterConfig::WriteMode::DIRECT);
    FileWriterConfig::GetInstance().SetBufferSize(kBufferSize);

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
    FileWriterConfig::GetInstance().SetMode(
        FileWriterConfig::WriteMode::BUFFERED);

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
    FileWriterConfig::GetInstance().SetMode(
        FileWriterConfig::WriteMode::DIRECT);
    FileWriterConfig::GetInstance().SetBufferSize(kBufferSize);

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
    FileWriterConfig::GetInstance().SetMode(
        FileWriterConfig::WriteMode::DIRECT);
    FileWriterConfig::GetInstance().SetBufferSize(kBufferSize);

    std::string filename = (test_dir_ / "aligned_write.txt").string();
    FileWriter writer(filename);

    // Create 4KB aligned data using posix_memalign
    void* aligned_data = nullptr;
    if (posix_memalign(&aligned_data, 4096, kBufferSize) != 0) {
        throw std::runtime_error("Failed to allocate aligned memory");
    }
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
    FileWriterConfig::GetInstance().SetMode(
        FileWriterConfig::WriteMode::DIRECT);
    FileWriterConfig::GetInstance().SetBufferSize(kBufferSize);

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
    FileWriterConfig::GetInstance().SetMode(
        FileWriterConfig::WriteMode::DIRECT);
    FileWriterConfig::GetInstance().SetBufferSize(kBufferSize);

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
    FileWriterConfig::GetInstance().SetMode(
        FileWriterConfig::WriteMode::DIRECT);
    FileWriterConfig::GetInstance().SetBufferSize(kBufferSize);

    std::string invalid_path = "/invalid/path/file.txt";
    EXPECT_THROW(FileWriter writer(invalid_path), std::runtime_error);
}

// Test writing to a file that already exists
TEST_F(FileWriterTest, ExistingFileWithDirectIO) {
    FileWriterConfig::GetInstance().SetMode(
        FileWriterConfig::WriteMode::DIRECT);
    FileWriterConfig::GetInstance().SetBufferSize(kBufferSize);

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

// Test config FileWriterConfig with very small buffer size
TEST_F(FileWriterTest, SmallBufferSizeWriteWithDirectIO) {
    const size_t small_buffer_size = 64;  // 64 bytes
    FileWriterConfig::GetInstance().SetMode(
        FileWriterConfig::WriteMode::DIRECT);
    FileWriterConfig::GetInstance().SetBufferSize(small_buffer_size);
    auto real_buffer_size = FileWriterConfig::GetInstance().GetBufferSize();
    EXPECT_EQ(real_buffer_size, kBufferSize);
}

// Test config FileWriterConfig with unaligned buffer size
TEST_F(FileWriterTest, UnalignedBufferSizeWriteWithDirectIO) {
    const size_t unaligned_buffer_size = kBufferSize + 1;  // Not aligned to 4KB
    FileWriterConfig::GetInstance().SetMode(
        FileWriterConfig::WriteMode::DIRECT);
    FileWriterConfig::GetInstance().SetBufferSize(unaligned_buffer_size);
    auto real_buffer_size = FileWriterConfig::GetInstance().GetBufferSize();
    EXPECT_EQ(real_buffer_size, 2 * kBufferSize);
}

// Test config FileWriterConfig with zero buffer size
TEST_F(FileWriterTest, ZeroBufferSizeWriteWithDirectIO) {
    const size_t zero_buffer_size = 0;
    FileWriterConfig::GetInstance().SetMode(
        FileWriterConfig::WriteMode::DIRECT);
    FileWriterConfig::GetInstance().SetBufferSize(zero_buffer_size);
    auto real_buffer_size = FileWriterConfig::GetInstance().GetBufferSize();
    EXPECT_EQ(real_buffer_size, kBufferSize);
}

// Test config FileWriterConfig with very large buffer size
TEST_F(FileWriterTest, LargeBufferSizeWriteWithDirectIO) {
    const size_t large_buffer_size = 1024 * 1024;  // 1MB
    FileWriterConfig::GetInstance().SetMode(
        FileWriterConfig::WriteMode::DIRECT);
    FileWriterConfig::GetInstance().SetBufferSize(large_buffer_size);

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
    EXPECT_THROW(
        {
            FileWriterConfig::GetInstance().SetMode(
                static_cast<FileWriterConfig::WriteMode>(mode));
            FileWriterConfig::GetInstance().SetBufferSize(kBufferSize);
        },
        std::invalid_argument);
}

TEST_F(FileWriterTest, HalfAlignedDataWriteWithDirectIO) {
    const size_t aligned_buffer_size = 2 * kBufferSize;
    std::string filename = (test_dir_ / "half_aligned_buffer.txt").string();
    FileWriter writer(filename);

    char* aligned_buffer = nullptr;
    int ret = posix_memalign(reinterpret_cast<void**>(&aligned_buffer),
                             kBufferSize,
                             aligned_buffer_size);
    ASSERT_EQ(ret, 0);

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
    char* aligned_data = nullptr;
    ASSERT_EQ(posix_memalign((void**)&aligned_data, alignment, large_size), 0);
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
