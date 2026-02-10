// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "test_utils/Constants.h"
#include "milvus-storage/filesystem/fs.h"
#include "storage/IndexEntryDirectStreamWriter.h"
#include "storage/IndexEntryReader.h"
#include "storage/RemoteInputStream.h"
#include "storage/RemoteOutputStream.h"

using namespace milvus::storage;

namespace {

std::string
GetRootPath() {
    return TestLocalPath + "index_writer_test";
}
const std::string kV3FilePath = "test_v3_index";

std::vector<uint8_t>
GeneratePattern(size_t size) {
    std::vector<uint8_t> data(size);
    for (size_t i = 0; i < size; ++i) {
        data[i] = static_cast<uint8_t>(i % 256);
    }
    return data;
}

void
VerifyPattern(const std::vector<uint8_t>& data, size_t expected_size) {
    ASSERT_EQ(data.size(), expected_size);
    for (size_t i = 0; i < expected_size; ++i) {
        ASSERT_EQ(data[i], static_cast<uint8_t>(i % 256))
            << "Mismatch at byte " << i;
    }
}

}  // namespace

class IndexEntryWriterV3Test : public testing::Test {
 public:
    void
    SetUp() override {
        auto conf = milvus_storage::ArrowFileSystemConfig();
        conf.storage_type = "local";
        conf.root_path = GetRootPath();
        auto result = milvus_storage::CreateArrowFileSystem(conf);
        ASSERT_TRUE(result.ok()) << result.status().ToString();
        fs_ = result.ValueOrDie();
    }

    void
    TearDown() override {
        if (fs_) {
            fs_->DeleteDirContents("");
        }
    }

 protected:
    std::shared_ptr<milvus::OutputStream>
    CreateOutputStream(const std::string& path) {
        auto result = fs_->OpenOutputStream(path);
        EXPECT_TRUE(result.ok()) << result.status().ToString();
        return std::make_shared<RemoteOutputStream>(
            std::move(result.ValueOrDie()));
    }

    std::shared_ptr<milvus::InputStream>
    CreateInputStream(const std::string& path) {
        auto result = fs_->OpenInputFile(path);
        EXPECT_TRUE(result.ok()) << result.status().ToString();
        return std::make_shared<RemoteInputStream>(
            std::move(result.ValueOrDie()));
    }

    int64_t
    GetFileSize(const std::string& path) {
        auto info = fs_->GetFileInfo(path);
        EXPECT_TRUE(info.ok()) << info.status().ToString();
        return info.ValueOrDie().size();
    }

    milvus_storage::ArrowFileSystemPtr fs_;
};

TEST_F(IndexEntryWriterV3Test, SmallMemoryEntryRoundtrip) {
    const std::string file_path = kV3FilePath + "_small";
    const size_t entry_size =
        512 * 1024;  // 512 KB, well under 1MB cache threshold
    auto data = GeneratePattern(entry_size);

    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("small_entry", data.data(), data.size());
        writer.Finish();
    }

    auto input = CreateInputStream(file_path);
    int64_t file_size = GetFileSize(file_path);
    auto reader = IndexEntryReader::Open(input, file_size);

    auto entry = reader->ReadEntry("small_entry");
    VerifyPattern(entry.data, entry_size);
}

TEST_F(IndexEntryWriterV3Test, LargeMemoryEntryRoundtrip) {
    const std::string file_path = kV3FilePath + "_large";
    const size_t entry_size =
        17 * 1024 * 1024;  // 17 MB, exceeds 16MB threshold
    auto data = GeneratePattern(entry_size);

    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("large_entry", data.data(), data.size());
        writer.Finish();
    }

    auto input = CreateInputStream(file_path);
    int64_t file_size = GetFileSize(file_path);
    auto reader = IndexEntryReader::Open(input, file_size);

    auto entry = reader->ReadEntry("large_entry");
    VerifyPattern(entry.data, entry_size);
}

TEST_F(IndexEntryWriterV3Test, FdEntryRoundtrip) {
    const std::string file_path = kV3FilePath + "_fd";
    const size_t entry_size = 2 * 1024 * 1024;  // 2 MB
    auto data = GeneratePattern(entry_size);

    std::string tmp_relative = "fd_source.bin";
    std::string tmp_absolute = GetRootPath() + "/" + tmp_relative;
    {
        auto tmp_out = CreateOutputStream(tmp_relative);
        tmp_out->Write(data.data(), data.size());
        tmp_out->Close();
    }

    int fd = ::open(tmp_absolute.c_str(), O_RDONLY);
    ASSERT_NE(fd, -1) << "Failed to open temp file: " << strerror(errno);

    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("fd_entry", fd, entry_size);
        writer.Finish();
    }
    ::close(fd);

    auto input = CreateInputStream(file_path);
    int64_t file_size = GetFileSize(file_path);
    auto reader = IndexEntryReader::Open(input, file_size);

    auto entry = reader->ReadEntry("fd_entry");
    VerifyPattern(entry.data, entry_size);
}

TEST_F(IndexEntryWriterV3Test, MultipleEntriesRoundtrip) {
    const std::string file_path = kV3FilePath + "_multi";
    const size_t meta_size = 128;
    const size_t data_size = 4 * 1024 * 1024;  // 4 MB

    auto meta_data = GeneratePattern(meta_size);
    auto index_data = GeneratePattern(data_size);

    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("META", meta_data.data(), meta_data.size());
        writer.WriteEntry("DATA", index_data.data(), index_data.size());
        writer.Finish();
    }

    auto input = CreateInputStream(file_path);
    int64_t file_size = GetFileSize(file_path);
    auto reader = IndexEntryReader::Open(input, file_size);

    auto meta_entry = reader->ReadEntry("META");
    VerifyPattern(meta_entry.data, meta_size);

    auto data_entry = reader->ReadEntry("DATA");
    VerifyPattern(data_entry.data, data_size);
}

TEST_F(IndexEntryWriterV3Test, DuplicateNameThrows) {
    const std::string file_path = kV3FilePath + "_dup";
    auto data = GeneratePattern(64);

    auto output = CreateOutputStream(file_path);
    IndexEntryDirectStreamWriter writer(output);
    writer.WriteEntry("entry", data.data(), data.size());

    EXPECT_THROW(writer.WriteEntry("entry", data.data(), data.size()),
                 milvus::SegcoreError);
}

TEST_F(IndexEntryWriterV3Test, ReadEntryToFile) {
    const std::string file_path = kV3FilePath + "_tofile";
    const size_t entry_size = 1024 * 1024;  // 1 MB
    auto data = GeneratePattern(entry_size);

    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("file_entry", data.data(), data.size());
        writer.Finish();
    }

    auto input = CreateInputStream(file_path);
    int64_t file_size = GetFileSize(file_path);
    auto reader = IndexEntryReader::Open(input, file_size);

    std::string local_file = GetRootPath() + "/read_entry_output.bin";
    reader->ReadEntryToFile("file_entry", local_file);

    std::ifstream ifs(local_file, std::ios::binary | std::ios::ate);
    ASSERT_TRUE(ifs.is_open());
    size_t read_size = ifs.tellg();
    ASSERT_EQ(read_size, entry_size);

    ifs.seekg(0);
    std::vector<uint8_t> read_data(read_size);
    ifs.read(reinterpret_cast<char*>(read_data.data()), read_size);
    VerifyPattern(read_data, entry_size);

    ::unlink(local_file.c_str());
}

TEST_F(IndexEntryWriterV3Test, ReadEntriesToFiles) {
    const std::string file_path = kV3FilePath + "_tofiles";
    const size_t size_a = 256 * 1024;
    const size_t size_b = 512 * 1024;

    auto data_a = GeneratePattern(size_a);
    auto data_b = GeneratePattern(size_b);

    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("entry_a", data_a.data(), data_a.size());
        writer.WriteEntry("entry_b", data_b.data(), data_b.size());
        writer.Finish();
    }

    auto input = CreateInputStream(file_path);
    int64_t file_size = GetFileSize(file_path);
    auto reader = IndexEntryReader::Open(input, file_size);

    std::string file_a = GetRootPath() + "/output_a.bin";
    std::string file_b = GetRootPath() + "/output_b.bin";

    std::vector<std::pair<std::string, std::string>> pairs = {
        {"entry_a", file_a}, {"entry_b", file_b}};
    reader->ReadEntriesToFiles(pairs);

    auto verify_file = [](const std::string& path, size_t expected_size) {
        std::ifstream ifs(path, std::ios::binary | std::ios::ate);
        ASSERT_TRUE(ifs.is_open()) << "Failed to open: " << path;
        size_t read_size = ifs.tellg();
        ASSERT_EQ(read_size, expected_size);
        ifs.seekg(0);
        std::vector<uint8_t> buf(read_size);
        ifs.read(reinterpret_cast<char*>(buf.data()), read_size);
        VerifyPattern(buf, expected_size);
    };

    verify_file(file_a, size_a);
    verify_file(file_b, size_b);

    ::unlink(file_a.c_str());
    ::unlink(file_b.c_str());
}

TEST_F(IndexEntryWriterV3Test, GetEntryNames) {
    const std::string file_path = kV3FilePath + "_names";
    auto data = GeneratePattern(64);

    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("alpha", data.data(), data.size());
        writer.WriteEntry("beta", data.data(), data.size());
        writer.WriteEntry("gamma", data.data(), data.size());
        writer.Finish();
    }

    auto input = CreateInputStream(file_path);
    int64_t file_size = GetFileSize(file_path);
    auto reader = IndexEntryReader::Open(input, file_size);

    auto names = reader->GetEntryNames();
    ASSERT_EQ(names.size(), 3);
    EXPECT_EQ(names[0], "alpha");
    EXPECT_EQ(names[1], "beta");
    EXPECT_EQ(names[2], "gamma");
}

TEST_F(IndexEntryWriterV3Test, EntryNotFound) {
    const std::string file_path = kV3FilePath + "_notfound";
    auto data = GeneratePattern(64);

    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("exists", data.data(), data.size());
        writer.Finish();
    }

    auto input = CreateInputStream(file_path);
    int64_t file_size = GetFileSize(file_path);
    auto reader = IndexEntryReader::Open(input, file_size);

    EXPECT_THROW(reader->ReadEntry("does_not_exist"), milvus::SegcoreError);
}

TEST_F(IndexEntryWriterV3Test, SmallEntryCache) {
    const std::string file_path = kV3FilePath + "_cache";
    const size_t entry_size = 256;  // well under 1MB cache threshold
    auto data = GeneratePattern(entry_size);

    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("cached_entry", data.data(), data.size());
        writer.Finish();
    }

    auto input = CreateInputStream(file_path);
    int64_t file_size = GetFileSize(file_path);
    auto reader = IndexEntryReader::Open(input, file_size);

    auto entry1 = reader->ReadEntry("cached_entry");
    auto entry2 = reader->ReadEntry("cached_entry");

    ASSERT_EQ(entry1.data.size(), entry2.data.size());
    EXPECT_EQ(
        std::memcmp(entry1.data.data(), entry2.data.data(), entry1.data.size()),
        0);
    VerifyPattern(entry1.data, entry_size);
}

TEST_F(IndexEntryWriterV3Test, GetTotalBytesWritten) {
    const std::string file_path = kV3FilePath + "_totalbytes";
    const size_t entry_size = 1024;
    auto data = GeneratePattern(entry_size);

    auto output = CreateOutputStream(file_path);
    IndexEntryDirectStreamWriter writer(output);
    writer.WriteEntry("entry", data.data(), data.size());
    writer.Finish();

    size_t total = writer.GetTotalBytesWritten();
    EXPECT_GT(total, entry_size);

    int64_t actual_file_size = GetFileSize(file_path);
    EXPECT_EQ(total, static_cast<size_t>(actual_file_size));
}

TEST_F(IndexEntryWriterV3Test, WriteAfterFinishThrows) {
    const std::string file_path = kV3FilePath + "_afterfinish";
    auto data = GeneratePattern(64);

    auto output = CreateOutputStream(file_path);
    IndexEntryDirectStreamWriter writer(output);
    writer.WriteEntry("entry", data.data(), data.size());
    writer.Finish();

    EXPECT_THROW(writer.WriteEntry("another", data.data(), data.size()),
                 milvus::SegcoreError);
}

TEST_F(IndexEntryWriterV3Test, MixedSizeEntries) {
    const std::string file_path = kV3FilePath + "_mixed";
    const size_t tiny_size = 48;
    const size_t large_size = 17 * 1024 * 1024;  // 17 MB

    auto tiny_data = GeneratePattern(tiny_size);
    auto large_data = GeneratePattern(large_size);

    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("tiny_meta", tiny_data.data(), tiny_data.size());
        writer.WriteEntry("large_data", large_data.data(), large_data.size());
        writer.Finish();
    }

    auto input = CreateInputStream(file_path);
    int64_t file_size = GetFileSize(file_path);
    auto reader = IndexEntryReader::Open(input, file_size);

    auto names = reader->GetEntryNames();
    ASSERT_EQ(names.size(), 2);
    EXPECT_EQ(names[0], "tiny_meta");
    EXPECT_EQ(names[1], "large_data");

    auto tiny_entry = reader->ReadEntry("tiny_meta");
    VerifyPattern(tiny_entry.data, tiny_size);

    auto large_entry = reader->ReadEntry("large_data");
    VerifyPattern(large_entry.data, large_size);
}
