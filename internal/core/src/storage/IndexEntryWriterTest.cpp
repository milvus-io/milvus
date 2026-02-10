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
#include "storage/IndexEntryEncryptedLocalWriter.h"
#include "storage/IndexEntryReader.h"
#include "storage/PluginLoader.h"
#include "storage/RemoteInputStream.h"
#include "storage/RemoteOutputStream.h"

using namespace milvus::storage;

namespace {

// Simple XOR-based mock cipher for testing (NOT for production use!)
class MockEncryptor : public plugin::IEncryptor {
 public:
    explicit MockEncryptor(uint8_t key) : key_(key) {
    }

    std::string
    Encrypt(const std::string& plaintext) const override {
        return Encrypt(plaintext.data(), plaintext.size());
    }

    std::string
    Encrypt(std::string_view plaintext) const override {
        return Encrypt(plaintext.data(), plaintext.size());
    }

    std::string
    Encrypt(const void* data, size_t len) const override {
        // Format: [1-byte key][XOR'd data]
        std::string result;
        result.reserve(1 + len);
        result.push_back(static_cast<char>(key_));
        const auto* src = static_cast<const uint8_t*>(data);
        for (size_t i = 0; i < len; i++) {
            result.push_back(static_cast<char>(src[i] ^ key_));
        }
        return result;
    }

    std::string
    GetKey() const override {
        return std::string(1, static_cast<char>(key_));
    }

 private:
    uint8_t key_;
};

class MockDecryptor : public plugin::IDecryptor {
 public:
    std::string
    Decrypt(const std::string& ciphertext) const override {
        return Decrypt(ciphertext.data(), ciphertext.size());
    }

    std::string
    Decrypt(std::string_view ciphertext) const override {
        return Decrypt(ciphertext.data(), ciphertext.size());
    }

    std::string
    Decrypt(const void* data, size_t len) const override {
        if (len < 1) {
            return "";
        }
        const auto* src = static_cast<const uint8_t*>(data);
        uint8_t key = src[0];
        std::string result;
        result.reserve(len - 1);
        for (size_t i = 1; i < len; i++) {
            result.push_back(static_cast<char>(src[i] ^ key));
        }
        return result;
    }

    std::string
    GetKey() const override {
        return "";
    }
};

class MockCipherPlugin : public plugin::ICipherPlugin {
 public:
    std::string
    getPluginName() const override {
        return "CipherPlugin";
    }

    void
    Update(int64_t, int64_t, const std::string&) override {
    }

    std::pair<std::shared_ptr<plugin::IEncryptor>, std::string>
    GetEncryptor(int64_t, int64_t) const override {
        return {std::make_shared<MockEncryptor>(0x5A), "mock_edek"};
    }

    std::shared_ptr<plugin::IDecryptor>
    GetDecryptor(int64_t, int64_t, const std::string&) const override {
        return std::make_shared<MockDecryptor>();
    }
};

}  // namespace

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
    ASSERT_EQ(names.size(), 4);
    EXPECT_EQ(names[0], "alpha");
    EXPECT_EQ(names[1], "beta");
    EXPECT_EQ(names[2], "gamma");
    EXPECT_EQ(names[3], "__meta__");
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
    ASSERT_EQ(names.size(), 3);
    EXPECT_EQ(names[0], "tiny_meta");
    EXPECT_EQ(names[1], "large_data");
    EXPECT_EQ(names[2], "__meta__");

    auto tiny_entry = reader->ReadEntry("tiny_meta");
    VerifyPattern(tiny_entry.data, tiny_size);

    auto large_entry = reader->ReadEntry("large_data");
    VerifyPattern(large_entry.data, large_size);
}

TEST_F(IndexEntryWriterV3Test, SetMetaRoundtrip) {
    const std::string file_path = kV3FilePath + "_meta";
    auto data = GeneratePattern(256);

    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("data", data.data(), data.size());
        writer.PutMeta("index_type", std::string("bitmap"));
        writer.PutMeta("build_id", 42);
        writer.Finish();
    }

    auto input = CreateInputStream(file_path);
    int64_t file_size = GetFileSize(file_path);
    auto reader = IndexEntryReader::Open(input, file_size);

    // Verify __meta__ entry is present
    auto names = reader->GetEntryNames();
    EXPECT_EQ(names.back(), "__meta__");

    // Read and verify the meta content via GetMeta
    EXPECT_EQ(reader->GetMeta<std::string>("index_type"), "bitmap");
    EXPECT_EQ(reader->GetMeta<int>("build_id"), 42);

    // Regular data entry should still be readable
    auto data_entry = reader->ReadEntry("data");
    VerifyPattern(data_entry.data, 256);
}

TEST_F(IndexEntryWriterV3Test, EmptyMetaRoundtrip) {
    const std::string file_path = kV3FilePath + "_emptymeta";
    auto data = GeneratePattern(128);

    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("entry", data.data(), data.size());
        // No PutMeta calls - meta_json_ defaults to empty JSON object
        writer.Finish();
    }

    auto input = CreateInputStream(file_path);
    int64_t file_size = GetFileSize(file_path);
    auto reader = IndexEntryReader::Open(input, file_size);

    // __meta__ entry should exist with empty JSON object "{}"
    auto meta_entry = reader->ReadEntry("__meta__");
    std::string meta_str(reinterpret_cast<const char*>(meta_entry.data.data()),
                         meta_entry.data.size());
    EXPECT_EQ(meta_str, "{}");

    auto data_entry = reader->ReadEntry("entry");
    VerifyPattern(data_entry.data, 128);
}

TEST_F(IndexEntryWriterV3Test, ReadEntriesToFilesMultiRangeParallel) {
    // Test ReadEntriesToFiles with multiple large entries, each requiring
    // multiple ranges (> 16MB each). This tests the parallel download path
    // where all ranges from all entries are submitted at once.
    const std::string file_path = kV3FilePath + "_multirange";

    // Each entry > 16MB to require multiple ranges
    const size_t size_a = 35 * 1024 * 1024;  // 35 MB = 3 ranges
    const size_t size_b = 50 * 1024 * 1024;  // 50 MB = 4 ranges
    const size_t size_c = 20 * 1024 * 1024;  // 20 MB = 2 ranges
    // Total: 9 parallel range tasks across 3 entries

    auto data_a = GeneratePattern(size_a);
    auto data_b = GeneratePattern(size_b);
    auto data_c = GeneratePattern(size_c);

    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("entry_a", data_a.data(), data_a.size());
        writer.WriteEntry("entry_b", data_b.data(), data_b.size());
        writer.WriteEntry("entry_c", data_c.data(), data_c.size());
        writer.Finish();
    }

    auto input = CreateInputStream(file_path);
    int64_t file_size = GetFileSize(file_path);
    auto reader = IndexEntryReader::Open(input, file_size);

    std::string file_a = GetRootPath() + "/multirange_a.bin";
    std::string file_b = GetRootPath() + "/multirange_b.bin";
    std::string file_c = GetRootPath() + "/multirange_c.bin";

    std::vector<std::pair<std::string, std::string>> pairs = {
        {"entry_a", file_a}, {"entry_b", file_b}, {"entry_c", file_c}};
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
    verify_file(file_c, size_c);

    ::unlink(file_a.c_str());
    ::unlink(file_b.c_str());
    ::unlink(file_c.c_str());
}

// =============================================================================
// Edge Case Tests: Directory Table and Meta Entry size boundaries
// =============================================================================

TEST_F(IndexEntryWriterV3Test, LargeDirectoryTableNeedsSecondIO) {
    // Create many entries with long names to make directory table > 64KB
    // This tests the second IO path in ReadFooterAndDirectory()
    const std::string file_path = kV3FilePath + "_largedir";
    auto small_data = GeneratePattern(64);

    // Each entry in directory table is roughly:
    // {"name":"...", "offset":N, "size":N, "crc32":"XXXXXXXX"}
    // ~60 bytes per entry + name length
    // Need ~1100 entries with 50-char names to exceed 64KB
    const size_t num_entries = 1200;
    const std::string name_prefix =
        "entry_with_a_very_long_name_to_inflate_dir_";

    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        for (size_t i = 0; i < num_entries; i++) {
            std::string name = name_prefix + std::to_string(i);
            writer.WriteEntry(name, small_data.data(), small_data.size());
        }
        writer.Finish();
    }

    auto input = CreateInputStream(file_path);
    int64_t file_size = GetFileSize(file_path);
    auto reader = IndexEntryReader::Open(input, file_size);

    auto names = reader->GetEntryNames();
    // +1 for __meta__
    ASSERT_EQ(names.size(), num_entries + 1);

    // Verify a few entries
    auto entry0 = reader->ReadEntry(name_prefix + "0");
    VerifyPattern(entry0.data, 64);

    auto entry_last = reader->ReadEntry(name_prefix + "1199");
    VerifyPattern(entry_last.data, 64);
}

TEST_F(IndexEntryWriterV3Test, DirectoryFitsButMetaDoesNotFitFirstIO) {
    // Directory table fits in first 64KB, but meta entry is large enough
    // that dir_size + meta_entry_size > 64KB - 32
    // This tests the boundary where we need second IO for meta only
    const std::string file_path = kV3FilePath + "_largemetasmalldir";

    // Create a large meta JSON
    // We need meta_entry_size to be large (> ~60KB)
    // but directory table should be small (< 64KB - 32 - meta_size)
    auto data = GeneratePattern(1024);

    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("data", data.data(), data.size());

        // Add a large meta value (~70KB of repeated string)
        std::string large_value(70 * 1024, 'X');
        writer.PutMeta("large_field", large_value);
        writer.Finish();
    }

    auto input = CreateInputStream(file_path);
    int64_t file_size = GetFileSize(file_path);
    auto reader = IndexEntryReader::Open(input, file_size);

    // Verify meta can be read
    auto meta_value = reader->GetMeta<std::string>("large_field");
    ASSERT_EQ(meta_value.size(), 70 * 1024);
    ASSERT_EQ(meta_value, std::string(70 * 1024, 'X'));

    // Verify data entry
    auto entry = reader->ReadEntry("data");
    VerifyPattern(entry.data, 1024);
}

TEST_F(IndexEntryWriterV3Test, LargeMetaEntryMultiRange) {
    // Meta entry > 16MB requires multiple range reads
    const std::string file_path = kV3FilePath + "_hugemetamultirange";

    auto data = GeneratePattern(256);

    {
        auto output = CreateOutputStream(file_path);
        IndexEntryDirectStreamWriter writer(output);
        writer.WriteEntry("data", data.data(), data.size());

        // Create meta with ~20MB of data (will need 2 ranges to read)
        std::string huge_value(20 * 1024 * 1024, 'Y');
        writer.PutMeta("huge_field", huge_value);
        writer.Finish();
    }

    auto input = CreateInputStream(file_path);
    int64_t file_size = GetFileSize(file_path);
    auto reader = IndexEntryReader::Open(input, file_size);

    // Verify large meta
    auto meta_value = reader->GetMeta<std::string>("huge_field");
    ASSERT_EQ(meta_value.size(), 20 * 1024 * 1024);
    ASSERT_EQ(meta_value[0], 'Y');
    ASSERT_EQ(meta_value[meta_value.size() - 1], 'Y');

    // Verify data entry
    auto entry = reader->ReadEntry("data");
    VerifyPattern(entry.data, 256);
}

// =============================================================================
// Encrypted Entry Tests (using MockCipherPlugin)
// =============================================================================

class IndexEntryEncryptedV3Test : public IndexEntryWriterV3Test {
 protected:
    void
    SetUp() override {
        IndexEntryWriterV3Test::SetUp();
        mock_cipher_ = std::make_shared<MockCipherPlugin>();
    }

    std::shared_ptr<MockCipherPlugin> mock_cipher_;
};

TEST_F(IndexEntryEncryptedV3Test, EncryptedSmallEntryRoundtrip) {
    const std::string file_path = kV3FilePath + "_enc_small";
    const size_t entry_size = 1024;
    auto data = GeneratePattern(entry_size);

    // Use small slice_size for testing multi-slice behavior
    const size_t slice_size = 512;

    {
        // Note: remote_path should be relative to fs root
        IndexEntryEncryptedLocalWriter writer(file_path,
                                              fs_,
                                              mock_cipher_,
                                              /*ez_id=*/1,
                                              /*collection_id=*/100,
                                              GetRootPath(),
                                              slice_size);
        writer.WriteEntry("enc_entry", data.data(), data.size());
        writer.Finish();
    }

    // Verify file was created
    auto info = fs_->GetFileInfo(file_path);
    ASSERT_TRUE(info.ok());
    EXPECT_GT(info.ValueOrDie().size(), entry_size);  // ciphertext > plaintext
}

TEST_F(IndexEntryEncryptedV3Test, EncryptedMultiSliceEntry) {
    // Entry larger than slice_size, requiring multiple slices
    const std::string file_path = kV3FilePath + "_enc_multislice";
    const size_t slice_size = 1024;            // 1KB slices
    const size_t entry_size = 5 * 1024 + 100;  // 5KB + 100B = 6 slices
    auto data = GeneratePattern(entry_size);

    {
        IndexEntryEncryptedLocalWriter writer(file_path,
                                              fs_,
                                              mock_cipher_,
                                              /*ez_id=*/1,
                                              /*collection_id=*/100,
                                              GetRootPath(),
                                              slice_size);
        writer.WriteEntry("large_enc", data.data(), data.size());
        writer.Finish();
    }

    auto info = fs_->GetFileInfo(file_path);
    ASSERT_TRUE(info.ok());
    EXPECT_GT(info.ValueOrDie().size(), entry_size);
}

TEST_F(IndexEntryEncryptedV3Test, EncryptedMultipleEntriesMultiSlice) {
    // Multiple entries, each requiring multiple slices
    const std::string file_path = kV3FilePath + "_enc_multi_multi";
    const size_t slice_size = 1024;  // 1KB slices

    const size_t size_a = 3 * 1024 + 500;  // 4 slices
    const size_t size_b = 2 * 1024 + 100;  // 3 slices
    const size_t size_c = 5 * 1024;        // 5 slices

    auto data_a = GeneratePattern(size_a);
    auto data_b = GeneratePattern(size_b);
    auto data_c = GeneratePattern(size_c);

    {
        IndexEntryEncryptedLocalWriter writer(file_path,
                                              fs_,
                                              mock_cipher_,
                                              /*ez_id=*/1,
                                              /*collection_id=*/100,
                                              GetRootPath(),
                                              slice_size);
        writer.WriteEntry("entry_a", data_a.data(), data_a.size());
        writer.WriteEntry("entry_b", data_b.data(), data_b.size());
        writer.WriteEntry("entry_c", data_c.data(), data_c.size());
        writer.Finish();
    }

    auto info = fs_->GetFileInfo(file_path);
    ASSERT_TRUE(info.ok());
    EXPECT_GT(info.ValueOrDie().size(), size_a + size_b + size_c);
}

TEST_F(IndexEntryEncryptedV3Test, EncryptedLargeMetaMultiSlice) {
    // Large meta entry requiring multiple slices
    const std::string file_path = kV3FilePath + "_enc_largemeta";
    const size_t slice_size = 1024;  // 1KB slices

    auto data = GeneratePattern(256);

    {
        IndexEntryEncryptedLocalWriter writer(file_path,
                                              fs_,
                                              mock_cipher_,
                                              /*ez_id=*/1,
                                              /*collection_id=*/100,
                                              GetRootPath(),
                                              slice_size);
        writer.WriteEntry("data", data.data(), data.size());

        // Meta will be ~5KB = 5 slices
        std::string meta_value(5000, 'M');
        writer.PutMeta("large_meta", meta_value);
        writer.Finish();
    }

    auto info = fs_->GetFileInfo(file_path);
    ASSERT_TRUE(info.ok());
}

TEST_F(IndexEntryEncryptedV3Test, EncryptedFdEntryMultiSlice) {
    // Test fd-based entry with multiple slices
    const std::string file_path = kV3FilePath + "_enc_fd";
    const size_t slice_size = 1024;            // 1KB slices
    const size_t entry_size = 4 * 1024 + 200;  // 5 slices
    auto data = GeneratePattern(entry_size);

    // Write source file
    std::string tmp_relative = "enc_fd_source.bin";
    std::string tmp_absolute = GetRootPath() + "/" + tmp_relative;
    {
        auto tmp_out = CreateOutputStream(tmp_relative);
        tmp_out->Write(data.data(), data.size());
        tmp_out->Close();
    }

    int fd = ::open(tmp_absolute.c_str(), O_RDONLY);
    ASSERT_NE(fd, -1);

    {
        IndexEntryEncryptedLocalWriter writer(file_path,
                                              fs_,
                                              mock_cipher_,
                                              /*ez_id=*/1,
                                              /*collection_id=*/100,
                                              GetRootPath(),
                                              slice_size);
        writer.WriteEntry("fd_entry", fd, entry_size);
        writer.Finish();
    }
    ::close(fd);

    auto info = fs_->GetFileInfo(file_path);
    ASSERT_TRUE(info.ok());
    EXPECT_GT(info.ValueOrDie().size(), entry_size);

    ::unlink(tmp_absolute.c_str());
}
