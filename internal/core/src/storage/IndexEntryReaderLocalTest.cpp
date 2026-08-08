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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "filemanager/InputStream.h"
#include "filemanager/OutputStream.h"
#include "local/FileSystem.h"
#include "storage/IndexEntryDirectStreamWriter.h"
#include "storage/IndexEntryReader.h"

namespace milvus::storage {
namespace {

class VectorOutputStream final : public milvus::OutputStream {
 public:
    size_t
    Tell() const override {
        return data_.size();
    }

    size_t
    Write(const void* data, size_t size) override {
        auto* bytes = static_cast<const uint8_t*>(data);
        data_.insert(data_.end(), bytes, bytes + size);
        return size;
    }

    size_t
    Write(int fd, size_t size) override {
        std::vector<uint8_t> buffer(std::min<size_t>(size, 64 * 1024));
        size_t total = 0;
        while (total < size) {
            auto count = ::read(
                fd, buffer.data(), std::min(buffer.size(), size - total));
            AssertInfo(count > 0, "failed to read source file");
            Write(buffer.data(), count);
            total += count;
        }
        return total;
    }

    void
    Close() override {
    }

    std::vector<uint8_t>
    TakeData() {
        return std::move(data_);
    }

 private:
    std::vector<uint8_t> data_;
};

class VectorInputStream final : public milvus::InputStream {
 public:
    explicit VectorInputStream(std::vector<uint8_t> data)
        : data_(std::move(data)) {
    }

    size_t
    Size() const override {
        return data_.size();
    }

    bool
    Seek(int64_t offset) override {
        if (offset < 0 || static_cast<size_t>(offset) > data_.size()) {
            return false;
        }
        position_ = offset;
        return true;
    }

    size_t
    Tell() const override {
        return position_;
    }

    bool
    Eof() const override {
        return position_ == data_.size();
    }

    size_t
    Read(void* output, size_t size) override {
        auto count = ReadAt(output, position_, size);
        position_ += count;
        return count;
    }

    size_t
    ReadAt(void* output, size_t offset, size_t size) override {
        if (offset >= data_.size()) {
            return 0;
        }
        auto count = std::min(size, data_.size() - offset);
        std::memcpy(output, data_.data() + offset, count);
        return count;
    }

    size_t
    Read(int fd, size_t size) override {
        std::vector<uint8_t> buffer(std::min<size_t>(size, 64 * 1024));
        size_t total = 0;
        while (total < size) {
            auto count =
                Read(buffer.data(), std::min(buffer.size(), size - total));
            if (count == 0) {
                break;
            }
            auto written = ::write(fd, buffer.data(), count);
            AssertInfo(written == static_cast<ssize_t>(count),
                       "failed to write destination file");
            total += count;
        }
        return total;
    }

 private:
    std::vector<uint8_t> data_;
    size_t position_{0};
};

std::vector<uint8_t>
Pattern(size_t size, uint8_t seed) {
    std::vector<uint8_t> data(size);
    for (size_t i = 0; i < size; ++i) {
        data[i] = static_cast<uint8_t>(seed + i);
    }
    return data;
}

std::unique_ptr<IndexEntryReader>
MakeReader(
    const std::vector<std::pair<std::string, std::vector<uint8_t>>>& entries) {
    auto output = std::make_shared<VectorOutputStream>();
    IndexEntryDirectStreamWriter writer(output);
    for (const auto& [name, data] : entries) {
        writer.WriteEntry(name, data.data(), data.size());
    }
    writer.Finish();

    auto input = std::make_shared<VectorInputStream>(output->TakeData());
    auto size = input->Size();
    return IndexEntryReader::Open(std::move(input), size);
}

class IndexEntryReaderLocalTest : public testing::Test {
 protected:
    void
    SetUp() override {
        root_ = std::filesystem::temp_directory_path() /
                "milvus_index_entry_reader_local" /
                testing::UnitTest::GetInstance()->current_test_info()->name();
        files_ = local::FileSystem::Open(root_);
    }

    void
    TearDown() override {
        std::filesystem::remove_all(root_);
    }

    local::io::WritableFile
    OpenOutput(const std::string& path) {
        return files_->OpenForWrite(
            local::Path(path),
            local::WriteOptions{.create = true, .truncate = true});
    }

    std::vector<uint8_t>
    ReadOutput(const std::string& path) {
        auto input = files_->OpenForRead(local::Path(path));
        std::vector<uint8_t> data(input.Size());
        input.ReadAt(0, std::as_writable_bytes(std::span(data)));
        return data;
    }

    std::filesystem::path root_;
    std::optional<local::FileSystem> files_;
};

TEST_F(IndexEntryReaderLocalTest, StreamsEntryToWritableFile) {
    auto data = Pattern(3 * 1024 * 1024 + 19, 7);
    auto reader = MakeReader({{"data", data}});

    reader->ReadEntryStreamToFile(
        "data", OpenOutput("single.bin"), io::Priority::HIGH);

    EXPECT_EQ(ReadOutput("single.bin"), data);
}

TEST_F(IndexEntryReaderLocalTest, StreamsEntriesToWritableFiles) {
    auto first = Pattern(1024 * 1024 + 7, 11);
    auto second = Pattern(2 * 1024 * 1024 + 13, 29);
    auto reader = MakeReader({{"first", first}, {"second", second}});

    std::vector<std::pair<std::string, local::io::WritableFile>> outputs;
    outputs.emplace_back("first", OpenOutput("first.bin"));
    outputs.emplace_back("second", OpenOutput("second.bin"));
    reader->ReadEntriesStreamToFiles(std::move(outputs), io::Priority::HIGH);

    EXPECT_EQ(ReadOutput("first.bin"), first);
    EXPECT_EQ(ReadOutput("second.bin"), second);
}

TEST_F(IndexEntryReaderLocalTest, PathAdapterKeepsBatchBehavior) {
    auto first = Pattern(1024 * 1024 + 5, 17);
    auto second = Pattern(1024 * 1024 + 9, 31);
    auto reader = MakeReader({{"first", first}, {"second", second}});

    std::vector<std::pair<std::string, std::string>> outputs = {
        {"first", (root_ / "first.path.bin").string()},
        {"second", (root_ / "second.path.bin").string()}};
    reader->ReadEntriesStreamToFiles(outputs, io::Priority::HIGH);

    EXPECT_EQ(ReadOutput("first.path.bin"), first);
    EXPECT_EQ(ReadOutput("second.path.bin"), second);
}

}  // namespace
}  // namespace milvus::storage
