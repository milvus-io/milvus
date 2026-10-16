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

#include <arrow/buffer.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/io/interfaces.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "milvus-storage/format/vortex/vortex_format_reader.h"
#include "milvus-storage/lob_column/lob_column_manager.h"
#include "milvus-storage/lob_column/lob_column_writer.h"
#include "milvus-storage/lob_column/lob_reference.h"
#include "segcore/TextColumnCache.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::segcore {
namespace {

struct TextReadRange {
    std::string path;
    int64_t offset;
    int64_t bytes;
};

struct TextReadSnapshot {
    size_t opens = 0;
    std::vector<TextReadRange> ranges;

    int64_t
    Bytes() const {
        int64_t bytes = 0;
        for (const auto& range : ranges) {
            bytes += range.bytes;
        }
        return bytes;
    }
};

class TextReadTrace {
 public:
    void
    Open() {
        std::lock_guard<std::mutex> lock(mutex_);
        ++snapshot_.opens;
    }

    void
    Read(const std::string& path, int64_t offset, int64_t bytes) {
        std::lock_guard<std::mutex> lock(mutex_);
        snapshot_.ranges.push_back({path, offset, bytes});
    }

    TextReadSnapshot
    Take() {
        std::lock_guard<std::mutex> lock(mutex_);
        auto result = std::move(snapshot_);
        snapshot_ = {};
        return result;
    }

 private:
    std::mutex mutex_;
    TextReadSnapshot snapshot_;
};

// Count logical filesystem reads, even when the OS serves them from page cache.
// Both ReadAt overloads are covered; Arrow's default ReadAsync also calls ReadAt.
class CountingTextFile : public arrow::io::RandomAccessFile {
 public:
    CountingTextFile(std::shared_ptr<arrow::io::RandomAccessFile> file,
                     std::shared_ptr<TextReadTrace> trace,
                     std::string path)
        : file_(std::move(file)),
          trace_(std::move(trace)),
          path_(std::move(path)) {
    }

    arrow::Status
    Close() override {
        return file_->Close();
    }
    bool
    closed() const override {
        return file_->closed();
    }
    arrow::Result<int64_t>
    Tell() const override {
        return file_->Tell();
    }
    arrow::Status
    Seek(int64_t offset) override {
        return file_->Seek(offset);
    }
    arrow::Result<int64_t>
    GetSize() override {
        return file_->GetSize();
    }

    arrow::Result<int64_t>
    Read(int64_t bytes, void* out) override {
        ARROW_ASSIGN_OR_RAISE(auto offset, file_->Tell());
        ARROW_ASSIGN_OR_RAISE(auto read, file_->Read(bytes, out));
        trace_->Read(path_, offset, read);
        return read;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>>
    Read(int64_t bytes) override {
        ARROW_ASSIGN_OR_RAISE(auto offset, file_->Tell());
        ARROW_ASSIGN_OR_RAISE(auto buffer, file_->Read(bytes));
        trace_->Read(path_, offset, buffer->size());
        return buffer;
    }

    arrow::Result<int64_t>
    ReadAt(int64_t offset, int64_t bytes, void* out) override {
        ARROW_ASSIGN_OR_RAISE(auto read, file_->ReadAt(offset, bytes, out));
        trace_->Read(path_, offset, read);
        return read;
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>>
    ReadAt(int64_t offset, int64_t bytes) override {
        ARROW_ASSIGN_OR_RAISE(auto buffer, file_->ReadAt(offset, bytes));
        trace_->Read(path_, offset, buffer->size());
        return buffer;
    }

 private:
    std::shared_ptr<arrow::io::RandomAccessFile> file_;
    std::shared_ptr<TextReadTrace> trace_;
    std::string path_;
};

class CountingTextFileSystem : public arrow::fs::LocalFileSystem {
 public:
    explicit CountingTextFileSystem(std::shared_ptr<TextReadTrace> trace)
        : trace_(std::move(trace)) {
    }

    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFile(const std::string& path) override {
        ARROW_ASSIGN_OR_RAISE(auto file,
                              arrow::fs::LocalFileSystem::OpenInputFile(path));
        trace_->Open();
        return std::make_shared<CountingTextFile>(
            std::move(file), trace_, path);
    }

    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFile(const arrow::fs::FileInfo& info) override {
        return OpenInputFile(info.path());
    }

 private:
    std::shared_ptr<TextReadTrace> trace_;
};

// Exclude the union of ranges observed during metadata-only open. No hardcoded
// footer length or assumption that every ReadAt is a payload read is needed.
int64_t
BytesOutsideMetadata(const TextReadSnapshot& reads,
                     const TextReadSnapshot& metadata) {
    int64_t bytes = 0;
    for (const auto& read : reads.ranges) {
        std::vector<std::pair<int64_t, int64_t>> covered;
        const auto end = read.offset + read.bytes;
        for (const auto& meta : metadata.ranges) {
            if (meta.path == read.path) {
                auto begin = std::max(read.offset, meta.offset);
                auto stop = std::min(end, meta.offset + meta.bytes);
                if (begin < stop) {
                    covered.emplace_back(begin, stop);
                }
            }
        }
        std::sort(covered.begin(), covered.end());
        auto cursor = read.offset;
        for (const auto& [begin, stop] : covered) {
            bytes += std::max<int64_t>(0, begin - cursor);
            cursor = std::max(cursor, stop);
        }
        bytes += end - cursor;
    }
    return bytes;
}

class TextColumnCacheReadTest : public ::testing::TestWithParam<bool> {
 protected:
    std::vector<std::string>
    Read(TextColumnCache& cache,
         const std::shared_ptr<arrow::fs::FileSystem>& fs,
         const milvus_storage::lob_column::LobColumnConfig& config,
         const std::vector<milvus_storage::lob_column::EncodedRef>& refs) {
        if (!GetParam()) {
            return cache.ReadBatch(
                config.lob_base_path, fs, config.properties, refs);
        }
        google::protobuf::RepeatedPtrField<std::string> dst;
        for (size_t i = 0; i < refs.size(); ++i) {
            dst.Add();
        }
        cache.ReadBatchInto(
            config.lob_base_path, fs, config.properties, refs, &dst);
        return {dst.begin(), dst.end()};
    }

    void
    RecordReads(const std::string& name,
                const TextReadSnapshot& snapshot,
                const TextReadSnapshot& metadata) {
        RecordProperty(name + "_opens", std::to_string(snapshot.opens));
        RecordProperty(name + "_read_calls",
                       std::to_string(snapshot.ranges.size()));
        RecordProperty(name + "_read_bytes", std::to_string(snapshot.Bytes()));
        RecordProperty(
            name + "_non_metadata_bytes",
            std::to_string(BytesOutsideMetadata(snapshot, metadata)));
    }
};

INSTANTIATE_TEST_SUITE_P(ReadAPIs,
                         TextColumnCacheReadTest,
                         ::testing::Bool(),
                         [](const ::testing::TestParamInfo<bool>& info) {
                             return info.param ? "ReadBatchInto" : "ReadBatch";
                         });

TEST_P(TextColumnCacheReadTest, RepeatedLobReadsReuseReadersButReadDataAgain) {
    using namespace milvus_storage::lob_column;
    auto directory = storage::LocalDirectory::CreateOwned(
        std::filesystem::temp_directory_path().string(),
        "text-cache-read-XXXXXX",
        "TEXT cache test");
    auto trace = std::make_shared<TextReadTrace>();
    auto fs = std::make_shared<CountingTextFileSystem>(trace);
    LobColumnConfig config;
    config.lob_base_path = directory->Path() + "/lobs/105";
    config.field_id = 105;
    config.inline_threshold = 64 * 1024;
    config.max_lob_file_bytes = 8 * 1024 * 1024;
    config.flush_threshold_bytes = 4 * 1024 * 1024;

    // Deterministic, poorly compressible UTF-8 data keeps the LOB payload much
    // larger than the initial footer read; tiny/repeated strings can hide I/O.
    std::mt19937 generator(20261015);
    std::vector<std::string> texts(8, std::string(128 * 1024, ' '));
    for (auto& text : texts) {
        for (auto& ch : text) {
            ch = static_cast<char>('!' + generator() % 90);
        }
    }
    auto manager_result = LobColumnManager::Create(fs, config);
    ASSERT_TRUE(manager_result.ok()) << manager_result.status();
    auto manager = std::move(manager_result).ValueOrDie();
    auto writer_result = manager->CreateWriter();
    ASSERT_TRUE(writer_result.ok()) << writer_result.status();
    auto writer = std::move(writer_result).ValueOrDie();
    auto encoded_result = writer->WriteBatch(texts);
    ASSERT_TRUE(encoded_result.ok()) << encoded_result.status();
    auto encoded = std::move(encoded_result).ValueOrDie();
    auto files_result = writer->Close();
    ASSERT_TRUE(files_result.ok()) << files_result.status();
    auto files = std::move(files_result).ValueOrDie();
    ASSERT_EQ(files.size(), 1);
    ASSERT_EQ(encoded.size(), texts.size());
    std::vector<EncodedRef> refs;
    for (const auto& ref : encoded) {
        ASSERT_EQ(ref.size(), LOB_REFERENCE_SIZE);
        ASSERT_EQ(ref.front(), FLAG_LOB_REFERENCE);
        refs.push_back({ref.data(), ref.size()});
    }

    trace->Take();  // Exclude any writer-side activity.
    {
        milvus_storage::vortex::VortexFormatReader metadata_reader(
            fs, nullptr, files.front().path, config.properties, {});
        auto status = metadata_reader.open();
        ASSERT_TRUE(status.ok()) << status;
    }
    const auto metadata = trace->Take();
    ASSERT_GT(metadata.Bytes(), 0);

    TextColumnCache cache;
    ASSERT_EQ(Read(cache, fs, config, refs), texts);
    const auto first = trace->Take();
    RecordReads("first", first, metadata);
    ASSERT_GT(first.opens, 0);
    ASSERT_GT(BytesOutsideMetadata(first, metadata), 0);

    // Same cache, reader, file and rows, with all previous result strings gone.
    // This pins current behavior, not a desired content-cache guarantee.
    for (const auto& name : {"second", "third"}) {
        ASSERT_EQ(Read(cache, fs, config, refs), texts);
        const auto again = trace->Take();
        RecordReads(name, again, metadata);
        EXPECT_EQ(again.opens, 0) << "file readers should be reused";
        EXPECT_GT(BytesOutsideMetadata(again, metadata), 0)
            << "current LOB path caches readers, not previously read payload";
    }

    // Control: clearing the reader cache must cause the file to be opened again.
    cache.Clear();
    ASSERT_EQ(Read(cache, fs, config, refs), texts);
    const auto reopened = trace->Take();
    RecordReads("after_clear", reopened, metadata);
    EXPECT_GT(reopened.opens, 0);
    EXPECT_GT(BytesOutsideMetadata(reopened, metadata), 0);
}

TEST_P(TextColumnCacheReadTest, InlineTextNeedsNoFileReads) {
    using namespace milvus_storage::lob_column;
    auto trace = std::make_shared<TextReadTrace>();
    auto fs = std::make_shared<CountingTextFileSystem>(trace);
    LobColumnConfig config;
    // No such file or directory is created: inline values must be self-contained.
    config.lob_base_path = "unused-inline-lob-path";
    const std::string text = "inline text";
    auto encoded = EncodeInlineText(text);
    std::vector<EncodedRef> refs{{encoded.data(), encoded.size()}};
    TextColumnCache cache;
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(Read(cache, fs, config, refs),
                  std::vector<std::string>{text});
    }
    auto snapshot = trace->Take();
    EXPECT_EQ(snapshot.opens, 0);
    EXPECT_TRUE(snapshot.ranges.empty());
}

}  // namespace
}  // namespace milvus::segcore
