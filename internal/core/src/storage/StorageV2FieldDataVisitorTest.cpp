// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <arrow/api.h>
#include <parquet/arrow/writer.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <future>
#include <mutex>
#include <numeric>
#include <thread>
#include <vector>

#include "arrow/filesystem/filesystem.h"
#include "arrow/io/interfaces.h"
#include "common/Schema.h"
#include "folly/ScopeGuard.h"
#include "milvus-storage/common/constants.h"
#include "milvus-storage/common/metadata.h"
#include "milvus-storage/format/parquet/file_reader.h"
#include "storage/Util.h"
#include "storage/artifact/LocalDirectory.h"

namespace milvus::storage::test {
namespace {

// Count actual worker reader lifetimes, including readers still opening or
// unwinding. Caller-thread metadata reads are deliberately excluded.
struct WorkerReadState {
    std::thread::id caller = std::this_thread::get_id();
    std::atomic<size_t> opened{0};
    std::atomic<size_t> active{0};
    std::function<arrow::Status(int64_t, int64_t)> before_read;
};

class TrackedInputFile : public arrow::io::RandomAccessFile {
 public:
    TrackedInputFile(std::shared_ptr<arrow::io::RandomAccessFile> file,
                     std::shared_ptr<WorkerReadState> state)
        : file_(std::move(file)), state_(std::move(state)) {
    }

    ~TrackedInputFile() override {
        (void)file_->Close();
        --state_->active;
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
    Seek(int64_t position) override {
        return file_->Seek(position);
    }

    arrow::Result<int64_t>
    GetSize() override {
        return file_->GetSize();
    }

    arrow::Result<int64_t>
    Read(int64_t nbytes, void* out) override {
        return file_->Read(nbytes, out);
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>>
    Read(int64_t nbytes) override {
        return file_->Read(nbytes);
    }

    arrow::Result<int64_t>
    ReadAt(int64_t position, int64_t nbytes, void* out) override {
        if (state_->before_read) {
            ARROW_RETURN_NOT_OK(state_->before_read(position, nbytes));
        }
        return file_->ReadAt(position, nbytes, out);
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>>
    ReadAt(int64_t position, int64_t nbytes) override {
        if (state_->before_read) {
            ARROW_RETURN_NOT_OK(state_->before_read(position, nbytes));
        }
        return file_->ReadAt(position, nbytes);
    }

 private:
    std::shared_ptr<arrow::io::RandomAccessFile> file_;
    std::shared_ptr<WorkerReadState> state_;
};

class TrackingFileSystem : public arrow::fs::SubTreeFileSystem {
 public:
    TrackingFileSystem(milvus_storage::ArrowFileSystemPtr fs,
                       std::shared_ptr<WorkerReadState> state)
        : SubTreeFileSystem("", std::move(fs)), state_(std::move(state)) {
    }

    using SubTreeFileSystem::OpenInputFile;

    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFile(const std::string& path) override {
        if (std::this_thread::get_id() == state_->caller) {
            return SubTreeFileSystem::OpenInputFile(path);
        }
        ++state_->opened;
        ++state_->active;
        auto failed_open = folly::makeGuard([&] { --state_->active; });
        ARROW_ASSIGN_OR_RAISE(auto file,
                              SubTreeFileSystem::OpenInputFile(path));
        std::shared_ptr<arrow::io::RandomAccessFile> tracked =
            std::make_shared<TrackedInputFile>(std::move(file), state_);
        failed_open.dismiss();
        return tracked;
    }

 private:
    std::shared_ptr<WorkerReadState> state_;
};

class StorageV2FieldDataVisitorTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        original_slice_size_ = FILE_SLICE_SIZE.load();
        FILE_SLICE_SIZE.store(DEFAULT_FIELD_MAX_MEMORY_LIMIT / kParallelDegree);
        directory_ = LocalDirectory::CreateOwned(
            std::filesystem::temp_directory_path().string(),
            "milvus-storage-v2-visitor-XXXXXX",
            "storage-v2 visitor test");
        StorageConfig config;
        config.storage_type = "local";
        config.root_path = directory_->Path();
        fs_ = InitArrowFileSystem(config);
        auto schema = std::make_shared<Schema>();
        field_id_ = schema->AddDebugField("value", DataType::INT64).get();
        schema_ = schema->ConvertToArrowSchema();
    }

    void
    TearDown() override {
        FILE_SLICE_SIZE.store(original_slice_size_);
    }

    void
    WriteFile(int file_id, int64_t first_value) {
        const auto parent =
            directory_->Path() + "/" + std::to_string(field_id_);
        ASSERT_TRUE(fs_->CreateDir(parent).ok());
        const auto path = parent + "/" + std::to_string(file_id);
        auto sink_result = fs_->OpenOutputStream(path);
        ASSERT_TRUE(sink_result.ok()) << sink_result.status().ToString();
        auto sink = sink_result.ValueOrDie();
        auto writer_result = ::parquet::arrow::FileWriter::Open(
            *schema_, arrow::default_memory_pool(), sink);
        ASSERT_TRUE(writer_result.ok()) << writer_result.status().ToString();
        auto writer = std::move(writer_result).ValueOrDie();
        milvus_storage::RowGroupMetadataVector row_groups;
        // ParquetFileWriter::Flush retains batches below the 1 MiB
        // row-group threshold. Write explicit physical groups so this reader
        // fixture can exercise many small groups without large payloads.
        for (int64_t group = 0; group < kGroups; ++group) {
            arrow::Int64Builder builder;
            for (int64_t row = 0; row < kRowsPerGroup; ++row) {
                ASSERT_TRUE(
                    builder.Append(first_value + group * kRowsPerGroup + row)
                        .ok());
            }
            std::shared_ptr<arrow::Array> values;
            ASSERT_TRUE(builder.Finish(&values).ok());
            ASSERT_TRUE(writer->NewBufferedRowGroup().ok());
            ASSERT_TRUE(writer
                            ->WriteRecordBatch(*arrow::RecordBatch::Make(
                                schema_, kRowsPerGroup, {values}))
                            .ok());
            row_groups.Add(milvus_storage::RowGroupMetadata(
                kGroupBytes, kRowsPerGroup, group * kRowsPerGroup));
        }
        milvus_storage::GroupFieldIDList groups;
        groups.AddFieldIDList(milvus_storage::FieldIDList({field_id_}));
        ASSERT_TRUE(
            writer
                ->AddKeyValueMetadata(arrow::key_value_metadata(
                    {milvus_storage::GROUP_FIELD_ID_LIST_META_KEY,
                     milvus_storage::ROW_GROUP_META_KEY,
                     milvus_storage::STORAGE_VERSION_KEY},
                    {groups.Serialize(), row_groups.Serialize(), "1.0.0"}))
                .ok());
        ASSERT_TRUE(writer->Close().ok());
        ASSERT_TRUE(sink->Close().ok());
        auto reader_result =
            milvus_storage::FileRowGroupReader::Make(fs_, path);
        ASSERT_TRUE(reader_result.ok()) << reader_result.status().ToString();
        auto reader = reader_result.ValueOrDie();
        const auto metadata = reader->file_metadata();
        const auto parquet_metadata = metadata->GetParquetMetadata();
        data_offsets_.clear();
        data_offsets_.reserve(kGroups);
        for (int64_t group = 0; group < kGroups; ++group) {
            const auto column =
                parquet_metadata->RowGroup(group)->ColumnChunk(0);
            data_offsets_.push_back(column->has_dictionary_page()
                                        ? column->dictionary_page_offset()
                                        : column->data_page_offset());
        }
        ASSERT_TRUE(reader->Close().ok());
        if (files_.empty()) {
            files_.emplace_back();
        }
        files_.front().push_back(path);
    }

    static std::vector<int64_t>
    Values(const std::vector<FieldDataPtr>& batches) {
        std::vector<int64_t> values;
        for (const auto& batch : batches) {
            const auto* data = static_cast<const int64_t*>(batch->Data());
            values.insert(values.end(), data, data + batch->get_num_rows());
        }
        return values;
    }

    static std::vector<int64_t>
    Sequence(int64_t first, size_t count) {
        std::vector<int64_t> values(count);
        std::iota(values.begin(), values.end(), first);
        return values;
    }

    VisitOutcome
    Visit(const FieldDataVisitor& visitor,
          milvus_storage::ArrowFileSystemPtr fs,
          size_t window_bytes = kWindowBytes,
          size_t max_rows = 0,
          size_t offset = 0) {
        return detail::VisitFieldDataFromStorageV2Paged(files_,
                                                        field_id_,
                                                        DataType::INT64,
                                                        DataType::NONE,
                                                        1,
                                                        std::move(fs),
                                                        visitor,
                                                        max_rows,
                                                        offset,
                                                        window_bytes);
    }

    void
    ExpectJoined(const std::shared_ptr<WorkerReadState>& workers) {
        EXPECT_EQ(workers->opened.load(), 3);
        EXPECT_EQ(workers->active.load(), 0);
    }

    static constexpr size_t kParallelDegree = 4;
    static constexpr int64_t kRowsPerGroup = 4;
    static constexpr int64_t kGroups = 10;
    static constexpr size_t kGroupBytes = kRowsPerGroup * sizeof(int64_t);
    static constexpr size_t kWindowBytes = 5 * kGroupBytes;
    static constexpr int64_t kRowsPerFile = kRowsPerGroup * kGroups;
    int64_t original_slice_size_;
    int64_t field_id_;
    std::shared_ptr<LocalDirectory> directory_;
    milvus_storage::ArrowFileSystemPtr fs_;
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::vector<std::string>> files_;
    std::vector<int64_t> data_offsets_;
};

TEST_F(StorageV2FieldDataVisitorTest, SmallWindowsPreserveContentsAndOrder) {
    ASSERT_NO_FATAL_FAILURE(WriteFile(10, kRowsPerFile));
    ASSERT_NO_FATAL_FAILURE(WriteFile(2, 0));
    std::vector<FieldDataPtr> batches;
    EXPECT_EQ(Visit(
                  [&](FieldDataPtr batch) {
                      batches.push_back(std::move(batch));
                      return VisitControl::Continue;
                  },
                  fs_),
              VisitOutcome::Exhausted);
    EXPECT_EQ(Values(batches), Sequence(0, 2 * kRowsPerFile));
    // Five-group windows split into three blocks, proving parallel delivery.
    ASSERT_EQ(batches.size(), 12);
    EXPECT_EQ(batches.front()->get_num_rows(), 2 * kRowsPerGroup);
}

TEST_F(StorageV2FieldDataVisitorTest, StopInsideWindowDrainsAndJoins) {
    ASSERT_NO_FATAL_FAILURE(WriteFile(2, 0));
    auto workers = std::make_shared<WorkerReadState>();
    auto fs = std::make_shared<TrackingFileSystem>(fs_, workers);
    size_t calls = 0;
    EXPECT_EQ(Visit(
                  [&](FieldDataPtr batch) {
                      ++calls;
                      EXPECT_EQ(Values({batch}), Sequence(0, 8));
                      return VisitControl::Stop;
                  },
                  fs),
              VisitOutcome::Stopped);
    EXPECT_EQ(calls, 1);
    ExpectJoined(workers);
}

TEST_F(StorageV2FieldDataVisitorTest,
       ProducerFailureWaitsForOtherWindowReaders) {
    ASSERT_NO_FATAL_FAILURE(WriteFile(1000, 0));
    auto workers = std::make_shared<WorkerReadState>();
    auto fs = std::make_shared<TrackingFileSystem>(fs_, workers);
    std::mutex mutex;
    std::condition_variable cv;
    bool blocked = false;
    bool failed = false;
    bool release = false;
    // Match physical data offsets so the first block fails and the second
    // remains live, independently of thread-pool scheduling. Footer reads
    // begin at zero for this small fixture and do not enter either branch.
    ASSERT_GT(data_offsets_[0], 0);
    workers->before_read = [&](int64_t position, int64_t) {
        std::unique_lock lock(mutex);
        if (position == data_offsets_[0]) {
            failed = true;
            cv.notify_all();
            return arrow::Status::IOError("injected producer failure");
        }
        if (position == data_offsets_[2]) {
            blocked = true;
            cv.notify_all();
            if (!cv.wait_for(
                    lock, std::chrono::seconds(10), [&] { return release; })) {
                return arrow::Status::IOError("reader release timed out");
            }
        }
        return arrow::Status::OK();
    };
    auto result = std::async(std::launch::async, [&] {
        workers->caller = std::this_thread::get_id();
        return Visit([](FieldDataPtr) { return VisitControl::Continue; }, fs);
    });
    auto release_guard = folly::makeGuard([&] {
        std::lock_guard lock(mutex);
        release = true;
        cv.notify_all();
    });
    {
        std::unique_lock lock(mutex);
        EXPECT_TRUE(cv.wait_for(
            lock, std::chrono::seconds(5), [&] { return blocked && failed; }));
    }
    // A failed block must not let its sibling outlive this call. Keep a
    // sibling blocked until this assertion has observed the pending join.
    EXPECT_EQ(result.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    {
        std::lock_guard lock(mutex);
        release = true;
        cv.notify_all();
    }
    try {
        result.get();
        FAIL() << "expected producer failure";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), StorageError);
        EXPECT_NE(std::string(error.what()).find("injected producer failure"),
                  std::string::npos);
    }
    ExpectJoined(workers);
}

TEST_F(StorageV2FieldDataVisitorTest, PaginationCrossesByteWindows) {
    ASSERT_NO_FATAL_FAILURE(WriteFile(2, 0));
    std::vector<FieldDataPtr> batches;
    EXPECT_EQ(Visit(
                  [&](FieldDataPtr batch) {
                      batches.push_back(std::move(batch));
                      return VisitControl::Continue;
                  },
                  fs_,
                  2 * kGroupBytes,
                  21,
                  10),
              VisitOutcome::Exhausted);
    EXPECT_EQ(Values(batches), Sequence(8, 24));
}

TEST_F(StorageV2FieldDataVisitorTest, SliceLargerThanBudgetKeepsOneWorker) {
    ASSERT_NO_FATAL_FAILURE(WriteFile(2, 0));
    FILE_SLICE_SIZE.store(DEFAULT_FIELD_MAX_MEMORY_LIMIT + 1);
    const auto full = GetFieldDatasFromStorageV2(
        files_, field_id_, DataType::INT64, DataType::NONE, 1, fs_);
    EXPECT_EQ(Values(full), Sequence(0, kRowsPerFile));
    ASSERT_EQ(full.size(), 1);
    // TearDown restores FILE_SLICE_SIZE even after a failed assertion.
}

}  // namespace
}  // namespace milvus::storage::test
