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
#include <unistd.h>

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/EasyAssert.h"
#include "folly/executors/ManualExecutor.h"
#include "storage/RemoteInputStream.h"
#include "test_utils/AsyncLoadTestUtils.h"

namespace milvus::storage {
namespace {

struct ReadOutcome {
    arrow::Status status;
    int64_t bytes_read;
    bool advance_position_on_error = false;
    std::string data;
};

class FailingRandomAccessFile : public arrow::io::RandomAccessFile {
 public:
    explicit FailingRandomAccessFile(
        std::vector<ReadOutcome> read_outcomes,
        std::string content = "abcdefghijklmnop",
        std::vector<arrow::Status> seek_outcomes = {})
        : read_outcomes_(std::move(read_outcomes)),
          content_(std::move(content)),
          seek_outcomes_(std::move(seek_outcomes)) {
    }

    arrow::Status
    Close() override {
        closed_ = true;
        return arrow::Status::OK();
    }

    arrow::Result<int64_t>
    Tell() const override {
        return position_;
    }

    bool
    closed() const override {
        return closed_;
    }

    arrow::Status
    Seek(int64_t position) override {
        seek_positions_.push_back(position);
        if (next_seek_outcome_ < seek_outcomes_.size()) {
            auto outcome = seek_outcomes_[next_seek_outcome_++];
            if (!outcome.ok()) {
                return outcome;
            }
        }
        position_ = position;
        return arrow::Status::OK();
    }

    arrow::Result<int64_t>
    Read(int64_t nbytes, void* out) override {
        ++read_calls_;
        if (next_outcome_ < read_outcomes_.size()) {
            auto outcome = read_outcomes_[next_outcome_++];
            if (!outcome.status.ok()) {
                FillBuffer(outcome.bytes_read, out, outcome.data);
                if (outcome.advance_position_on_error) {
                    position_ += outcome.bytes_read;
                }
                return outcome.status;
            }
            return ReadOk(outcome.bytes_read, out, outcome.data);
        }

        return ReadOk(nbytes, out);
    }

    arrow::Result<std::shared_ptr<arrow::Buffer>>
    Read(int64_t) override {
        return arrow::Status::NotImplemented("buffer reads are not needed");
    }

    arrow::Result<int64_t>
    ReadAt(int64_t position, int64_t nbytes, void* out) override {
        position_ = position;
        return Read(nbytes, out);
    }

    arrow::Result<int64_t>
    GetSize() override {
        return static_cast<int64_t>(content_.size());
    }

    int
    read_calls() const {
        return read_calls_;
    }

    const std::vector<int64_t>&
    seek_positions() const {
        return seek_positions_;
    }

 private:
    arrow::Result<int64_t>
    ReadOk(int64_t nbytes, void* out, const std::string& override_data = "") {
        FillBuffer(nbytes, out, override_data);
        position_ += nbytes;
        return nbytes;
    }

    void
    FillBuffer(int64_t nbytes,
               void* out,
               const std::string& override_data = "") const {
        auto* out_data = static_cast<uint8_t*>(out);
        for (int64_t i = 0; i < nbytes; ++i) {
            out_data[i] = static_cast<uint8_t>(
                override_data.empty()
                    ? content_[static_cast<size_t>(position_ + i) %
                               content_.size()]
                    : override_data[static_cast<size_t>(i) %
                                    override_data.size()]);
        }
    }

    std::vector<ReadOutcome> read_outcomes_;
    std::string content_;
    std::vector<arrow::Status> seek_outcomes_;
    std::vector<int64_t> seek_positions_;
    size_t next_outcome_ = 0;
    size_t next_seek_outcome_ = 0;
    int64_t position_ = 0;
    int read_calls_ = 0;
    bool closed_ = false;
};

class TempFile {
 public:
    TempFile() {
        fd_ = ::mkstemp(path_);
    }

    ~TempFile() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
        ::unlink(path_);
    }

    int
    fd() const {
        return fd_;
    }

 private:
    char path_[48] = "/tmp/milvus_remote_input_stream_test_XXXXXX";
    int fd_ = -1;
};

class PipeFile {
 public:
    PipeFile() {
        if (::pipe(fds_) != 0) {
            fds_[0] = -1;
            fds_[1] = -1;
        }
    }

    ~PipeFile() {
        if (fds_[0] >= 0) {
            ::close(fds_[0]);
        }
        if (fds_[1] >= 0) {
            ::close(fds_[1]);
        }
    }

    int
    write_fd() const {
        return fds_[1];
    }

 private:
    int fds_[2] = {-1, -1};
};

arrow::Status
InternalFailedFlushStatus() {
    return arrow::Status::IOError(
        "google::cloud::Status(INTERNAL: Failed to flush response stream)");
}

arrow::Status
UnavailableFailedFlushStatus() {
    return arrow::Status::IOError(
        "google::cloud::Status(UNAVAILABLE: Failed to flush response stream)");
}

arrow::Status
InternalDifferentStatus() {
    return arrow::Status::IOError(
        "google::cloud::Status(INTERNAL: connection reset)");
}

arrow::Status
SeekResetStatus() {
    return arrow::Status::IOError("seek reset failed");
}

class AsyncOpenFileSystem : public arrow::fs::LocalFileSystem {
 public:
    explicit AsyncOpenFileSystem(
        arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> file)
        : file_(std::move(file)) {
    }

    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFile(const std::string&) override {
        ADD_FAILURE() << "The async stream must use OpenInputFileAsync";
        return arrow::Status::NotImplemented("sync open");
    }

    arrow::Future<std::shared_ptr<arrow::io::RandomAccessFile>>
    OpenInputFileAsync(const std::string& path) override {
        opened_path = path;
        return arrow::Future<
            std::shared_ptr<arrow::io::RandomAccessFile>>::MakeFinished(file_);
    }

    std::string opened_path;

 private:
    arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> file_;
};

TEST(RemoteInputStreamTest, AsyncOpenCachesNativeSizeAndPreservesObjectPaths) {
    for (const bool is_index : {true, false}) {
        auto file = std::make_shared<milvus::test::ControlledDirectReadFile>(
            std::vector<uint8_t>{'a', 'b', 'c'});
        auto size = arrow::Future<int64_t>::Make();
        file->SetSizeFuture(size);
        auto fs = std::make_shared<AsyncOpenFileSystem>(file);
        FileManagerContext ctx(FieldDataMeta{},
                               IndexMeta{},
                               std::make_shared<LocalChunkManager>("root"),
                               fs);
        ctx.set_stats_base_path("stats/text");
        MemFileManagerImpl manager(ctx);
        auto opened = manager.OpenInputStreamAsync("local/packed.v3", is_index)
                          .via(ResolveAsyncLoadExecutor(
                              {}, proto::common::LoadPriority::HIGH));
        EXPECT_TRUE(file->WaitForSizeCall());
        EXPECT_FALSE(opened.isReady());
        size.MarkFinished(3);
        auto stream = std::move(opened).get();
        EXPECT_EQ(stream->Size(), 3);
        EXPECT_EQ(stream->Size(), 3);
        EXPECT_EQ(file->GetSizeCalls(), 0);
        EXPECT_EQ(
            fs->opened_path,
            (is_index ? manager.GetRemoteIndexObjectPrefix() : "stats/text") +
                "/packed.v3");

        std::array<char, 4> data{};
        EXPECT_EQ(
            std::move(stream->ReadAtAsync(data.data(), 1, data.size())).get(),
            2);
        EXPECT_EQ(std::string(data.data(), 2), "bc");
        EXPECT_EQ(data[2], 0);
        EXPECT_EQ(stream->ReadAtAsync(nullptr, 3, 0).get(), 0);
        EXPECT_EQ(file->ReadAtCalls(), 0);
        EXPECT_EQ(file->AsyncReadCalls(), 0);
        EXPECT_EQ(file->DirectReadCalls().size(), 1);
    }
}

TEST(RemoteInputStreamTest, AsyncOpenPreservesOpenAndSizeErrors) {
    const auto status = milvus_storage::MakeExtendError(
        milvus_storage::ExtendStatusCode::StorageTransientThrottling,
        "throttled");
    for (const bool fail_open : {true, false}) {
        auto file = std::make_shared<milvus::test::ControlledDirectReadFile>(
            std::vector<uint8_t>{'a'});
        file->SetSizeFuture(arrow::Future<int64_t>::MakeFinished(status));
        auto fs = fail_open ? std::make_shared<AsyncOpenFileSystem>(status)
                            : std::make_shared<AsyncOpenFileSystem>(file);
        try {
            folly::coro::blockingWait(
                RemoteInputStream::OpenAsync(fs, "packed"));
            FAIL() << "expected storage error";
        } catch (const SegcoreError& error) {
            EXPECT_EQ(error.get_error_code(),
                      milvus_storage::ToSegcoreError(status).get_error_code());
        }
        EXPECT_EQ(file->GetSizeCalls(), 0);
    }
}

TEST(RemoteInputStreamTest, AsyncOpenRejectsNegativeSize) {
    auto file = std::make_shared<milvus::test::ControlledDirectReadFile>(
        std::vector<uint8_t>{'a'});
    file->SetSizeFuture(arrow::Future<int64_t>::MakeFinished(-1));
    auto fs = std::make_shared<AsyncOpenFileSystem>(file);
    EXPECT_THROW(
        folly::coro::blockingWait(RemoteInputStream::OpenAsync(fs, "packed")),
        SegcoreError);
}

TEST(RemoteInputStreamTest, AsyncFallbackCopiesOnCallerExecutor) {
    class PendingReadFile : public milvus::test::AsyncTrackingRandomAccessFile {
     public:
        PendingReadFile() : AsyncTrackingRandomAccessFile({'a', 'b', 'c'}) {
        }

        arrow::Future<std::shared_ptr<arrow::Buffer>>
        ReadAsync(const arrow::io::IOContext&, int64_t, int64_t) override {
            started = true;
            return pending;
        }

        bool started{false};
        arrow::Future<std::shared_ptr<arrow::Buffer>> pending =
            arrow::Future<std::shared_ptr<arrow::Buffer>>::Make();
    };

    auto file = std::make_shared<PendingReadFile>();
    RemoteInputStream stream(file);
    folly::ManualExecutor executor;
    std::array<char, 3> data{};
    auto read = stream.ReadAtAsync(data.data(), 0, data.size()).via(&executor);
    executor.drain();
    EXPECT_TRUE(file->started);
    EXPECT_FALSE(read.isReady());

    // Completing Arrow IO must only enqueue the copy on the caller's executor.
    file->pending.MarkFinished(arrow::Buffer::FromString("abc"));
    EXPECT_EQ(data, (std::array<char, 3>{}));
    EXPECT_FALSE(read.isReady());
    executor.drain();
    EXPECT_EQ(std::move(read).get(), data.size());
    EXPECT_EQ(std::string(data.data(), data.size()), "abc");
}

TEST(RemoteInputStreamTest, RetriesInternalFailedFlushError) {
    auto file =
        std::make_shared<FailingRandomAccessFile>(std::vector<ReadOutcome>{
            {InternalFailedFlushStatus(), 0}, {arrow::Status::OK(), 4}});
    auto* file_ptr = file.get();
    RemoteInputStream stream(std::move(file));

    std::vector<uint8_t> data(4);
    EXPECT_EQ(stream.Read(data.data(), data.size()), data.size());
    EXPECT_EQ(file_ptr->read_calls(), 2);
}

TEST(RemoteInputStreamTest, RetriesFailedFlushWithoutInternalError) {
    auto file =
        std::make_shared<FailingRandomAccessFile>(std::vector<ReadOutcome>{
            {UnavailableFailedFlushStatus(), 0}, {arrow::Status::OK(), 4}});
    auto* file_ptr = file.get();
    RemoteInputStream stream(std::move(file));

    std::vector<uint8_t> data(4);
    EXPECT_EQ(stream.Read(data.data(), data.size()), data.size());
    EXPECT_EQ(file_ptr->read_calls(), 2);
}

TEST(RemoteInputStreamTest, RestoresPositionBeforeRetryingSequentialRead) {
    auto file =
        std::make_shared<FailingRandomAccessFile>(std::vector<ReadOutcome>{
            {InternalFailedFlushStatus(), 2, true}, {arrow::Status::OK(), 4}});
    auto* file_ptr = file.get();
    RemoteInputStream stream(std::move(file));

    std::vector<uint8_t> data(4);
    EXPECT_EQ(stream.Read(data.data(), data.size()), data.size());
    EXPECT_EQ(std::string(data.begin(), data.end()), "abcd");
    EXPECT_EQ(stream.Tell(), 4);
    EXPECT_EQ(file_ptr->read_calls(), 2);
    EXPECT_EQ(file_ptr->seek_positions(), (std::vector<int64_t>{0}));
}

TEST(RemoteInputStreamTest, SeeksBackToOriginalOffsetBeforeRetryingRead) {
    auto file =
        std::make_shared<FailingRandomAccessFile>(std::vector<ReadOutcome>{
            {InternalFailedFlushStatus(), 2, true}, {arrow::Status::OK(), 4}});
    auto* file_ptr = file.get();
    RemoteInputStream stream(std::move(file));
    ASSERT_TRUE(stream.Seek(5));

    std::vector<uint8_t> data(4);
    EXPECT_EQ(stream.Read(data.data(), data.size()), data.size());
    EXPECT_EQ(std::string(data.begin(), data.end()), "fghi");
    EXPECT_EQ(stream.Tell(), 9);
    EXPECT_EQ(file_ptr->read_calls(), 2);
    EXPECT_EQ(file_ptr->seek_positions(), (std::vector<int64_t>{5, 5}));
}

TEST(RemoteInputStreamTest, RestoresPositionBeforeRetryingReadToFile) {
    auto file =
        std::make_shared<FailingRandomAccessFile>(std::vector<ReadOutcome>{
            {InternalFailedFlushStatus(), 2, true}, {arrow::Status::OK(), 4}});
    auto* file_ptr = file.get();
    RemoteInputStream stream(std::move(file));
    TempFile temp_file;
    ASSERT_GE(temp_file.fd(), 0);

    EXPECT_EQ(stream.Read(temp_file.fd(), 4), 4);
    ASSERT_EQ(::lseek(temp_file.fd(), 0, SEEK_SET), 0);

    std::array<char, 4> data{};
    ASSERT_EQ(::read(temp_file.fd(), data.data(), data.size()),
              static_cast<ssize_t>(data.size()));
    EXPECT_EQ(std::string(data.data(), data.size()), "abcd");
    EXPECT_EQ(stream.Tell(), 4);
    EXPECT_EQ(file_ptr->read_calls(), 2);
    EXPECT_EQ(file_ptr->seek_positions(), (std::vector<int64_t>{0}));
}

TEST(RemoteInputStreamTest, WritesOnlyActualBytesReadAfterRetryingReadToFile) {
    auto file =
        std::make_shared<FailingRandomAccessFile>(std::vector<ReadOutcome>{
            {InternalFailedFlushStatus(), 4, false, "zzzz"},
            {arrow::Status::OK(), 2},
            {arrow::Status::OK(), 2}});
    auto* file_ptr = file.get();
    RemoteInputStream stream(std::move(file));
    TempFile temp_file;
    ASSERT_GE(temp_file.fd(), 0);

    EXPECT_EQ(stream.Read(temp_file.fd(), 4), 4);
    ASSERT_EQ(::lseek(temp_file.fd(), 0, SEEK_SET), 0);

    std::array<char, 4> data{};
    ASSERT_EQ(::read(temp_file.fd(), data.data(), data.size()),
              static_cast<ssize_t>(data.size()));
    EXPECT_EQ(std::string(data.data(), data.size()), "abcd");
    EXPECT_EQ(stream.Tell(), 4);
    EXPECT_EQ(file_ptr->read_calls(), 3);
}

TEST(RemoteInputStreamTest, ThrowsWhenReadToFileMakesNoProgress) {
    auto file =
        std::make_shared<FailingRandomAccessFile>(std::vector<ReadOutcome>{
            {arrow::Status::OK(), 0}, {arrow::Status::OK(), 4}});
    auto* file_ptr = file.get();
    RemoteInputStream stream(std::move(file));
    TempFile temp_file;
    ASSERT_GE(temp_file.fd(), 0);

    EXPECT_THROW(stream.Read(temp_file.fd(), 4), milvus::SegcoreError);
    EXPECT_EQ(stream.Tell(), 0);
    EXPECT_EQ(file_ptr->read_calls(), 1);
}

TEST(RemoteInputStreamTest, ThrowsWhenReadToFileFsyncFails) {
    auto file = std::make_shared<FailingRandomAccessFile>(
        std::vector<ReadOutcome>{{arrow::Status::OK(), 4}});
    auto* file_ptr = file.get();
    RemoteInputStream stream(std::move(file));
    PipeFile pipe_file;
    ASSERT_GE(pipe_file.write_fd(), 0);

    try {
        stream.Read(pipe_file.write_fd(), 4);
        FAIL() << "expected SegcoreError";
    } catch (const milvus::SegcoreError& e) {
        std::string message = e.what();
        EXPECT_NE(message.find("Failed to fsync file"), std::string::npos);
    }
    EXPECT_EQ(stream.Tell(), 4);
    EXPECT_EQ(file_ptr->read_calls(), 1);
}

TEST(RemoteInputStreamTest, ReportsOriginalReadErrorWhenRetryResetFails) {
    auto file = std::make_shared<FailingRandomAccessFile>(
        std::vector<ReadOutcome>{{InternalFailedFlushStatus(), 2, true}},
        "abcdefghijklmnop",
        std::vector<arrow::Status>{SeekResetStatus()});
    RemoteInputStream stream(std::move(file));

    std::vector<uint8_t> data(4);
    try {
        stream.Read(data.data(), data.size());
        FAIL() << "expected SegcoreError";
    } catch (const milvus::SegcoreError& e) {
        std::string message = e.what();
        EXPECT_NE(message.find("seek reset failed"), std::string::npos);
        EXPECT_NE(message.find("offset: 0"), std::string::npos);
        EXPECT_NE(message.find("Failed to flush response stream"),
                  std::string::npos);
    }
}

TEST(RemoteInputStreamTest, DoesNotRetryInternalErrorWithoutFailedFlush) {
    auto file =
        std::make_shared<FailingRandomAccessFile>(std::vector<ReadOutcome>{
            {InternalDifferentStatus(), 0}, {arrow::Status::OK(), 4}});
    auto* file_ptr = file.get();
    RemoteInputStream stream(std::move(file));

    std::vector<uint8_t> data(4);
    EXPECT_THROW(stream.Read(data.data(), data.size()), milvus::SegcoreError);
    EXPECT_EQ(file_ptr->read_calls(), 1);
}

TEST(RemoteInputStreamTest, RetriesFailedFlushNonIoError) {
    auto file =
        std::make_shared<FailingRandomAccessFile>(std::vector<ReadOutcome>{
            {arrow::Status::Invalid("google::cloud::Status(INTERNAL: Failed to "
                                    "flush response stream)"),
             0},
            {arrow::Status::OK(), 4}});
    auto* file_ptr = file.get();
    RemoteInputStream stream(std::move(file));

    std::vector<uint8_t> data(4);
    EXPECT_EQ(stream.Read(data.data(), data.size()), data.size());
    EXPECT_EQ(file_ptr->read_calls(), 2);
}

}  // namespace
}  // namespace milvus::storage
