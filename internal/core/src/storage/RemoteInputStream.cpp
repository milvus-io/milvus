#include <unistd.h>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include "log/Log.h"

#include "RemoteInputStream.h"
#include "arrow/io/interfaces.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/buffer.h"
#include "arrow/util/thread_pool.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "folly/Try.h"
#include "folly/coro/Promise.h"
#include "folly/coro/WithCancellation.h"
#include "milvus-storage/common/extend_status.h"
#include "milvus-storage/filesystem/async_random_access_file.h"
#include "storage/StatusToErrorCode.h"

namespace milvus::storage {
namespace {

constexpr int kRemoteInputStreamMaxReadRetries = 5;
constexpr const char* kFailedFlushResponseStreamError =
    "Failed to flush response stream";

// Preserve Arrow status details until the stream has classified retryability.
template <typename T>
folly::coro::Future<arrow::Result<T>>
AwaitFileResult(arrow::Future<T> arrow_future) {
    auto [promise, future] =
        folly::coro::makePromiseContract<arrow::Result<T>>();
    auto completion = std::make_shared<folly::coro::Promise<arrow::Result<T>>>(
        std::move(promise));
    arrow_future.AddCallback([completion](const arrow::Result<T>& result) {
        completion->setValue(result);
    });
    return std::move(future);
}

// currently only retry failed flush response stream error
bool
IsRetryableReadError(const arrow::Status& status) {
    return status.ToString().find(kFailedFlushResponseStreamError) !=
           std::string::npos;
}

template <typename ReadFunc, typename ResetFunc>
arrow::Result<int64_t>
ReadWithRetry(const char* operation,
              size_t size,
              size_t file_size,
              size_t offset,
              ReadFunc&& read_func,
              ResetFunc&& reset_func) {
    auto result = read_func();
    int retries = 0;
    int64_t sleep_ms = 1;
    for (int retry = 1; !result.ok() && IsRetryableReadError(result.status()) &&
                        retry <= kRemoteInputStreamMaxReadRetries;
         ++retry) {
        retries = retry;
        LOG_WARN(
            "Failed to {} from remote input stream, retry {}/{}, size: {}, "
            "file size: {}, offset: {}, error is: {}, sleep {} ms before retry",
            operation,
            retry,
            kRemoteInputStreamMaxReadRetries,
            size,
            file_size,
            offset,
            result.status().ToString(),
            sleep_ms);
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
        sleep_ms *= 2;
        auto reset_status = reset_func();
        if (!reset_status.ok()) {
            auto read_status = result.status();
            LOG_WARN(
                "Failed to reset remote input stream before retrying {}, "
                "size: {}, file size: {}, offset: {}, original error is: {}, "
                "current error is: {}",
                operation,
                size,
                file_size,
                offset,
                read_status.ToString(),
                reset_status.ToString());
            return reset_status.WithMessage(
                "Failed to reset remote input stream before retrying ",
                operation,
                ", size: ",
                std::to_string(size),
                ", file size: ",
                std::to_string(file_size),
                ", offset: ",
                std::to_string(offset),
                ", original error is: ",
                read_status.ToString(),
                ", current error is: ",
                reset_status.ToString());
        }
        result = read_func();
    }
    if (result.ok() && retries > 0) {
        LOG_INFO(
            "Succeeded to {} from remote input stream after {} retries, size: "
            "{}, file size: {}, offset: {}",
            operation,
            retries,
            size,
            file_size,
            offset);
    }
    return result;
}

}  // namespace

RemoteInputStream::RemoteInputStream(
    std::shared_ptr<arrow::io::RandomAccessFile>&& remote_file)
    : remote_file_(std::move(remote_file)) {
    auto status = remote_file_->GetSize();
    if (!status.ok()) {
        ThrowInfo(ArrowStatusToErrorCode(status.status()),
                  "Failed to get size of remote file: {}",
                  status.status().ToString());
    }
    file_size_ = static_cast<size_t>(status.ValueOrDie());
}

folly::coro::Task<std::shared_ptr<InputStream>>
RemoteInputStream::OpenAsync(std::shared_ptr<arrow::fs::FileSystem> fs,
                             std::string path) {
    auto opened = co_await folly::coro::co_withCancellation(
        folly::CancellationToken{},
        AwaitFileResult(fs->OpenInputFileAsync(path)));
    if (!opened.ok()) {
        throw milvus_storage::ToSegcoreError(opened.status());
    }
    auto file = std::move(*opened);
    folly::coro::Future<arrow::Result<int64_t>> size_future;
    if (auto* native =
            dynamic_cast<milvus_storage::NonBlockingRandomAccessFile*>(
                file.get())) {
        size_future = AwaitFileResult(native->GetSizeAsync());
    } else {
        // Arrow's task runner does not transport thrown C++ exceptions. Keep
        // the generic filesystem fallback's exception type across the I/O hop.
        auto [promise, future] =
            folly::coro::makePromiseContract<arrow::Result<int64_t>>();
        auto completion =
            std::make_shared<folly::coro::Promise<arrow::Result<int64_t>>>(
                std::move(promise));
        auto submitted =
            file->io_context().executor()->Spawn([file, completion] {
                completion->setResult(
                    folly::makeTryWith([&] { return file->GetSize(); }));
            });
        if (!submitted.ok()) {
            throw milvus_storage::ToSegcoreError(submitted);
        }
        size_future = std::move(future);
    }
    auto size = co_await folly::coro::co_withCancellation(
        folly::CancellationToken{}, std::move(size_future));
    if (!size.ok()) {
        throw milvus_storage::ToSegcoreError(size.status());
    }
    AssertInfo(*size >= 0, "Negative remote file size: {}", *size);
    co_return std::shared_ptr<InputStream>(
        new RemoteInputStream(std::move(file), static_cast<size_t>(*size)));
}

folly::SemiFuture<size_t>
RemoteInputStream::ReadAtAsync(void* data, size_t offset, size_t size) {
    return ReadAtAsyncImpl(data, offset, size).semi();
}

folly::coro::Task<size_t>
RemoteInputStream::ReadAtAsyncImpl(void* data, size_t offset, size_t size) {
    AssertInfo((data != nullptr || size == 0) &&
                   offset <= std::numeric_limits<int64_t>::max(),
               "Invalid async stream read offset or destination");
    if (size == 0 || offset >= file_size_) {
        co_return 0;
    }
    const auto bytes = std::min(size, file_size_ - offset);
    auto* native = dynamic_cast<milvus_storage::NonBlockingRandomAccessFile*>(
        remote_file_.get());
    for (int attempt = 0;; ++attempt) {
        arrow::Result<int64_t> result;
        if (native != nullptr) {
            result = co_await folly::coro::co_withCancellation(
                folly::CancellationToken{},
                AwaitFileResult(native->ReadAtAsyncInto(
                    offset, bytes, static_cast<uint8_t*>(data))));
        } else {
            auto read = co_await folly::coro::co_withCancellation(
                folly::CancellationToken{},
                AwaitFileResult(remote_file_->ReadAsync(offset, bytes)));
            if (!read.ok()) {
                result = read.status();
            } else {
                // Resume on the caller's executor before copying a whole slice.
                // Arrow's completion callback only forwards the buffer.
                const auto& buffer = *read;
                if (buffer == nullptr || buffer->size() < 0 ||
                    static_cast<uint64_t>(buffer->size()) > bytes) {
                    result =
                        arrow::Status::IOError("Invalid async read buffer");
                } else {
                    if (buffer->size() != 0) {
                        std::memcpy(data, buffer->data(), buffer->size());
                    }
                    result = buffer->size();
                }
            }
        }
        if (result.ok()) {
            AssertInfo(*result >= 0 && static_cast<uint64_t>(*result) <= bytes,
                       "Invalid async read count: {} for {} bytes",
                       *result,
                       bytes);
            co_return static_cast<size_t>(*result);
        }
        const auto detail =
            milvus_storage::ExtendStatusDetail::UnwrapStatus(result.status());
        const bool retryable = detail ? detail->retryable()
                                      : IsRetryableReadError(result.status());
        if (!retryable || attempt == kRemoteInputStreamMaxReadRetries) {
            throw milvus_storage::ToSegcoreError(result.status());
        }
        co_await folly::coro::co_withCancellation(
            folly::CancellationToken{},
            folly::futures::sleep(std::chrono::milliseconds(1 << attempt)));
    }
}

size_t
RemoteInputStream::Read(void* data, size_t size) {
    auto offset = static_cast<int64_t>(Tell());
    auto status = ReadWithRetry(
        "read",
        size,
        file_size_,
        offset,
        [this, size, data]() { return remote_file_->Read(size, data); },
        [this, offset]() { return remote_file_->Seek(offset); });
    if (!status.ok()) {
        ThrowInfo(
            ArrowStatusToErrorCode(status.status()),
            "Failed to read from remote input stream, operation: read, offset: "
            "{}, size: {}, file size: {}, error: {}",
            offset,
            size,
            file_size_,
            status.status().ToString());
    }
    return static_cast<size_t>(status.ValueOrDie());
}

size_t
RemoteInputStream::ReadAt(void* data, size_t offset, size_t size) {
    auto status = ReadWithRetry(
        "read at offset",
        size,
        file_size_,
        offset,
        [this, offset, size, data]() {
            return remote_file_->ReadAt(offset, size, data);
        },
        []() { return arrow::Status::OK(); });
    if (!status.ok()) {
        ThrowInfo(ArrowStatusToErrorCode(status.status()),
                  "Failed to read from remote input stream, operation: read at "
                  "offset, offset: {}, size: {}, file size: {}, error: {}",
                  offset,
                  size,
                  file_size_,
                  status.status().ToString());
    }
    return static_cast<size_t>(status.ValueOrDie());
}

size_t
RemoteInputStream::Read(int fd, size_t size) {
    size_t read_batch_size =
        std::min(size, static_cast<size_t>(DEFAULT_INDEX_FILE_SLICE_SIZE));
    size_t rest_size = size;
    std::vector<uint8_t> data(read_batch_size);

    while (rest_size > 0) {
        size_t read_size = std::min(rest_size, read_batch_size);
        auto offset = static_cast<int64_t>(Tell());
        auto status = ReadWithRetry(
            "read to file",
            read_size,
            file_size_,
            offset,
            [this, read_size, &data]() {
                return remote_file_->Read(read_size, data.data());
            },
            [this, offset]() { return remote_file_->Seek(offset); });
        if (!status.ok()) {
            ThrowInfo(
                ArrowStatusToErrorCode(status.status()),
                "Failed to read from remote input stream, operation: read "
                "to file, offset: {}, size: {}, rest size: {}, file size: "
                "{}, error: {}",
                offset,
                read_size,
                rest_size,
                file_size_,
                status.status().ToString());
        }
        auto bytes_read = status.ValueOrDie();
        if (!(bytes_read > 0)) {
            ThrowInfo(
                ErrorCode::FileReadFailed,
                "Failed to read from remote input stream, operation: read "
                "to file, offset: {}, read zero bytes, rest size: {}, file "
                "size: {}",
                offset,
                rest_size,
                file_size_);
        }
        if (!(bytes_read <= static_cast<int64_t>(read_size))) {
            ThrowInfo(ErrorCode::FileReadFailed,
                      "Remote input stream returned more bytes than requested, "
                      "operation: read to file, offset: {}, bytes read: {}, "
                      "size: {}, file size: {}",
                      offset,
                      bytes_read,
                      read_size,
                      file_size_);
        }
        auto bytes_to_write = static_cast<size_t>(bytes_read);
        ssize_t ret = ::write(fd, data.data(), bytes_to_write);
        if (ret != static_cast<ssize_t>(bytes_to_write)) {
            ThrowInfo(ErrorCode::FileWriteFailed,
                      "Failed to write to file: {}",
                      strerror(errno));
        }
        rest_size -= bytes_to_write;
    }
    auto fsync_ret = ::fsync(fd);
    int saved_errno = errno;
    if (fsync_ret != 0) {
        ThrowInfo(ErrorCode::FileWriteFailed,
                  "Failed to fsync file, errno: {}",
                  saved_errno);
    }
    return size;
}

size_t
RemoteInputStream::Tell() const {
    auto status = remote_file_->Tell();
    if (!status.ok()) {
        ThrowInfo(ArrowStatusToErrorCode(status.status()),
                  "Failed to tell input stream: {}",
                  status.status().ToString());
    }
    return static_cast<size_t>(status.ValueOrDie());
}

bool
RemoteInputStream::Eof() const {
    return Tell() >= file_size_;
}

bool
RemoteInputStream::Seek(int64_t offset) {
    auto status = remote_file_->Seek(offset);
    return status.ok();
}

size_t
RemoteInputStream::Size() const {
    return file_size_;
}

}  // namespace milvus::storage
