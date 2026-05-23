#include <unistd.h>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include "log/Log.h"

#include "RemoteInputStream.h"
#include "arrow/io/interfaces.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"

namespace milvus::storage {
namespace {

constexpr int kRemoteInputStreamMaxReadRetries = 5;
constexpr const char* kFailedFlushResponseStreamError =
    "Failed to flush response stream";

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
    AssertInfo(status.ok(), "Failed to get size of remote file");
    file_size_ = static_cast<size_t>(status.ValueOrDie());
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
    AssertInfo(
        status.ok(),
        "Failed to read from remote input stream, operation: read, offset: {}, "
        "size: {}, file size: {}, error: {}",
        offset,
        size,
        file_size_,
        status.status().ToString());
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
    AssertInfo(status.ok(),
               "Failed to read from remote input stream, operation: read at "
               "offset, offset: {}, size: {}, file size: {}, error: {}",
               offset,
               size,
               file_size_,
               status.status().ToString());
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
        AssertInfo(status.ok(),
                   "Failed to read from remote input stream, operation: read "
                   "to file, offset: {}, size: {}, rest size: {}, file size: "
                   "{}, error: {}",
                   offset,
                   read_size,
                   rest_size,
                   file_size_,
                   status.status().ToString());
        auto bytes_read = status.ValueOrDie();
        AssertInfo(bytes_read > 0,
                   "Failed to read from remote input stream, operation: read "
                   "to file, offset: {}, read zero bytes, rest size: {}, file "
                   "size: {}",
                   offset,
                   rest_size,
                   file_size_);
        AssertInfo(bytes_read <= static_cast<int64_t>(read_size),
                   "Remote input stream returned more bytes than requested, "
                   "operation: read to file, offset: {}, bytes read: {}, "
                   "size: {}, file size: {}",
                   offset,
                   bytes_read,
                   read_size,
                   file_size_);
        auto bytes_to_write = static_cast<size_t>(bytes_read);
        ssize_t ret = ::write(fd, data.data(), bytes_to_write);
        AssertInfo(ret == static_cast<ssize_t>(bytes_to_write),
                   "Failed to write to file");
        rest_size -= bytes_to_write;
    }
    auto fsync_ret = ::fsync(fd);
    int saved_errno = errno;
    AssertInfo(fsync_ret == 0, "Failed to fsync file, errno: {}", saved_errno);
    return size;
}

size_t
RemoteInputStream::Tell() const {
    auto status = remote_file_->Tell();
    AssertInfo(status.ok(), "Failed to tell input stream");
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
