#include <unistd.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <string>
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

constexpr int kRemoteInputStreamMaxReadRetries = 2;
constexpr const char* kFailedFlushResponseStreamError =
    "Failed to flush response stream";

bool
IsRetryableReadError(const arrow::Status& status) {
    return status.ToString().find(kFailedFlushResponseStreamError) !=
           std::string::npos;
}

template <typename ReadFunc>
arrow::Result<int64_t>
ReadWithRetry(const char* operation,
              size_t size,
              size_t file_size,
              ReadFunc&& read_func) {
    auto result = read_func();
    for (int retry = 1; !result.ok() && IsRetryableReadError(result.status()) &&
                        retry <= kRemoteInputStreamMaxReadRetries;
         ++retry) {
        LOG_WARN(
            "Failed to {} from remote input stream, retry {}/{}, size: {}, "
            "file size: {}, error is: {}",
            operation,
            retry,
            kRemoteInputStreamMaxReadRetries,
            size,
            file_size,
            result.status().ToString());
        result = read_func();
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
    auto status = ReadWithRetry("read", size, file_size_, [this, size, data]() {
        return remote_file_->Read(size, data);
    });
    AssertInfo(
        status.ok(),
        "Failed to read from remote input stream, size: {}, file size: {}",
        size,
        file_size_);
    return static_cast<size_t>(status.ValueOrDie());
}

size_t
RemoteInputStream::ReadAt(void* data, size_t offset, size_t size) {
    auto status = ReadWithRetry(
        "read at offset", size, file_size_, [this, offset, size, data]() {
            return remote_file_->ReadAt(offset, size, data);
        });
    AssertInfo(status.ok(), "Failed to read from input stream");
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
        auto status = ReadWithRetry(
            "read to file", read_size, file_size_, [this, read_size, &data]() {
                return remote_file_->Read(read_size, data.data());
            });
        AssertInfo(status.ok(), "Failed to read from input stream");
        ssize_t ret = ::write(fd, data.data(), read_size);
        AssertInfo(ret == static_cast<ssize_t>(read_size),
                   "Failed to write to file");
        rest_size -= read_size;
    }
    ::fsync(fd);
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
