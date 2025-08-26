#include <unistd.h>
#include "RemoteInputStream.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"

namespace milvus::storage {

RemoteInputStream::RemoteInputStream(
    std::shared_ptr<arrow::io::RandomAccessFile>&& remote_file)
    : remote_file_(std::move(remote_file)) {
    auto status = remote_file_->GetSize();
    AssertInfo(status.ok(), "Failed to get size of remote file");
    file_size_ = static_cast<size_t>(status.ValueOrDie());
}

size_t
RemoteInputStream::Read(void* data, size_t size) {
    auto status = remote_file_->Read(size, data);
    AssertInfo(status.ok(), "Failed to read from input stream");
    return static_cast<size_t>(status.ValueOrDie());
}

size_t
RemoteInputStream::ReadAt(void* data, size_t offset, size_t size) {
    auto status = remote_file_->ReadAt(offset, size, data);
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
        auto status = remote_file_->Read(read_size, data.data());
        AssertInfo(status.ok(), "Failed to read from input stream");
        ssize_t ret = ::write(fd, data.data(), read_size);
        AssertInfo(ret == static_cast<ssize_t>(read_size),
                   "Failed to write to file");
        ::fsync(fd);
        rest_size -= read_size;
    }
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