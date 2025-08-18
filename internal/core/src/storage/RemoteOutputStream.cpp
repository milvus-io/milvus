#include "RemoteOutputStream.h"
#include <cstddef>
#include <unistd.h>
#include "common/Consts.h"
#include "common/EasyAssert.h"

namespace milvus::storage {

RemoteOutputStream::RemoteOutputStream(
    std::shared_ptr<arrow::io::OutputStream>&& output_stream)
    : output_stream_(std::move(output_stream)) {
}

size_t
RemoteOutputStream::Tell() const {
    auto status = output_stream_->Tell();
    AssertInfo(status.ok(), "Failed to tell output stream");
    return status.ValueOrDie();
}

size_t
RemoteOutputStream::Write(const void* data, size_t size) {
    auto status = output_stream_->Write(data, size);
    AssertInfo(status.ok(), "Failed to write to output stream");
    return size;
}

size_t
RemoteOutputStream::Write(int fd, size_t size) {
    size_t read_batch_size =
        std::min(size, static_cast<size_t>(DEFAULT_INDEX_FILE_SLICE_SIZE));
    size_t rest_size = size;
    std::vector<uint8_t> data(read_batch_size);

    while (rest_size > 0) {
        size_t read_size = std::min(rest_size, read_batch_size);
        auto read_status = ::read(fd, data.data(), read_size);
        AssertInfo(read_status == static_cast<ssize_t>(read_size),
                   "Failed to read from file");
        auto write_status = output_stream_->Write(data.data(), read_size);
        AssertInfo(write_status.ok(), "Failed to write to output stream");
        rest_size -= read_size;
    }

    return size;
}
}  // namespace milvus::storage