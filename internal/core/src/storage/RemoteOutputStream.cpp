#include "RemoteOutputStream.h"

#include <unistd.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "milvus-storage/filesystem/async_output_stream.h"
#include "storage/StatusToErrorCode.h"

namespace milvus::storage {

RemoteOutputStream::RemoteOutputStream(
    std::shared_ptr<arrow::io::OutputStream>&& output_stream,
    size_t request_size)
    : output_stream_(std::move(output_stream)), request_size_(request_size) {
}

RemoteOutputStream::~RemoteOutputStream() {
}

size_t
RemoteOutputStream::Tell() const {
    auto status = output_stream_->Tell();
    if (!status.ok()) {
        ThrowInfo(ArrowStatusToErrorCode(status.status()),
                  "Failed to tell output stream: {}",
                  status.status().ToString());
    }
    return status.ValueOrDie();
}

size_t
RemoteOutputStream::Write(const void* data, size_t size) {
    return DoWrite(data, size);
}

size_t
RemoteOutputStream::Write(const std::shared_ptr<arrow::Buffer>& buffer) {
    return DoWrite(buffer->data(), buffer->size(), buffer);
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
        if (!(read_status == static_cast<ssize_t>(read_size))) {
            ThrowInfo(ErrorCode::FileReadFailed, "Failed to read from file");
        }
        Write(data.data(), read_size);
        rest_size -= read_size;
    }

    return size;
}

void
RemoteOutputStream::Close() {
    auto status = output_stream_->Close();
    if (!status.ok()) {
        ThrowInfo(ArrowStatusToErrorCode(status),
                  "Failed to close output stream: {}",
                  status.ToString());
    }
}

size_t
RemoteOutputStream::DoWrite(const void* data,
                            size_t size,
                            const std::shared_ptr<arrow::Buffer>& buffer) {
    auto* async =
        dynamic_cast<milvus_storage::AsyncOutputStream*>(output_stream_.get());
    size_t offset = 0;
    do {
        const auto chunk_size =
            request_size_ ? std::min(request_size_, size - offset) : size;
        auto chunk =
            buffer ? arrow::SliceBuffer(buffer, offset, chunk_size) : nullptr;
        const auto* chunk_data =
            offset ? static_cast<const uint8_t*>(data) + offset : data;
        for (;;) {
            auto status = chunk ? output_stream_->Write(chunk)
                                : output_stream_->Write(chunk_data, chunk_size);
            if (request_size_ && async && status.IsCapacityError()) {
                // Rejected writes consume no bytes. Wait for a free request
                // slot and retry this same slice, not the entire entry.
                const auto ready =
                    async->WaitForCapacityAsync(chunk_size).status();
                if (ready.ok()) {
                    continue;
                }
                if (!ready.IsNotImplemented()) {
                    status = ready;
                }
            }
            if (!status.ok()) {
                ThrowInfo(ArrowStatusToErrorCode(status),
                          "Failed to write to output stream: {}",
                          status.ToString());
            }
            break;
        }
        offset += chunk_size;
    } while (offset < size);
    return size;
}
}  // namespace milvus::storage
