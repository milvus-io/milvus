#include "RemoteOutputStream.h"

#include <unistd.h>
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <utility>
#include <vector>

#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "log/Log.h"
#include "storage/StatusToErrorCode.h"

namespace milvus::storage {

RemoteOutputStream::RemoteOutputStream(
    std::shared_ptr<arrow::io::OutputStream>&& output_stream)
    : output_stream_(std::move(output_stream)) {
}

RemoteOutputStream::~RemoteOutputStream() {
    // A failed Write skips Close. Discard an unfinished upload rather than
    // publishing partial data or leaving its multipart upload on the server.
    try {
        if (!output_stream_->closed()) {
            auto status = output_stream_->Abort();
            if (!status.ok()) {
                LOG_WARN("Failed to abort remote output stream: {}",
                         status.ToString());
            }
        }
    } catch (const std::exception& e) {
        LOG_WARN("Failed to abort remote output stream: {}", e.what());
    } catch (...) {
        LOG_WARN("Failed to abort remote output stream: unknown exception");
    }
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
    auto status = output_stream_->Write(data, size);
    if (!status.ok()) {
        ThrowInfo(ArrowStatusToErrorCode(status),
                  "Failed to write to output stream: {}",
                  status.ToString());
    }
    return size;
}

size_t
RemoteOutputStream::Write(const std::shared_ptr<arrow::Buffer>& buffer) {
    const auto position = Tell();
    int64_t offset = 0;
    auto batch_size = std::min(buffer->size(), DEFAULT_INDEX_FILE_SLICE_SIZE);
    do {
        const auto size = std::min(batch_size, buffer->size() - offset);
        auto slice = arrow::SliceBuffer(buffer, offset, size);
        auto status = output_stream_->Write(slice);
        // Baseline native S3 rejects a full queue before accepting bytes.
        // Retry only an untouched slice, after draining admitted writes.
        if (status.IsCapacityError() && Tell() == position + offset) {
            auto flush_status = output_stream_->Flush();
            if (!flush_status.ok()) {
                ThrowInfo(ArrowStatusToErrorCode(flush_status),
                          "Failed to flush output stream: {}",
                          flush_status.ToString());
            }
            status = output_stream_->Write(slice);
            if (status.IsCapacityError() && Tell() == position + offset &&
                size > 1) {
                // A single write may exceed even an empty queue's capacity.
                batch_size = size / 2;
                continue;
            }
        }
        if (!status.ok()) {
            ThrowInfo(ArrowStatusToErrorCode(status),
                      "Failed to write to output stream: {}",
                      status.ToString());
        }
        offset += size;
    } while (offset < buffer->size());
    return buffer->size();
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
        auto write_status = output_stream_->Write(data.data(), read_size);
        if (!write_status.ok()) {
            ThrowInfo(ArrowStatusToErrorCode(write_status),
                      "Failed to write to output stream: {}",
                      write_status.ToString());
        }
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
}  // namespace milvus::storage
