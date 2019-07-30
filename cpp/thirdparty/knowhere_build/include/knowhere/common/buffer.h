#pragma once

#include <memory>
#include "arrow/buffer.h"


namespace zilliz {
namespace knowhere {

using Buffer = arrow::Buffer;
using BufferPtr = std::shared_ptr<Buffer>;
using MutableBuffer = arrow::MutableBuffer;
using MutableBufferPtr = std::shared_ptr<MutableBuffer>;

namespace internal {

struct BufferDeleter {
    void operator()(Buffer *buffer) {
        free((void *) buffer->data());
    }
};

}
inline BufferPtr
MakeBufferSmart(uint8_t *data, const int64_t size) {
    return BufferPtr(new Buffer(data, size), internal::BufferDeleter());
}

inline MutableBufferPtr
MakeMutableBufferSmart(uint8_t *data, const int64_t size) {
    return MutableBufferPtr(new MutableBuffer(data, size), internal::BufferDeleter());
}

inline BufferPtr
MakeBuffer(uint8_t *data, const int64_t size) {
    return std::make_shared<Buffer>(data, size);
}

inline MutableBufferPtr
MakeMutableBuffer(uint8_t *data, const int64_t size) {
    return std::make_shared<MutableBuffer>(data, size);
}

} // namespace knowhere
} // namespace zilliz
