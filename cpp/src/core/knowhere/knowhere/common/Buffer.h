// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>

#include <arrow/buffer.h>

namespace zilliz {
namespace knowhere {

using Buffer = arrow::Buffer;
using BufferPtr = std::shared_ptr<Buffer>;
using MutableBuffer = arrow::MutableBuffer;
using MutableBufferPtr = std::shared_ptr<MutableBuffer>;

namespace internal {

struct BufferDeleter {
    void
    operator()(Buffer* buffer) {
        free((void*)buffer->data());
    }
};
}

inline BufferPtr
MakeBufferSmart(uint8_t* data, const int64_t size) {
    return BufferPtr(new Buffer(data, size), internal::BufferDeleter());
}

inline MutableBufferPtr
MakeMutableBufferSmart(uint8_t* data, const int64_t size) {
    return MutableBufferPtr(new MutableBuffer(data, size), internal::BufferDeleter());
}

inline BufferPtr
MakeBuffer(uint8_t* data, const int64_t size) {
    return std::make_shared<Buffer>(data, size);
}

inline MutableBufferPtr
MakeMutableBuffer(uint8_t* data, const int64_t size) {
    return std::make_shared<MutableBuffer>(data, size);
}

}  // namespace knowhere
}  // namespace zilliz
