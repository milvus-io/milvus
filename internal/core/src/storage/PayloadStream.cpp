// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "PayloadStream.h"

namespace wrapper {

PayloadOutputStream::PayloadOutputStream() {
    buffer_.reserve(1024 * 1024);
    closed_ = false;
}

PayloadOutputStream::~PayloadOutputStream() noexcept {
}

arrow::Status
PayloadOutputStream::Close() {
    closed_ = true;
    return arrow::Status::OK();
}

arrow::Result<int64_t>
PayloadOutputStream::Tell() const {
    return arrow::Result<int64_t>(buffer_.size());
}

bool
PayloadOutputStream::closed() const {
    return closed_;
}

arrow::Status
PayloadOutputStream::Write(const void* data, int64_t nbytes) {
    if (nbytes <= 0)
        return arrow::Status::OK();
    auto size = buffer_.size();
    buffer_.resize(size + nbytes);
    std::memcpy(buffer_.data() + size, data, nbytes);
    return arrow::Status::OK();
}

arrow::Status
PayloadOutputStream::Flush() {
    return arrow::Status::OK();
}

const std::vector<uint8_t>&
PayloadOutputStream::Buffer() const {
    return buffer_;
}

PayloadInputStream::PayloadInputStream(const uint8_t* data, int64_t size)
    : data_(data), size_(size), tell_(0), closed_(false) {
}

PayloadInputStream::~PayloadInputStream() noexcept {
}

arrow::Status
PayloadInputStream::Close() {
    closed_ = true;
    return arrow::Status::OK();
}

bool
PayloadInputStream::closed() const {
    return closed_;
}

arrow::Result<int64_t>
PayloadInputStream::Tell() const {
    return arrow::Result<int64_t>(tell_);
}

arrow::Status
PayloadInputStream::Seek(int64_t position) {
    if (position < 0 || position >= size_)
        return arrow::Status::IOError("invalid position");
    tell_ = position;
    return arrow::Status::OK();
}

arrow::Result<int64_t>
PayloadInputStream::Read(int64_t nbytes, void* out) {
    auto remain = size_ - tell_;
    if (nbytes > remain)
        nbytes = remain;
    std::memcpy(out, data_ + tell_, nbytes);
    tell_ += nbytes;
    return arrow::Result<int64_t>(nbytes);
}

arrow::Result<std::shared_ptr<arrow::Buffer>>
PayloadInputStream::Read(int64_t nbytes) {
    auto remain = size_ - tell_;
    if (nbytes > remain)
        nbytes = remain;
    auto buf = std::make_shared<arrow::Buffer>(data_ + tell_, nbytes);
    tell_ += nbytes;
    return arrow::Result<std::shared_ptr<arrow::Buffer>>(buf);
}

arrow::Result<int64_t>
PayloadInputStream::GetSize() {
    return arrow::Result<int64_t>(size_);
}

}  // namespace wrapper
