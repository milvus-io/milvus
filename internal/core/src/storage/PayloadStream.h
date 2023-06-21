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

#pragma once

#include <vector>
#include <memory>

#include <arrow/api.h>
#include <arrow/io/api.h>

#include "storage/Types.h"

namespace milvus::storage {

class PayloadOutputStream;
class PayloadInputStream;

struct Payload {
    DataType data_type;
    const uint8_t* raw_data;
    int64_t rows;
    std::optional<int> dimension;
};

class PayloadOutputStream : public arrow::io::OutputStream {
 public:
    PayloadOutputStream();
    ~PayloadOutputStream() noexcept;

    arrow::Status
    Close() override;
    arrow::Result<int64_t>
    Tell() const override;
    bool
    closed() const override;
    arrow::Status
    Write(const void* data, int64_t nbytes) override;
    arrow::Status
    Flush() override;

 public:
    const std::vector<uint8_t>&
    Buffer() const;

 private:
    std::vector<uint8_t> buffer_;
    bool closed_;
};

class PayloadInputStream : public arrow::io::RandomAccessFile {
 public:
    PayloadInputStream(const uint8_t* data, int64_t size);
    ~PayloadInputStream() noexcept;

    arrow::Status
    Close() override;
    arrow::Result<int64_t>
    Tell() const override;
    bool
    closed() const override;
    arrow::Status
    Seek(int64_t position) override;
    arrow::Result<int64_t>
    Read(int64_t nbytes, void* out) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>>
    Read(int64_t nbytes) override;
    arrow::Result<int64_t>
    GetSize() override;

 private:
    const uint8_t* data_;
    const int64_t size_;
    int64_t tell_;
    bool closed_;
};

}  // namespace milvus::storage
