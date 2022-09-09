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

#include <memory>
#include <vector>

#include "storage/PayloadStream.h"
#include <parquet/arrow/writer.h>

namespace milvus::storage {
class PayloadWriter {
 public:
    explicit PayloadWriter(const DataType column_type);
    explicit PayloadWriter(const DataType column_type, int dim);
    ~PayloadWriter() = default;

    void
    add_payload(const Payload& raw_data);

    void
    add_one_string_payload(const char* str, int str_size);

    void
    finish();

    bool
    has_finished();

    const std::vector<uint8_t>&
    get_payload_buffer() const;

    int
    get_payload_length() const {
        return rows_;
    }

 private:
    void
    init_dimension(int dim);

 private:
    DataType column_type_;
    std::shared_ptr<arrow::ArrayBuilder> builder_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<PayloadOutputStream> output_;
    std::atomic<int> rows_ = 0;
    std::optional<int> dimension_;  // binary vector, float vector
};
}  // namespace milvus::storage
