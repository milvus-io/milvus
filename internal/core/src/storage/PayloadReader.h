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
#include <parquet/arrow/reader.h>

#include "storage/PayloadStream.h"
#include "storage/FieldData.h"

namespace milvus::storage {

class PayloadReader {
 public:
    explicit PayloadReader(const uint8_t* data, int length, DataType data_type);

    ~PayloadReader() = default;

    void
    init(std::shared_ptr<arrow::io::BufferReader> buffer);

    const FieldDataPtr
    get_field_data() const {
        return field_data_;
    }

 private:
    DataType column_type_;
    int dim_;
    FieldDataPtr field_data_;
};

}  // namespace milvus::storage
