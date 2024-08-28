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
#include <arrow/record_batch.h>
#include <parquet/arrow/reader.h>

#include "common/FieldData.h"
#include "storage/PayloadStream.h"

namespace milvus::storage {

class PayloadReader {
 public:
    explicit PayloadReader(const uint8_t* data,
                           int length,
                           DataType data_type,
                           bool nullable,
                           bool is_field_data = true);

    ~PayloadReader() = default;

    void
    init(std::shared_ptr<arrow::io::BufferReader> buffer, bool is_field_data);

    const FieldDataPtr
    get_field_data() const {
        return field_data_;
    }

    std::shared_ptr<arrow::RecordBatchReader>
    get_reader() {
        return record_batch_reader_;
    }

    std::shared_ptr<parquet::arrow::FileReader>
    get_file_reader() {
        return arrow_reader_;
    }

 private:
    DataType column_type_;
    int dim_;
    bool nullable_;
    FieldDataPtr field_data_;

    std::shared_ptr<parquet::arrow::FileReader> arrow_reader_;
    std::shared_ptr<arrow::RecordBatchReader> record_batch_reader_;
};

}  // namespace milvus::storage
