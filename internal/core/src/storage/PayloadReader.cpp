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

#include "storage/PayloadReader.h"
#include "exceptions/EasyAssert.h"
#include "storage/FieldDataFactory.h"
#include "storage/Util.h"

namespace milvus::storage {
PayloadReader::PayloadReader(std::shared_ptr<PayloadInputStream> input,
                             DataType data_type)
    : column_type_(data_type) {
    init(std::move(input));
}

PayloadReader::PayloadReader(const uint8_t* data,
                             int length,
                             DataType data_type)
    : column_type_(data_type) {
    auto input = std::make_shared<storage::PayloadInputStream>(data, length);
    init(input);
}

void
PayloadReader::init(std::shared_ptr<PayloadInputStream> input) {
    auto mem_pool = arrow::default_memory_pool();
    // TODO :: Stream read file data, avoid copying
    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto st = parquet::arrow::OpenFile(input, mem_pool, &reader);
    AssertInfo(st.ok(), "failed to get arrow file reader");
    std::shared_ptr<arrow::Table> table;
    st = reader->ReadTable(&table);
    AssertInfo(st.ok(), "failed to get reader data to arrow table");
    auto column = table->column(0);
    AssertInfo(column != nullptr, "returned arrow column is null");
    AssertInfo(column->chunks().size() == 1,
               "arrow chunk size in arrow column should be 1");
    auto array = column->chunk(0);
    AssertInfo(array != nullptr, "empty arrow array of PayloadReader");
    dim_ = datatype_is_vector(column_type_)
               ? GetDimensionFromArrowArray(array, column_type_)
               : 1;
    field_data_ =
        FieldDataFactory::GetInstance().CreateFieldData(column_type_, dim_);
    field_data_->FillFieldData(array);
}

}  // namespace milvus::storage
