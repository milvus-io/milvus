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

#include "storage/PayloadWriter.h"
#include "exceptions/EasyAssert.h"
#include "common/FieldMeta.h"
#include "storage/Util.h"

namespace milvus::storage {

// create payload writer for numeric data type
PayloadWriter::PayloadWriter(const DataType column_type) : column_type_(column_type) {
    builder_ = CreateArrowBuilder(column_type);
    schema_ = CreateArrowSchema(column_type);
}

// create payload writer for vector data type
PayloadWriter::PayloadWriter(const DataType column_type, int dim) : column_type_(column_type) {
    init_dimension(dim);
}

void
PayloadWriter::init_dimension(int dim) {
    if (dimension_.has_value()) {
        AssertInfo(dimension_ == dim, "init dimension with diff values repeatedly");
        return;
    }

    dimension_ = dim;
    builder_ = CreateArrowBuilder(column_type_, dim);
    schema_ = CreateArrowSchema(column_type_, dim);
}

void
PayloadWriter::add_one_string_payload(const char* str, int str_size) {
    AssertInfo(output_ == nullptr, "payload writer has been finished");
    AssertInfo(milvus::datatype_is_string(column_type_), "mismatch data type");
    AddOneStringToArrowBuilder(builder_, str, str_size);
    rows_.fetch_add(1);
}

void
PayloadWriter::add_payload(const Payload& raw_data) {
    AssertInfo(output_ == nullptr, "payload writer has been finished");
    AssertInfo(column_type_ == raw_data.data_type, "mismatch data type");
    AssertInfo(builder_ != nullptr, "empty arrow builder");
    if (milvus::datatype_is_vector(column_type_)) {
        AssertInfo(dimension_.has_value(), "dimension has not been inited");
        AssertInfo(dimension_ == raw_data.dimension, "inconsistent dimension");
    }

    AddPayloadToArrowBuilder(builder_, raw_data);
    rows_.fetch_add(raw_data.rows);
}

void
PayloadWriter::finish() {
    AssertInfo(output_ == nullptr, "payload writer has been finished");
    std::shared_ptr<arrow::Array> array;
    auto ast = builder_->Finish(&array);
    AssertInfo(ast.ok(), "builder failed to finish");

    auto table = arrow::Table::Make(schema_, {array});
    output_ = std::make_shared<storage::PayloadOutputStream>();
    auto mem_pool = arrow::default_memory_pool();
    ast = parquet::arrow::WriteTable(
        *table, mem_pool, output_, 1024 * 1024 * 1024,
        parquet::WriterProperties::Builder().compression(arrow::Compression::ZSTD)->compression_level(3)->build());
    AssertInfo(ast.ok(), "write data to output stream failed");
}

bool
PayloadWriter::has_finished() {
    return output_ != nullptr;
}

const std::vector<uint8_t>&
PayloadWriter::get_payload_buffer() const {
    AssertInfo(output_ != nullptr, "payload writer has not been finished");
    return output_->Buffer();
}

}  // namespace milvus::storage
