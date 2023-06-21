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
#include "storage/Util.h"
#include "parquet/column_reader.h"
#include "arrow/io/api.h"
#include "arrow/status.h"
#include "parquet/arrow/reader.h"

namespace milvus::storage {

PayloadReader::PayloadReader(const uint8_t* data,
                             int length,
                             DataType data_type)
    : column_type_(data_type) {
    auto input = std::make_shared<arrow::io::BufferReader>(data, length);
    init(input);
}

void
PayloadReader::init(std::shared_ptr<arrow::io::BufferReader> input) {
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    // Configure general Parquet reader settings
    auto reader_properties = parquet::ReaderProperties(pool);
    reader_properties.set_buffer_size(4096 * 4);
    reader_properties.enable_buffered_stream();

    // Configure Arrow-specific Parquet reader settings
    auto arrow_reader_props = parquet::ArrowReaderProperties();
    arrow_reader_props.set_batch_size(128 * 1024);  // default 64 * 1024
    arrow_reader_props.set_pre_buffer(false);

    parquet::arrow::FileReaderBuilder reader_builder;
    auto st = reader_builder.Open(input, reader_properties);
    AssertInfo(st.ok(), "file to read file");
    reader_builder.memory_pool(pool);
    reader_builder.properties(arrow_reader_props);

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    st = reader_builder.Build(&arrow_reader);
    AssertInfo(st.ok(), "build file reader");

    int64_t column_index = 0;
    auto file_meta = arrow_reader->parquet_reader()->metadata();
    // LOG_SEGCORE_INFO_ << "serialized parquet metadata, num row group  " <<
    // std::to_string(file_meta->num_row_groups())
    //                   << ", num column " << std::to_string(file_meta->num_columns()) << ", num rows "
    //                   << std::to_string(file_meta->num_rows()) << ", type width "
    //                   << std::to_string(file_meta->schema()->Column(column_index)->type_length());
    dim_ = datatype_is_vector(column_type_)
               ? GetDimensionFromFileMetaData(
                     file_meta->schema()->Column(column_index), column_type_)
               : 1;
    auto total_num_rows = file_meta->num_rows();

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    st = arrow_reader->GetRecordBatchReader(&rb_reader);
    AssertInfo(st.ok(), "get record batch reader");

    field_data_ = CreateFieldData(column_type_, dim_, total_num_rows);
    for (arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch :
         *rb_reader) {
        AssertInfo(maybe_batch.ok(), "get batch record success");
        auto array = maybe_batch.ValueOrDie()->column(column_index);
        field_data_->FillFieldData(array);
    }
    AssertInfo(field_data_->IsFull(), "field data hasn't been filled done");
    // LOG_SEGCORE_INFO_ << "Peak arrow memory pool size " << pool->max_memory();
}

}  // namespace milvus::storage
