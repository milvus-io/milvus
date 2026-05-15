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

#include "arrow/io/api.h"
#include "arrow/status.h"
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "parquet/arrow/reader.h"
#include "parquet/column_reader.h"
#include "storage/PayloadReader.h"
#include "storage/Util.h"

namespace milvus::storage {

PayloadReader::PayloadReader(const milvus::FieldDataPtr& fieldData)
    : column_type_(fieldData->get_data_type()),
      nullable_(fieldData->IsNullable()) {
    field_data_ = fieldData;
}

PayloadReader::PayloadReader(const uint8_t* data,
                             int length,
                             DataType data_type,
                             bool nullable,
                             bool is_field_data)
    : column_type_(data_type), nullable_(nullable) {
    init(data, length, is_field_data);
}

void
PayloadReader::init(const uint8_t* data, int length, bool is_field_data) {
    if (column_type_ == DataType::NONE) {
        payload_buf_ = std::make_shared<BytesBuf>(data, length);
    } else {
        auto input = std::make_shared<arrow::io::BufferReader>(data, length);
        arrow::MemoryPool* pool = arrow::default_memory_pool();
        // Configure general Parquet reader settings
        auto reader_properties = parquet::ReaderProperties(pool);
        reader_properties.set_buffer_size(4096 * 4);
        // reader_properties.enable_buffered_stream();

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

        // dim is unused for sparse float vector
        dim_ =
            (IsVectorDataType(column_type_) &&
             !IsVectorArrayDataType(column_type_) &&
             !IsSparseFloatVectorDataType(column_type_))
                ? GetDimensionFromFileMetaData(
                      file_meta->schema()->Column(column_index), column_type_)
                : 1;

        // For VectorArray, get element type and dim from Arrow schema metadata
        auto element_type = DataType::NONE;
        if (IsVectorArrayDataType(column_type_)) {
            std::shared_ptr<arrow::Schema> arrow_schema;
            st = arrow_reader->GetSchema(&arrow_schema);
            AssertInfo(st.ok(), "Failed to get arrow schema for VectorArray");
            AssertInfo(arrow_schema->num_fields() == 1,
                       "VectorArray should have exactly 1 field, got {}",
                       arrow_schema->num_fields());

            auto field = arrow_schema->field(0);
            AssertInfo(field->HasMetadata(),
                       "VectorArray field is missing metadata");

            auto metadata = field->metadata();
            AssertInfo(metadata != nullptr, "VectorArray metadata is null");

            // Get element type
            AssertInfo(
                metadata->Contains(ELEMENT_TYPE_KEY_FOR_ARROW),
                "VectorArray metadata missing required 'elementType' field");
            auto element_type_str =
                metadata->Get(ELEMENT_TYPE_KEY_FOR_ARROW).ValueOrDie();
            auto element_type_int = std::stoi(element_type_str);
            element_type = static_cast<DataType>(element_type_int);

            // Get dimension from metadata
            AssertInfo(metadata->Contains(DIM_KEY),
                       "VectorArray metadata missing required 'dim' field");
            auto dim_str = metadata->Get(DIM_KEY).ValueOrDie();
            dim_ = std::stoi(dim_str);
            AssertInfo(
                dim_ > 0, "VectorArray dim must be positive, got {}", dim_);
        }

        std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
        st = arrow_reader->GetRecordBatchReader(&rb_reader);
        AssertInfo(st.ok(), "get record batch reader");

        if (is_field_data) {
            auto total_num_rows = file_meta->num_rows();

            // Create FieldData, passing element_type for VectorArray
            field_data_ = CreateFieldData(
                column_type_, element_type, nullable_, dim_, total_num_rows);

            for (arrow::Result<std::shared_ptr<arrow::RecordBatch>>
                     maybe_batch : *rb_reader) {
                AssertInfo(maybe_batch.ok(), "get batch record success");
                auto array = maybe_batch.ValueOrDie()->column(column_index);
                // to read
                field_data_->FillFieldData(array);
            }

            AssertInfo(field_data_->IsFull(),
                       "field data hasn't been filled done");
        } else {
            arrow_reader_ = std::move(arrow_reader);
            record_batch_reader_ = std::move(rb_reader);
        }
    }
}

}  // namespace milvus::storage
