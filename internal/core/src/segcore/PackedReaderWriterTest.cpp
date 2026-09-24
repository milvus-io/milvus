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

#include <arrow/array/array_base.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/util/key_value_metadata.h>
#include <cstdint>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "common/common_type_c.h"
#include "gtest/gtest.h"
#include "milvus-storage/common/constants.h"
#include "segcore/column_groups_c.h"
#include "segcore/packed_reader_c.h"
#include "segcore/packed_writer_c.h"
#include "test_utils/Constants.h"

TEST(CPackedTest, PackedWriterAndReader) {
    std::vector<int64_t> test_data(5);
    std::iota(test_data.begin(), test_data.end(), 0);

    auto builder = std::make_shared<arrow::Int64Builder>();
    auto status = builder->AppendValues(test_data.begin(), test_data.end());
    ASSERT_TRUE(status.ok());
    auto res = builder->Finish();
    ASSERT_TRUE(res.ok());
    std::shared_ptr<arrow::Array> array = res.ValueOrDie();

    auto schema = arrow::schema(
        {arrow::field("int64",
                      arrow::int64(),
                      false,
                      arrow::key_value_metadata(
                          {milvus_storage::ARROW_FIELD_ID_KEY}, {"100"}))});
    auto origin_schema = arrow::schema({schema->fields()[0]->Copy()});
    auto batch = arrow::RecordBatch::Make(schema, array->length(), {array});

    struct ArrowSchema c_write_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_write_schema).ok());

    struct ArrowSchema c_origin_schema;
    ASSERT_TRUE(arrow::ExportSchema(*origin_schema, &c_origin_schema).ok());

    const int64_t buffer_size = 10 * 1024 * 1024;
    std::string root_path = TestLocalPath;
    std::string file_path = TestLocalPath + "0";
    char* path = const_cast<char*>(root_path.c_str());
    char* paths[] = {const_cast<char*>(file_path.c_str())};
    int64_t part_upload_size = 0;

    CColumnSplits cgs = NewCColumnSplits();
    int group[] = {0};
    AddCColumnSplit(cgs, group, 1);

    CPackedWriter c_packed_writer = nullptr;
    auto c_status = NewPackedWriter(&c_write_schema,
                                    buffer_size,
                                    paths,
                                    1,
                                    part_upload_size,
                                    cgs,
                                    &c_packed_writer,
                                    nullptr);
    EXPECT_EQ(c_status.error_code, 0);
    EXPECT_NE(c_packed_writer, nullptr);

    struct ArrowArray carray;
    struct ArrowSchema cschema;
    ASSERT_TRUE(arrow::ExportRecordBatch(*batch, &carray, &cschema).ok());

    struct ArrowArray arrays[] = {carray};
    struct ArrowSchema array_schemas[] = {cschema};

    c_status = WriteRecordBatch(
        c_packed_writer, arrays, array_schemas, &c_origin_schema);
    EXPECT_EQ(c_status.error_code, 0);

    c_status = CloseWriter(c_packed_writer);
    EXPECT_EQ(c_status.error_code, 0);

    struct ArrowSchema c_read_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_read_schema).ok());
    CPackedReader c_packed_reader = nullptr;
    c_status = NewPackedReader(
        paths, 1, &c_read_schema, buffer_size, 0, &c_packed_reader, nullptr);
    EXPECT_EQ(c_status.error_code, 0);
    EXPECT_NE(c_packed_reader, nullptr);

    struct ArrowArray read_array {};
    struct ArrowSchema read_schema {};
    c_status = ReadNext(c_packed_reader, &read_array, &read_schema);
    ASSERT_EQ(c_status.error_code, 0);
    ASSERT_NE(read_array.release, nullptr);
    ASSERT_NE(read_schema.release, nullptr);
    auto imported = arrow::ImportRecordBatch(&read_array, &read_schema);
    ASSERT_TRUE(imported.ok());
    auto read_batch = imported.ValueOrDie();
    EXPECT_TRUE(batch->Equals(*read_batch));
    EXPECT_EQ(read_array.release, nullptr);
    EXPECT_EQ(read_schema.release, nullptr);

    // The same caller-owned structs can be reused after import; EOF leaves
    // both outputs released without affecting the previously imported batch.
    c_status = ReadNext(c_packed_reader, &read_array, &read_schema);
    EXPECT_EQ(c_status.error_code, 0);
    EXPECT_EQ(read_array.release, nullptr);
    EXPECT_EQ(read_schema.release, nullptr);

    c_status = CloseReader(c_packed_reader);
    EXPECT_EQ(c_status.error_code, 0);
    EXPECT_TRUE(batch->Equals(*read_batch));
    FreeCColumnSplits(cgs);
}

namespace {

// Writes the int64 values 0..num_rows-1 into one packed file at `file_path`.
void
WriteInt64File(const std::string& file_path,
               int64_t num_rows,
               std::shared_ptr<arrow::Schema>* out_schema) {
    std::vector<int64_t> values(num_rows);
    std::iota(values.begin(), values.end(), 0);
    arrow::Int64Builder builder;
    ASSERT_TRUE(builder.AppendValues(values).ok());
    auto array = builder.Finish().ValueOrDie();

    auto schema = arrow::schema(
        {arrow::field("int64",
                      arrow::int64(),
                      false,
                      arrow::key_value_metadata(
                          {milvus_storage::ARROW_FIELD_ID_KEY}, {"100"}))});
    *out_schema = schema;
    auto batch = arrow::RecordBatch::Make(schema, array->length(), {array});

    struct ArrowSchema c_write_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_write_schema).ok());
    struct ArrowSchema c_origin_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_origin_schema).ok());

    char* paths[] = {const_cast<char*>(file_path.c_str())};
    CColumnSplits cgs = NewCColumnSplits();
    int group[] = {0};
    AddCColumnSplit(cgs, group, 1);

    CPackedWriter writer = nullptr;
    auto status = NewPackedWriter(
        &c_write_schema, 10 * 1024 * 1024, paths, 1, 0, cgs, &writer, nullptr);
    ASSERT_EQ(status.error_code, 0);

    struct ArrowArray carray;
    struct ArrowSchema cschema;
    ASSERT_TRUE(arrow::ExportRecordBatch(*batch, &carray, &cschema).ok());
    struct ArrowArray arrays[] = {carray};
    struct ArrowSchema array_schemas[] = {cschema};
    status = WriteRecordBatch(writer, arrays, array_schemas, &c_origin_schema);
    ASSERT_EQ(status.error_code, 0);
    status = CloseWriter(writer);
    ASSERT_EQ(status.error_code, 0);
    FreeCColumnSplits(cgs);
}

}  // namespace

// The eager mode must return the same rows, in the same order, as the lazy
// mode. The last mode uses a read buffer smaller than the data, so the reader
// takes the file in more than one round.
TEST(CPackedTest, PackedReaderEagerPrebuffer) {
    // 8 bytes per row: about 3MB of values.
    const int64_t num_rows = 400 * 1000;
    const std::string file_path = TestLocalPath + "eager_ranges_0";
    std::shared_ptr<arrow::Schema> schema;
    ASSERT_NO_FATAL_FAILURE(WriteInt64File(file_path, num_rows, &schema));

    struct ReadMode {
        int64_t buffer_size;
        bool eager_prebuffer;
    };
    const ReadMode modes[] = {
        {10 * 1024 * 1024, false},         // lazy, one round
        {10 * 1024 * 1024, true},          // eager, one round
        {1024 * 1024 + 512 * 1024, true},  // eager, several rounds
    };
    for (const auto& mode : modes) {
        SCOPED_TRACE(
            "buffer_size=" + std::to_string(mode.buffer_size) +
            " eager_prebuffer=" + std::to_string(mode.eager_prebuffer));
        struct ArrowSchema c_read_schema;
        ASSERT_TRUE(arrow::ExportSchema(*schema, &c_read_schema).ok());
        char* paths[] = {const_cast<char*>(file_path.c_str())};
        CPackedReader reader = nullptr;
        auto status = NewPackedReader(paths,
                                      1,
                                      &c_read_schema,
                                      mode.buffer_size,
                                      mode.eager_prebuffer,
                                      &reader,
                                      nullptr);
        ASSERT_EQ(status.error_code, 0);
        ASSERT_NE(reader, nullptr);

        int64_t next = 0;
        while (true) {
            struct ArrowArray read_array {};
            struct ArrowSchema read_schema {};
            status = ReadNext(reader, &read_array, &read_schema);
            ASSERT_EQ(status.error_code, 0);
            if (read_array.release == nullptr) {
                break;
            }
            auto imported = arrow::ImportRecordBatch(&read_array, &read_schema);
            ASSERT_TRUE(imported.ok());
            auto column = std::static_pointer_cast<arrow::Int64Array>(
                imported.ValueOrDie()->column(0));
            for (int64_t i = 0; i < column->length(); ++i) {
                ASSERT_EQ(column->Value(i), next);
                ++next;
            }
        }
        EXPECT_EQ(next, num_rows);

        status = CloseReader(reader);
        EXPECT_EQ(status.error_code, 0);
    }
}
