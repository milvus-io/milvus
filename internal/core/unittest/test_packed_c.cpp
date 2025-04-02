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

#include <gtest/gtest.h>
#include "milvus-storage/common/constants.h"
#include "segcore/packed_writer_c.h"
#include "segcore/packed_reader_c.h"
#include "segcore/arrow_fs_c.h"
#include <arrow/c/bridge.h>
#include <arrow/c/helpers.h>
#include <arrow/array.h>
#include <arrow/record_batch.h>
#include <arrow/array/builder_primitive.h>
#include "arrow/table_builder.h"
#include "arrow/type_fwd.h"
#include <arrow/util/key_value_metadata.h>
#include <numeric>

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
    auto batch = arrow::RecordBatch::Make(schema, array->length(), {array});

    struct ArrowSchema c_write_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_write_schema).ok());

    const int64_t buffer_size = 10 * 1024 * 1024;
    char* path = const_cast<char*>("/tmp");
    char* paths[] = {const_cast<char*>("/tmp/0")};
    int64_t part_upload_size = 0;

    CColumnGroups cgs = NewCColumnGroups();
    int group[] = {0};
    AddCColumnGroup(cgs, group, 1);

    auto c_status = InitLocalArrowFileSystemSingleton(path);
    EXPECT_EQ(c_status.error_code, 0);
    CPackedWriter c_packed_writer = nullptr;
    c_status = NewPackedWriter(&c_write_schema,
                               buffer_size,
                               paths,
                               1,
                               part_upload_size,
                               cgs,
                               &c_packed_writer);
    EXPECT_EQ(c_status.error_code, 0);
    EXPECT_NE(c_packed_writer, nullptr);

    struct ArrowArray carray;
    struct ArrowSchema cschema;
    ASSERT_TRUE(arrow::ExportRecordBatch(*batch, &carray, &cschema).ok());

    c_status = WriteRecordBatch(c_packed_writer, &carray, &cschema);
    EXPECT_EQ(c_status.error_code, 0);

    c_status = CloseWriter(c_packed_writer);
    EXPECT_EQ(c_status.error_code, 0);

    struct ArrowSchema c_read_schema;
    ASSERT_TRUE(arrow::ExportSchema(*schema, &c_read_schema).ok());
    CPackedReader c_packed_reader = nullptr;
    c_status = NewPackedReader(
        paths, 1, &c_read_schema, buffer_size, &c_packed_reader);
    EXPECT_EQ(c_status.error_code, 0);
    EXPECT_NE(c_packed_reader, nullptr);

    c_status = CloseReader(c_packed_reader);
    EXPECT_EQ(c_status.error_code, 0);
    FreeCColumnGroups(cgs);
}