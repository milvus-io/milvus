#include <gtest/gtest.h>
#include <fstream>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include "ParquetWraper.h"
#include "ColumnType.h"

static void WriteToFile(CBuffer cb) {
  auto data_file = std::ofstream("/tmp/wrapper_test_data.dat", std::ios::binary);
  data_file.write(cb.data, cb.length);
  data_file.close();
}

static std::shared_ptr<arrow::Table> ReadFromFile() {
  std::shared_ptr<arrow::io::ReadableFile> infile;
  auto rst = arrow::io::ReadableFile::Open("/tmp/wrapper_test_data.dat");
  if (!rst.ok()) return nullptr;
  infile = *rst;

  std::shared_ptr<arrow::Table> table;
  std::unique_ptr<parquet::arrow::FileReader> reader;
  auto st = parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader);
  if (!st.ok()) return nullptr;
  st = reader->ReadTable(&table);
  if (!st.ok()) return nullptr;
  return table;
}

TEST(wrapper, boolean) {
  auto payload = NewPayloadWriter(ColumnType::BOOL);
  bool data[] = {true, false, true, false};

  auto st = AddBooleanToPayload(payload, data, 4);
  ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
  st = FinishPayload(payload);
  ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
  auto cb = GetPayloadBuffer(payload);
  ASSERT_GT(cb.length, 0);
  ASSERT_NE(cb.data, nullptr);

  WriteToFile(cb);

  auto nums = GetPayloadNums(payload);
  ASSERT_EQ(nums, 4);
  st = ReleasePayload(payload);
  ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);

  auto table = ReadFromFile();
  ASSERT_NE(table, nullptr);

  auto chunks = table->column(0)->chunks();
  ASSERT_EQ(chunks.size(), 1);

  auto bool_array = std::dynamic_pointer_cast<arrow::BooleanArray>(chunks[0]);
  ASSERT_NE(bool_array, nullptr);

  ASSERT_EQ(bool_array->Value(0), true);
  ASSERT_EQ(bool_array->Value(1), false);
  ASSERT_EQ(bool_array->Value(2), true);
  ASSERT_EQ(bool_array->Value(3), false);
}

#define NUMERIC_TEST(TEST_NAME, COLUMN_TYPE, DATA_TYPE, ADD_FUNC, ARRAY_TYPE) TEST(wrapper, TEST_NAME) {  \
auto payload = NewPayloadWriter(COLUMN_TYPE);                                                             \
DATA_TYPE data[] = {-1, 1, -100, 100};                                                                    \
                                                                                                          \
auto st = ADD_FUNC(payload, data, 4);                                                                     \
ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);                                                             \
st = FinishPayload(payload);                                                                              \
ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);                                                             \
auto cb = GetPayloadBuffer(payload);                                                                      \
ASSERT_GT(cb.length, 0);                                                                                  \
ASSERT_NE(cb.data, nullptr);                                                                              \
                                                                                                          \
WriteToFile(cb);                                                                                          \
                                                                                                          \
auto nums = GetPayloadNums(payload);                                                                      \
ASSERT_EQ(nums, 4);                                                                                       \
st = ReleasePayload(payload);                                                                             \
ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);                                                             \
                                                                                                          \
auto table = ReadFromFile();                                                                              \
ASSERT_NE(table, nullptr);                                                                                \
                                                                                                          \
auto chunks = table->column(0)->chunks();                                                                 \
ASSERT_EQ(chunks.size(), 1);                                                                              \
                                                                                                          \
auto bool_array = std::dynamic_pointer_cast<ARRAY_TYPE>(chunks[0]);                                       \
ASSERT_NE(bool_array, nullptr);                                                                           \
                                                                                                          \
ASSERT_EQ(bool_array->Value(0), -1);                                                                      \
ASSERT_EQ(bool_array->Value(1), 1);                                                                       \
ASSERT_EQ(bool_array->Value(2), -100);                                                                    \
ASSERT_EQ(bool_array->Value(3), 100);                                                                     \
}

NUMERIC_TEST(int8, ColumnType::INT8, int8_t, AddInt8ToPayload, arrow::Int8Array)
NUMERIC_TEST(int16, ColumnType::INT16, int16_t, AddInt16ToPayload, arrow::Int16Array)
NUMERIC_TEST(int32, ColumnType::INT32, int32_t, AddInt32ToPayload, arrow::Int32Array)
NUMERIC_TEST(int64, ColumnType::INT64, int64_t, AddInt64ToPayload, arrow::Int64Array)
NUMERIC_TEST(float32, ColumnType::FLOAT, float, AddFloatToPayload, arrow::FloatArray)
NUMERIC_TEST(float64, ColumnType::DOUBLE, double, AddDoubleToPayload, arrow::DoubleArray)


//TEST(wrapper, int8) {
//  auto payload = NewPayloadWriter(ColumnType::INT8);
//  int8_t data[] = {-1, 1, -100, 100};
//
//  auto st = AddInt8ToPayload(payload, data, 4);
//  ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
//  st = FinishPayload(payload);
//  ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
//  auto cb = GetPayloadBuffer(payload);
//  ASSERT_GT(cb.length, 0);
//  ASSERT_NE(cb.data, nullptr);
//
//  WriteToFile(cb);
//
//  auto nums = GetPayloadNums(payload);
//  ASSERT_EQ(nums, 4);
//  st = ReleasePayload(payload);
//  ASSERT_EQ(st.error_code, ErrorCode::SUCCESS);
//
//  auto table = ReadFromFile();
//  ASSERT_NE(table, nullptr);
//
//  auto chunks = table->column(0)->chunks();
//  ASSERT_EQ(chunks.size(), 1);
//
//  auto bool_array = std::dynamic_pointer_cast<arrow::Int8Array>(chunks[0]);
//  ASSERT_NE(bool_array, nullptr);
//
//  ASSERT_EQ(bool_array->Value(0), -1);
//  ASSERT_EQ(bool_array->Value(1), 1);
//  ASSERT_EQ(bool_array->Value(2), -100);
//  ASSERT_EQ(bool_array->Value(3), 100);
//}
