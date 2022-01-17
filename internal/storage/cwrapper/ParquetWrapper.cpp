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

#include "ParquetWrapper.h"
#include "PayloadStream.h"

static const char *ErrorMsg(const std::string &msg) {
  if (msg.empty()) return nullptr;
  auto ret = (char *) malloc(msg.size() + 1);
  std::memcpy(ret, msg.c_str(), msg.size());
  ret[msg.size()] = '\0';
  return ret;
}

extern "C"
CPayloadWriter NewPayloadWriter(int columnType) {
  auto p = new wrapper::PayloadWriter;
  p->builder = nullptr;
  p->schema = nullptr;
  p->output = nullptr;
  p->dimension = wrapper::EMPTY_DIMENSION;
  p->rows = 0;
  switch (static_cast<ColumnType>(columnType)) {
    case ColumnType::BOOL : {
      p->columnType = ColumnType::BOOL;
      p->builder = std::make_shared<arrow::BooleanBuilder>();
      p->schema = arrow::schema({arrow::field("val", arrow::boolean())});
      break;
    }
    case ColumnType::INT8 : {
      p->columnType = ColumnType::INT8;
      p->builder = std::make_shared<arrow::Int8Builder>();
      p->schema = arrow::schema({arrow::field("val", arrow::int8())});
      break;
    }
    case ColumnType::INT16 : {
      p->columnType = ColumnType::INT16;
      p->builder = std::make_shared<arrow::Int16Builder>();
      p->schema = arrow::schema({arrow::field("val", arrow::int16())});
      break;
    }
    case ColumnType::INT32 : {
      p->columnType = ColumnType::INT32;
      p->builder = std::make_shared<arrow::Int32Builder>();
      p->schema = arrow::schema({arrow::field("val", arrow::int32())});
      break;
    }
    case ColumnType::INT64 : {
      p->columnType = ColumnType::INT64;
      p->builder = std::make_shared<arrow::Int64Builder>();
      p->schema = arrow::schema({arrow::field("val", arrow::int64())});
      break;
    }
    case ColumnType::FLOAT : {
      p->columnType = ColumnType::FLOAT;
      p->builder = std::make_shared<arrow::FloatBuilder>();
      p->schema = arrow::schema({arrow::field("val", arrow::float32())});
      break;
    }
    case ColumnType::DOUBLE : {
      p->columnType = ColumnType::DOUBLE;
      p->builder = std::make_shared<arrow::DoubleBuilder>();
      p->schema = arrow::schema({arrow::field("val", arrow::float64())});
      break;
    }
    case ColumnType::STRING : {
      p->columnType = ColumnType::STRING;
      p->builder = std::make_shared<arrow::StringBuilder>();
      p->schema = arrow::schema({arrow::field("val", arrow::utf8())});
      break;
    }
    case ColumnType::VECTOR_BINARY : {
      p->columnType = ColumnType::VECTOR_BINARY;
      p->dimension = wrapper::EMPTY_DIMENSION;
      break;
    }
    case ColumnType::VECTOR_FLOAT : {
      p->columnType = ColumnType::VECTOR_FLOAT;
      p->dimension = wrapper::EMPTY_DIMENSION;
      break;
    }
    default: {
      delete p;
      return nullptr;
    }
  }
  return reinterpret_cast<CPayloadWriter>(p);
}

template<typename DT, typename BT>
CStatus AddValuesToPayload(CPayloadWriter payloadWriter, DT *values, int length) {
  CStatus st;
  st.error_code = static_cast<int>(ErrorCode::SUCCESS);
  st.error_msg = nullptr;
  if (length <= 0) return st;

  auto p = reinterpret_cast<wrapper::PayloadWriter *>(payloadWriter);
  auto builder = std::dynamic_pointer_cast<BT>(p->builder);
  if (builder == nullptr) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = ErrorMsg("incorrect data type");
    return st;
  }

  if (p->output != nullptr) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = ErrorMsg("payload has finished");
    return st;
  }

  auto ast = builder->AppendValues(values, values + length);
  if (!ast.ok()) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = ErrorMsg(ast.message());
    return st;
  }
  p->rows += length;
  return st;
}

extern "C"
CStatus AddBooleanToPayload(CPayloadWriter payloadWriter, bool *values, int length) {
  return AddValuesToPayload<bool, arrow::BooleanBuilder>(payloadWriter, values, length);
}

extern "C"
CStatus AddInt8ToPayload(CPayloadWriter payloadWriter, int8_t *values, int length) {
  return AddValuesToPayload<int8_t, arrow::Int8Builder>(payloadWriter, values, length);
}

extern "C"
CStatus AddInt16ToPayload(CPayloadWriter payloadWriter, int16_t *values, int length) {
  return AddValuesToPayload<int16_t, arrow::Int16Builder>(payloadWriter, values, length);
}

extern "C"
CStatus AddInt32ToPayload(CPayloadWriter payloadWriter, int32_t *values, int length) {
  return AddValuesToPayload<int32_t, arrow::Int32Builder>(payloadWriter, values, length);
}

extern "C"
CStatus AddInt64ToPayload(CPayloadWriter payloadWriter, int64_t *values, int length) {
  return AddValuesToPayload<int64_t, arrow::Int64Builder>(payloadWriter, values, length);
}

extern "C"
CStatus AddFloatToPayload(CPayloadWriter payloadWriter, float *values, int length) {
  return AddValuesToPayload<float, arrow::FloatBuilder>(payloadWriter, values, length);
}

extern "C"
CStatus AddDoubleToPayload(CPayloadWriter payloadWriter, double *values, int length) {
  return AddValuesToPayload<double, arrow::DoubleBuilder>(payloadWriter, values, length);
}

extern "C"
CStatus AddOneStringToPayload(CPayloadWriter payloadWriter, char *cstr, int str_size) {
  CStatus st;
  st.error_code = static_cast<int>(ErrorCode::SUCCESS);
  st.error_msg = nullptr;

  auto p = reinterpret_cast<wrapper::PayloadWriter *>(payloadWriter);
  auto builder = std::dynamic_pointer_cast<arrow::StringBuilder>(p->builder);
  if (builder == nullptr) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = ErrorMsg("incorrect data type");
    return st;
  }
  if (p->output != nullptr) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = ErrorMsg("payload has finished");
    return st;
  }
  arrow::Status ast;
  if (cstr == nullptr || str_size < 0) {
    ast = builder->AppendNull();
  } else {
    ast = builder->Append(cstr, str_size);
  }
  if (!ast.ok()) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = ErrorMsg(ast.message());
    return st;
  }
  p->rows++;
  return st;
}

extern "C"
CStatus AddBinaryVectorToPayload(CPayloadWriter payloadWriter, uint8_t *values, int dimension, int length) {
  CStatus st;
  st.error_code = static_cast<int>(ErrorCode::SUCCESS);
  st.error_msg = nullptr;
  if (length <= 0) return st;

  auto p = reinterpret_cast<wrapper::PayloadWriter *>(payloadWriter);
  if (p->dimension == wrapper::EMPTY_DIMENSION) {
    if ((dimension % 8) || (dimension <= 0)) {
      st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
      st.error_msg = ErrorMsg("incorrect dimension value");
      return st;
    }
    if (p->builder != nullptr) {
      st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
      st.error_msg = ErrorMsg("incorrect data type");
      return st;
    }
    p->builder = std::make_shared<arrow::FixedSizeBinaryBuilder>(arrow::fixed_size_binary(dimension / 8));
    p->schema = arrow::schema({arrow::field("val", arrow::fixed_size_binary(dimension / 8))});
    p->dimension = dimension;
  } else if (p->dimension != dimension) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = ErrorMsg("dimension changed");
    return st;
  }
  auto builder = std::dynamic_pointer_cast<arrow::FixedSizeBinaryBuilder>(p->builder);
  if (builder == nullptr) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = ErrorMsg("incorrect data type");
    return st;
  }
  if (p->output != nullptr) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = ErrorMsg("payload has finished");
    return st;
  }
  auto ast = builder->AppendValues(values, length);
  if (!ast.ok()) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = ErrorMsg(ast.message());
    return st;
  }
  p->rows += length;
  return st;
}

extern "C"
CStatus AddFloatVectorToPayload(CPayloadWriter payloadWriter, float *values, int dimension, int length) {
  CStatus st;
  st.error_code = static_cast<int>(ErrorCode::SUCCESS);
  st.error_msg = nullptr;
  if (length <= 0) return st;

  auto p = reinterpret_cast<wrapper::PayloadWriter *>(payloadWriter);
  if (p->dimension == wrapper::EMPTY_DIMENSION) {
    if (p->builder != nullptr) {
      st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
      st.error_msg = ErrorMsg("incorrect data type");
      return st;
    }
    p->builder = std::make_shared<arrow::FixedSizeBinaryBuilder>(
        arrow::fixed_size_binary(dimension * sizeof(float)));
    p->schema = arrow::schema({arrow::field("val", arrow::fixed_size_binary(dimension * sizeof(float)))});
    p->dimension = dimension;
  } else if (p->dimension != dimension) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = ErrorMsg("dimension changed");
    return st;
  }
  auto builder = std::dynamic_pointer_cast<arrow::FixedSizeBinaryBuilder>(p->builder);
  if (builder == nullptr) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = ErrorMsg("incorrect data type");
    return st;
  }
  if (p->output != nullptr) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = ErrorMsg("payload has finished");
    return st;
  }
  auto ast = builder->AppendValues(reinterpret_cast<const uint8_t *>(values), length);
  if (!ast.ok()) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = ErrorMsg(ast.message());
    return st;
  }
  p->rows += length;
  return st;
}

extern "C"
CStatus FinishPayloadWriter(CPayloadWriter payloadWriter) {
  CStatus st;
  st.error_code = static_cast<int>(ErrorCode::SUCCESS);
  st.error_msg = nullptr;
  auto p = reinterpret_cast<wrapper::PayloadWriter *>(payloadWriter);
  if (p->builder == nullptr) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = ErrorMsg("arrow builder is nullptr");
    return st;
  }
  if (p->output == nullptr) {
    std::shared_ptr<arrow::Array> array;
    auto ast = p->builder->Finish(&array);
    if (!ast.ok()) {
      st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
      st.error_msg = ErrorMsg(ast.message());
      return st;
    }
    auto table = arrow::Table::Make(p->schema, {array});
    p->output = std::make_shared<wrapper::PayloadOutputStream>();
    auto mem_pool = arrow::default_memory_pool();
    ast = parquet::arrow::WriteTable(*table, mem_pool, p->output, 1024 * 1024 * 1024);
    if (!ast.ok()) {
      st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
      st.error_msg = ErrorMsg(ast.message());
      return st;
    }
  }
  return st;
}

CBuffer GetPayloadBufferFromWriter(CPayloadWriter payloadWriter) {
  CBuffer buf;

  auto p = reinterpret_cast<wrapper::PayloadWriter *>(payloadWriter);
  if (p->output == nullptr) {
    buf.length = 0;
    buf.data = nullptr;
	return buf;
  }
  auto &output = p->output->Buffer();
  buf.length = static_cast<int>(output.size());
  buf.data = (char *) (output.data());
  return buf;
}

int GetPayloadLengthFromWriter(CPayloadWriter payloadWriter) {
  auto p = reinterpret_cast<wrapper::PayloadWriter *>(payloadWriter);
  return p->rows;
}

extern "C"
void ReleasePayloadWriter(CPayloadWriter handler) {
  auto p = reinterpret_cast<wrapper::PayloadWriter *>(handler);
  if (p != nullptr) delete p;
  arrow::default_memory_pool()->ReleaseUnused();
}

extern "C"
CPayloadReader NewPayloadReader(int columnType, uint8_t *buffer, int64_t buf_size) {
  auto p = new wrapper::PayloadReader;
  p->bValues = nullptr;
  p->input = std::make_shared<wrapper::PayloadInputStream>(buffer, buf_size);
  auto mem_pool = arrow::default_memory_pool();
  auto st = parquet::arrow::OpenFile(p->input, mem_pool, &p->reader);
  if (!st.ok()) {
    delete p;
    return nullptr;
  }
  st = p->reader->ReadTable(&p->table);
  if (!st.ok()) {
    delete p;
    return nullptr;
  }
  p->column = p->table->column(0);
  assert(p->column != nullptr);
  assert(p->column->chunks().size() == 1);
  p->array = p->column->chunk(0);
  switch (columnType) {
    case ColumnType::BOOL :
    case ColumnType::INT8 :
    case ColumnType::INT16 :
    case ColumnType::INT32 :
    case ColumnType::INT64 :
    case ColumnType::FLOAT :
    case ColumnType::DOUBLE :
    case ColumnType::STRING :
    case ColumnType::VECTOR_BINARY :
    case ColumnType::VECTOR_FLOAT : {
      break;
    }
    default: {
      delete p;
      return nullptr;
    }
  }
  return reinterpret_cast<CPayloadReader>(p);
}

extern "C"
CStatus GetBoolFromPayload(CPayloadReader payloadReader, bool **values, int *length) {
  CStatus st;
  st.error_code = static_cast<int>(ErrorCode::SUCCESS);
  st.error_msg = nullptr;
  auto p = reinterpret_cast<wrapper::PayloadReader *>(payloadReader);
  if (p->bValues == nullptr) {
    auto array = std::dynamic_pointer_cast<arrow::BooleanArray>(p->array);
    if (array == nullptr) {
      st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
      st.error_msg = ErrorMsg("incorrect data type");
      return st;
    }
    int len = array->length();
    p->bValues = new bool[len];
    for (int i = 0; i < len; i++) {
      p->bValues[i] = array->Value(i);
    }
  }
  *values = p->bValues;
  *length = p->array->length();
  return st;
}

template<typename DT, typename AT>
CStatus GetValuesFromPayload(CPayloadReader payloadReader, DT **values, int *length) {
  CStatus st;
  st.error_code = static_cast<int>(ErrorCode::SUCCESS);
  st.error_msg = nullptr;
  auto p = reinterpret_cast<wrapper::PayloadReader *>(payloadReader);
  auto array = std::dynamic_pointer_cast<AT>(p->array);
  if (array == nullptr) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = ErrorMsg("incorrect data type");
    return st;
  }
  *length = array->length();
  *values = (DT *) array->raw_values();
  return st;
}

extern "C"
CStatus GetInt8FromPayload(CPayloadReader payloadReader, int8_t **values, int *length) {
  return GetValuesFromPayload<int8_t, arrow::Int8Array>(payloadReader, values, length);
}

extern "C"
CStatus GetInt16FromPayload(CPayloadReader payloadReader, int16_t **values, int *length) {
  return GetValuesFromPayload<int16_t, arrow::Int16Array>(payloadReader, values, length);
}

extern "C"
CStatus GetInt32FromPayload(CPayloadReader payloadReader, int32_t **values, int *length) {
  return GetValuesFromPayload<int32_t, arrow::Int32Array>(payloadReader, values, length);
}

extern "C"
CStatus GetInt64FromPayload(CPayloadReader payloadReader, int64_t **values, int *length) {
  return GetValuesFromPayload<int64_t, arrow::Int64Array>(payloadReader, values, length);
}

extern "C"
CStatus GetFloatFromPayload(CPayloadReader payloadReader, float **values, int *length) {
  return GetValuesFromPayload<float, arrow::FloatArray>(payloadReader, values, length);
}

extern "C"
CStatus GetDoubleFromPayload(CPayloadReader payloadReader, double **values, int *length) {
  return GetValuesFromPayload<double, arrow::DoubleArray>(payloadReader, values, length);
}

extern "C"
CStatus GetOneStringFromPayload(CPayloadReader payloadReader, int idx, char **cstr, int *str_size) {
  CStatus st;
  st.error_code = static_cast<int>(ErrorCode::SUCCESS);
  st.error_msg = nullptr;
  auto p = reinterpret_cast<wrapper::PayloadReader *>(payloadReader);
  auto array = std::dynamic_pointer_cast<arrow::StringArray>(p->array);
  if (array == nullptr) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = ErrorMsg("Incorrect data type");
    return st;
  }
  if (idx >= array->length()) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = ErrorMsg("memory overflow");
    return st;
  }
  arrow::StringArray::offset_type length;
  *cstr = (char *) array->GetValue(idx, &length);
  *str_size = length;
  return st;
}

extern "C"
CStatus GetBinaryVectorFromPayload(CPayloadReader payloadReader, uint8_t **values, int *dimension, int *length) {
  CStatus st;
  st.error_code = static_cast<int>(ErrorCode::SUCCESS);
  st.error_msg = nullptr;
  auto p = reinterpret_cast<wrapper::PayloadReader *>(payloadReader);
  auto array = std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(p->array);
  if (array == nullptr) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = ErrorMsg("Incorrect data type");
    return st;
  }
  *dimension = array->byte_width() * 8;
  *length = array->length();
  *values = (uint8_t *) array->raw_values();
  return st;
}

extern "C"
CStatus GetFloatVectorFromPayload(CPayloadReader payloadReader, float **values, int *dimension, int *length) {
  CStatus st;
  st.error_code = static_cast<int>(ErrorCode::SUCCESS);
  st.error_msg = nullptr;
  auto p = reinterpret_cast<wrapper::PayloadReader *>(payloadReader);
  auto array = std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(p->array);
  if (array == nullptr) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = ErrorMsg("Incorrect data type");
    return st;
  }
  *dimension = array->byte_width() / sizeof(float);
  *length = array->length();
  *values = (float *) array->raw_values();
  return st;
}

extern "C"
int GetPayloadLengthFromReader(CPayloadReader payloadReader) {
  auto p = reinterpret_cast<wrapper::PayloadReader *>(payloadReader);
  if (p->array == nullptr) return 0;
  return p->array->length();
}

extern "C"
void ReleasePayloadReader(CPayloadReader payloadReader) {
  auto p = reinterpret_cast<wrapper::PayloadReader *>(payloadReader);
  if (p != nullptr) {
    delete[] p->bValues;
    delete p;
  }
  arrow::default_memory_pool()->ReleaseUnused();
}
