#include "ParquetWraper.h"
#include "PayloadStream.h"
#include "parquet/arrow/writer.h"

extern "C" CPayloadWriter NewPayloadWriter(int columnType) {
  auto p = new wrapper::PayloadWriter;
  p->builder = nullptr;
  p->schema = nullptr;
  p->output = nullptr;
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
      //TODO, arrow array builder
      break;
    }
    case ColumnType::VECTOR_FLOAT : {
      p->columnType == ColumnType::VECTOR_FLOAT;
      //TODO, arrow array builder
      break;
    }
    default: {
      delete p;
      return nullptr;
    }
  }
  return reinterpret_cast<void *>(p);
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
    st.error_msg = "incorrect data type";
    return st;
  }

  if (p->output != nullptr) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = "payload has finished";
    return st;
  }

  auto ast = builder->AppendValues(values, values + length);
  if (!ast.ok()) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = "add value into arrow array failed";
    return st;
  }
  p->rows += length;
  return st;
}

extern "C" CStatus AddBooleanToPayload(CPayloadWriter payloadWriter, bool *values, int length) {
  return AddValuesToPayload<bool, arrow::BooleanBuilder>(payloadWriter, values, length);
}

extern "C" CStatus AddInt8ToPayload(CPayloadWriter payloadWriter, int8_t *values, int length) {
  return AddValuesToPayload<int8_t, arrow::Int8Builder>(payloadWriter, values, length);
}
extern "C" CStatus AddInt16ToPayload(CPayloadWriter payloadWriter, int16_t *values, int length) {
  return AddValuesToPayload<int16_t, arrow::Int16Builder>(payloadWriter, values, length);
}
extern "C" CStatus AddInt32ToPayload(CPayloadWriter payloadWriter, int32_t *values, int length) {
  return AddValuesToPayload<int32_t, arrow::Int32Builder>(payloadWriter, values, length);
}
extern "C" CStatus AddInt64ToPayload(CPayloadWriter payloadWriter, int64_t *values, int length) {
  return AddValuesToPayload<int64_t, arrow::Int64Builder>(payloadWriter, values, length);
}
extern "C" CStatus AddFloatToPayload(CPayloadWriter payloadWriter, float *values, int length) {
  return AddValuesToPayload<float, arrow::FloatBuilder>(payloadWriter, values, length);
}
extern "C" CStatus AddDoubleToPayload(CPayloadWriter payloadWriter, double *values, int length) {
  return AddValuesToPayload<double, arrow::DoubleBuilder>(payloadWriter, values, length);
}

extern "C" CStatus FinishPayload(CPayloadWriter payloadWriter) {
  CStatus st;
  st.error_code = static_cast<int>(ErrorCode::SUCCESS);
  st.error_msg = nullptr;
  auto p = reinterpret_cast<wrapper::PayloadWriter *>(payloadWriter);
  if (p->builder == nullptr) {
    st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
    st.error_msg = "arrow builder is nullptr";
    return st;
  }
  if (p->output == nullptr) {
    std::shared_ptr<arrow::Array> array;
    auto ast = p->builder->Finish(&array);
    if (!ast.ok()) {
      st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
      st.error_msg = "failed to finish array builder";
      return st;
    }
    auto table = arrow::Table::Make(p->schema, {array});
    p->output = std::make_shared<wrapper::PayloadOutputStream>();
    ast = parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), p->output, 1024 * 1024);
    if (!ast.ok()) {
      st.error_code = static_cast<int>(ErrorCode::UNEXPECTED_ERROR);
      st.error_msg = "failed to write parquet buffer";
      return st;
    }
  }
  return st;
}

CBuffer GetPayloadBuffer(CPayloadWriter payloadWriter) {
  CBuffer buf;

  auto p = reinterpret_cast<wrapper::PayloadWriter *>(payloadWriter);
  if (p->output == nullptr) {
    buf.length = 0;
    buf.data = nullptr;
  }
  auto &output = p->output->Buffer();
  buf.length = static_cast<int>(output.size());
  buf.data = (char *) (output.data());
  return buf;
}

int GetPayloadNums(CPayloadWriter payloadWriter) {
  auto p = reinterpret_cast<wrapper::PayloadWriter *>(payloadWriter);
  return p->rows;
}

extern "C" CStatus ReleasePayload(CPayloadWriter handler) {
  CStatus st;
  st.error_code = static_cast<int>(ErrorCode::SUCCESS);
  st.error_msg = nullptr;
  auto p = reinterpret_cast<wrapper::PayloadWriter *>(handler);
  if (p != nullptr) delete p;
  return st;
}