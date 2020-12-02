#pragma once

#include <arrow/api.h>
#include <arrow/io/interfaces.h>
#include "ColumnType.h"

namespace wrapper {

class PayloadOutputStream;

struct PayloadWriter {
  ColumnType columnType;
  std::shared_ptr<arrow::ArrayBuilder> builder;
  std::shared_ptr<arrow::Schema> schema;
  std::shared_ptr<PayloadOutputStream> output;
  int rows;
};

class PayloadOutputStream : public arrow::io::OutputStream {
 public:
  PayloadOutputStream();
  ~PayloadOutputStream();

  arrow::Status Close() override;
  arrow::Result<int64_t> Tell() const override;
  bool closed() const override;
  arrow::Status Write(const void *data, int64_t nbytes) override;
  arrow::Status Flush() override;

 public:
  const std::vector<uint8_t> &Buffer() const;

 private:
  std::vector<uint8_t> buffer_;
  bool closed_;
};

class PayloadInputStream : public arrow::io::RandomAccessFile {
 public:
  PayloadInputStream(const void *data, int64_t size);
  ~PayloadInputStream();


};

}