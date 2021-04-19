#include "PayloadStream.h"

namespace wrapper {

PayloadOutputStream::PayloadOutputStream() {
  buffer_.reserve(1024 * 1024);
  closed_ = false;
}

PayloadOutputStream::~PayloadOutputStream() noexcept {

}

arrow::Status PayloadOutputStream::Close() {
  closed_ = true;
  return arrow::Status::OK();
}

arrow::Result<int64_t> PayloadOutputStream::Tell() const {
  return arrow::Result<int64_t>(buffer_.size());
}

bool PayloadOutputStream::closed() const {
  return closed_;
}

arrow::Status PayloadOutputStream::Write(const void *data, int64_t nbytes) {
  if (nbytes <= 0) return arrow::Status::OK();
  auto size = buffer_.size();
  buffer_.resize(size + nbytes);
  std::memcpy(buffer_.data() + size, data, nbytes);
  return arrow::Status::OK();
}

arrow::Status PayloadOutputStream::Flush() {
  return arrow::Status::OK();
}

const std::vector<uint8_t> &PayloadOutputStream::Buffer() const {
  return buffer_;
}

}

