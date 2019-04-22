//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/trace/file_trace_reader_writer.h"

#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/trace_replay.h"

namespace rocksdb {

const unsigned int FileTraceReader::kBufferSize = 1024;  // 1KB

FileTraceReader::FileTraceReader(
    std::unique_ptr<RandomAccessFileReader>&& reader)
    : file_reader_(std::move(reader)),
      offset_(0),
      buffer_(new char[kBufferSize]) {}

FileTraceReader::~FileTraceReader() {
  Close();
  delete[] buffer_;
}

Status FileTraceReader::Close() {
  file_reader_.reset();
  return Status::OK();
}

Status FileTraceReader::Read(std::string* data) {
  assert(file_reader_ != nullptr);
  Status s = file_reader_->Read(offset_, kTraceMetadataSize, &result_, buffer_);
  if (!s.ok()) {
    return s;
  }
  if (result_.size() == 0) {
    // No more data to read
    // Todo: Come up with a better way to indicate end of data. May be this
    // could be avoided once footer is introduced.
    return Status::Incomplete();
  }
  if (result_.size() < kTraceMetadataSize) {
    return Status::Corruption("Corrupted trace file.");
  }
  *data = result_.ToString();
  offset_ += kTraceMetadataSize;

  uint32_t payload_len =
      DecodeFixed32(&buffer_[kTraceTimestampSize + kTraceTypeSize]);

  // Read Payload
  unsigned int bytes_to_read = payload_len;
  unsigned int to_read =
      bytes_to_read > kBufferSize ? kBufferSize : bytes_to_read;
  while (to_read > 0) {
    s = file_reader_->Read(offset_, to_read, &result_, buffer_);
    if (!s.ok()) {
      return s;
    }
    if (result_.size() < to_read) {
      return Status::Corruption("Corrupted trace file.");
    }
    data->append(result_.data(), result_.size());

    offset_ += to_read;
    bytes_to_read -= to_read;
    to_read = bytes_to_read > kBufferSize ? kBufferSize : bytes_to_read;
  }

  return s;
}

FileTraceWriter::~FileTraceWriter() { Close(); }

Status FileTraceWriter::Close() {
  file_writer_.reset();
  return Status::OK();
}

Status FileTraceWriter::Write(const Slice& data) {
  return file_writer_->Append(data);
}

uint64_t FileTraceWriter::GetFileSize() { return file_writer_->GetFileSize(); }

Status NewFileTraceReader(Env* env, const EnvOptions& env_options,
                          const std::string& trace_filename,
                          std::unique_ptr<TraceReader>* trace_reader) {
  std::unique_ptr<RandomAccessFile> trace_file;
  Status s = env->NewRandomAccessFile(trace_filename, &trace_file, env_options);
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<RandomAccessFileReader> file_reader;
  file_reader.reset(
      new RandomAccessFileReader(std::move(trace_file), trace_filename));
  trace_reader->reset(new FileTraceReader(std::move(file_reader)));
  return s;
}

Status NewFileTraceWriter(Env* env, const EnvOptions& env_options,
                          const std::string& trace_filename,
                          std::unique_ptr<TraceWriter>* trace_writer) {
  std::unique_ptr<WritableFile> trace_file;
  Status s = env->NewWritableFile(trace_filename, &trace_file, env_options);
  if (!s.ok()) {
    return s;
  }

  std::unique_ptr<WritableFileWriter> file_writer;
  file_writer.reset(new WritableFileWriter(std::move(trace_file),
                                           trace_filename, env_options));
  trace_writer->reset(new FileTraceWriter(std::move(file_writer)));
  return s;
}

}  // namespace rocksdb
