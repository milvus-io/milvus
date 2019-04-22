//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"
#include "db/log_writer.h"
#include "rocksdb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"
#include "util/random.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {
namespace log {

// Construct a string of the specified length made out of the supplied
// partial string.
static std::string BigString(const std::string& partial_string, size_t n) {
  std::string result;
  while (result.size() < n) {
    result.append(partial_string);
  }
  result.resize(n);
  return result;
}

// Construct a string from a number
static std::string NumberString(int n) {
  char buf[50];
  snprintf(buf, sizeof(buf), "%d.", n);
  return std::string(buf);
}

// Return a skewed potentially long string
static std::string RandomSkewedString(int i, Random* rnd) {
  return BigString(NumberString(i), rnd->Skewed(17));
}

class LogTest : public ::testing::TestWithParam<int> {
 private:
  class StringSource : public SequentialFile {
   public:
    Slice& contents_;
    bool force_error_;
    size_t force_error_position_;
    bool force_eof_;
    size_t force_eof_position_;
    bool returned_partial_;
    explicit StringSource(Slice& contents) :
      contents_(contents),
      force_error_(false),
      force_error_position_(0),
      force_eof_(false),
      force_eof_position_(0),
      returned_partial_(false) { }

    Status Read(size_t n, Slice* result, char* scratch) override {
      EXPECT_TRUE(!returned_partial_) << "must not Read() after eof/error";

      if (force_error_) {
        if (force_error_position_ >= n) {
          force_error_position_ -= n;
        } else {
          *result = Slice(contents_.data(), force_error_position_);
          contents_.remove_prefix(force_error_position_);
          force_error_ = false;
          returned_partial_ = true;
          return Status::Corruption("read error");
        }
      }

      if (contents_.size() < n) {
        n = contents_.size();
        returned_partial_ = true;
      }

      if (force_eof_) {
        if (force_eof_position_ >= n) {
          force_eof_position_ -= n;
        } else {
          force_eof_ = false;
          n = force_eof_position_;
          returned_partial_ = true;
        }
      }

      // By using scratch we ensure that caller has control over the
      // lifetime of result.data()
      memcpy(scratch, contents_.data(), n);
      *result = Slice(scratch, n);

      contents_.remove_prefix(n);
      return Status::OK();
    }

    Status Skip(uint64_t n) override {
      if (n > contents_.size()) {
        contents_.clear();
        return Status::NotFound("in-memory file skipepd past end");
      }

      contents_.remove_prefix(n);

      return Status::OK();
    }
  };

  class ReportCollector : public Reader::Reporter {
   public:
    size_t dropped_bytes_;
    std::string message_;

    ReportCollector() : dropped_bytes_(0) { }
    void Corruption(size_t bytes, const Status& status) override {
      dropped_bytes_ += bytes;
      message_.append(status.ToString());
    }
  };

  std::string& dest_contents() {
    auto dest =
      dynamic_cast<test::StringSink*>(writer_.file()->writable_file());
    assert(dest);
    return dest->contents_;
  }

  const std::string& dest_contents() const {
    auto dest =
      dynamic_cast<const test::StringSink*>(writer_.file()->writable_file());
    assert(dest);
    return dest->contents_;
  }

  void reset_source_contents() {
    auto src = dynamic_cast<StringSource*>(reader_.file()->file());
    assert(src);
    src->contents_ = dest_contents();
  }

  Slice reader_contents_;
  std::unique_ptr<WritableFileWriter> dest_holder_;
  std::unique_ptr<SequentialFileReader> source_holder_;
  ReportCollector report_;
  Writer writer_;
  Reader reader_;

  // Record metadata for testing initial offset functionality
  static size_t initial_offset_record_sizes_[];
  uint64_t initial_offset_last_record_offsets_[4];

 public:
  LogTest()
      : reader_contents_(),
        dest_holder_(test::GetWritableFileWriter(
            new test::StringSink(&reader_contents_), "" /* don't care */)),
        source_holder_(test::GetSequentialFileReader(
            new StringSource(reader_contents_), "" /* file name */)),
        writer_(std::move(dest_holder_), 123, GetParam()),
        reader_(nullptr, std::move(source_holder_), &report_,
                true /* checksum */, 123 /* log_number */,
                false /* retry_after_eof */) {
    int header_size = GetParam() ? kRecyclableHeaderSize : kHeaderSize;
    initial_offset_last_record_offsets_[0] = 0;
    initial_offset_last_record_offsets_[1] = header_size + 10000;
    initial_offset_last_record_offsets_[2] = 2 * (header_size + 10000);
    initial_offset_last_record_offsets_[3] = 2 * (header_size + 10000) +
                                             (2 * log::kBlockSize - 1000) +
                                             3 * header_size;
  }

  Slice* get_reader_contents() { return &reader_contents_; }

  void Write(const std::string& msg) {
    writer_.AddRecord(Slice(msg));
  }

  size_t WrittenBytes() const {
    return dest_contents().size();
  }

  std::string Read(const WALRecoveryMode wal_recovery_mode =
                       WALRecoveryMode::kTolerateCorruptedTailRecords) {
    std::string scratch;
    Slice record;
    if (reader_.ReadRecord(&record, &scratch, wal_recovery_mode)) {
      return record.ToString();
    } else {
      return "EOF";
    }
  }

  void IncrementByte(int offset, char delta) {
    dest_contents()[offset] += delta;
  }

  void SetByte(int offset, char new_byte) {
    dest_contents()[offset] = new_byte;
  }

  void ShrinkSize(int bytes) {
    auto dest =
      dynamic_cast<test::StringSink*>(writer_.file()->writable_file());
    assert(dest);
    dest->Drop(bytes);
  }

  void FixChecksum(int header_offset, int len, bool recyclable) {
    // Compute crc of type/len/data
    int header_size = recyclable ? kRecyclableHeaderSize : kHeaderSize;
    uint32_t crc = crc32c::Value(&dest_contents()[header_offset + 6],
                                 header_size - 6 + len);
    crc = crc32c::Mask(crc);
    EncodeFixed32(&dest_contents()[header_offset], crc);
  }

  void ForceError(size_t position = 0) {
    auto src = dynamic_cast<StringSource*>(reader_.file()->file());
    src->force_error_ = true;
    src->force_error_position_ = position;
  }

  size_t DroppedBytes() const {
    return report_.dropped_bytes_;
  }

  std::string ReportMessage() const {
    return report_.message_;
  }

  void ForceEOF(size_t position = 0) {
    auto src = dynamic_cast<StringSource*>(reader_.file()->file());
    src->force_eof_ = true;
    src->force_eof_position_ = position;
  }

  void UnmarkEOF() {
    auto src = dynamic_cast<StringSource*>(reader_.file()->file());
    src->returned_partial_ = false;
    reader_.UnmarkEOF();
  }

  bool IsEOF() {
    return reader_.IsEOF();
  }

  // Returns OK iff recorded error message contains "msg"
  std::string MatchError(const std::string& msg) const {
    if (report_.message_.find(msg) == std::string::npos) {
      return report_.message_;
    } else {
      return "OK";
    }
  }

  void WriteInitialOffsetLog() {
    for (int i = 0; i < 4; i++) {
      std::string record(initial_offset_record_sizes_[i],
                         static_cast<char>('a' + i));
      Write(record);
    }
  }

};

size_t LogTest::initial_offset_record_sizes_[] =
    {10000,  // Two sizable records in first block
     10000,
     2 * log::kBlockSize - 1000,  // Span three blocks
     1};

TEST_P(LogTest, Empty) { ASSERT_EQ("EOF", Read()); }

TEST_P(LogTest, ReadWrite) {
  Write("foo");
  Write("bar");
  Write("");
  Write("xxxx");
  ASSERT_EQ("foo", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("xxxx", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("EOF", Read());  // Make sure reads at eof work
}

TEST_P(LogTest, ManyBlocks) {
  for (int i = 0; i < 100000; i++) {
    Write(NumberString(i));
  }
  for (int i = 0; i < 100000; i++) {
    ASSERT_EQ(NumberString(i), Read());
  }
  ASSERT_EQ("EOF", Read());
}

TEST_P(LogTest, Fragmentation) {
  Write("small");
  Write(BigString("medium", 50000));
  Write(BigString("large", 100000));
  ASSERT_EQ("small", Read());
  ASSERT_EQ(BigString("medium", 50000), Read());
  ASSERT_EQ(BigString("large", 100000), Read());
  ASSERT_EQ("EOF", Read());
}

TEST_P(LogTest, MarginalTrailer) {
  // Make a trailer that is exactly the same length as an empty record.
  int header_size = GetParam() ? kRecyclableHeaderSize : kHeaderSize;
  const int n = kBlockSize - 2 * header_size;
  Write(BigString("foo", n));
  ASSERT_EQ((unsigned int)(kBlockSize - header_size), WrittenBytes());
  Write("");
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
}

TEST_P(LogTest, MarginalTrailer2) {
  // Make a trailer that is exactly the same length as an empty record.
  int header_size = GetParam() ? kRecyclableHeaderSize : kHeaderSize;
  const int n = kBlockSize - 2 * header_size;
  Write(BigString("foo", n));
  ASSERT_EQ((unsigned int)(kBlockSize - header_size), WrittenBytes());
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(0U, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST_P(LogTest, ShortTrailer) {
  int header_size = GetParam() ? kRecyclableHeaderSize : kHeaderSize;
  const int n = kBlockSize - 2 * header_size + 4;
  Write(BigString("foo", n));
  ASSERT_EQ((unsigned int)(kBlockSize - header_size + 4), WrittenBytes());
  Write("");
  Write("bar");
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
}

TEST_P(LogTest, AlignedEof) {
  int header_size = GetParam() ? kRecyclableHeaderSize : kHeaderSize;
  const int n = kBlockSize - 2 * header_size + 4;
  Write(BigString("foo", n));
  ASSERT_EQ((unsigned int)(kBlockSize - header_size + 4), WrittenBytes());
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_EQ("EOF", Read());
}

TEST_P(LogTest, RandomRead) {
  const int N = 500;
  Random write_rnd(301);
  for (int i = 0; i < N; i++) {
    Write(RandomSkewedString(i, &write_rnd));
  }
  Random read_rnd(301);
  for (int i = 0; i < N; i++) {
    ASSERT_EQ(RandomSkewedString(i, &read_rnd), Read());
  }
  ASSERT_EQ("EOF", Read());
}

// Tests of all the error paths in log_reader.cc follow:

TEST_P(LogTest, ReadError) {
  Write("foo");
  ForceError();
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ((unsigned int)kBlockSize, DroppedBytes());
  ASSERT_EQ("OK", MatchError("read error"));
}

TEST_P(LogTest, BadRecordType) {
  Write("foo");
  // Type is stored in header[6]
  IncrementByte(6, 100);
  FixChecksum(0, 3, false);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3U, DroppedBytes());
  ASSERT_EQ("OK", MatchError("unknown record type"));
}

TEST_P(LogTest, TruncatedTrailingRecordIsIgnored) {
  Write("foo");
  ShrinkSize(4);   // Drop all payload as well as a header byte
  ASSERT_EQ("EOF", Read());
  // Truncated last record is ignored, not treated as an error
  ASSERT_EQ(0U, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST_P(LogTest, TruncatedTrailingRecordIsNotIgnored) {
  Write("foo");
  ShrinkSize(4);  // Drop all payload as well as a header byte
  ASSERT_EQ("EOF", Read(WALRecoveryMode::kAbsoluteConsistency));
  // Truncated last record is ignored, not treated as an error
  ASSERT_GT(DroppedBytes(), 0U);
  ASSERT_EQ("OK", MatchError("Corruption: truncated header"));
}

TEST_P(LogTest, BadLength) {
  int header_size = GetParam() ? kRecyclableHeaderSize : kHeaderSize;
  const int kPayloadSize = kBlockSize - header_size;
  Write(BigString("bar", kPayloadSize));
  Write("foo");
  // Least significant size byte is stored in header[4].
  IncrementByte(4, 1);
  if (!GetParam()) {
    ASSERT_EQ("foo", Read());
    ASSERT_EQ(kBlockSize, DroppedBytes());
    ASSERT_EQ("OK", MatchError("bad record length"));
  } else {
    ASSERT_EQ("EOF", Read());
  }
}

TEST_P(LogTest, BadLengthAtEndIsIgnored) {
  Write("foo");
  ShrinkSize(1);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(0U, DroppedBytes());
  ASSERT_EQ("", ReportMessage());
}

TEST_P(LogTest, BadLengthAtEndIsNotIgnored) {
  Write("foo");
  ShrinkSize(1);
  ASSERT_EQ("EOF", Read(WALRecoveryMode::kAbsoluteConsistency));
  ASSERT_GT(DroppedBytes(), 0U);
  ASSERT_EQ("OK", MatchError("Corruption: truncated header"));
}

TEST_P(LogTest, ChecksumMismatch) {
  Write("foooooo");
  IncrementByte(0, 14);
  ASSERT_EQ("EOF", Read());
  if (!GetParam()) {
    ASSERT_EQ(14U, DroppedBytes());
    ASSERT_EQ("OK", MatchError("checksum mismatch"));
  } else {
    ASSERT_EQ(0U, DroppedBytes());
    ASSERT_EQ("", ReportMessage());
  }
}

TEST_P(LogTest, UnexpectedMiddleType) {
  Write("foo");
  SetByte(6, static_cast<char>(GetParam() ? kRecyclableMiddleType : kMiddleType));
  FixChecksum(0, 3, !!GetParam());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3U, DroppedBytes());
  ASSERT_EQ("OK", MatchError("missing start"));
}

TEST_P(LogTest, UnexpectedLastType) {
  Write("foo");
  SetByte(6, static_cast<char>(GetParam() ? kRecyclableLastType : kLastType));
  FixChecksum(0, 3, !!GetParam());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3U, DroppedBytes());
  ASSERT_EQ("OK", MatchError("missing start"));
}

TEST_P(LogTest, UnexpectedFullType) {
  Write("foo");
  Write("bar");
  SetByte(6, static_cast<char>(GetParam() ? kRecyclableFirstType : kFirstType));
  FixChecksum(0, 3, !!GetParam());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3U, DroppedBytes());
  ASSERT_EQ("OK", MatchError("partial record without end"));
}

TEST_P(LogTest, UnexpectedFirstType) {
  Write("foo");
  Write(BigString("bar", 100000));
  SetByte(6, static_cast<char>(GetParam() ? kRecyclableFirstType : kFirstType));
  FixChecksum(0, 3, !!GetParam());
  ASSERT_EQ(BigString("bar", 100000), Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3U, DroppedBytes());
  ASSERT_EQ("OK", MatchError("partial record without end"));
}

TEST_P(LogTest, MissingLastIsIgnored) {
  Write(BigString("bar", kBlockSize));
  // Remove the LAST block, including header.
  ShrinkSize(14);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("", ReportMessage());
  ASSERT_EQ(0U, DroppedBytes());
}

TEST_P(LogTest, MissingLastIsNotIgnored) {
  Write(BigString("bar", kBlockSize));
  // Remove the LAST block, including header.
  ShrinkSize(14);
  ASSERT_EQ("EOF", Read(WALRecoveryMode::kAbsoluteConsistency));
  ASSERT_GT(DroppedBytes(), 0U);
  ASSERT_EQ("OK", MatchError("Corruption: error reading trailing data"));
}

TEST_P(LogTest, PartialLastIsIgnored) {
  Write(BigString("bar", kBlockSize));
  // Cause a bad record length in the LAST block.
  ShrinkSize(1);
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ("", ReportMessage());
  ASSERT_EQ(0U, DroppedBytes());
}

TEST_P(LogTest, PartialLastIsNotIgnored) {
  Write(BigString("bar", kBlockSize));
  // Cause a bad record length in the LAST block.
  ShrinkSize(1);
  ASSERT_EQ("EOF", Read(WALRecoveryMode::kAbsoluteConsistency));
  ASSERT_GT(DroppedBytes(), 0U);
  ASSERT_EQ("OK", MatchError(
                      "Corruption: truncated headerCorruption: "
                      "error reading trailing data"));
}

TEST_P(LogTest, ErrorJoinsRecords) {
  // Consider two fragmented records:
  //    first(R1) last(R1) first(R2) last(R2)
  // where the middle two fragments disappear.  We do not want
  // first(R1),last(R2) to get joined and returned as a valid record.

  // Write records that span two blocks
  Write(BigString("foo", kBlockSize));
  Write(BigString("bar", kBlockSize));
  Write("correct");

  // Wipe the middle block
  for (unsigned int offset = kBlockSize; offset < 2*kBlockSize; offset++) {
    SetByte(offset, 'x');
  }

  if (!GetParam()) {
    ASSERT_EQ("correct", Read());
    ASSERT_EQ("EOF", Read());
    size_t dropped = DroppedBytes();
    ASSERT_LE(dropped, 2 * kBlockSize + 100);
    ASSERT_GE(dropped, 2 * kBlockSize);
  } else {
    ASSERT_EQ("EOF", Read());
  }
}

TEST_P(LogTest, ClearEofSingleBlock) {
  Write("foo");
  Write("bar");
  int header_size = GetParam() ? kRecyclableHeaderSize : kHeaderSize;
  ForceEOF(3 + header_size + 2);
  ASSERT_EQ("foo", Read());
  UnmarkEOF();
  ASSERT_EQ("bar", Read());
  ASSERT_TRUE(IsEOF());
  ASSERT_EQ("EOF", Read());
  Write("xxx");
  UnmarkEOF();
  ASSERT_EQ("xxx", Read());
  ASSERT_TRUE(IsEOF());
}

TEST_P(LogTest, ClearEofMultiBlock) {
  size_t num_full_blocks = 5;
  int header_size = GetParam() ? kRecyclableHeaderSize : kHeaderSize;
  size_t n = (kBlockSize - header_size) * num_full_blocks + 25;
  Write(BigString("foo", n));
  Write(BigString("bar", n));
  ForceEOF(n + num_full_blocks * header_size + header_size + 3);
  ASSERT_EQ(BigString("foo", n), Read());
  ASSERT_TRUE(IsEOF());
  UnmarkEOF();
  ASSERT_EQ(BigString("bar", n), Read());
  ASSERT_TRUE(IsEOF());
  Write(BigString("xxx", n));
  UnmarkEOF();
  ASSERT_EQ(BigString("xxx", n), Read());
  ASSERT_TRUE(IsEOF());
}

TEST_P(LogTest, ClearEofError) {
  // If an error occurs during Read() in UnmarkEOF(), the records contained
  // in the buffer should be returned on subsequent calls of ReadRecord()
  // until no more full records are left, whereafter ReadRecord() should return
  // false to indicate that it cannot read any further.

  Write("foo");
  Write("bar");
  UnmarkEOF();
  ASSERT_EQ("foo", Read());
  ASSERT_TRUE(IsEOF());
  Write("xxx");
  ForceError(0);
  UnmarkEOF();
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
}

TEST_P(LogTest, ClearEofError2) {
  Write("foo");
  Write("bar");
  UnmarkEOF();
  ASSERT_EQ("foo", Read());
  Write("xxx");
  ForceError(3);
  UnmarkEOF();
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
  ASSERT_EQ(3U, DroppedBytes());
  ASSERT_EQ("OK", MatchError("read error"));
}

TEST_P(LogTest, Recycle) {
  if (!GetParam()) {
    return;  // test is only valid for recycled logs
  }
  Write("foo");
  Write("bar");
  Write("baz");
  Write("bif");
  Write("blitz");
  while (get_reader_contents()->size() < log::kBlockSize * 2) {
    Write("xxxxxxxxxxxxxxxx");
  }
  std::unique_ptr<WritableFileWriter> dest_holder(test::GetWritableFileWriter(
      new test::OverwritingStringSink(get_reader_contents()),
      "" /* don't care */));
  Writer recycle_writer(std::move(dest_holder), 123, true);
  recycle_writer.AddRecord(Slice("foooo"));
  recycle_writer.AddRecord(Slice("bar"));
  ASSERT_GE(get_reader_contents()->size(), log::kBlockSize * 2);
  ASSERT_EQ("foooo", Read());
  ASSERT_EQ("bar", Read());
  ASSERT_EQ("EOF", Read());
}

INSTANTIATE_TEST_CASE_P(bool, LogTest, ::testing::Values(0, 2));

class RetriableLogTest : public ::testing::TestWithParam<int> {
 private:
  class ReportCollector : public Reader::Reporter {
   public:
    size_t dropped_bytes_;
    std::string message_;

    ReportCollector() : dropped_bytes_(0) {}
    void Corruption(size_t bytes, const Status& status) override {
      dropped_bytes_ += bytes;
      message_.append(status.ToString());
    }
  };

  Slice contents_;
  std::unique_ptr<WritableFileWriter> dest_holder_;
  std::unique_ptr<Writer> log_writer_;
  Env* env_;
  EnvOptions env_options_;
  const std::string test_dir_;
  const std::string log_file_;
  std::unique_ptr<WritableFileWriter> writer_;
  std::unique_ptr<SequentialFileReader> reader_;
  ReportCollector report_;
  std::unique_ptr<Reader> log_reader_;

 public:
  RetriableLogTest()
      : contents_(),
        dest_holder_(nullptr),
        log_writer_(nullptr),
        env_(Env::Default()),
        test_dir_(test::PerThreadDBPath("retriable_log_test")),
        log_file_(test_dir_ + "/log"),
        writer_(nullptr),
        reader_(nullptr),
        log_reader_(nullptr) {}

  Status SetupTestEnv() {
    dest_holder_.reset(test::GetWritableFileWriter(
        new test::StringSink(&contents_), "" /* file name */));
    assert(dest_holder_ != nullptr);
    log_writer_.reset(new Writer(std::move(dest_holder_), 123, GetParam()));
    assert(log_writer_ != nullptr);

    Status s;
    s = env_->CreateDirIfMissing(test_dir_);
    std::unique_ptr<WritableFile> writable_file;
    if (s.ok()) {
      s = env_->NewWritableFile(log_file_, &writable_file, env_options_);
    }
    if (s.ok()) {
      writer_.reset(new WritableFileWriter(std::move(writable_file), log_file_,
                                           env_options_));
      assert(writer_ != nullptr);
    }
    std::unique_ptr<SequentialFile> seq_file;
    if (s.ok()) {
      s = env_->NewSequentialFile(log_file_, &seq_file, env_options_);
    }
    if (s.ok()) {
      reader_.reset(new SequentialFileReader(std::move(seq_file), log_file_));
      assert(reader_ != nullptr);
      log_reader_.reset(new Reader(nullptr, std::move(reader_), &report_,
                                   true /* checksum */, 123 /* log_number */,
                                   true /* retry_after_eof */));
      assert(log_reader_ != nullptr);
    }
    return s;
  }

  std::string contents() {
    auto file =
        dynamic_cast<test::StringSink*>(log_writer_->file()->writable_file());
    assert(file != nullptr);
    return file->contents_;
  }

  void Encode(const std::string& msg) { log_writer_->AddRecord(Slice(msg)); }

  void Write(const Slice& data) {
    writer_->Append(data);
    writer_->Sync(true);
  }

  std::string Read() {
    auto wal_recovery_mode = WALRecoveryMode::kTolerateCorruptedTailRecords;
    std::string scratch;
    Slice record;
    if (log_reader_->ReadRecord(&record, &scratch, wal_recovery_mode)) {
      return record.ToString();
    } else {
      return "Read error";
    }
  }
};

TEST_P(RetriableLogTest, TailLog_PartialHeader) {
  ASSERT_OK(SetupTestEnv());
  std::vector<int> remaining_bytes_in_last_record;
  size_t header_size = GetParam() ? kRecyclableHeaderSize : kHeaderSize;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"RetriableLogTest::TailLog:AfterPart1",
        "RetriableLogTest::TailLog:BeforeReadRecord"},
       {"LogReader::ReadMore:FirstEOF",
        "RetriableLogTest::TailLog:BeforePart2"}});
  SyncPoint::GetInstance()->EnableProcessing();

  size_t delta = header_size - 1;
  port::Thread log_writer_thread([&]() {
    size_t old_sz = contents().size();
    Encode("foo");
    size_t new_sz = contents().size();
    std::string part1 = contents().substr(old_sz, delta);
    std::string part2 =
        contents().substr(old_sz + delta, new_sz - old_sz - delta);
    Write(Slice(part1));
    TEST_SYNC_POINT("RetriableLogTest::TailLog:AfterPart1");
    TEST_SYNC_POINT("RetriableLogTest::TailLog:BeforePart2");
    Write(Slice(part2));
  });

  std::string record;
  port::Thread log_reader_thread([&]() {
    TEST_SYNC_POINT("RetriableLogTest::TailLog:BeforeReadRecord");
    record = Read();
  });
  log_reader_thread.join();
  log_writer_thread.join();
  ASSERT_EQ("foo", record);
}

TEST_P(RetriableLogTest, TailLog_FullHeader) {
  ASSERT_OK(SetupTestEnv());
  std::vector<int> remaining_bytes_in_last_record;
  size_t header_size = GetParam() ? kRecyclableHeaderSize : kHeaderSize;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"RetriableLogTest::TailLog:AfterPart1",
        "RetriableLogTest::TailLog:BeforeReadRecord"},
       {"LogReader::ReadMore:FirstEOF",
        "RetriableLogTest::TailLog:BeforePart2"}});
  SyncPoint::GetInstance()->EnableProcessing();

  size_t delta = header_size + 1;
  port::Thread log_writer_thread([&]() {
    size_t old_sz = contents().size();
    Encode("foo");
    size_t new_sz = contents().size();
    std::string part1 = contents().substr(old_sz, delta);
    std::string part2 =
        contents().substr(old_sz + delta, new_sz - old_sz - delta);
    Write(Slice(part1));
    TEST_SYNC_POINT("RetriableLogTest::TailLog:AfterPart1");
    TEST_SYNC_POINT("RetriableLogTest::TailLog:BeforePart2");
    Write(Slice(part2));
  });

  std::string record;
  port::Thread log_reader_thread([&]() {
    TEST_SYNC_POINT("RetriableLogTest::TailLog:BeforeReadRecord");
    record = Read();
  });
  log_reader_thread.join();
  log_writer_thread.join();
  ASSERT_EQ("foo", record);
}

INSTANTIATE_TEST_CASE_P(bool, RetriableLogTest, ::testing::Values(0, 2));

}  // namespace log
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
