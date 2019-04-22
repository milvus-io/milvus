//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef ROCKSDB_LITE
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "db/transaction_log_impl.h"
#include <inttypes.h>
#include "db/write_batch_internal.h"
#include "util/file_reader_writer.h"

namespace rocksdb {

TransactionLogIteratorImpl::TransactionLogIteratorImpl(
    const std::string& dir, const ImmutableDBOptions* options,
    const TransactionLogIterator::ReadOptions& read_options,
    const EnvOptions& soptions, const SequenceNumber seq,
    std::unique_ptr<VectorLogPtr> files, VersionSet const* const versions,
    const bool seq_per_batch)
    : dir_(dir),
      options_(options),
      read_options_(read_options),
      soptions_(soptions),
      startingSequenceNumber_(seq),
      files_(std::move(files)),
      started_(false),
      isValid_(false),
      currentFileIndex_(0),
      currentBatchSeq_(0),
      currentLastSeq_(0),
      versions_(versions),
      seq_per_batch_(seq_per_batch) {
  assert(files_ != nullptr);
  assert(versions_ != nullptr);

  reporter_.env = options_->env;
  reporter_.info_log = options_->info_log.get();
  SeekToStartSequence(); // Seek till starting sequence
}

Status TransactionLogIteratorImpl::OpenLogFile(
    const LogFile* logFile,
    std::unique_ptr<SequentialFileReader>* file_reader) {
  Env* env = options_->env;
  std::unique_ptr<SequentialFile> file;
  std::string fname;
  Status s;
  EnvOptions optimized_env_options = env->OptimizeForLogRead(soptions_);
  if (logFile->Type() == kArchivedLogFile) {
    fname = ArchivedLogFileName(dir_, logFile->LogNumber());
    s = env->NewSequentialFile(fname, &file, optimized_env_options);
  } else {
    fname = LogFileName(dir_, logFile->LogNumber());
    s = env->NewSequentialFile(fname, &file, optimized_env_options);
    if (!s.ok()) {
      //  If cannot open file in DB directory.
      //  Try the archive dir, as it could have moved in the meanwhile.
      fname = ArchivedLogFileName(dir_, logFile->LogNumber());
      s = env->NewSequentialFile(fname, &file, optimized_env_options);
    }
  }
  if (s.ok()) {
    file_reader->reset(new SequentialFileReader(std::move(file), fname));
  }
  return s;
}

BatchResult TransactionLogIteratorImpl::GetBatch()  {
  assert(isValid_);  //  cannot call in a non valid state.
  BatchResult result;
  result.sequence = currentBatchSeq_;
  result.writeBatchPtr = std::move(currentBatch_);
  return result;
}

Status TransactionLogIteratorImpl::status() {
  return currentStatus_;
}

bool TransactionLogIteratorImpl::Valid() {
  return started_ && isValid_;
}

bool TransactionLogIteratorImpl::RestrictedRead(
    Slice* record,
    std::string* scratch) {
  // Don't read if no more complete entries to read from logs
  if (currentLastSeq_ >= versions_->LastSequence()) {
    return false;
  }
  return currentLogReader_->ReadRecord(record, scratch);
}

void TransactionLogIteratorImpl::SeekToStartSequence(
    uint64_t startFileIndex,
    bool strict) {
  std::string scratch;
  Slice record;
  started_ = false;
  isValid_ = false;
  if (files_->size() <= startFileIndex) {
    return;
  }
  Status s = OpenLogReader(files_->at(static_cast<size_t>(startFileIndex)).get());
  if (!s.ok()) {
    currentStatus_ = s;
    reporter_.Info(currentStatus_.ToString().c_str());
    return;
  }
  while (RestrictedRead(&record, &scratch)) {
    if (record.size() < WriteBatchInternal::kHeader) {
      reporter_.Corruption(
        record.size(), Status::Corruption("very small log record"));
      continue;
    }
    UpdateCurrentWriteBatch(record);
    if (currentLastSeq_ >= startingSequenceNumber_) {
      if (strict && currentBatchSeq_ != startingSequenceNumber_) {
        currentStatus_ = Status::Corruption("Gap in sequence number. Could not "
                                            "seek to required sequence number");
        reporter_.Info(currentStatus_.ToString().c_str());
        return;
      } else if (strict) {
        reporter_.Info("Could seek required sequence number. Iterator will "
                       "continue.");
      }
      isValid_ = true;
      started_ = true; // set started_ as we could seek till starting sequence
      return;
    } else {
      isValid_ = false;
    }
  }

  // Could not find start sequence in first file. Normally this must be the
  // only file. Otherwise log the error and let the iterator return next entry
  // If strict is set, we want to seek exactly till the start sequence and it
  // should have been present in the file we scanned above
  if (strict) {
    currentStatus_ = Status::Corruption("Gap in sequence number. Could not "
                                        "seek to required sequence number");
    reporter_.Info(currentStatus_.ToString().c_str());
  } else if (files_->size() != 1) {
    currentStatus_ = Status::Corruption("Start sequence was not found, "
                                        "skipping to the next available");
    reporter_.Info(currentStatus_.ToString().c_str());
    // Let NextImpl find the next available entry. started_ remains false
    // because we don't want to check for gaps while moving to start sequence
    NextImpl(true);
  }
}

void TransactionLogIteratorImpl::Next() {
  return NextImpl(false);
}

void TransactionLogIteratorImpl::NextImpl(bool internal) {
  std::string scratch;
  Slice record;
  isValid_ = false;
  if (!internal && !started_) {
    // Runs every time until we can seek to the start sequence
    return SeekToStartSequence();
  }
  while(true) {
    assert(currentLogReader_);
    if (currentLogReader_->IsEOF()) {
      currentLogReader_->UnmarkEOF();
    }
    while (RestrictedRead(&record, &scratch)) {
      if (record.size() < WriteBatchInternal::kHeader) {
        reporter_.Corruption(
          record.size(), Status::Corruption("very small log record"));
        continue;
      } else {
        // started_ should be true if called by application
        assert(internal || started_);
        // started_ should be false if called internally
        assert(!internal || !started_);
        UpdateCurrentWriteBatch(record);
        if (internal && !started_) {
          started_ = true;
        }
        return;
      }
    }

    // Open the next file
    if (currentFileIndex_ < files_->size() - 1) {
      ++currentFileIndex_;
      Status s = OpenLogReader(files_->at(currentFileIndex_).get());
      if (!s.ok()) {
        isValid_ = false;
        currentStatus_ = s;
        return;
      }
    } else {
      isValid_ = false;
      if (currentLastSeq_ == versions_->LastSequence()) {
        currentStatus_ = Status::OK();
      } else {
        currentStatus_ = Status::Corruption("NO MORE DATA LEFT");
      }
      return;
    }
  }
}

bool TransactionLogIteratorImpl::IsBatchExpected(
    const WriteBatch* batch,
    const SequenceNumber expectedSeq) {
  assert(batch);
  SequenceNumber batchSeq = WriteBatchInternal::Sequence(batch);
  if (batchSeq != expectedSeq) {
    char buf[200];
    snprintf(buf, sizeof(buf),
             "Discontinuity in log records. Got seq=%" PRIu64
             ", Expected seq=%" PRIu64 ", Last flushed seq=%" PRIu64
             ".Log iterator will reseek the correct batch.",
             batchSeq, expectedSeq, versions_->LastSequence());
    reporter_.Info(buf);
    return false;
  }
  return true;
}

void TransactionLogIteratorImpl::UpdateCurrentWriteBatch(const Slice& record) {
  std::unique_ptr<WriteBatch> batch(new WriteBatch());
  WriteBatchInternal::SetContents(batch.get(), record);

  SequenceNumber expectedSeq = currentLastSeq_ + 1;
  // If the iterator has started, then confirm that we get continuous batches
  if (started_ && !IsBatchExpected(batch.get(), expectedSeq)) {
    // Seek to the batch having expected sequence number
    if (expectedSeq < files_->at(currentFileIndex_)->StartSequence()) {
      // Expected batch must lie in the previous log file
      // Avoid underflow.
      if (currentFileIndex_ != 0) {
        currentFileIndex_--;
      }
    }
    startingSequenceNumber_ = expectedSeq;
    // currentStatus_ will be set to Ok if reseek succeeds
    // Note: this is still ok in seq_pre_batch_ && two_write_queuesp_ mode
    // that allows gaps in the WAL since it will still skip over the gap.
    currentStatus_ = Status::NotFound("Gap in sequence numbers");
    // In seq_per_batch_ mode, gaps in the seq are possible so the strict mode
    // should be disabled
    return SeekToStartSequence(currentFileIndex_, !seq_per_batch_);
  }

  struct BatchCounter : public WriteBatch::Handler {
    SequenceNumber sequence_;
    BatchCounter(SequenceNumber sequence) : sequence_(sequence) {}
    Status MarkNoop(bool empty_batch) override {
      if (!empty_batch) {
        sequence_++;
      }
      return Status::OK();
    }
    Status MarkEndPrepare(const Slice&) override {
      sequence_++;
      return Status::OK();
    }
    Status MarkCommit(const Slice&) override {
      sequence_++;
      return Status::OK();
    }

    Status PutCF(uint32_t /*cf*/, const Slice& /*key*/,
                 const Slice& /*val*/) override {
      return Status::OK();
    }
    Status DeleteCF(uint32_t /*cf*/, const Slice& /*key*/) override {
      return Status::OK();
    }
    Status SingleDeleteCF(uint32_t /*cf*/, const Slice& /*key*/) override {
      return Status::OK();
    }
    Status MergeCF(uint32_t /*cf*/, const Slice& /*key*/,
                   const Slice& /*val*/) override {
      return Status::OK();
    }
    Status MarkBeginPrepare(bool) override { return Status::OK(); }
    Status MarkRollback(const Slice&) override { return Status::OK(); }
  };

  currentBatchSeq_ = WriteBatchInternal::Sequence(batch.get());
  if (seq_per_batch_) {
    BatchCounter counter(currentBatchSeq_);
    batch->Iterate(&counter);
    currentLastSeq_ = counter.sequence_;
  } else {
    currentLastSeq_ =
        currentBatchSeq_ + WriteBatchInternal::Count(batch.get()) - 1;
  }
  // currentBatchSeq_ can only change here
  assert(currentLastSeq_ <= versions_->LastSequence());

  currentBatch_ = std::move(batch);
  isValid_ = true;
  currentStatus_ = Status::OK();
}

Status TransactionLogIteratorImpl::OpenLogReader(const LogFile* logFile) {
  std::unique_ptr<SequentialFileReader> file;
  Status s = OpenLogFile(logFile, &file);
  if (!s.ok()) {
    return s;
  }
  assert(file);
  currentLogReader_.reset(
      new log::Reader(options_->info_log, std::move(file), &reporter_,
                      read_options_.verify_checksums_, logFile->LogNumber(),
                      false /* retry_after_eof */));
  return Status::OK();
}
}  //  namespace rocksdb
#endif  // ROCKSDB_LITE
