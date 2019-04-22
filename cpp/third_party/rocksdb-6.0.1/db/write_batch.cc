//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring
//    kTypeDeletion varstring
//    kTypeSingleDeletion varstring
//    kTypeRangeDeletion varstring varstring
//    kTypeMerge varstring varstring
//    kTypeColumnFamilyValue varint32 varstring varstring
//    kTypeColumnFamilyDeletion varint32 varstring
//    kTypeColumnFamilySingleDeletion varint32 varstring
//    kTypeColumnFamilyRangeDeletion varint32 varstring varstring
//    kTypeColumnFamilyMerge varint32 varstring varstring
//    kTypeBeginPrepareXID varstring
//    kTypeEndPrepareXID
//    kTypeCommitXID varstring
//    kTypeRollbackXID varstring
//    kTypeBeginPersistedPrepareXID varstring
//    kTypeBeginUnprepareXID varstring
//    kTypeNoop
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "rocksdb/write_batch.h"

#include <map>
#include <stack>
#include <stdexcept>
#include <type_traits>
#include <vector>

#include "db/column_family.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/flush_scheduler.h"
#include "db/memtable.h"
#include "db/merge_context.h"
#include "db/snapshot_impl.h"
#include "db/write_batch_internal.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/statistics.h"
#include "rocksdb/merge_operator.h"
#include "util/coding.h"
#include "util/duplicate_detector.h"
#include "util/string_util.h"
#include "util/util.h"

namespace rocksdb {

// anon namespace for file-local types
namespace {

enum ContentFlags : uint32_t {
  DEFERRED = 1 << 0,
  HAS_PUT = 1 << 1,
  HAS_DELETE = 1 << 2,
  HAS_SINGLE_DELETE = 1 << 3,
  HAS_MERGE = 1 << 4,
  HAS_BEGIN_PREPARE = 1 << 5,
  HAS_END_PREPARE = 1 << 6,
  HAS_COMMIT = 1 << 7,
  HAS_ROLLBACK = 1 << 8,
  HAS_DELETE_RANGE = 1 << 9,
  HAS_BLOB_INDEX = 1 << 10,
  HAS_BEGIN_UNPREPARE = 1 << 11,
};

struct BatchContentClassifier : public WriteBatch::Handler {
  uint32_t content_flags = 0;

  Status PutCF(uint32_t, const Slice&, const Slice&) override {
    content_flags |= ContentFlags::HAS_PUT;
    return Status::OK();
  }

  Status DeleteCF(uint32_t, const Slice&) override {
    content_flags |= ContentFlags::HAS_DELETE;
    return Status::OK();
  }

  Status SingleDeleteCF(uint32_t, const Slice&) override {
    content_flags |= ContentFlags::HAS_SINGLE_DELETE;
    return Status::OK();
  }

  Status DeleteRangeCF(uint32_t, const Slice&, const Slice&) override {
    content_flags |= ContentFlags::HAS_DELETE_RANGE;
    return Status::OK();
  }

  Status MergeCF(uint32_t, const Slice&, const Slice&) override {
    content_flags |= ContentFlags::HAS_MERGE;
    return Status::OK();
  }

  Status PutBlobIndexCF(uint32_t, const Slice&, const Slice&) override {
    content_flags |= ContentFlags::HAS_BLOB_INDEX;
    return Status::OK();
  }

  Status MarkBeginPrepare(bool unprepare) override {
    content_flags |= ContentFlags::HAS_BEGIN_PREPARE;
    if (unprepare) {
      content_flags |= ContentFlags::HAS_BEGIN_UNPREPARE;
    }
    return Status::OK();
  }

  Status MarkEndPrepare(const Slice&) override {
    content_flags |= ContentFlags::HAS_END_PREPARE;
    return Status::OK();
  }

  Status MarkCommit(const Slice&) override {
    content_flags |= ContentFlags::HAS_COMMIT;
    return Status::OK();
  }

  Status MarkRollback(const Slice&) override {
    content_flags |= ContentFlags::HAS_ROLLBACK;
    return Status::OK();
  }
};

}  // anon namespace

struct SavePoints {
  std::stack<SavePoint> stack;
};

WriteBatch::WriteBatch(size_t reserved_bytes, size_t max_bytes)
    : save_points_(nullptr), content_flags_(0), max_bytes_(max_bytes), rep_() {
  rep_.reserve((reserved_bytes > WriteBatchInternal::kHeader) ?
    reserved_bytes : WriteBatchInternal::kHeader);
  rep_.resize(WriteBatchInternal::kHeader);
}

WriteBatch::WriteBatch(const std::string& rep)
    : save_points_(nullptr),
      content_flags_(ContentFlags::DEFERRED),
      max_bytes_(0),
      rep_(rep) {}

WriteBatch::WriteBatch(std::string&& rep)
    : save_points_(nullptr),
      content_flags_(ContentFlags::DEFERRED),
      max_bytes_(0),
      rep_(std::move(rep)) {}

WriteBatch::WriteBatch(const WriteBatch& src)
    : save_points_(src.save_points_),
      wal_term_point_(src.wal_term_point_),
      content_flags_(src.content_flags_.load(std::memory_order_relaxed)),
      max_bytes_(src.max_bytes_),
      rep_(src.rep_) {}

WriteBatch::WriteBatch(WriteBatch&& src) noexcept
    : save_points_(std::move(src.save_points_)),
      wal_term_point_(std::move(src.wal_term_point_)),
      content_flags_(src.content_flags_.load(std::memory_order_relaxed)),
      max_bytes_(src.max_bytes_),
      rep_(std::move(src.rep_)) {}

WriteBatch& WriteBatch::operator=(const WriteBatch& src) {
  if (&src != this) {
    this->~WriteBatch();
    new (this) WriteBatch(src);
  }
  return *this;
}

WriteBatch& WriteBatch::operator=(WriteBatch&& src) {
  if (&src != this) {
    this->~WriteBatch();
    new (this) WriteBatch(std::move(src));
  }
  return *this;
}

WriteBatch::~WriteBatch() { delete save_points_; }

WriteBatch::Handler::~Handler() { }

void WriteBatch::Handler::LogData(const Slice& /*blob*/) {
  // If the user has not specified something to do with blobs, then we ignore
  // them.
}

bool WriteBatch::Handler::Continue() {
  return true;
}

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(WriteBatchInternal::kHeader);

  content_flags_.store(0, std::memory_order_relaxed);

  if (save_points_ != nullptr) {
    while (!save_points_->stack.empty()) {
      save_points_->stack.pop();
    }
  }

  wal_term_point_.clear();
}

int WriteBatch::Count() const {
  return WriteBatchInternal::Count(this);
}

uint32_t WriteBatch::ComputeContentFlags() const {
  auto rv = content_flags_.load(std::memory_order_relaxed);
  if ((rv & ContentFlags::DEFERRED) != 0) {
    BatchContentClassifier classifier;
    Iterate(&classifier);
    rv = classifier.content_flags;

    // this method is conceptually const, because it is performing a lazy
    // computation that doesn't affect the abstract state of the batch.
    // content_flags_ is marked mutable so that we can perform the
    // following assignment
    content_flags_.store(rv, std::memory_order_relaxed);
  }
  return rv;
}

void WriteBatch::MarkWalTerminationPoint() {
  wal_term_point_.size = GetDataSize();
  wal_term_point_.count = Count();
  wal_term_point_.content_flags = content_flags_;
}

bool WriteBatch::HasPut() const {
  return (ComputeContentFlags() & ContentFlags::HAS_PUT) != 0;
}

bool WriteBatch::HasDelete() const {
  return (ComputeContentFlags() & ContentFlags::HAS_DELETE) != 0;
}

bool WriteBatch::HasSingleDelete() const {
  return (ComputeContentFlags() & ContentFlags::HAS_SINGLE_DELETE) != 0;
}

bool WriteBatch::HasDeleteRange() const {
  return (ComputeContentFlags() & ContentFlags::HAS_DELETE_RANGE) != 0;
}

bool WriteBatch::HasMerge() const {
  return (ComputeContentFlags() & ContentFlags::HAS_MERGE) != 0;
}

bool ReadKeyFromWriteBatchEntry(Slice* input, Slice* key, bool cf_record) {
  assert(input != nullptr && key != nullptr);
  // Skip tag byte
  input->remove_prefix(1);

  if (cf_record) {
    // Skip column_family bytes
    uint32_t cf;
    if (!GetVarint32(input, &cf)) {
      return false;
    }
  }

  // Extract key
  return GetLengthPrefixedSlice(input, key);
}

bool WriteBatch::HasBeginPrepare() const {
  return (ComputeContentFlags() & ContentFlags::HAS_BEGIN_PREPARE) != 0;
}

bool WriteBatch::HasEndPrepare() const {
  return (ComputeContentFlags() & ContentFlags::HAS_END_PREPARE) != 0;
}

bool WriteBatch::HasCommit() const {
  return (ComputeContentFlags() & ContentFlags::HAS_COMMIT) != 0;
}

bool WriteBatch::HasRollback() const {
  return (ComputeContentFlags() & ContentFlags::HAS_ROLLBACK) != 0;
}

Status ReadRecordFromWriteBatch(Slice* input, char* tag,
                                uint32_t* column_family, Slice* key,
                                Slice* value, Slice* blob, Slice* xid) {
  assert(key != nullptr && value != nullptr);
  *tag = (*input)[0];
  input->remove_prefix(1);
  *column_family = 0;  // default
  switch (*tag) {
    case kTypeColumnFamilyValue:
      if (!GetVarint32(input, column_family)) {
        return Status::Corruption("bad WriteBatch Put");
      }
      FALLTHROUGH_INTENDED;
    case kTypeValue:
      if (!GetLengthPrefixedSlice(input, key) ||
          !GetLengthPrefixedSlice(input, value)) {
        return Status::Corruption("bad WriteBatch Put");
      }
      break;
    case kTypeColumnFamilyDeletion:
    case kTypeColumnFamilySingleDeletion:
      if (!GetVarint32(input, column_family)) {
        return Status::Corruption("bad WriteBatch Delete");
      }
      FALLTHROUGH_INTENDED;
    case kTypeDeletion:
    case kTypeSingleDeletion:
      if (!GetLengthPrefixedSlice(input, key)) {
        return Status::Corruption("bad WriteBatch Delete");
      }
      break;
    case kTypeColumnFamilyRangeDeletion:
      if (!GetVarint32(input, column_family)) {
        return Status::Corruption("bad WriteBatch DeleteRange");
      }
      FALLTHROUGH_INTENDED;
    case kTypeRangeDeletion:
      // for range delete, "key" is begin_key, "value" is end_key
      if (!GetLengthPrefixedSlice(input, key) ||
          !GetLengthPrefixedSlice(input, value)) {
        return Status::Corruption("bad WriteBatch DeleteRange");
      }
      break;
    case kTypeColumnFamilyMerge:
      if (!GetVarint32(input, column_family)) {
        return Status::Corruption("bad WriteBatch Merge");
      }
      FALLTHROUGH_INTENDED;
    case kTypeMerge:
      if (!GetLengthPrefixedSlice(input, key) ||
          !GetLengthPrefixedSlice(input, value)) {
        return Status::Corruption("bad WriteBatch Merge");
      }
      break;
    case kTypeColumnFamilyBlobIndex:
      if (!GetVarint32(input, column_family)) {
        return Status::Corruption("bad WriteBatch BlobIndex");
      }
      FALLTHROUGH_INTENDED;
    case kTypeBlobIndex:
      if (!GetLengthPrefixedSlice(input, key) ||
          !GetLengthPrefixedSlice(input, value)) {
        return Status::Corruption("bad WriteBatch BlobIndex");
      }
      break;
    case kTypeLogData:
      assert(blob != nullptr);
      if (!GetLengthPrefixedSlice(input, blob)) {
        return Status::Corruption("bad WriteBatch Blob");
      }
      break;
    case kTypeNoop:
    case kTypeBeginPrepareXID:
      // This indicates that the prepared batch is also persisted in the db.
      // This is used in WritePreparedTxn
    case kTypeBeginPersistedPrepareXID:
      // This is used in WriteUnpreparedTxn
    case kTypeBeginUnprepareXID:
      break;
    case kTypeEndPrepareXID:
      if (!GetLengthPrefixedSlice(input, xid)) {
        return Status::Corruption("bad EndPrepare XID");
      }
      break;
    case kTypeCommitXID:
      if (!GetLengthPrefixedSlice(input, xid)) {
        return Status::Corruption("bad Commit XID");
      }
      break;
    case kTypeRollbackXID:
      if (!GetLengthPrefixedSlice(input, xid)) {
        return Status::Corruption("bad Rollback XID");
      }
      break;
    default:
      return Status::Corruption("unknown WriteBatch tag");
  }
  return Status::OK();
}

Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < WriteBatchInternal::kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(WriteBatchInternal::kHeader);
  Slice key, value, blob, xid;
  // Sometimes a sub-batch starts with a Noop. We want to exclude such Noops as
  // the batch boundary symbols otherwise we would mis-count the number of
  // batches. We do that by checking whether the accumulated batch is empty
  // before seeing the next Noop.
  bool empty_batch = true;
  int found = 0;
  Status s;
  char tag = 0;
  uint32_t column_family = 0;  // default
  bool last_was_try_again = false;
  bool handler_continue = true;
  while (((s.ok() && !input.empty()) || UNLIKELY(s.IsTryAgain()))) {
    handler_continue = handler->Continue();
    if (!handler_continue) {
      break;
    }

    if (LIKELY(!s.IsTryAgain())) {
      last_was_try_again = false;
      tag = 0;
      column_family = 0;  // default

      s = ReadRecordFromWriteBatch(&input, &tag, &column_family, &key, &value,
                                   &blob, &xid);
      if (!s.ok()) {
        return s;
      }
    } else {
      assert(s.IsTryAgain());
      assert(!last_was_try_again); // to detect infinite loop bugs
      if (UNLIKELY(last_was_try_again)) {
        return Status::Corruption(
            "two consecutive TryAgain in WriteBatch handler; this is either a "
            "software bug or data corruption.");
      }
      last_was_try_again = true;
      s = Status::OK();
    }

    switch (tag) {
      case kTypeColumnFamilyValue:
      case kTypeValue:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_PUT));
        s = handler->PutCF(column_family, key, value);
        if (LIKELY(s.ok())) {
          empty_batch = false;
          found++;
        }
        break;
      case kTypeColumnFamilyDeletion:
      case kTypeDeletion:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_DELETE));
        s = handler->DeleteCF(column_family, key);
        if (LIKELY(s.ok())) {
          empty_batch = false;
          found++;
        }
        break;
      case kTypeColumnFamilySingleDeletion:
      case kTypeSingleDeletion:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_SINGLE_DELETE));
        s = handler->SingleDeleteCF(column_family, key);
        if (LIKELY(s.ok())) {
          empty_batch = false;
          found++;
        }
        break;
      case kTypeColumnFamilyRangeDeletion:
      case kTypeRangeDeletion:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_DELETE_RANGE));
        s = handler->DeleteRangeCF(column_family, key, value);
        if (LIKELY(s.ok())) {
          empty_batch = false;
          found++;
        }
        break;
      case kTypeColumnFamilyMerge:
      case kTypeMerge:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_MERGE));
        s = handler->MergeCF(column_family, key, value);
        if (LIKELY(s.ok())) {
          empty_batch = false;
          found++;
        }
        break;
      case kTypeColumnFamilyBlobIndex:
      case kTypeBlobIndex:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_BLOB_INDEX));
        s = handler->PutBlobIndexCF(column_family, key, value);
        if (LIKELY(s.ok())) {
          found++;
        }
        break;
      case kTypeLogData:
        handler->LogData(blob);
        // A batch might have nothing but LogData. It is still a batch.
        empty_batch = false;
        break;
      case kTypeBeginPrepareXID:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_BEGIN_PREPARE));
        handler->MarkBeginPrepare();
        empty_batch = false;
        if (!handler->WriteAfterCommit()) {
          s = Status::NotSupported(
              "WriteCommitted txn tag when write_after_commit_ is disabled (in "
              "WritePrepared/WriteUnprepared mode). If it is not due to "
              "corruption, the WAL must be emptied before changing the "
              "WritePolicy.");
        }
        if (handler->WriteBeforePrepare()) {
          s = Status::NotSupported(
              "WriteCommitted txn tag when write_before_prepare_ is enabled "
              "(in WriteUnprepared mode). If it is not due to corruption, the "
              "WAL must be emptied before changing the WritePolicy.");
        }
        break;
      case kTypeBeginPersistedPrepareXID:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_BEGIN_PREPARE));
        handler->MarkBeginPrepare();
        empty_batch = false;
        if (handler->WriteAfterCommit()) {
          s = Status::NotSupported(
              "WritePrepared/WriteUnprepared txn tag when write_after_commit_ "
              "is enabled (in default WriteCommitted mode). If it is not due "
              "to corruption, the WAL must be emptied before changing the "
              "WritePolicy.");
        }
        break;
      case kTypeBeginUnprepareXID:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_BEGIN_UNPREPARE));
        handler->MarkBeginPrepare(true /* unprepared */);
        empty_batch = false;
        if (handler->WriteAfterCommit()) {
          s = Status::NotSupported(
              "WriteUnprepared txn tag when write_after_commit_ is enabled (in "
              "default WriteCommitted mode). If it is not due to corruption, "
              "the WAL must be emptied before changing the WritePolicy.");
        }
        if (!handler->WriteBeforePrepare()) {
          s = Status::NotSupported(
              "WriteUnprepared txn tag when write_before_prepare_ is disabled "
              "(in WriteCommitted/WritePrepared mode). If it is not due to "
              "corruption, the WAL must be emptied before changing the "
              "WritePolicy.");
        }
        break;
      case kTypeEndPrepareXID:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_END_PREPARE));
        handler->MarkEndPrepare(xid);
        empty_batch = true;
        break;
      case kTypeCommitXID:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_COMMIT));
        handler->MarkCommit(xid);
        empty_batch = true;
        break;
      case kTypeRollbackXID:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_ROLLBACK));
        handler->MarkRollback(xid);
        empty_batch = true;
        break;
      case kTypeNoop:
        handler->MarkNoop(empty_batch);
        empty_batch = true;
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (!s.ok()) {
    return s;
  }
  if (handler_continue && found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

bool WriteBatchInternal::IsLatestPersistentState(const WriteBatch* b) {
  return b->is_latest_persistent_state_;
}

void WriteBatchInternal::SetAsLastestPersistentState(WriteBatch* b) {
  b->is_latest_persistent_state_ = true;
}

int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

size_t WriteBatchInternal::GetFirstOffset(WriteBatch* /*b*/) {
  return WriteBatchInternal::kHeader;
}

Status WriteBatchInternal::Put(WriteBatch* b, uint32_t column_family_id,
                               const Slice& key, const Slice& value) {
  if (key.size() > size_t{port::kMaxUint32}) {
    return Status::InvalidArgument("key is too large");
  }
  if (value.size() > size_t{port::kMaxUint32}) {
    return Status::InvalidArgument("value is too large");
  }

  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeValue));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyValue));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, key);
  PutLengthPrefixedSlice(&b->rep_, value);
  b->content_flags_.store(
      b->content_flags_.load(std::memory_order_relaxed) | ContentFlags::HAS_PUT,
      std::memory_order_relaxed);
  return save.commit();
}

Status WriteBatch::Put(ColumnFamilyHandle* column_family, const Slice& key,
                       const Slice& value) {
  return WriteBatchInternal::Put(this, GetColumnFamilyID(column_family), key,
                                 value);
}

Status WriteBatchInternal::CheckSlicePartsLength(const SliceParts& key,
                                                 const SliceParts& value) {
  size_t total_key_bytes = 0;
  for (int i = 0; i < key.num_parts; ++i) {
    total_key_bytes += key.parts[i].size();
  }
  if (total_key_bytes >= size_t{port::kMaxUint32}) {
    return Status::InvalidArgument("key is too large");
  }

  size_t total_value_bytes = 0;
  for (int i = 0; i < value.num_parts; ++i) {
    total_value_bytes += value.parts[i].size();
  }
  if (total_value_bytes >= size_t{port::kMaxUint32}) {
    return Status::InvalidArgument("value is too large");
  }
  return Status::OK();
}

Status WriteBatchInternal::Put(WriteBatch* b, uint32_t column_family_id,
                               const SliceParts& key, const SliceParts& value) {
  Status s = CheckSlicePartsLength(key, value);
  if (!s.ok()) {
    return s;
  }

  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeValue));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyValue));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSliceParts(&b->rep_, key);
  PutLengthPrefixedSliceParts(&b->rep_, value);
  b->content_flags_.store(
      b->content_flags_.load(std::memory_order_relaxed) | ContentFlags::HAS_PUT,
      std::memory_order_relaxed);
  return save.commit();
}

Status WriteBatch::Put(ColumnFamilyHandle* column_family, const SliceParts& key,
                       const SliceParts& value) {
  return WriteBatchInternal::Put(this, GetColumnFamilyID(column_family), key,
                                 value);
}

Status WriteBatchInternal::InsertNoop(WriteBatch* b) {
  b->rep_.push_back(static_cast<char>(kTypeNoop));
  return Status::OK();
}

Status WriteBatchInternal::MarkEndPrepare(WriteBatch* b, const Slice& xid,
                                          bool write_after_commit,
                                          bool unprepared_batch) {
  // a manually constructed batch can only contain one prepare section
  assert(b->rep_[12] == static_cast<char>(kTypeNoop));

  // all savepoints up to this point are cleared
  if (b->save_points_ != nullptr) {
    while (!b->save_points_->stack.empty()) {
      b->save_points_->stack.pop();
    }
  }

  // rewrite noop as begin marker
  b->rep_[12] = static_cast<char>(
      write_after_commit ? kTypeBeginPrepareXID
                         : (unprepared_batch ? kTypeBeginUnprepareXID
                                             : kTypeBeginPersistedPrepareXID));
  b->rep_.push_back(static_cast<char>(kTypeEndPrepareXID));
  PutLengthPrefixedSlice(&b->rep_, xid);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_END_PREPARE |
                              ContentFlags::HAS_BEGIN_PREPARE,
                          std::memory_order_relaxed);
  if (unprepared_batch) {
    b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                                ContentFlags::HAS_BEGIN_UNPREPARE,
                            std::memory_order_relaxed);
  }
  return Status::OK();
}

Status WriteBatchInternal::MarkCommit(WriteBatch* b, const Slice& xid) {
  b->rep_.push_back(static_cast<char>(kTypeCommitXID));
  PutLengthPrefixedSlice(&b->rep_, xid);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_COMMIT,
                          std::memory_order_relaxed);
  return Status::OK();
}

Status WriteBatchInternal::MarkRollback(WriteBatch* b, const Slice& xid) {
  b->rep_.push_back(static_cast<char>(kTypeRollbackXID));
  PutLengthPrefixedSlice(&b->rep_, xid);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_ROLLBACK,
                          std::memory_order_relaxed);
  return Status::OK();
}

Status WriteBatchInternal::Delete(WriteBatch* b, uint32_t column_family_id,
                                  const Slice& key) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_DELETE,
                          std::memory_order_relaxed);
  return save.commit();
}

Status WriteBatch::Delete(ColumnFamilyHandle* column_family, const Slice& key) {
  return WriteBatchInternal::Delete(this, GetColumnFamilyID(column_family),
                                    key);
}

Status WriteBatchInternal::Delete(WriteBatch* b, uint32_t column_family_id,
                                  const SliceParts& key) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSliceParts(&b->rep_, key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_DELETE,
                          std::memory_order_relaxed);
  return save.commit();
}

Status WriteBatch::Delete(ColumnFamilyHandle* column_family,
                          const SliceParts& key) {
  return WriteBatchInternal::Delete(this, GetColumnFamilyID(column_family),
                                    key);
}

Status WriteBatchInternal::SingleDelete(WriteBatch* b,
                                        uint32_t column_family_id,
                                        const Slice& key) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeSingleDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilySingleDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_SINGLE_DELETE,
                          std::memory_order_relaxed);
  return save.commit();
}

Status WriteBatch::SingleDelete(ColumnFamilyHandle* column_family,
                                const Slice& key) {
  return WriteBatchInternal::SingleDelete(
      this, GetColumnFamilyID(column_family), key);
}

Status WriteBatchInternal::SingleDelete(WriteBatch* b,
                                        uint32_t column_family_id,
                                        const SliceParts& key) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeSingleDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilySingleDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSliceParts(&b->rep_, key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_SINGLE_DELETE,
                          std::memory_order_relaxed);
  return save.commit();
}

Status WriteBatch::SingleDelete(ColumnFamilyHandle* column_family,
                                const SliceParts& key) {
  return WriteBatchInternal::SingleDelete(
      this, GetColumnFamilyID(column_family), key);
}

Status WriteBatchInternal::DeleteRange(WriteBatch* b, uint32_t column_family_id,
                                       const Slice& begin_key,
                                       const Slice& end_key) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeRangeDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyRangeDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, begin_key);
  PutLengthPrefixedSlice(&b->rep_, end_key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_DELETE_RANGE,
                          std::memory_order_relaxed);
  return save.commit();
}

Status WriteBatch::DeleteRange(ColumnFamilyHandle* column_family,
                               const Slice& begin_key, const Slice& end_key) {
  return WriteBatchInternal::DeleteRange(this, GetColumnFamilyID(column_family),
                                         begin_key, end_key);
}

Status WriteBatchInternal::DeleteRange(WriteBatch* b, uint32_t column_family_id,
                                       const SliceParts& begin_key,
                                       const SliceParts& end_key) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeRangeDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyRangeDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSliceParts(&b->rep_, begin_key);
  PutLengthPrefixedSliceParts(&b->rep_, end_key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_DELETE_RANGE,
                          std::memory_order_relaxed);
  return save.commit();
}

Status WriteBatch::DeleteRange(ColumnFamilyHandle* column_family,
                               const SliceParts& begin_key,
                               const SliceParts& end_key) {
  return WriteBatchInternal::DeleteRange(this, GetColumnFamilyID(column_family),
                                         begin_key, end_key);
}

Status WriteBatchInternal::Merge(WriteBatch* b, uint32_t column_family_id,
                                 const Slice& key, const Slice& value) {
  if (key.size() > size_t{port::kMaxUint32}) {
    return Status::InvalidArgument("key is too large");
  }
  if (value.size() > size_t{port::kMaxUint32}) {
    return Status::InvalidArgument("value is too large");
  }

  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeMerge));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyMerge));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, key);
  PutLengthPrefixedSlice(&b->rep_, value);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_MERGE,
                          std::memory_order_relaxed);
  return save.commit();
}

Status WriteBatch::Merge(ColumnFamilyHandle* column_family, const Slice& key,
                         const Slice& value) {
  return WriteBatchInternal::Merge(this, GetColumnFamilyID(column_family), key,
                                   value);
}

Status WriteBatchInternal::Merge(WriteBatch* b, uint32_t column_family_id,
                                 const SliceParts& key,
                                 const SliceParts& value) {
  Status s = CheckSlicePartsLength(key, value);
  if (!s.ok()) {
    return s;
  }

  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeMerge));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyMerge));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSliceParts(&b->rep_, key);
  PutLengthPrefixedSliceParts(&b->rep_, value);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_MERGE,
                          std::memory_order_relaxed);
  return save.commit();
}

Status WriteBatch::Merge(ColumnFamilyHandle* column_family,
                         const SliceParts& key, const SliceParts& value) {
  return WriteBatchInternal::Merge(this, GetColumnFamilyID(column_family), key,
                                   value);
}

Status WriteBatchInternal::PutBlobIndex(WriteBatch* b,
                                        uint32_t column_family_id,
                                        const Slice& key, const Slice& value) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeBlobIndex));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyBlobIndex));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, key);
  PutLengthPrefixedSlice(&b->rep_, value);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_BLOB_INDEX,
                          std::memory_order_relaxed);
  return save.commit();
}

Status WriteBatch::PutLogData(const Slice& blob) {
  LocalSavePoint save(this);
  rep_.push_back(static_cast<char>(kTypeLogData));
  PutLengthPrefixedSlice(&rep_, blob);
  return save.commit();
}

void WriteBatch::SetSavePoint() {
  if (save_points_ == nullptr) {
    save_points_ = new SavePoints();
  }
  // Record length and count of current batch of writes.
  save_points_->stack.push(SavePoint(
      GetDataSize(), Count(), content_flags_.load(std::memory_order_relaxed)));
}

Status WriteBatch::RollbackToSavePoint() {
  if (save_points_ == nullptr || save_points_->stack.size() == 0) {
    return Status::NotFound();
  }

  // Pop the most recent savepoint off the stack
  SavePoint savepoint = save_points_->stack.top();
  save_points_->stack.pop();

  assert(savepoint.size <= rep_.size());
  assert(savepoint.count <= Count());

  if (savepoint.size == rep_.size()) {
    // No changes to rollback
  } else if (savepoint.size == 0) {
    // Rollback everything
    Clear();
  } else {
    rep_.resize(savepoint.size);
    WriteBatchInternal::SetCount(this, savepoint.count);
    content_flags_.store(savepoint.content_flags, std::memory_order_relaxed);
  }

  return Status::OK();
}

Status WriteBatch::PopSavePoint() {
  if (save_points_ == nullptr || save_points_->stack.size() == 0) {
    return Status::NotFound();
  }

  // Pop the most recent savepoint off the stack
  save_points_->stack.pop();

  return Status::OK();
}

class MemTableInserter : public WriteBatch::Handler {

  SequenceNumber sequence_;
  ColumnFamilyMemTables* const cf_mems_;
  FlushScheduler* const flush_scheduler_;
  const bool ignore_missing_column_families_;
  const uint64_t recovering_log_number_;
  // log number that all Memtables inserted into should reference
  uint64_t log_number_ref_;
  DBImpl* db_;
  const bool concurrent_memtable_writes_;
  bool       post_info_created_;

  bool* has_valid_writes_;
  // On some (!) platforms just default creating
  // a map is too expensive in the Write() path as they
  // cause memory allocations though unused.
  // Make creation optional but do not incur
  // std::unique_ptr additional allocation
  using MemPostInfoMap = std::map<MemTable*, MemTablePostProcessInfo>;
  using PostMapType = std::aligned_storage<sizeof(MemPostInfoMap)>::type;
  PostMapType mem_post_info_map_;
  // current recovered transaction we are rebuilding (recovery)
  WriteBatch* rebuilding_trx_;
  SequenceNumber rebuilding_trx_seq_;
  // Increase seq number once per each write batch. Otherwise increase it once
  // per key.
  bool seq_per_batch_;
  // Whether the memtable write will be done only after the commit
  bool write_after_commit_;
  // Whether memtable write can be done before prepare
  bool write_before_prepare_;
  // Whether this batch was unprepared or not
  bool unprepared_batch_;
  using DupDetector = std::aligned_storage<sizeof(DuplicateDetector)>::type;
  DupDetector       duplicate_detector_;
  bool              dup_dectector_on_;

  MemPostInfoMap& GetPostMap() {
    assert(concurrent_memtable_writes_);
    if(!post_info_created_) {
      new (&mem_post_info_map_) MemPostInfoMap();
      post_info_created_ = true;
    }
    return *reinterpret_cast<MemPostInfoMap*>(&mem_post_info_map_);
  }

  bool IsDuplicateKeySeq(uint32_t column_family_id, const Slice& key) {
    assert(!write_after_commit_);
    assert(rebuilding_trx_ != nullptr);
    if (!dup_dectector_on_) {
      new (&duplicate_detector_) DuplicateDetector(db_);
      dup_dectector_on_ = true;
    }
    return reinterpret_cast<DuplicateDetector*>
      (&duplicate_detector_)->IsDuplicateKeySeq(column_family_id, key, sequence_);
  }

 protected:
  bool WriteBeforePrepare() const override { return write_before_prepare_; }
  bool WriteAfterCommit() const override { return write_after_commit_; }

 public:
  // cf_mems should not be shared with concurrent inserters
  MemTableInserter(SequenceNumber _sequence, ColumnFamilyMemTables* cf_mems,
                   FlushScheduler* flush_scheduler,
                   bool ignore_missing_column_families,
                   uint64_t recovering_log_number, DB* db,
                   bool concurrent_memtable_writes,
                   bool* has_valid_writes = nullptr, bool seq_per_batch = false,
                   bool batch_per_txn = true)
      : sequence_(_sequence),
        cf_mems_(cf_mems),
        flush_scheduler_(flush_scheduler),
        ignore_missing_column_families_(ignore_missing_column_families),
        recovering_log_number_(recovering_log_number),
        log_number_ref_(0),
        db_(reinterpret_cast<DBImpl*>(db)),
        concurrent_memtable_writes_(concurrent_memtable_writes),
        post_info_created_(false),
        has_valid_writes_(has_valid_writes),
        rebuilding_trx_(nullptr),
        rebuilding_trx_seq_(0),
        seq_per_batch_(seq_per_batch),
        // Write after commit currently uses one seq per key (instead of per
        // batch). So seq_per_batch being false indicates write_after_commit
        // approach.
        write_after_commit_(!seq_per_batch),
        // WriteUnprepared can write WriteBatches per transaction, so
        // batch_per_txn being false indicates write_before_prepare.
        write_before_prepare_(!batch_per_txn),
        unprepared_batch_(false),
        duplicate_detector_(),
        dup_dectector_on_(false) {
    assert(cf_mems_);
  }

  ~MemTableInserter() override {
    if (dup_dectector_on_) {
      reinterpret_cast<DuplicateDetector*>
        (&duplicate_detector_)->~DuplicateDetector();
    }
    if (post_info_created_) {
      reinterpret_cast<MemPostInfoMap*>
        (&mem_post_info_map_)->~MemPostInfoMap();
    }
    delete rebuilding_trx_;
  }

  MemTableInserter(const MemTableInserter&) = delete;
  MemTableInserter& operator=(const MemTableInserter&) = delete;

  // The batch seq is regularly restarted; In normal mode it is set when
  // MemTableInserter is constructed in the write thread and in recovery mode it
  // is set when a batch, which is tagged with seq, is read from the WAL.
  // Within a sequenced batch, which could be a merge of multiple batches, we
  // have two policies to advance the seq: i) seq_per_key (default) and ii)
  // seq_per_batch. To implement the latter we need to mark the boundary between
  // the individual batches. The approach is this: 1) Use the terminating
  // markers to indicate the boundary (kTypeEndPrepareXID, kTypeCommitXID,
  // kTypeRollbackXID) 2) Terminate a batch with kTypeNoop in the absence of a
  // natural boundary marker.
  void MaybeAdvanceSeq(bool batch_boundry = false) {
    if (batch_boundry == seq_per_batch_) {
      sequence_++;
    }
  }

  void set_log_number_ref(uint64_t log) { log_number_ref_ = log; }

  SequenceNumber sequence() const { return sequence_; }

  void PostProcess() {
    assert(concurrent_memtable_writes_);
    // If post info was not created there is nothing
    // to process and no need to create on demand
    if(post_info_created_) {
      for (auto& pair : GetPostMap()) {
        pair.first->BatchPostProcess(pair.second);
      }
    }
  }

  bool SeekToColumnFamily(uint32_t column_family_id, Status* s) {
    // If we are in a concurrent mode, it is the caller's responsibility
    // to clone the original ColumnFamilyMemTables so that each thread
    // has its own instance.  Otherwise, it must be guaranteed that there
    // is no concurrent access
    bool found = cf_mems_->Seek(column_family_id);
    if (!found) {
      if (ignore_missing_column_families_) {
        *s = Status::OK();
      } else {
        *s = Status::InvalidArgument(
            "Invalid column family specified in write batch");
      }
      return false;
    }
    if (recovering_log_number_ != 0 &&
        recovering_log_number_ < cf_mems_->GetLogNumber()) {
      // This is true only in recovery environment (recovering_log_number_ is
      // always 0 in
      // non-recovery, regular write code-path)
      // * If recovering_log_number_ < cf_mems_->GetLogNumber(), this means that
      // column
      // family already contains updates from this log. We can't apply updates
      // twice because of update-in-place or merge workloads -- ignore the
      // update
      *s = Status::OK();
      return false;
    }

    if (has_valid_writes_ != nullptr) {
      *has_valid_writes_ = true;
    }

    if (log_number_ref_ > 0) {
      cf_mems_->GetMemTable()->RefLogContainingPrepSection(log_number_ref_);
    }

    return true;
  }

  Status PutCFImpl(uint32_t column_family_id, const Slice& key,
                   const Slice& value, ValueType value_type) {
    // optimize for non-recovery mode
    if (UNLIKELY(write_after_commit_ && rebuilding_trx_ != nullptr)) {
      WriteBatchInternal::Put(rebuilding_trx_, column_family_id, key, value);
      return Status::OK();
      // else insert the values to the memtable right away
    }

    Status seek_status;
    if (UNLIKELY(!SeekToColumnFamily(column_family_id, &seek_status))) {
      bool batch_boundry = false;
      if (rebuilding_trx_ != nullptr) {
        assert(!write_after_commit_);
        // The CF is probably flushed and hence no need for insert but we still
        // need to keep track of the keys for upcoming rollback/commit.
        WriteBatchInternal::Put(rebuilding_trx_, column_family_id, key, value);
        batch_boundry = IsDuplicateKeySeq(column_family_id, key);
      }
      MaybeAdvanceSeq(batch_boundry);
      return seek_status;
    }
    Status ret_status;

    MemTable* mem = cf_mems_->GetMemTable();
    auto* moptions = mem->GetImmutableMemTableOptions();
    // inplace_update_support is inconsistent with snapshots, and therefore with
    // any kind of transactions including the ones that use seq_per_batch
    assert(!seq_per_batch_ || !moptions->inplace_update_support);
    if (!moptions->inplace_update_support) {
      bool mem_res =
          mem->Add(sequence_, value_type, key, value,
                   concurrent_memtable_writes_, get_post_process_info(mem));
      if (UNLIKELY(!mem_res)) {
        assert(seq_per_batch_);
        ret_status = Status::TryAgain("key+seq exists");
        const bool BATCH_BOUNDRY = true;
        MaybeAdvanceSeq(BATCH_BOUNDRY);
      }
    } else if (moptions->inplace_callback == nullptr) {
      assert(!concurrent_memtable_writes_);
      mem->Update(sequence_, key, value);
    } else {
      assert(!concurrent_memtable_writes_);
      if (mem->UpdateCallback(sequence_, key, value)) {
      } else {
        // key not found in memtable. Do sst get, update, add
        SnapshotImpl read_from_snapshot;
        read_from_snapshot.number_ = sequence_;
        ReadOptions ropts;
        // it's going to be overwritten for sure, so no point caching data block
        // containing the old version
        ropts.fill_cache = false;
        ropts.snapshot = &read_from_snapshot;

        std::string prev_value;
        std::string merged_value;

        auto cf_handle = cf_mems_->GetColumnFamilyHandle();
        Status s = Status::NotSupported();
        if (db_ != nullptr && recovering_log_number_ == 0) {
          if (cf_handle == nullptr) {
            cf_handle = db_->DefaultColumnFamily();
          }
          s = db_->Get(ropts, cf_handle, key, &prev_value);
        }

        char* prev_buffer = const_cast<char*>(prev_value.c_str());
        uint32_t prev_size = static_cast<uint32_t>(prev_value.size());
        auto status = moptions->inplace_callback(s.ok() ? prev_buffer : nullptr,
                                                 s.ok() ? &prev_size : nullptr,
                                                 value, &merged_value);
        if (status == UpdateStatus::UPDATED_INPLACE) {
          // prev_value is updated in-place with final value.
          bool mem_res __attribute__((__unused__));
          mem_res = mem->Add(
              sequence_, value_type, key, Slice(prev_buffer, prev_size));
          assert(mem_res);
          RecordTick(moptions->statistics, NUMBER_KEYS_WRITTEN);
        } else if (status == UpdateStatus::UPDATED) {
          // merged_value contains the final value.
          bool mem_res __attribute__((__unused__));
          mem_res =
              mem->Add(sequence_, value_type, key, Slice(merged_value));
          assert(mem_res);
          RecordTick(moptions->statistics, NUMBER_KEYS_WRITTEN);
        }
      }
    }
    // optimize for non-recovery mode
    if (UNLIKELY(!ret_status.IsTryAgain() && rebuilding_trx_ != nullptr)) {
      assert(!write_after_commit_);
      // If the ret_status is TryAgain then let the next try to add the ky to
      // the rebuilding transaction object.
      WriteBatchInternal::Put(rebuilding_trx_, column_family_id, key, value);
    }
    // Since all Puts are logged in transaction logs (if enabled), always bump
    // sequence number. Even if the update eventually fails and does not result
    // in memtable add/update.
    MaybeAdvanceSeq();
    CheckMemtableFull();
    return ret_status;
  }

  Status PutCF(uint32_t column_family_id, const Slice& key,
               const Slice& value) override {
    return PutCFImpl(column_family_id, key, value, kTypeValue);
  }

  Status DeleteImpl(uint32_t /*column_family_id*/, const Slice& key,
                    const Slice& value, ValueType delete_type) {
    Status ret_status;
    MemTable* mem = cf_mems_->GetMemTable();
    bool mem_res =
        mem->Add(sequence_, delete_type, key, value,
                 concurrent_memtable_writes_, get_post_process_info(mem));
    if (UNLIKELY(!mem_res)) {
      assert(seq_per_batch_);
      ret_status = Status::TryAgain("key+seq exists");
      const bool BATCH_BOUNDRY = true;
      MaybeAdvanceSeq(BATCH_BOUNDRY);
    }
    MaybeAdvanceSeq();
    CheckMemtableFull();
    return ret_status;
  }

  Status DeleteCF(uint32_t column_family_id, const Slice& key) override {
    // optimize for non-recovery mode
    if (UNLIKELY(write_after_commit_ && rebuilding_trx_ != nullptr)) {
      WriteBatchInternal::Delete(rebuilding_trx_, column_family_id, key);
      return Status::OK();
      // else insert the values to the memtable right away
    }

    Status seek_status;
    if (UNLIKELY(!SeekToColumnFamily(column_family_id, &seek_status))) {
      bool batch_boundry = false;
      if (rebuilding_trx_ != nullptr) {
        assert(!write_after_commit_);
        // The CF is probably flushed and hence no need for insert but we still
        // need to keep track of the keys for upcoming rollback/commit.
        WriteBatchInternal::Delete(rebuilding_trx_, column_family_id, key);
        batch_boundry = IsDuplicateKeySeq(column_family_id, key);
      }
      MaybeAdvanceSeq(batch_boundry);
      return seek_status;
    }

    auto ret_status = DeleteImpl(column_family_id, key, Slice(), kTypeDeletion);
    // optimize for non-recovery mode
    if (UNLIKELY(!ret_status.IsTryAgain() && rebuilding_trx_ != nullptr)) {
      assert(!write_after_commit_);
      // If the ret_status is TryAgain then let the next try to add the ky to
      // the rebuilding transaction object.
      WriteBatchInternal::Delete(rebuilding_trx_, column_family_id, key);
    }
    return ret_status;
  }

  Status SingleDeleteCF(uint32_t column_family_id, const Slice& key) override {
    // optimize for non-recovery mode
    if (UNLIKELY(write_after_commit_ && rebuilding_trx_ != nullptr)) {
      WriteBatchInternal::SingleDelete(rebuilding_trx_, column_family_id, key);
      return Status::OK();
      // else insert the values to the memtable right away
    }

    Status seek_status;
    if (UNLIKELY(!SeekToColumnFamily(column_family_id, &seek_status))) {
      bool batch_boundry = false;
      if (rebuilding_trx_ != nullptr) {
        assert(!write_after_commit_);
        // The CF is probably flushed and hence no need for insert but we still
        // need to keep track of the keys for upcoming rollback/commit.
        WriteBatchInternal::SingleDelete(rebuilding_trx_, column_family_id,
                                         key);
        batch_boundry = IsDuplicateKeySeq(column_family_id, key);
      }
      MaybeAdvanceSeq(batch_boundry);
      return seek_status;
    }

    auto ret_status =
        DeleteImpl(column_family_id, key, Slice(), kTypeSingleDeletion);
    // optimize for non-recovery mode
    if (UNLIKELY(!ret_status.IsTryAgain() && rebuilding_trx_ != nullptr)) {
      assert(!write_after_commit_);
      // If the ret_status is TryAgain then let the next try to add the ky to
      // the rebuilding transaction object.
      WriteBatchInternal::SingleDelete(rebuilding_trx_, column_family_id, key);
    }
    return ret_status;
  }

  Status DeleteRangeCF(uint32_t column_family_id, const Slice& begin_key,
                       const Slice& end_key) override {
    // optimize for non-recovery mode
    if (UNLIKELY(write_after_commit_ && rebuilding_trx_ != nullptr)) {
      WriteBatchInternal::DeleteRange(rebuilding_trx_, column_family_id,
                                      begin_key, end_key);
      return Status::OK();
      // else insert the values to the memtable right away
    }

    Status seek_status;
    if (UNLIKELY(!SeekToColumnFamily(column_family_id, &seek_status))) {
      bool batch_boundry = false;
      if (rebuilding_trx_ != nullptr) {
        assert(!write_after_commit_);
        // The CF is probably flushed and hence no need for insert but we still
        // need to keep track of the keys for upcoming rollback/commit.
        WriteBatchInternal::DeleteRange(rebuilding_trx_, column_family_id,
                                        begin_key, end_key);
        // TODO(myabandeh): when transactional DeleteRange support is added,
        // check if end_key must also be added.
        batch_boundry = IsDuplicateKeySeq(column_family_id, begin_key);
      }
      MaybeAdvanceSeq(batch_boundry);
      return seek_status;
    }
    if (db_ != nullptr) {
      auto cf_handle = cf_mems_->GetColumnFamilyHandle();
      if (cf_handle == nullptr) {
        cf_handle = db_->DefaultColumnFamily();
      }
      auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(cf_handle)->cfd();
      if (!cfd->is_delete_range_supported()) {
        return Status::NotSupported(
            std::string("DeleteRange not supported for table type ") +
            cfd->ioptions()->table_factory->Name() + " in CF " +
            cfd->GetName());
      }
    }

    auto ret_status =
        DeleteImpl(column_family_id, begin_key, end_key, kTypeRangeDeletion);
    // optimize for non-recovery mode
    if (UNLIKELY(!ret_status.IsTryAgain() && rebuilding_trx_ != nullptr)) {
      assert(!write_after_commit_);
      // If the ret_status is TryAgain then let the next try to add the ky to
      // the rebuilding transaction object.
      WriteBatchInternal::DeleteRange(rebuilding_trx_, column_family_id,
                                      begin_key, end_key);
    }
    return ret_status;
  }

  Status MergeCF(uint32_t column_family_id, const Slice& key,
                 const Slice& value) override {
    assert(!concurrent_memtable_writes_);
    // optimize for non-recovery mode
    if (UNLIKELY(write_after_commit_ && rebuilding_trx_ != nullptr)) {
      WriteBatchInternal::Merge(rebuilding_trx_, column_family_id, key, value);
      return Status::OK();
      // else insert the values to the memtable right away
    }

    Status seek_status;
    if (UNLIKELY(!SeekToColumnFamily(column_family_id, &seek_status))) {
      bool batch_boundry = false;
      if (rebuilding_trx_ != nullptr) {
        assert(!write_after_commit_);
        // The CF is probably flushed and hence no need for insert but we still
        // need to keep track of the keys for upcoming rollback/commit.
        WriteBatchInternal::Merge(rebuilding_trx_, column_family_id, key,
                                  value);
        batch_boundry = IsDuplicateKeySeq(column_family_id, key);
      }
      MaybeAdvanceSeq(batch_boundry);
      return seek_status;
    }

    Status ret_status;
    MemTable* mem = cf_mems_->GetMemTable();
    auto* moptions = mem->GetImmutableMemTableOptions();
    bool perform_merge = false;

    // If we pass DB through and options.max_successive_merges is hit
    // during recovery, Get() will be issued which will try to acquire
    // DB mutex and cause deadlock, as DB mutex is already held.
    // So we disable merge in recovery
    if (moptions->max_successive_merges > 0 && db_ != nullptr &&
        recovering_log_number_ == 0) {
      LookupKey lkey(key, sequence_);

      // Count the number of successive merges at the head
      // of the key in the memtable
      size_t num_merges = mem->CountSuccessiveMergeEntries(lkey);

      if (num_merges >= moptions->max_successive_merges) {
        perform_merge = true;
      }
    }

    if (perform_merge) {
      // 1) Get the existing value
      std::string get_value;

      // Pass in the sequence number so that we also include previous merge
      // operations in the same batch.
      SnapshotImpl read_from_snapshot;
      read_from_snapshot.number_ = sequence_;
      ReadOptions read_options;
      read_options.snapshot = &read_from_snapshot;

      auto cf_handle = cf_mems_->GetColumnFamilyHandle();
      if (cf_handle == nullptr) {
        cf_handle = db_->DefaultColumnFamily();
      }
      db_->Get(read_options, cf_handle, key, &get_value);
      Slice get_value_slice = Slice(get_value);

      // 2) Apply this merge
      auto merge_operator = moptions->merge_operator;
      assert(merge_operator);

      std::string new_value;

      Status merge_status = MergeHelper::TimedFullMerge(
          merge_operator, key, &get_value_slice, {value}, &new_value,
          moptions->info_log, moptions->statistics, Env::Default());

      if (!merge_status.ok()) {
        // Failed to merge!
        // Store the delta in memtable
        perform_merge = false;
      } else {
        // 3) Add value to memtable
        bool mem_res = mem->Add(sequence_, kTypeValue, key, new_value);
        if (UNLIKELY(!mem_res)) {
          assert(seq_per_batch_);
          ret_status = Status::TryAgain("key+seq exists");
          const bool BATCH_BOUNDRY = true;
          MaybeAdvanceSeq(BATCH_BOUNDRY);
        }
      }
    }

    if (!perform_merge) {
      // Add merge operator to memtable
      bool mem_res = mem->Add(sequence_, kTypeMerge, key, value);
      if (UNLIKELY(!mem_res)) {
        assert(seq_per_batch_);
        ret_status = Status::TryAgain("key+seq exists");
        const bool BATCH_BOUNDRY = true;
        MaybeAdvanceSeq(BATCH_BOUNDRY);
      }
    }

    // optimize for non-recovery mode
    if (UNLIKELY(!ret_status.IsTryAgain() && rebuilding_trx_ != nullptr)) {
      assert(!write_after_commit_);
      // If the ret_status is TryAgain then let the next try to add the ky to
      // the rebuilding transaction object.
      WriteBatchInternal::Merge(rebuilding_trx_, column_family_id, key, value);
    }
    MaybeAdvanceSeq();
    CheckMemtableFull();
    return ret_status;
  }

  Status PutBlobIndexCF(uint32_t column_family_id, const Slice& key,
                        const Slice& value) override {
    // Same as PutCF except for value type.
    return PutCFImpl(column_family_id, key, value, kTypeBlobIndex);
  }

  void CheckMemtableFull() {
    if (flush_scheduler_ != nullptr) {
      auto* cfd = cf_mems_->current();
      assert(cfd != nullptr);
      if (cfd->mem()->ShouldScheduleFlush() &&
          cfd->mem()->MarkFlushScheduled()) {
        // MarkFlushScheduled only returns true if we are the one that
        // should take action, so no need to dedup further
        flush_scheduler_->ScheduleFlush(cfd);
      }
    }
  }

  // The write batch handler calls MarkBeginPrepare with unprepare set to true
  // if it encounters the kTypeBeginUnprepareXID marker.
  Status MarkBeginPrepare(bool unprepare) override {
    assert(rebuilding_trx_ == nullptr);
    assert(db_);

    if (recovering_log_number_ != 0) {
      // during recovery we rebuild a hollow transaction
      // from all encountered prepare sections of the wal
      if (db_->allow_2pc() == false) {
        return Status::NotSupported(
            "WAL contains prepared transactions. Open with "
            "TransactionDB::Open().");
      }

      // we are now iterating through a prepared section
      rebuilding_trx_ = new WriteBatch();
      rebuilding_trx_seq_ = sequence_;
      // We only call MarkBeginPrepare once per batch, and unprepared_batch_
      // is initialized to false by default.
      assert(!unprepared_batch_);
      unprepared_batch_ = unprepare;

      if (has_valid_writes_ != nullptr) {
        *has_valid_writes_ = true;
      }
    }

    return Status::OK();
  }

  Status MarkEndPrepare(const Slice& name) override {
    assert(db_);
    assert((rebuilding_trx_ != nullptr) == (recovering_log_number_ != 0));

    if (recovering_log_number_ != 0) {
      assert(db_->allow_2pc());
      size_t batch_cnt =
          write_after_commit_
              ? 0  // 0 will disable further checks
              : static_cast<size_t>(sequence_ - rebuilding_trx_seq_ + 1);
      db_->InsertRecoveredTransaction(recovering_log_number_, name.ToString(),
                                      rebuilding_trx_, rebuilding_trx_seq_,
                                      batch_cnt, unprepared_batch_);
      rebuilding_trx_ = nullptr;
    } else {
      assert(rebuilding_trx_ == nullptr);
    }
    const bool batch_boundry = true;
    MaybeAdvanceSeq(batch_boundry);

    return Status::OK();
  }

  Status MarkNoop(bool empty_batch) override {
    // A hack in pessimistic transaction could result into a noop at the start
    // of the write batch, that should be ignored.
    if (!empty_batch) {
      // In the absence of Prepare markers, a kTypeNoop tag indicates the end of
      // a batch. This happens when write batch commits skipping the prepare
      // phase.
      const bool batch_boundry = true;
      MaybeAdvanceSeq(batch_boundry);
    }
    return Status::OK();
  }

  Status MarkCommit(const Slice& name) override {
    assert(db_);

    Status s;

    if (recovering_log_number_ != 0) {
      // in recovery when we encounter a commit marker
      // we lookup this transaction in our set of rebuilt transactions
      // and commit.
      auto trx = db_->GetRecoveredTransaction(name.ToString());

      // the log containing the prepared section may have
      // been released in the last incarnation because the
      // data was flushed to L0
      if (trx != nullptr) {
        // at this point individual CF lognumbers will prevent
        // duplicate re-insertion of values.
        assert(log_number_ref_ == 0);
        if (write_after_commit_) {
          // write_after_commit_ can only have one batch in trx.
          assert(trx->batches_.size() == 1);
          const auto& batch_info = trx->batches_.begin()->second;
          // all inserts must reference this trx log number
          log_number_ref_ = batch_info.log_number_;
          s = batch_info.batch_->Iterate(this);
          log_number_ref_ = 0;
        }
        // else the values are already inserted before the commit

        if (s.ok()) {
          db_->DeleteRecoveredTransaction(name.ToString());
        }
        if (has_valid_writes_ != nullptr) {
          *has_valid_writes_ = true;
        }
      }
    } else {
      // When writes are not delayed until commit, there is no disconnect
      // between a memtable write and the WAL that supports it. So the commit
      // need not reference any log as the only log to which it depends.
      assert(!write_after_commit_ || log_number_ref_ > 0);
    }
    const bool batch_boundry = true;
    MaybeAdvanceSeq(batch_boundry);

    return s;
  }

  Status MarkRollback(const Slice& name) override {
    assert(db_);

    if (recovering_log_number_ != 0) {
      auto trx = db_->GetRecoveredTransaction(name.ToString());

      // the log containing the transactions prep section
      // may have been released in the previous incarnation
      // because we knew it had been rolled back
      if (trx != nullptr) {
        db_->DeleteRecoveredTransaction(name.ToString());
      }
    } else {
      // in non recovery we simply ignore this tag
    }

    const bool batch_boundry = true;
    MaybeAdvanceSeq(batch_boundry);

    return Status::OK();
  }

 private:
  MemTablePostProcessInfo* get_post_process_info(MemTable* mem) {
    if (!concurrent_memtable_writes_) {
      // No need to batch counters locally if we don't use concurrent mode.
      return nullptr;
    }
    return &GetPostMap()[mem];
  }
};

// This function can only be called in these conditions:
// 1) During Recovery()
// 2) During Write(), in a single-threaded write thread
// 3) During Write(), in a concurrent context where memtables has been cloned
// The reason is that it calls memtables->Seek(), which has a stateful cache
Status WriteBatchInternal::InsertInto(
    WriteThread::WriteGroup& write_group, SequenceNumber sequence,
    ColumnFamilyMemTables* memtables, FlushScheduler* flush_scheduler,
    bool ignore_missing_column_families, uint64_t recovery_log_number, DB* db,
    bool concurrent_memtable_writes, bool seq_per_batch, bool batch_per_txn) {
  MemTableInserter inserter(
      sequence, memtables, flush_scheduler, ignore_missing_column_families,
      recovery_log_number, db, concurrent_memtable_writes,
      nullptr /*has_valid_writes*/, seq_per_batch, batch_per_txn);
  for (auto w : write_group) {
    if (w->CallbackFailed()) {
      continue;
    }
    w->sequence = inserter.sequence();
    if (!w->ShouldWriteToMemtable()) {
      // In seq_per_batch_ mode this advances the seq by one.
      inserter.MaybeAdvanceSeq(true);
      continue;
    }
    SetSequence(w->batch, inserter.sequence());
    inserter.set_log_number_ref(w->log_ref);
    w->status = w->batch->Iterate(&inserter);
    if (!w->status.ok()) {
      return w->status;
    }
    assert(!seq_per_batch || w->batch_cnt != 0);
    assert(!seq_per_batch || inserter.sequence() - w->sequence == w->batch_cnt);
  }
  return Status::OK();
}

Status WriteBatchInternal::InsertInto(
    WriteThread::Writer* writer, SequenceNumber sequence,
    ColumnFamilyMemTables* memtables, FlushScheduler* flush_scheduler,
    bool ignore_missing_column_families, uint64_t log_number, DB* db,
    bool concurrent_memtable_writes, bool seq_per_batch, size_t batch_cnt,
    bool batch_per_txn) {
#ifdef NDEBUG
  (void)batch_cnt;
#endif
  assert(writer->ShouldWriteToMemtable());
  MemTableInserter inserter(
      sequence, memtables, flush_scheduler, ignore_missing_column_families,
      log_number, db, concurrent_memtable_writes, nullptr /*has_valid_writes*/,
      seq_per_batch, batch_per_txn);
  SetSequence(writer->batch, sequence);
  inserter.set_log_number_ref(writer->log_ref);
  Status s = writer->batch->Iterate(&inserter);
  assert(!seq_per_batch || batch_cnt != 0);
  assert(!seq_per_batch || inserter.sequence() - sequence == batch_cnt);
  if (concurrent_memtable_writes) {
    inserter.PostProcess();
  }
  return s;
}

Status WriteBatchInternal::InsertInto(
    const WriteBatch* batch, ColumnFamilyMemTables* memtables,
    FlushScheduler* flush_scheduler, bool ignore_missing_column_families,
    uint64_t log_number, DB* db, bool concurrent_memtable_writes,
    SequenceNumber* next_seq, bool* has_valid_writes, bool seq_per_batch,
    bool batch_per_txn) {
  MemTableInserter inserter(Sequence(batch), memtables, flush_scheduler,
                            ignore_missing_column_families, log_number, db,
                            concurrent_memtable_writes, has_valid_writes,
                            seq_per_batch, batch_per_txn);
  Status s = batch->Iterate(&inserter);
  if (next_seq != nullptr) {
    *next_seq = inserter.sequence();
  }
  if (concurrent_memtable_writes) {
    inserter.PostProcess();
  }
  return s;
}

Status WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= WriteBatchInternal::kHeader);
  b->rep_.assign(contents.data(), contents.size());
  b->content_flags_.store(ContentFlags::DEFERRED, std::memory_order_relaxed);
  return Status::OK();
}

Status WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src,
                                  const bool wal_only) {
  size_t src_len;
  int src_count;
  uint32_t src_flags;

  const SavePoint& batch_end = src->GetWalTerminationPoint();

  if (wal_only && !batch_end.is_cleared()) {
    src_len = batch_end.size - WriteBatchInternal::kHeader;
    src_count = batch_end.count;
    src_flags = batch_end.content_flags;
  } else {
    src_len = src->rep_.size() - WriteBatchInternal::kHeader;
    src_count = Count(src);
    src_flags = src->content_flags_.load(std::memory_order_relaxed);
  }

  SetCount(dst, Count(dst) + src_count);
  assert(src->rep_.size() >= WriteBatchInternal::kHeader);
  dst->rep_.append(src->rep_.data() + WriteBatchInternal::kHeader, src_len);
  dst->content_flags_.store(
      dst->content_flags_.load(std::memory_order_relaxed) | src_flags,
      std::memory_order_relaxed);
  return Status::OK();
}

size_t WriteBatchInternal::AppendedByteSize(size_t leftByteSize,
                                            size_t rightByteSize) {
  if (leftByteSize == 0 || rightByteSize == 0) {
    return leftByteSize + rightByteSize;
  } else {
    return leftByteSize + rightByteSize - WriteBatchInternal::kHeader;
  }
}

}  // namespace rocksdb
