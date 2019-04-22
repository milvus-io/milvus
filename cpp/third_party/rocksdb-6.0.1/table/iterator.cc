//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/iterator.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "util/arena.h"

namespace rocksdb {

Cleanable::Cleanable() {
  cleanup_.function = nullptr;
  cleanup_.next = nullptr;
}

Cleanable::~Cleanable() { DoCleanup(); }

Cleanable::Cleanable(Cleanable&& other) {
  *this = std::move(other);
}

Cleanable& Cleanable::operator=(Cleanable&& other) {
  if (this != &other) {
    cleanup_ = other.cleanup_;
    other.cleanup_.function = nullptr;
    other.cleanup_.next = nullptr;
  }
  return *this;
}

// If the entire linked list was on heap we could have simply add attach one
// link list to another. However the head is an embeded object to avoid the cost
// of creating objects for most of the use cases when the Cleanable has only one
// Cleanup to do. We could put evernything on heap if benchmarks show no
// negative impact on performance.
// Also we need to iterate on the linked list since there is no pointer to the
// tail. We can add the tail pointer but maintainin it might negatively impact
// the perforamnce for the common case of one cleanup where tail pointer is not
// needed. Again benchmarks could clarify that.
// Even without a tail pointer we could iterate on the list, find the tail, and
// have only that node updated without the need to insert the Cleanups one by
// one. This however would be redundant when the source Cleanable has one or a
// few Cleanups which is the case most of the time.
// TODO(myabandeh): if the list is too long we should maintain a tail pointer
// and have the entire list (minus the head that has to be inserted separately)
// merged with the target linked list at once.
void Cleanable::DelegateCleanupsTo(Cleanable* other) {
  assert(other != nullptr);
  if (cleanup_.function == nullptr) {
    return;
  }
  Cleanup* c = &cleanup_;
  other->RegisterCleanup(c->function, c->arg1, c->arg2);
  c = c->next;
  while (c != nullptr) {
    Cleanup* next = c->next;
    other->RegisterCleanup(c);
    c = next;
  }
  cleanup_.function = nullptr;
  cleanup_.next = nullptr;
}

void Cleanable::RegisterCleanup(Cleanable::Cleanup* c) {
  assert(c != nullptr);
  if (cleanup_.function == nullptr) {
    cleanup_.function = c->function;
    cleanup_.arg1 = c->arg1;
    cleanup_.arg2 = c->arg2;
    delete c;
  } else {
    c->next = cleanup_.next;
    cleanup_.next = c;
  }
}

void Cleanable::RegisterCleanup(CleanupFunction func, void* arg1, void* arg2) {
  assert(func != nullptr);
  Cleanup* c;
  if (cleanup_.function == nullptr) {
    c = &cleanup_;
  } else {
    c = new Cleanup;
    c->next = cleanup_.next;
    cleanup_.next = c;
  }
  c->function = func;
  c->arg1 = arg1;
  c->arg2 = arg2;
}

Status Iterator::GetProperty(std::string prop_name, std::string* prop) {
  if (prop == nullptr) {
    return Status::InvalidArgument("prop is nullptr");
  }
  if (prop_name == "rocksdb.iterator.is-key-pinned") {
    *prop = "0";
    return Status::OK();
  }
  return Status::InvalidArgument("Unidentified property.");
}

namespace {
class EmptyIterator : public Iterator {
 public:
  explicit EmptyIterator(const Status& s) : status_(s) { }
  bool Valid() const override { return false; }
  void Seek(const Slice& /*target*/) override {}
  void SeekForPrev(const Slice& /*target*/) override {}
  void SeekToFirst() override {}
  void SeekToLast() override {}
  void Next() override { assert(false); }
  void Prev() override { assert(false); }
  Slice key() const override {
    assert(false);
    return Slice();
  }
  Slice value() const override {
    assert(false);
    return Slice();
  }
  Status status() const override { return status_; }

 private:
  Status status_;
};

template <class TValue = Slice>
class EmptyInternalIterator : public InternalIteratorBase<TValue> {
 public:
  explicit EmptyInternalIterator(const Status& s) : status_(s) {}
  bool Valid() const override { return false; }
  void Seek(const Slice& /*target*/) override {}
  void SeekForPrev(const Slice& /*target*/) override {}
  void SeekToFirst() override {}
  void SeekToLast() override {}
  void Next() override { assert(false); }
  void Prev() override { assert(false); }
  Slice key() const override {
    assert(false);
    return Slice();
  }
  TValue value() const override {
    assert(false);
    return TValue();
  }
  Status status() const override { return status_; }

 private:
  Status status_;
};
}  // namespace

Iterator* NewEmptyIterator() {
  return new EmptyIterator(Status::OK());
}

Iterator* NewErrorIterator(const Status& status) {
  return new EmptyIterator(status);
}

template <class TValue>
InternalIteratorBase<TValue>* NewErrorInternalIterator(const Status& status) {
  return new EmptyInternalIterator<TValue>(status);
}
template InternalIteratorBase<BlockHandle>* NewErrorInternalIterator(
    const Status& status);
template InternalIteratorBase<Slice>* NewErrorInternalIterator(
    const Status& status);

template <class TValue>
InternalIteratorBase<TValue>* NewErrorInternalIterator(const Status& status,
                                                       Arena* arena) {
  if (arena == nullptr) {
    return NewErrorInternalIterator<TValue>(status);
  } else {
    auto mem = arena->AllocateAligned(sizeof(EmptyIterator));
    return new (mem) EmptyInternalIterator<TValue>(status);
  }
}
template InternalIteratorBase<BlockHandle>* NewErrorInternalIterator(
    const Status& status, Arena* arena);
template InternalIteratorBase<Slice>* NewErrorInternalIterator(
    const Status& status, Arena* arena);

template <class TValue>
InternalIteratorBase<TValue>* NewEmptyInternalIterator() {
  return new EmptyInternalIterator<TValue>(Status::OK());
}
template InternalIteratorBase<BlockHandle>* NewEmptyInternalIterator();
template InternalIteratorBase<Slice>* NewEmptyInternalIterator();

template <class TValue>
InternalIteratorBase<TValue>* NewEmptyInternalIterator(Arena* arena) {
  if (arena == nullptr) {
    return NewEmptyInternalIterator<TValue>();
  } else {
    auto mem = arena->AllocateAligned(sizeof(EmptyIterator));
    return new (mem) EmptyInternalIterator<TValue>(Status::OK());
  }
}
template InternalIteratorBase<BlockHandle>* NewEmptyInternalIterator(
    Arena* arena);
template InternalIteratorBase<Slice>* NewEmptyInternalIterator(Arena* arena);

}  // namespace rocksdb
