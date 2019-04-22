// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stdint.h>
#include "rocksdb/slice.h"

namespace rocksdb {

// Define all public custom types here.

// Represents a sequence number in a WAL file.
typedef uint64_t SequenceNumber;

// User-oriented representation of internal key types.
enum EntryType {
  kEntryPut,
  kEntryDelete,
  kEntrySingleDelete,
  kEntryMerge,
  kEntryRangeDeletion,
  kEntryBlobIndex,
  kEntryOther,
};

// <user key, sequence number, and entry type> tuple.
struct FullKey {
  Slice user_key;
  SequenceNumber sequence;
  EntryType type;

  FullKey()
      : sequence(0)
  {}  // Intentionally left uninitialized (for speed)
  FullKey(const Slice& u, const SequenceNumber& seq, EntryType t)
      : user_key(u), sequence(seq), type(t) { }
  std::string DebugString(bool hex = false) const;

  void clear() {
    user_key.clear();
    sequence = 0;
    type = EntryType::kEntryPut;
  }
};

// Parse slice representing internal key to FullKey
// Parsed FullKey is valid for as long as the memory pointed to by
// internal_key is alive.
bool ParseFullKey(const Slice& internal_key, FullKey* result);

}  //  namespace rocksdb
