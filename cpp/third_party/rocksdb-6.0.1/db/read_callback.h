// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/types.h"

namespace rocksdb {

class ReadCallback {
 public:
  virtual ~ReadCallback() {}

  // Will be called to see if the seq number visible; if not it moves on to
  // the next seq number.
  virtual bool IsVisible(SequenceNumber seq) = 0;

  // This is called to determine the maximum visible sequence number for the
  // current transaction for read-your-own-write semantics. This is so that
  // for write unprepared, we will not skip keys that are written by the
  // current transaction with the seek to snapshot optimization.
  //
  // For other uses, this returns zero, meaning that the current snapshot
  // sequence number is the maximum visible sequence number.
  inline virtual SequenceNumber MaxUnpreparedSequenceNumber() { return 0; };
};

}  //  namespace rocksdb
