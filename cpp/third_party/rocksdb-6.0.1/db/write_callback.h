// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "rocksdb/status.h"

namespace rocksdb {

class DB;

class WriteCallback {
 public:
  virtual ~WriteCallback() {}

  // Will be called while on the write thread before the write executes.  If
  // this function returns a non-OK status, the write will be aborted and this
  // status will be returned to the caller of DB::Write().
  virtual Status Callback(DB* db) = 0;

  // return true if writes with this callback can be batched with other writes
  virtual bool AllowWriteBatching() = 0;
};

}  //  namespace rocksdb
