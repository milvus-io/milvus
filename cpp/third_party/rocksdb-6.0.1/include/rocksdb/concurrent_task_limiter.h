//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "rocksdb/env.h"
#include "rocksdb/statistics.h"

namespace rocksdb {

class ConcurrentTaskLimiter {
 public:

  virtual ~ConcurrentTaskLimiter() {}

  // Returns a name that identifies this concurrent task limiter.
  virtual const std::string& GetName() const = 0;

  // Set max concurrent tasks.
  // limit = 0 means no new task allowed.
  // limit < 0 means no limitation.
  virtual void SetMaxOutstandingTask(int32_t limit) = 0;

  // Reset to unlimited max concurrent task.
  virtual void ResetMaxOutstandingTask() = 0;

  // Returns current outstanding task count.
  virtual int32_t GetOutstandingTask() const = 0;
};

// Create a ConcurrentTaskLimiter that can be shared with mulitple CFs
// across RocksDB instances to control concurrent tasks.
//
// @param name: Name of the limiter.
// @param limit: max concurrent tasks.
//        limit = 0 means no new task allowed.
//        limit < 0 means no limitation.
extern ConcurrentTaskLimiter* NewConcurrentTaskLimiter(
    const std::string& name, int32_t limit);

}  // namespace rocksdb
