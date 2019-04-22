//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Logger implementation that can be shared by all environments
// where enough posix functionality is available.

#pragma once

#include <atomic>

#include "rocksdb/env.h"

#include <stdint.h>
#include <windows.h>

namespace rocksdb {

class Env;

namespace port {

class WinLogger : public rocksdb::Logger {
 public:
  WinLogger(uint64_t (*gettid)(), Env* env, HANDLE file,
            const InfoLogLevel log_level = InfoLogLevel::ERROR_LEVEL);

  virtual ~WinLogger();

  WinLogger(const WinLogger&) = delete;

  WinLogger& operator=(const WinLogger&) = delete;

  void Flush() override;

  using rocksdb::Logger::Logv;
  void Logv(const char* format, va_list ap) override;

  size_t GetLogFileSize() const override;

  void DebugWriter(const char* str, int len);

protected:

    Status CloseImpl() override;

 private:
  HANDLE file_;
  uint64_t (*gettid_)();  // Return the thread id for the current thread
  std::atomic_size_t log_size_;
  std::atomic_uint_fast64_t last_flush_micros_;
  Env* env_;
  bool flush_pending_;

  Status CloseInternal();

  const static uint64_t flush_every_seconds_ = 5;
};

}

}  // namespace rocksdb
