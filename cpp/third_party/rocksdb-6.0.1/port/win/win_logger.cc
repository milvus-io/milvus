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

#include "port/win/win_logger.h"
#include "port/win/io_win.h"

#include <algorithm>
#include <stdio.h>
#include <time.h>
#include <fcntl.h>
#include <atomic>

#include "rocksdb/env.h"

#include "monitoring/iostats_context_imp.h"
#include "port/sys_time.h"

namespace rocksdb {

namespace port {

WinLogger::WinLogger(uint64_t (*gettid)(), Env* env, HANDLE file,
                     const InfoLogLevel log_level)
    : Logger(log_level),
      file_(file),
      gettid_(gettid),
      log_size_(0),
      last_flush_micros_(0),
      env_(env),
      flush_pending_(false) {
  assert(file_ != NULL);
  assert(file_ != INVALID_HANDLE_VALUE);
}

void WinLogger::DebugWriter(const char* str, int len) {
  assert(file_ != INVALID_HANDLE_VALUE);
  DWORD bytesWritten = 0;
  BOOL ret = WriteFile(file_, str, len, &bytesWritten, NULL);
  if (ret == FALSE) {
    std::string errSz = GetWindowsErrSz(GetLastError());
    fprintf(stderr, errSz.c_str());
  }
}

WinLogger::~WinLogger() { 
  CloseInternal();
}

Status WinLogger::CloseImpl() {
  return CloseInternal();
}

Status WinLogger::CloseInternal() {
  Status s;
  if (INVALID_HANDLE_VALUE != file_) {
    BOOL ret = FlushFileBuffers(file_);
    if (ret == 0) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError("Failed to flush LOG on Close() ", 
        lastError);
    }
    ret = CloseHandle(file_);
    // On error the return value is zero
    if (ret == 0 && s.ok()) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError("Failed to flush LOG on Close() ", 
        lastError);
    }
    file_ = INVALID_HANDLE_VALUE;
    closed_ = true;
  }
  return s;
}

void WinLogger::Flush() {
  assert(file_ != INVALID_HANDLE_VALUE);
  if (flush_pending_) {
    flush_pending_ = false;
    // With Windows API writes go to OS buffers directly so no fflush needed
    // unlike with C runtime API. We don't flush all the way to disk
    // for perf reasons.
  }

  last_flush_micros_ = env_->NowMicros();
}

void WinLogger::Logv(const char* format, va_list ap) {
  IOSTATS_TIMER_GUARD(logger_nanos);
  assert(file_ != INVALID_HANDLE_VALUE);

  const uint64_t thread_id = (*gettid_)();

  // We try twice: the first time with a fixed-size stack allocated buffer,
  // and the second time with a much larger dynamically allocated buffer.
  char buffer[500];
  std::unique_ptr<char[]> largeBuffer;
  for (int iter = 0; iter < 2; ++iter) {
    char* base;
    int bufsize;
    if (iter == 0) {
      bufsize = sizeof(buffer);
      base = buffer;
    } else {
      bufsize = 30000;
      largeBuffer.reset(new char[bufsize]);
      base = largeBuffer.get();
    }

    char* p = base;
    char* limit = base + bufsize;

    struct timeval now_tv;
    gettimeofday(&now_tv, nullptr);
    const time_t seconds = now_tv.tv_sec;
    struct tm t;
    localtime_s(&t, &seconds);
    p += snprintf(p, limit - p, "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ",
                  t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour,
                  t.tm_min, t.tm_sec, static_cast<int>(now_tv.tv_usec),
                  static_cast<long long unsigned int>(thread_id));

    // Print the message
    if (p < limit) {
      va_list backup_ap;
      va_copy(backup_ap, ap);
      int done = vsnprintf(p, limit - p, format, backup_ap);
      if (done > 0) {
        p += done;
      } else {
        continue;
      }
      va_end(backup_ap);
    }

    // Truncate to available space if necessary
    if (p >= limit) {
      if (iter == 0) {
        continue;  // Try again with larger buffer
      } else {
        p = limit - 1;
      }
    }

    // Add newline if necessary
    if (p == base || p[-1] != '\n') {
      *p++ = '\n';
    }

    assert(p <= limit);
    const size_t write_size = p - base;

    DWORD bytesWritten = 0;
    BOOL ret = WriteFile(file_, base, static_cast<DWORD>(write_size),
      &bytesWritten, NULL);
    if (ret == FALSE) {
      std::string errSz = GetWindowsErrSz(GetLastError());
      fprintf(stderr, errSz.c_str());
    }

    flush_pending_ = true;
    assert((bytesWritten == write_size) || (ret == FALSE));
    if (bytesWritten > 0) {
      log_size_ += write_size;
    }

    uint64_t now_micros =
        static_cast<uint64_t>(now_tv.tv_sec) * 1000000 + now_tv.tv_usec;
    if (now_micros - last_flush_micros_ >= flush_every_seconds_ * 1000000) {
      flush_pending_ = false;
      // With Windows API writes go to OS buffers directly so no fflush needed
      // unlike with C runtime API. We don't flush all the way to disk
      // for perf reasons.
      last_flush_micros_ = now_micros;
    }
    break;
  }
}

size_t WinLogger::GetLogFileSize() const { return log_size_; }

}

}  // namespace rocksdb
