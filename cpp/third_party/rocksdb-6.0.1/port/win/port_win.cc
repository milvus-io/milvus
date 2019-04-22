//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#if !defined(OS_WIN) && !defined(WIN32) && !defined(_WIN32)
#error Windows Specific Code
#endif

#include "port/win/port_win.h"

#include <io.h>
#include "port/dirent.h"
#include "port/sys_time.h"

#include <cstdlib>
#include <stdio.h>
#include <assert.h>
#include <string.h>

#include <memory>
#include <exception>
#include <chrono>

#ifdef ROCKSDB_WINDOWS_UTF8_FILENAMES
// utf8 <-> utf16
#include <string>
#include <locale>
#include <codecvt>
#endif

#include "util/logging.h"

namespace rocksdb {
namespace port {

#ifdef ROCKSDB_WINDOWS_UTF8_FILENAMES
std::string utf16_to_utf8(const std::wstring& utf16) {
  std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>,wchar_t> convert;
  return convert.to_bytes(utf16);
}

std::wstring utf8_to_utf16(const std::string& utf8) {
  std::wstring_convert<std::codecvt_utf8_utf16<wchar_t>> converter;
  return converter.from_bytes(utf8);
}
#endif

void gettimeofday(struct timeval* tv, struct timezone* /* tz */) {
  using namespace std::chrono;

  microseconds usNow(
      duration_cast<microseconds>(system_clock::now().time_since_epoch()));

  seconds secNow(duration_cast<seconds>(usNow));

  tv->tv_sec = static_cast<long>(secNow.count());
  tv->tv_usec = static_cast<long>(usNow.count() -
      duration_cast<microseconds>(secNow).count());
}

Mutex::~Mutex() {}

CondVar::~CondVar() {}

void CondVar::Wait() {
  // Caller must ensure that mutex is held prior to calling this method
  std::unique_lock<std::mutex> lk(mu_->getLock(), std::adopt_lock);
#ifndef NDEBUG
  mu_->locked_ = false;
#endif
  cv_.wait(lk);
#ifndef NDEBUG
  mu_->locked_ = true;
#endif
  // Release ownership of the lock as we don't want it to be unlocked when
  // it goes out of scope (as we adopted the lock and didn't lock it ourselves)
  lk.release();
}

bool CondVar::TimedWait(uint64_t abs_time_us) {

  using namespace std::chrono;

  // MSVC++ library implements wait_until in terms of wait_for so
  // we need to convert absolute wait into relative wait.
  microseconds usAbsTime(abs_time_us);

  microseconds usNow(
    duration_cast<microseconds>(system_clock::now().time_since_epoch()));
  microseconds relTimeUs =
    (usAbsTime > usNow) ? (usAbsTime - usNow) : microseconds::zero();

  // Caller must ensure that mutex is held prior to calling this method
  std::unique_lock<std::mutex> lk(mu_->getLock(), std::adopt_lock);
#ifndef NDEBUG
  mu_->locked_ = false;
#endif
  std::cv_status cvStatus = cv_.wait_for(lk, relTimeUs);
#ifndef NDEBUG
  mu_->locked_ = true;
#endif
  // Release ownership of the lock as we don't want it to be unlocked when
  // it goes out of scope (as we adopted the lock and didn't lock it ourselves)
  lk.release();

  if (cvStatus == std::cv_status::timeout) {
    return true;
  }

  return false;
}

void CondVar::Signal() { cv_.notify_one(); }

void CondVar::SignalAll() { cv_.notify_all(); }

int PhysicalCoreID() { return GetCurrentProcessorNumber(); }

void InitOnce(OnceType* once, void (*initializer)()) {
  std::call_once(once->flag_, initializer);
}

// Private structure, exposed only by pointer
struct DIR {
  HANDLE      handle_;
  bool        firstread_;
  RX_WIN32_FIND_DATA data_;
  dirent entry_;

  DIR() : handle_(INVALID_HANDLE_VALUE),
    firstread_(true) {}

  DIR(const DIR&) = delete;
  DIR& operator=(const DIR&) = delete;

  ~DIR() {
    if (INVALID_HANDLE_VALUE != handle_) {
      ::FindClose(handle_);
    }
  }
};

DIR* opendir(const char* name) {
  if (!name || *name == 0) {
    errno = ENOENT;
    return nullptr;
  }

  std::string pattern(name);
  pattern.append("\\").append("*");

  std::unique_ptr<DIR> dir(new DIR);

  dir->handle_ = RX_FindFirstFileEx(RX_FN(pattern).c_str(), 
    FindExInfoBasic, // Do not want alternative name
    &dir->data_,
    FindExSearchNameMatch,
    NULL, // lpSearchFilter
    0);

  if (dir->handle_ == INVALID_HANDLE_VALUE) {
    return nullptr;
  }

  RX_FILESTRING x(dir->data_.cFileName, RX_FNLEN(dir->data_.cFileName));
  strcpy_s(dir->entry_.d_name, sizeof(dir->entry_.d_name), 
           FN_TO_RX(x).c_str());

  return dir.release();
}

struct dirent* readdir(DIR* dirp) {
  if (!dirp || dirp->handle_ == INVALID_HANDLE_VALUE) {
    errno = EBADF;
    return nullptr;
  }

  if (dirp->firstread_) {
    dirp->firstread_ = false;
    return &dirp->entry_;
  }

  auto ret = RX_FindNextFile(dirp->handle_, &dirp->data_);

  if (ret == 0) {
    return nullptr;
  }

  RX_FILESTRING x(dirp->data_.cFileName, RX_FNLEN(dirp->data_.cFileName));
  strcpy_s(dirp->entry_.d_name, sizeof(dirp->entry_.d_name), 
           FN_TO_RX(x).c_str());

  return &dirp->entry_;
}

int closedir(DIR* dirp) {
  delete dirp;
  return 0;
}

int truncate(const char* path, int64_t length) {
  if (path == nullptr) {
    errno = EFAULT;
    return -1;
  }
  return rocksdb::port::Truncate(path, length);
}

int Truncate(std::string path, int64_t len) {

  if (len < 0) {
    errno = EINVAL;
    return -1;
  }

  HANDLE hFile =
      RX_CreateFile(RX_FN(path).c_str(), GENERIC_READ | GENERIC_WRITE,
                 FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                 NULL,           // Security attrs
                 OPEN_EXISTING,  // Truncate existing file only
                 FILE_ATTRIBUTE_NORMAL, NULL);

  if (INVALID_HANDLE_VALUE == hFile) {
    auto lastError = GetLastError();
    if (lastError == ERROR_FILE_NOT_FOUND) {
      errno = ENOENT;
    } else if (lastError == ERROR_ACCESS_DENIED) {
      errno = EACCES;
    } else {
      errno = EIO;
    }
    return -1;
  }

  int result = 0;
  FILE_END_OF_FILE_INFO end_of_file;
  end_of_file.EndOfFile.QuadPart = len;

  if (!SetFileInformationByHandle(hFile, FileEndOfFileInfo, &end_of_file,
                                  sizeof(FILE_END_OF_FILE_INFO))) {
    errno = EIO;
    result = -1;
  }

  CloseHandle(hFile);
  return result;
}

void Crash(const std::string& srcfile, int srcline) {
  fprintf(stdout, "Crashing at %s:%d\n", srcfile.c_str(), srcline);
  fflush(stdout);
  abort();
}

int GetMaxOpenFiles() { return -1; }

}  // namespace port
}  // namespace rocksdb
