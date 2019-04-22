//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "port/win/win_thread.h"

#include <assert.h>
#include <process.h> // __beginthreadex
#include <windows.h>

#include <stdexcept>
#include <system_error>
#include <thread>

namespace rocksdb {
namespace port {

struct WindowsThread::Data {

  std::function<void()> func_;
  uintptr_t             handle_;

  Data(std::function<void()>&& func) :
    func_(std::move(func)),
    handle_(0) {
  }

  Data(const Data&) = delete;
  Data& operator=(const Data&) = delete;

  static unsigned int __stdcall ThreadProc(void* arg);
};


void WindowsThread::Init(std::function<void()>&& func) {

  data_ = std::make_shared<Data>(std::move(func));
  // We create another instance of std::shared_ptr to get an additional ref
  // since we may detach and destroy this instance before the threadproc
  // may start to run. We choose to allocate this additional ref on the heap
  // so we do not need to synchronize and allow this thread to proceed
  std::unique_ptr<std::shared_ptr<Data>> th_data(new std::shared_ptr<Data>(data_));

  data_->handle_ = _beginthreadex(NULL,
    0,    // stack size
    &Data::ThreadProc,
    th_data.get(),
    0,   // init flag
    &th_id_);

  if (data_->handle_ == 0) {
    throw std::system_error(std::make_error_code(
      std::errc::resource_unavailable_try_again),
      "Unable to create a thread");
  }
  th_data.release();
}

WindowsThread::WindowsThread() :
  data_(nullptr),
  th_id_(0)
{}


WindowsThread::~WindowsThread() {
  // Must be joined or detached
  // before destruction.
  // This is the same as std::thread
  if (data_) {
    if (joinable()) {
      assert(false);
      std::terminate();
    }
    data_.reset();
  }
}

WindowsThread::WindowsThread(WindowsThread&& o) noexcept :
  WindowsThread() {
  *this = std::move(o);
}

WindowsThread& WindowsThread::operator=(WindowsThread&& o) noexcept {

  if (joinable()) {
    assert(false);
    std::terminate();
  }

  data_ = std::move(o.data_);

  // Per spec both instances will have the same id
  th_id_ = o.th_id_;

  return *this;
}

bool WindowsThread::joinable() const {
  return (data_ && data_->handle_ != 0);
}

WindowsThread::native_handle_type WindowsThread::native_handle() const {
  return reinterpret_cast<native_handle_type>(data_->handle_);
}

unsigned WindowsThread::hardware_concurrency() {
  return std::thread::hardware_concurrency();
}

void WindowsThread::join() {

  if (!joinable()) {
    assert(false);
    throw std::system_error(
      std::make_error_code(std::errc::invalid_argument),
      "Thread is no longer joinable");
  }

  if (GetThreadId(GetCurrentThread()) == th_id_) {
    assert(false);
    throw std::system_error(
      std::make_error_code(std::errc::resource_deadlock_would_occur),
      "Can not join itself");
  }

  auto ret = WaitForSingleObject(reinterpret_cast<HANDLE>(data_->handle_),
    INFINITE);
  if (ret != WAIT_OBJECT_0) {
    auto lastError = GetLastError();
    assert(false);
    throw std::system_error(static_cast<int>(lastError),
      std::system_category(),
      "WaitForSingleObjectFailed: thread join");
  }

  BOOL rc;
  rc = CloseHandle(reinterpret_cast<HANDLE>(data_->handle_));
  assert(rc != 0);
  data_->handle_ = 0;
}

bool WindowsThread::detach() {

  if (!joinable()) {
    assert(false);
    throw std::system_error(
      std::make_error_code(std::errc::invalid_argument),
      "Thread is no longer available");
  }

  BOOL ret = CloseHandle(reinterpret_cast<HANDLE>(data_->handle_));
  data_->handle_ = 0;

  return (ret != 0);
}

void  WindowsThread::swap(WindowsThread& o) {
  data_.swap(o.data_);
  std::swap(th_id_, o.th_id_);
}

unsigned int __stdcall  WindowsThread::Data::ThreadProc(void* arg) {
  auto ptr = reinterpret_cast<std::shared_ptr<Data>*>(arg);
  std::unique_ptr<std::shared_ptr<Data>> data(ptr);
  (*data)->func_();
  return 0;
}
} // namespace port
} // namespace rocksdb
