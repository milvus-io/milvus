//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <memory>
#include <functional>
#include <type_traits>

namespace rocksdb {
namespace port {

// This class is a replacement for std::thread
// 2 reasons we do not like std::thread:
//  -- is that it dynamically allocates its internals that are automatically
//     freed when  the thread terminates and not on the destruction of the
//     object. This makes it difficult to control the source of memory
//     allocation 
//  -  This implements Pimpl so we can easily replace the guts of the
//      object in our private version if necessary.
class WindowsThread {

  struct Data;

  std::shared_ptr<Data>  data_;
  unsigned int           th_id_;

  void Init(std::function<void()>&&);

public:

  typedef void* native_handle_type;

  // Construct with no thread
  WindowsThread();

  // Template constructor
  // 
  // This templated constructor accomplishes several things
  //
  // - Allows the class as whole to be not a template
  //
  // - take "universal" references to support both _lvalues and _rvalues
  //
  // -  because this constructor is a catchall case in many respects it
  //    may prevent us from using both the default __ctor, the move __ctor.
  //    Also it may circumvent copy __ctor deletion. To work around this
  //    we make sure this one has at least one argument and eliminate
  //    it from the overload  selection when WindowsThread is the first
  //    argument.
  //
  // - construct with Fx(Ax...) with a variable number of types/arguments.
  //
  // - Gathers together the callable object with its arguments and constructs
  //   a single callable entity
  //
  // - Makes use of std::function to convert it to a specification-template
  //   dependent type that both checks the signature conformance to ensure
  //   that all of the necessary arguments are provided and allows pimpl
  //   implementation.
  template<class Fn,
    class... Args,
    class = typename std::enable_if<
      !std::is_same<typename std::decay<Fn>::type,
                    WindowsThread>::value>::type>
  explicit WindowsThread(Fn&& fx, Args&&... ax) :
      WindowsThread() {

    // Use binder to create a single callable entity
    auto binder = std::bind(std::forward<Fn>(fx),
      std::forward<Args>(ax)...);
    // Use std::function to take advantage of the type erasure
    // so we can still hide implementation within pimpl
    // This also makes sure that the binder signature is compliant
    std::function<void()> target = binder;

    Init(std::move(target));
  }


  ~WindowsThread();

  WindowsThread(const WindowsThread&) = delete;

  WindowsThread& operator=(const WindowsThread&) = delete;

  WindowsThread(WindowsThread&&) noexcept;

  WindowsThread& operator=(WindowsThread&&) noexcept;

  bool joinable() const;

  unsigned int get_id() const { return th_id_; }

  native_handle_type native_handle() const;

  static unsigned hardware_concurrency();

  void join();

  bool detach();

  void swap(WindowsThread&);
};
} // namespace port
} // namespace rocksdb

namespace std {
  inline
  void swap(rocksdb::port::WindowsThread& th1, 
    rocksdb::port::WindowsThread& th2) {
    th1.swap(th2);
  }
} // namespace std

