/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_EXCEPTION_INCLUDED
#define FAISS_EXCEPTION_INCLUDED

#include <exception>
#include <string>

namespace faiss {

/// Base class for Faiss exceptions
class FaissException : public std::exception {
 public:
  explicit FaissException(const std::string& msg);

  FaissException(const std::string& msg,
                 const char* funcName,
                 const char* file,
                 int line);

  /// from std::exception
  const char* what() const noexcept override;

  std::string msg;
};


/** bare-bones unique_ptr
 * this one deletes with delete [] */
template<class T>
struct ScopeDeleter {
    const T * ptr;
    explicit ScopeDeleter (const T* ptr = nullptr): ptr (ptr) {}
    void release () {ptr = nullptr; }
    void set (const T * ptr_in) { ptr = ptr_in; }
    void swap (ScopeDeleter<T> &other) {std::swap (ptr, other.ptr); }
    ~ScopeDeleter () {
        delete [] ptr;
    }
};

/** same but deletes with the simple delete (least common case) */
template<class T>
struct ScopeDeleter1 {
    const T * ptr;
    explicit ScopeDeleter1 (const T* ptr = nullptr): ptr (ptr) {}
    void release () {ptr = nullptr; }
    void set (const T * ptr_in) { ptr = ptr_in; }
    void swap (ScopeDeleter1<T> &other) {std::swap (ptr, other.ptr); }
    ~ScopeDeleter1 () {
        delete ptr;
    }
};



}


#endif
