/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include "FaissException.h"

namespace faiss {

FaissException::FaissException(const std::string& m)
    : msg(m) {
}

FaissException::FaissException(const std::string& m,
                               const char* funcName,
                               const char* file,
                               int line) {
  int size = snprintf(nullptr, 0, "Error in %s at %s:%d: %s",
                      funcName, file, line, m.c_str());
  msg.resize(size + 1);
  snprintf(&msg[0], msg.size(), "Error in %s at %s:%d: %s",
           funcName, file, line, m.c_str());
}

const char*
FaissException::what() const noexcept {
  return msg.c_str();
}

}
