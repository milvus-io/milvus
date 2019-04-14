/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c++ -*-

#include "error_c.h"
#include "error_impl.h"
#include "FaissException.h"
#include <exception>

thread_local std::exception_ptr faiss_last_exception;

const char* faiss_get_last_error() {
    if (faiss_last_exception) {
        try {
            std::rethrow_exception(faiss_last_exception);
        } catch (std::exception& e) {
            return e.what();
        }
    }
    return nullptr;
}
