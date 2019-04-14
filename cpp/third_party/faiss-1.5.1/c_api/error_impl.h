/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c++ -*-

#include <exception>

/** global variable for holding the last exception thrown by
 * calls to Faiss functions through the C API
 */
extern thread_local std::exception_ptr faiss_last_exception;
