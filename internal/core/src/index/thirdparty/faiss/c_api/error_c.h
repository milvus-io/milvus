/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c -*-

#ifndef FAISS_ERROR_C_H
#define FAISS_ERROR_C_H

#ifdef __cplusplus
extern "C" {
#endif

/// An error code which depends on the exception thrown from the previous 
/// operation. See `faiss_get_last_error` to retrieve the error message.
typedef enum FaissErrorCode {
    /// No error
    OK = 0,
    /// Any exception other than Faiss or standard C++ library exceptions
    UNKNOWN_EXCEPT = -1,
    /// Faiss library exception
    FAISS_EXCEPT = -2,
    /// Standard C++ library exception
    STD_EXCEPT = -4
} FaissErrorCode;

/**
 * Get the error message of the last failed operation performed by Faiss.
 * The given pointer is only invalid until another Faiss function is
 * called.
 */
const char* faiss_get_last_error();

#ifdef __cplusplus
}
#endif

#endif