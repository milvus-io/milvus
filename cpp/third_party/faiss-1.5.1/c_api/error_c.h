/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c -*-

#ifndef FAISS_ERROR_C_H
#define FAISS_ERROR_C_H

#ifdef __cplusplus
extern "C" {
#endif

typedef enum FaissErrorCode {
    OK = 0,
    UNKNOWN_EXCEPT = -1,
    FAISS_EXCEPT = -2,
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