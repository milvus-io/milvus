/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

//  Copyright 2004-present Facebook. All Rights Reserved
// -*- c++ -*-
// I/O code for indexes

#include "clone_index_c.h"
#include "clone_index.h"
#include "macros_impl.h"

using faiss::Index;

int faiss_clone_index (const FaissIndex *idx, FaissIndex **p_out) {
    try {
        auto out = faiss::clone_index(reinterpret_cast<const Index*>(idx));
        *p_out = reinterpret_cast<FaissIndex*>(out);
    } CATCH_AND_HANDLE
}