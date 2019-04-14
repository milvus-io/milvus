/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

//  Copyright 2004-present Facebook. All Rights Reserved
// -*- c++ -*-
// I/O code for indexes

#include "index_io_c.h"
#include "index_io.h"
#include "macros_impl.h"

using faiss::Index;

int faiss_write_index(const FaissIndex *idx, FILE *f) {
    try {
        faiss::write_index(reinterpret_cast<const Index*>(idx), f);
    } CATCH_AND_HANDLE
}

int faiss_write_index_fname(const FaissIndex *idx, const char *fname) {
    try {
        faiss::write_index(reinterpret_cast<const Index*>(idx), fname);
    } CATCH_AND_HANDLE
}

int faiss_read_index(FILE *f, int io_flags, FaissIndex **p_out) {
    try {
        auto out = faiss::read_index(f, io_flags);
        *p_out = reinterpret_cast<FaissIndex*>(out);
    } CATCH_AND_HANDLE
}

int faiss_read_index_fname(const char *fname, int io_flags, FaissIndex **p_out) {
    try {
        auto out = faiss::read_index(fname, io_flags);
        *p_out = reinterpret_cast<FaissIndex*>(out);
    } CATCH_AND_HANDLE
}

int faiss_clone_index (const FaissIndex *idx, FaissIndex **p_out) {
    try {
        auto out = faiss::clone_index(reinterpret_cast<const Index*>(idx));
        *p_out = reinterpret_cast<FaissIndex*>(out);
    } CATCH_AND_HANDLE
}
