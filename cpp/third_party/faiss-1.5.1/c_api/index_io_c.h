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


#ifndef FAISS_INDEX_IO_C_H
#define FAISS_INDEX_IO_C_H

#include <stdio.h>
#include "faiss_c.h"
#include "Index_c.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Write index to a file.
 * This is equivalent to `faiss::write_index` when a file descriptor is provided.
 */
int faiss_write_index(const FaissIndex *idx, FILE *f);

/** Write index to a file.
 * This is equivalent to `faiss::write_index` when a file path is provided.
 */
int faiss_write_index_fname(const FaissIndex *idx, const char *fname);

#define FAISS_IO_FLAG_MMAP 1
#define FAISS_IO_FLAG_READ_ONLY 2

/** Read index from a file.
 * This is equivalent to `faiss:read_index` when a file descriptor is given.
 */
int faiss_read_index(FILE *f, int io_flags, FaissIndex **p_out);

/** Read index from a file.
 * This is equivalent to `faiss:read_index` when a file path is given.
 */
int faiss_read_index_fname(const char *fname, int io_flags, FaissIndex **p_out);

/* cloning functions */

/** Clone an index. This is equivalent to `faiss::clone_index` */
int faiss_clone_index (const FaissIndex *, FaissIndex ** p_out);

#ifdef __cplusplus
}
#endif
#endif
