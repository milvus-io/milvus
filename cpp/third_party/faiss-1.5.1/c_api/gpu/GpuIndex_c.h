/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c -*-

#ifndef FAISS_GPU_INDEX_C_H
#define FAISS_GPU_INDEX_C_H

#include "faiss_c.h"

#ifdef __cplusplus
extern "C" {
#endif

FAISS_DECLARE_CLASS(GpuIndexConfig)

FAISS_DECLARE_GETTER(GpuIndexConfig, int, device)

FAISS_DECLARE_CLASS_INHERITED(GpuIndex, Index)

#ifdef __cplusplus
}
#endif

#endif
