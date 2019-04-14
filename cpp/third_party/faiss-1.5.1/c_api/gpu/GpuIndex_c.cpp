/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c++ -*-

#include "gpu/GpuIndex.h"
#include "GpuIndex_c.h"
#include "macros_impl.h"

using faiss::gpu::GpuIndexConfig;

DEFINE_GETTER(GpuIndexConfig, int, device)
