/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c -*-

#ifndef FAISS_INDEX_PRETRANSFORM_C_H
#define FAISS_INDEX_PRETRANSFORM_C_H

#include "faiss_c.h"
#include "Index_c.h"

#ifdef __cplusplus
extern "C" {
#endif

FAISS_DECLARE_CLASS(IndexPreTransform)
FAISS_DECLARE_DESTRUCTOR(IndexPreTransform)
FAISS_DECLARE_INDEX_DOWNCAST(IndexPreTransform)

FAISS_DECLARE_GETTER(IndexPreTransform, FaissIndex*, index)

#ifdef __cplusplus
}
#endif


#endif
