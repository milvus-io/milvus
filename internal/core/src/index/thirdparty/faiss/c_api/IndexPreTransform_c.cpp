/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c++ -*-

#include "IndexPreTransform_c.h"
#include "IndexPreTransform.h"
#include "macros_impl.h"

using faiss::Index;
using faiss::IndexPreTransform;

DEFINE_DESTRUCTOR(IndexPreTransform)
DEFINE_INDEX_DOWNCAST(IndexPreTransform)

DEFINE_GETTER_PERMISSIVE(IndexPreTransform, FaissIndex*, index)
