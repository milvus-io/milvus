/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once


#include <stdint.h>
#include <stdio.h>

namespace faiss {


/** partitions the table into 0:q and q:n where all elements above q are >= all
 * elements below q (for C = CMax, for CMin comparisons are reversed)
 *
 * Returns the partition threshold. The elements q:n are destroyed on output.
 */
template<class C>
typename C::T partition_fuzzy(
    typename C::T *vals, typename C::TI * ids, size_t n,
    size_t q_min, size_t q_max, size_t * q_out);



} // namespace faiss
