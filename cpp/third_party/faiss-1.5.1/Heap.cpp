/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

/* Function for soft heap */

#include "Heap.h"


namespace faiss {


template <typename C>
void HeapArray<C>::heapify ()
{
#pragma omp parallel for
    for (size_t j = 0; j < nh; j++)
        heap_heapify<C> (k, val + j * k, ids + j * k);
}

template <typename C>
void HeapArray<C>::reorder ()
{
#pragma omp parallel for
    for (size_t j = 0; j < nh; j++)
        heap_reorder<C> (k, val + j * k, ids + j * k);
}

template <typename C>
void HeapArray<C>::addn (size_t nj, const T *vin, TI j0,
                         size_t i0, long ni)
{
    if (ni == -1) ni = nh;
    assert (i0 >= 0 && i0 + ni <= nh);
#pragma omp parallel for
    for (size_t i = i0; i < i0 + ni; i++) {
        T * __restrict simi = get_val(i);
        TI * __restrict idxi = get_ids (i);
        const T *ip_line = vin + (i - i0) * nj;

        for (size_t j = 0; j < nj; j++) {
            T ip = ip_line [j];
            if (C::cmp(simi[0], ip)) {
                heap_pop<C> (k, simi, idxi);
                heap_push<C> (k, simi, idxi, ip, j + j0);
            }
        }
    }
}

template <typename C>
void HeapArray<C>::addn_with_ids (
     size_t nj, const T *vin, const TI *id_in,
     long id_stride, size_t i0, long ni)
{
    if (id_in == nullptr) {
        addn (nj, vin, 0, i0, ni);
        return;
    }
    if (ni == -1) ni = nh;
    assert (i0 >= 0 && i0 + ni <= nh);
#pragma omp parallel for
    for (size_t i = i0; i < i0 + ni; i++) {
        T * __restrict simi = get_val(i);
        TI * __restrict idxi = get_ids (i);
        const T *ip_line = vin + (i - i0) * nj;
        const TI *id_line = id_in + (i - i0) * id_stride;

        for (size_t j = 0; j < nj; j++) {
            T ip = ip_line [j];
            if (C::cmp(simi[0], ip)) {
                heap_pop<C> (k, simi, idxi);
                heap_push<C> (k, simi, idxi, ip, id_line [j]);
            }
        }
    }
}

template <typename C>
void HeapArray<C>::per_line_extrema (
                   T * out_val,
                   TI * out_ids) const
{
#pragma omp parallel for
    for (size_t j = 0; j < nh; j++) {
        long imin = -1;
        typename C::T xval = C::Crev::neutral ();
        const typename C::T * x_ = val + j * k;
        for (size_t i = 0; i < k; i++)
            if (C::cmp (x_[i], xval)) {
                xval = x_[i];
                imin = i;
            }
        if (out_val)
            out_val[j] = xval;

        if (out_ids) {
            if (ids && imin != -1)
                out_ids[j] = ids [j * k + imin];
            else
                out_ids[j] = imin;
        }
    }
}




// explicit instanciations

template struct HeapArray<CMin <float, long> >;
template struct HeapArray<CMax <float, long> >;
template struct HeapArray<CMin <int, long> >;
template struct HeapArray<CMax <int, long> >;


}  // END namespace fasis
