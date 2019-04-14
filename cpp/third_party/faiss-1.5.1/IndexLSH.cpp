/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include "IndexLSH.h"

#include <cstdio>
#include <cstring>

#include <algorithm>

#include "utils.h"
#include "hamming.h"
#include "FaissAssert.h"


namespace faiss {

/***************************************************************
 * IndexLSH
 ***************************************************************/


IndexLSH::IndexLSH (idx_t d, int nbits, bool rotate_data, bool train_thresholds):
    Index(d), nbits(nbits), rotate_data(rotate_data),
    train_thresholds (train_thresholds), rrot(d, nbits)
{
    is_trained = !train_thresholds;

    bytes_per_vec = (nbits + 7) / 8;

    if (rotate_data) {
        rrot.init(5);
    } else {
        FAISS_THROW_IF_NOT (d >= nbits);
    }
}

IndexLSH::IndexLSH ():
    nbits (0), bytes_per_vec(0), rotate_data (false), train_thresholds (false)
{
}


const float * IndexLSH::apply_preprocess (idx_t n, const float *x) const
{

    float *xt = nullptr;
    if (rotate_data) {
        // also applies bias if exists
        xt = rrot.apply (n, x);
    } else if (d != nbits) {
        xt = new float [nbits * n];
        float *xp = xt;
        for (idx_t i = 0; i < n; i++) {
            const float *xl = x + i * d;
            for (int j = 0; j < nbits; j++)
                *xp++ = xl [j];
        }
    }

    if (train_thresholds) {

        if (xt == NULL) {
            xt = new float [nbits * n];
            memcpy (xt, x, sizeof(*x) * n * nbits);
        }

        float *xp = xt;
        for (idx_t i = 0; i < n; i++)
            for (int j = 0; j < nbits; j++)
                *xp++ -= thresholds [j];
    }

    return xt ? xt : x;
}



void IndexLSH::train (idx_t n, const float *x)
{
    if (train_thresholds) {
        thresholds.resize (nbits);
        train_thresholds = false;
        const float *xt = apply_preprocess (n, x);
        ScopeDeleter<float> del (xt == x ? nullptr : xt);
        train_thresholds = true;

        float * transposed_x = new float [n * nbits];
        ScopeDeleter<float> del2 (transposed_x);

        for (idx_t i = 0; i < n; i++)
            for (idx_t j = 0; j < nbits; j++)
                transposed_x [j * n + i] = xt [i * nbits + j];

        for (idx_t i = 0; i < nbits; i++) {
            float *xi = transposed_x + i * n;
            // std::nth_element
            std::sort (xi, xi + n);
            if (n % 2 == 1)
                thresholds [i] = xi [n / 2];
            else
                thresholds [i] = (xi [n / 2 - 1] + xi [n / 2]) / 2;

        }
    }
    is_trained = true;
}


void IndexLSH::add (idx_t n, const float *x)
{
    FAISS_THROW_IF_NOT (is_trained);
    const float *xt = apply_preprocess (n, x);
    ScopeDeleter<float> del (xt == x ? nullptr : xt);

    codes.resize ((ntotal + n) * bytes_per_vec);
    fvecs2bitvecs (xt, &codes[ntotal * bytes_per_vec], nbits, n);
    ntotal += n;
}


void IndexLSH::search (
        idx_t n,
        const float *x,
        idx_t k,
        float *distances,
        idx_t *labels) const
{
    FAISS_THROW_IF_NOT (is_trained);
    const float *xt = apply_preprocess (n, x);
    ScopeDeleter<float> del (xt == x ? nullptr : xt);

    uint8_t * qcodes = new uint8_t [n * bytes_per_vec];
    ScopeDeleter<uint8_t> del2 (qcodes);

    fvecs2bitvecs (xt, qcodes, nbits, n);

    int * idistances = new int [n * k];
    ScopeDeleter<int> del3 (idistances);

    int_maxheap_array_t res = { size_t(n), size_t(k), labels, idistances};

    hammings_knn_hc (&res, qcodes, codes.data(),
                     ntotal, bytes_per_vec, true);


    // convert distances to floats
    for (int i = 0; i < k * n; i++)
        distances[i] = idistances[i];

}


void IndexLSH::transfer_thresholds (LinearTransform *vt) {
    if (!train_thresholds) return;
    FAISS_THROW_IF_NOT (nbits == vt->d_out);
    if (!vt->have_bias) {
        vt->b.resize (nbits, 0);
        vt->have_bias = true;
    }
    for (int i = 0; i < nbits; i++)
        vt->b[i] -= thresholds[i];
    train_thresholds = false;
    thresholds.clear();
}

void IndexLSH::reset() {
    codes.clear();
    ntotal = 0;
}


} // namespace faiss
