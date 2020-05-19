/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-


#include <faiss/IndexLattice.h>
#include <faiss/utils/hamming.h>    // for the bitstring routines
#include <faiss/impl/FaissAssert.h>
#include <faiss/utils/distances.h>

namespace faiss {


IndexLattice::IndexLattice (idx_t d, int nsq, int scale_nbit, int r2):
    Index (d),
    nsq (nsq),
    dsq (d / nsq),
    zn_sphere_codec (dsq, r2),
    scale_nbit (scale_nbit)
{
    FAISS_THROW_IF_NOT (d % nsq == 0);

    lattice_nbit = 0;
    while (!( ((uint64_t)1 << lattice_nbit) >= zn_sphere_codec.nv)) {
        lattice_nbit++;
    }

    int total_nbit = (lattice_nbit + scale_nbit) * nsq;

    code_size = (total_nbit + 7) / 8;

    is_trained = false;
}

void IndexLattice::train(idx_t n, const float* x)
{
    // compute ranges per sub-block
    trained.resize (nsq * 2);
    float * mins = trained.data();
    float * maxs = trained.data() + nsq;
    for (int sq = 0; sq < nsq; sq++) {
        mins[sq] = HUGE_VAL;
        maxs[sq] = -1;
    }

    for (idx_t i = 0; i < n; i++) {
        for (int sq = 0; sq < nsq; sq++) {
            float norm2 = fvec_norm_L2sqr (x + i * d + sq * dsq, dsq);
            if (norm2 > maxs[sq]) maxs[sq] = norm2;
            if (norm2 < mins[sq]) mins[sq] = norm2;
        }
    }

    for (int sq = 0; sq < nsq; sq++) {
        mins[sq] = sqrtf (mins[sq]);
        maxs[sq] = sqrtf (maxs[sq]);
    }

    is_trained = true;
}

/* The standalone codec interface */
size_t IndexLattice::sa_code_size () const
{
    return code_size;
}



void IndexLattice::sa_encode (idx_t n, const float *x, uint8_t *codes) const
{

    const float * mins = trained.data();
    const float * maxs = mins + nsq;
    int64_t sc = int64_t(1) << scale_nbit;

#pragma omp parallel for
    for (idx_t i = 0; i < n; i++) {
        BitstringWriter wr(codes + i * code_size, code_size);
        const float *xi = x + i * d;
        for (int j = 0; j < nsq; j++) {
            float nj =
                (sqrtf(fvec_norm_L2sqr(xi, dsq)) - mins[j])
                * sc / (maxs[j] - mins[j]);
            if (nj < 0) nj = 0;
            if (nj >= sc) nj = sc - 1;
            wr.write((int64_t)nj, scale_nbit);
            wr.write(zn_sphere_codec.encode(xi), lattice_nbit);
            xi += dsq;
        }
    }
}

void IndexLattice::sa_decode (idx_t n, const uint8_t *codes, float *x) const
{
    const float * mins = trained.data();
    const float * maxs = mins + nsq;
    float sc = int64_t(1) << scale_nbit;
    float r = sqrtf(zn_sphere_codec.r2);

#pragma omp parallel for
    for (idx_t i = 0; i < n; i++) {
        BitstringReader rd(codes + i * code_size, code_size);
        float *xi = x + i * d;
        for (int j = 0; j < nsq; j++) {
            float norm =
                (rd.read (scale_nbit) + 0.5) *
                (maxs[j] - mins[j]) / sc + mins[j];
            norm /= r;
            zn_sphere_codec.decode (rd.read (lattice_nbit), xi);
            for (int l = 0; l < dsq; l++) {
                xi[l] *= norm;
            }
            xi += dsq;
        }
    }
}

void IndexLattice::add(idx_t , const float* )
{
    FAISS_THROW_MSG("not implemented");
}


void  IndexLattice::search(idx_t , const float* , idx_t ,
                           float* , idx_t* ) const
{
    FAISS_THROW_MSG("not implemented");
}


void IndexLattice::reset()
{
    FAISS_THROW_MSG("not implemented");
}


}  // namespace faiss
