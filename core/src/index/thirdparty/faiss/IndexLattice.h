/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_INDEX_LATTICE_H
#define FAISS_INDEX_LATTICE_H


#include <vector>

#include <faiss/IndexIVF.h>
#include <faiss/impl/lattice_Zn.h>

namespace faiss {





/** Index that encodes a vector with a series of Zn lattice quantizers
 */
struct IndexLattice: Index {

    /// number of sub-vectors
    int nsq;
    /// dimension of sub-vectors
    size_t dsq;

    /// the lattice quantizer
    ZnSphereCodecAlt zn_sphere_codec;

    /// nb bits used to encode the scale, per subvector
    int scale_nbit, lattice_nbit;
    /// total, in bytes
    size_t code_size;

    /// mins and maxes of the vector norms, per subquantizer
    std::vector<float> trained;

    IndexLattice (idx_t d, int nsq, int scale_nbit, int r2);

    void train(idx_t n, const float* x) override;

    /* The standalone codec interface */
    size_t sa_code_size () const override;

    void sa_encode (idx_t n, const float *x,
                          uint8_t *bytes) const override;

    void sa_decode (idx_t n, const uint8_t *bytes,
                            float *x) const override;

    /// not implemented
    void add(idx_t n, const float* x) override;
    void search(idx_t n, const float* x, idx_t k,
                float* distances, idx_t* labels) const override;
    void reset() override;

};

} // namespace faiss

#endif
