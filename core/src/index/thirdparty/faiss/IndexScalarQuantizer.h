/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_INDEX_SCALAR_QUANTIZER_H
#define FAISS_INDEX_SCALAR_QUANTIZER_H

#include <stdint.h>
#include <vector>

#include <faiss/IndexIVF.h>
#include <faiss/impl/ScalarQuantizer.h>


namespace faiss {

/**
 * The uniform quantizer has a range [vmin, vmax]. The range can be
 * the same for all dimensions (uniform) or specific per dimension
 * (default).
 */




struct IndexScalarQuantizer: Index {
    /// Used to encode the vectors
    ScalarQuantizer sq;

    /// Codes. Size ntotal * pq.code_size
    std::vector<uint8_t> codes;

    size_t code_size;

    /** Constructor.
     *
     * @param d      dimensionality of the input vectors
     * @param M      number of subquantizers
     * @param nbits  number of bit per subvector index
     */
    IndexScalarQuantizer (int d,
                          ScalarQuantizer::QuantizerType qtype,
                          MetricType metric = METRIC_L2);

    IndexScalarQuantizer ();

    void train(idx_t n, const float* x) override;

    void add(idx_t n, const float* x) override;

    void search(
        idx_t n,
        const float* x,
        idx_t k,
        float* distances,
        idx_t* labels) const override;

    void reset() override;

    void reconstruct_n(idx_t i0, idx_t ni, float* recons) const override;

    void reconstruct(idx_t key, float* recons) const override;

    DistanceComputer *get_distance_computer () const override;

    /* standalone codec interface */
    size_t sa_code_size () const override;

    void sa_encode (idx_t n, const float *x,
                          uint8_t *bytes) const override;

    void sa_decode (idx_t n, const uint8_t *bytes,
                            float *x) const override;


};


 /** An IVF implementation where the components of the residuals are
 * encoded with a scalar uniform quantizer. All distance computations
 * are asymmetric, so the encoded vectors are decoded and approximate
 * distances are computed.
 */

struct IndexIVFScalarQuantizer: IndexIVF {
    ScalarQuantizer sq;
    bool by_residual;

    IndexIVFScalarQuantizer(Index *quantizer, size_t d, size_t nlist,
                            ScalarQuantizer::QuantizerType qtype,
                            MetricType metric = METRIC_L2,
                            bool encode_residual = true);

    IndexIVFScalarQuantizer();

    void train_residual(idx_t n, const float* x) override;

    void encode_vectors(idx_t n, const float* x,
                        const idx_t *list_nos,
                        uint8_t * codes,
                        bool include_listnos=false) const override;

    void add_with_ids(idx_t n, const float* x, const idx_t* xids) override;

    InvertedListScanner *get_InvertedListScanner (bool store_pairs)
        const override;


    void reconstruct_from_offset (int64_t list_no, int64_t offset,
                                  float* recons) const override;

    /* standalone codec interface */
    void sa_decode (idx_t n, const uint8_t *bytes,
                            float *x) const override;

};


}


#endif
