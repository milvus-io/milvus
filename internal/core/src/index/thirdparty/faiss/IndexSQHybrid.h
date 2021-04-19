/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_INDEX_SQ_HYBRID_H
#define FAISS_INDEX_SQ_HYBRID_H

#include <stdint.h>
#include <vector>

#include <faiss/IndexIVF.h>
#include <faiss/impl/ScalarQuantizer.h>
#include <faiss/impl/ScalarQuantizerOp.h>


namespace faiss {

 /** An IVF implementation where the components of the residuals are
 * encoded with a scalar uniform quantizer. All distance computations
 * are asymmetric, so the encoded vectors are decoded and approximate
 * distances are computed.
 */

struct IndexIVFSQHybrid: IndexIVF {
    ScalarQuantizer sq;
    bool by_residual;

    IndexIVFSQHybrid(Index *quantizer, size_t d, size_t nlist,
                            QuantizerType qtype,
                            MetricType metric = METRIC_L2,
                            bool encode_residual = true);

    IndexIVFSQHybrid();

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
