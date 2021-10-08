/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <cstdio>
#include <algorithm>

#include <omp.h>

#include <faiss/utils/utils.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/impl/AuxIndexStructures.h>
#include <faiss/impl/ScalarQuantizer.h>
#include <faiss/IndexSQHybrid.h>

namespace faiss {

/*******************************************************************
 * IndexIVFSQHybrid implementation
 ********************************************************************/

IndexIVFSQHybrid::IndexIVFSQHybrid (
            Index *quantizer, size_t d, size_t nlist,
            QuantizerType qtype,
            MetricType metric, bool encode_residual)
    : IndexIVF(quantizer, d, nlist, 0, metric),
      sq(d, qtype),
      by_residual(encode_residual)
{
    code_size = sq.code_size;
    // was not known at construction time
    invlists->code_size = code_size;
    is_trained = false;
}

IndexIVFSQHybrid::IndexIVFSQHybrid ():
    IndexIVF(),
    by_residual(true)
{
}

void IndexIVFSQHybrid::train_residual (idx_t n, const float *x)
{
    sq.train_residual(n, x, quantizer, by_residual, verbose);
}

void IndexIVFSQHybrid::encode_vectors(idx_t n, const float* x,
                                             const idx_t *list_nos,
                                             uint8_t * codes,
                                             bool include_listnos) const
{
    std::unique_ptr<Quantizer> squant (sq.select_quantizer ());
    size_t coarse_size = include_listnos ? coarse_code_size () : 0;
    memset(codes, 0, (code_size + coarse_size) * n);

#pragma omp parallel if(n > 1)
    {
        std::vector<float> residual (d);

#pragma omp for
        for (size_t i = 0; i < n; i++) {
            int64_t list_no = list_nos [i];
            if (list_no >= 0) {
                const float *xi = x + i * d;
                uint8_t *code = codes + i * (code_size + coarse_size);
                if (by_residual) {
                    quantizer->compute_residual (
                          xi, residual.data(), list_no);
                    xi = residual.data ();
                }
                if (coarse_size) {
                    encode_listno (list_no, code);
                }
                squant->encode_vector (xi, code + coarse_size);
            }
        }
    }
}

void IndexIVFSQHybrid::sa_decode (idx_t n, const uint8_t *codes,
                                                 float *x) const
{
    std::unique_ptr<Quantizer> squant (sq.select_quantizer ());
    size_t coarse_size = coarse_code_size ();

#pragma omp parallel if(n > 1)
    {
        std::vector<float> residual (d);

#pragma omp for
        for (size_t i = 0; i < n; i++) {
            const uint8_t *code = codes + i * (code_size + coarse_size);
            int64_t list_no = decode_listno (code);
            float *xi = x + i * d;
            squant->decode_vector (code + coarse_size, xi);
            if (by_residual) {
                quantizer->reconstruct (list_no, residual.data());
                for (size_t j = 0; j < d; j++) {
                    xi[j] += residual[j];
                }
            }
        }
    }
}



void IndexIVFSQHybrid::add_with_ids
       (idx_t n, const float * x, const idx_t *xids)
{
    FAISS_THROW_IF_NOT (is_trained);
    std::unique_ptr<int64_t []> idx (new int64_t [n]);
    quantizer->assign (n, x, idx.get());
    size_t nadd = 0;
    std::unique_ptr<Quantizer> squant(sq.select_quantizer ());

#pragma omp parallel reduction(+: nadd)
    {
        std::vector<float> residual (d);
        std::vector<uint8_t> one_code (code_size);
        int nt = omp_get_num_threads();
        int rank = omp_get_thread_num();

        // each thread takes care of a subset of lists
        for (size_t i = 0; i < n; i++) {
            int64_t list_no = idx [i];
            if (list_no >= 0 && list_no % nt == rank) {
                int64_t id = xids ? xids[i] : ntotal + i;

                const float * xi = x + i * d;
                if (by_residual) {
                    quantizer->compute_residual (xi, residual.data(), list_no);
                    xi = residual.data();
                }

                memset (one_code.data(), 0, code_size);
                squant->encode_vector (xi, one_code.data());

                invlists->add_entry (list_no, id, one_code.data());

                nadd++;

            }
        }
    }
    ntotal += n;
}





InvertedListScanner* IndexIVFSQHybrid::get_InvertedListScanner
    (bool store_pairs) const
{
    return sq.select_InvertedListScanner (metric_type, quantizer, store_pairs,
                                          by_residual);
}


void IndexIVFSQHybrid::reconstruct_from_offset (int64_t list_no,
                                                       int64_t offset,
                                                       float* recons) const
{
    std::vector<float> centroid(d);
    quantizer->reconstruct (list_no, centroid.data());

    const uint8_t* code = invlists->get_single_code (list_no, offset);
    sq.decode (code, recons, 1);
    for (int i = 0; i < d; ++i) {
        recons[i] += centroid[i];
    }
}




} // namespace faiss
