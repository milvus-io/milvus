/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/IndexScalarQuantizer.h>

#include <cstdio>
#include <algorithm>

#include <omp.h>

#include <faiss/utils/utils.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/impl/AuxIndexStructures.h>
#include <faiss/impl/ScalarQuantizer.h>

namespace faiss {



/*******************************************************************
 * IndexScalarQuantizer implementation
 ********************************************************************/

IndexScalarQuantizer::IndexScalarQuantizer
                      (int d, ScalarQuantizer::QuantizerType qtype,
                       MetricType metric):
          Index(d, metric),
          sq (d, qtype)
{
    is_trained =
        qtype == ScalarQuantizer::QT_fp16 ||
        qtype == ScalarQuantizer::QT_8bit_direct;
    code_size = sq.code_size;
}


IndexScalarQuantizer::IndexScalarQuantizer ():
    IndexScalarQuantizer(0, ScalarQuantizer::QT_8bit)
{}

void IndexScalarQuantizer::train(idx_t n, const float* x)
{
    sq.train(n, x);
    is_trained = true;
}

void IndexScalarQuantizer::add(idx_t n, const float* x)
{
    FAISS_THROW_IF_NOT (is_trained);
    codes.resize ((n + ntotal) * code_size);
    sq.compute_codes (x, &codes[ntotal * code_size], n);
    ntotal += n;
}


void IndexScalarQuantizer::search(
        idx_t n,
        const float* x,
        idx_t k,
        float* distances,
        idx_t* labels) const
{
    FAISS_THROW_IF_NOT (is_trained);
    FAISS_THROW_IF_NOT (metric_type == METRIC_L2 ||
                        metric_type == METRIC_INNER_PRODUCT);

#pragma omp parallel
    {
        InvertedListScanner* scanner = sq.select_InvertedListScanner
            (metric_type, nullptr, true);
        ScopeDeleter1<InvertedListScanner> del(scanner);

#pragma omp for
        for (size_t i = 0; i < n; i++) {
            float * D = distances + k * i;
            idx_t * I = labels + k * i;
            // re-order heap
            if (metric_type == METRIC_L2) {
                maxheap_heapify (k, D, I);
            } else {
                minheap_heapify (k, D, I);
            }
            scanner->set_query (x + i * d);
            scanner->scan_codes (ntotal, codes.data(),
                                 nullptr, D, I, k);

            // re-order heap
            if (metric_type == METRIC_L2) {
                maxheap_reorder (k, D, I);
            } else {
                minheap_reorder (k, D, I);
            }
        }
    }

}


DistanceComputer *IndexScalarQuantizer::get_distance_computer () const
{
    ScalarQuantizer::SQDistanceComputer *dc =
        sq.get_distance_computer (metric_type);
    dc->code_size = sq.code_size;
    dc->codes = codes.data();
    return dc;
}


void IndexScalarQuantizer::reset()
{
    codes.clear();
    ntotal = 0;
}

void IndexScalarQuantizer::reconstruct_n(
             idx_t i0, idx_t ni, float* recons) const
{
    std::unique_ptr<ScalarQuantizer::Quantizer> squant(sq.select_quantizer ());
    for (size_t i = 0; i < ni; i++) {
        squant->decode_vector(&codes[(i + i0) * code_size], recons + i * d);
    }
}

void IndexScalarQuantizer::reconstruct(idx_t key, float* recons) const
{
    reconstruct_n(key, 1, recons);
}

/* Codec interface */
size_t IndexScalarQuantizer::sa_code_size () const
{
    return sq.code_size;
}

void IndexScalarQuantizer::sa_encode (idx_t n, const float *x,
                      uint8_t *bytes) const
{
    FAISS_THROW_IF_NOT (is_trained);
    sq.compute_codes (x, bytes, n);
}

void IndexScalarQuantizer::sa_decode (idx_t n, const uint8_t *bytes,
                                              float *x) const
{
    FAISS_THROW_IF_NOT (is_trained);
    sq.decode(bytes, x, n);
}



/*******************************************************************
 * IndexIVFScalarQuantizer implementation
 ********************************************************************/

IndexIVFScalarQuantizer::IndexIVFScalarQuantizer (
            Index *quantizer, size_t d, size_t nlist,
            ScalarQuantizer::QuantizerType qtype,
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

IndexIVFScalarQuantizer::IndexIVFScalarQuantizer ():
    IndexIVF(),
    by_residual(true)
{
}

void IndexIVFScalarQuantizer::train_residual (idx_t n, const float *x)
{
    sq.train_residual(n, x, quantizer, by_residual, verbose);
}

void IndexIVFScalarQuantizer::encode_vectors(idx_t n, const float* x,
                                             const idx_t *list_nos,
                                             uint8_t * codes,
                                             bool include_listnos) const
{
    std::unique_ptr<ScalarQuantizer::Quantizer> squant (sq.select_quantizer ());
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

void IndexIVFScalarQuantizer::sa_decode (idx_t n, const uint8_t *codes,
                                                 float *x) const
{
    std::unique_ptr<ScalarQuantizer::Quantizer> squant (sq.select_quantizer ());
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



void IndexIVFScalarQuantizer::add_with_ids
       (idx_t n, const float * x, const idx_t *xids)
{
    FAISS_THROW_IF_NOT (is_trained);
    std::unique_ptr<int64_t []> idx (new int64_t [n]);
    quantizer->assign (n, x, idx.get());
    size_t nadd = 0;
    std::unique_ptr<ScalarQuantizer::Quantizer> squant(sq.select_quantizer ());

    DirectMapAdd dm_add (direct_map, n, xids);

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

                size_t ofs = invlists->add_entry (list_no, id, one_code.data());

                dm_add.add (i, list_no, ofs);
                nadd++;

            } else if (rank == 0 && list_no == -1) {
                dm_add.add (i, -1, 0);
            }
        }
    }


    ntotal += n;
}





InvertedListScanner* IndexIVFScalarQuantizer::get_InvertedListScanner
    (bool store_pairs) const
{
    return sq.select_InvertedListScanner (metric_type, quantizer, store_pairs,
                                          by_residual);
}


void IndexIVFScalarQuantizer::reconstruct_from_offset (int64_t list_no,
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
