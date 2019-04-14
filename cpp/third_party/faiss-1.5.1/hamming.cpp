/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

/*
 * Implementation of Hamming related functions (distances, smallest distance
 * selection with regular heap|radix and probabilistic heap|radix.
 *
 * IMPLEMENTATION NOTES
 * Bitvectors are generally assumed to be multiples of 64 bits.
 *
 * hamdis_t is used for distances because at this time
 * it is not clear how we will need to balance
 * - flexibility in vector size (unclear more than 2^16 or even 2^8 bitvectors)
 * - memory usage
 * - cache-misses when dealing with large volumes of data (lower bits is better)
 *
 * The hamdis_t should optimally be compatibe with one of the Torch Storage
 * (Byte,Short,Long) and therefore should be signed for 2-bytes and 4-bytes
*/

#include "hamming.h"

#include <vector>
#include <memory>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <assert.h>
#include <limits.h>

#include "Heap.h"
#include "FaissAssert.h"

static const size_t BLOCKSIZE_QUERY = 8192;


namespace faiss {

size_t hamming_batch_size = 65536;

static const uint8_t hamdis_tab_ham_bytes[256] = {
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8
};


/* Elementary Hamming distance computation: unoptimized  */
template <size_t nbits, typename T>
T hamming (const uint8_t *bs1,
           const uint8_t *bs2)
{
    const size_t nbytes = nbits / 8;
    size_t i;
    T h = 0;
    for (i = 0; i < nbytes; i++)
        h += (T) hamdis_tab_ham_bytes[bs1[i]^bs2[i]];
    return h;
}


/* Hamming distances for multiples of 64 bits */
template <size_t nbits>
hamdis_t hamming (const uint64_t * bs1, const uint64_t * bs2)
{
    const size_t nwords = nbits / 64;
    size_t i;
    hamdis_t h = 0;
    for (i = 0; i < nwords; i++)
        h += popcount64 (bs1[i] ^ bs2[i]);
    return h;
}



/* specialized (optimized) functions */
template <>
hamdis_t hamming<64> (const uint64_t * pa, const uint64_t * pb)
{
    return popcount64 (pa[0] ^ pb[0]);
}


template <>
hamdis_t hamming<128> (const uint64_t *pa, const uint64_t *pb)
{
    return popcount64 (pa[0] ^ pb[0]) + popcount64(pa[1] ^ pb[1]);
}


template <>
hamdis_t hamming<256> (const uint64_t * pa, const uint64_t * pb)
{
    return  popcount64 (pa[0] ^ pb[0])
          + popcount64 (pa[1] ^ pb[1])
          + popcount64 (pa[2] ^ pb[2])
          + popcount64 (pa[3] ^ pb[3]);
}


/* Hamming distances for multiple of 64 bits */
hamdis_t hamming (
        const uint64_t * bs1,
        const uint64_t * bs2,
        size_t nwords)
{
    size_t i;
    hamdis_t h = 0;
    for (i = 0; i < nwords; i++)
        h += popcount64 (bs1[i] ^ bs2[i]);
    return h;
}



template <size_t nbits>
void hammings (
        const uint64_t * bs1,
        const uint64_t * bs2,
        size_t n1, size_t n2,
        hamdis_t * dis)

{
    size_t i, j;
    const size_t nwords = nbits / 64;
    for (i = 0; i < n1; i++) {
      const uint64_t * __restrict bs1_ = bs1 + i * nwords;
      hamdis_t * __restrict dis_ = dis + i * n2;
      for (j = 0; j < n2; j++)
        dis_[j] = hamming<nbits>(bs1_, bs2 + j * nwords);
    }
}



void hammings (
        const uint64_t * bs1,
        const uint64_t * bs2,
        size_t n1,
        size_t n2,
        size_t nwords,
        hamdis_t * __restrict dis)
{
    size_t i, j;
    n1 *= nwords;
    n2 *= nwords;
    for (i = 0; i < n1; i+=nwords) {
        const uint64_t * bs1_ = bs1+i;
        for (j = 0; j < n2; j+=nwords)
            dis[j] = hamming (bs1_, bs2+j, nwords);
    }
}




/* Count number of matches given a max threshold */
template <size_t nbits>
void hamming_count_thres (
        const uint64_t * bs1,
        const uint64_t * bs2,
        size_t n1,
        size_t n2,
        hamdis_t ht,
        size_t * nptr)
{
    const size_t nwords = nbits / 64;
    size_t i, j, posm = 0;
    const uint64_t * bs2_ = bs2;

    for (i = 0; i < n1; i++) {
        bs2 = bs2_;
        for (j = 0; j < n2; j++) {
            /* collect the match only if this satisfies the threshold */
            if (hamming <nbits> (bs1, bs2) <= ht)
                posm++;
            bs2 += nwords;
        }
        bs1 += nwords;  /* next signature */
    }
    *nptr = posm;
}


template <size_t nbits>
void crosshamming_count_thres (
        const uint64_t * dbs,
        size_t n,
        int ht,
        size_t * nptr)
{
    const size_t nwords = nbits / 64;
    size_t i, j, posm = 0;
    const uint64_t * bs1 = dbs;
    for (i = 0; i < n; i++) {
        const uint64_t * bs2 = bs1 + 2;
        for (j = i + 1; j < n; j++) {
            /* collect the match only if this satisfies the threshold */
            if (hamming <nbits> (bs1, bs2) <= ht)
                posm++;
            bs2 += nwords;
        }
        bs1 += nwords;
    }
    *nptr = posm;
}


template <size_t nbits>
size_t match_hamming_thres (
        const uint64_t * bs1,
        const uint64_t * bs2,
        size_t n1,
        size_t n2,
        int ht,
        long * idx,
        hamdis_t * hams)
{
    const size_t nwords = nbits / 64;
    size_t i, j, posm = 0;
    hamdis_t h;
    const uint64_t * bs2_ = bs2;
    for (i = 0; i < n1; i++) {
        bs2 = bs2_;
        for (j = 0; j < n2; j++) {
            /* Here perform the real work of computing the distance */
            h = hamming <nbits> (bs1, bs2);

            /* collect the match only if this satisfies the threshold */
            if (h <= ht) {
                /* Enough space to store another match ? */
                *idx = i; idx++;
                *idx = j; idx++;
                *hams = h;
                hams++;
                posm++;
            }
            bs2+=nwords;  /* next signature */
        }
        bs1+=nwords;
    }
    return posm;
}


/* Return closest neighbors w.r.t Hamming distance, using a heap. */
template <class HammingComputer>
static
void hammings_knn_hc (
        int bytes_per_code,
        int_maxheap_array_t * ha,
        const uint8_t * bs1,
        const uint8_t * bs2,
        size_t n2,
        bool order = true,
        bool init_heap = true)
{
    size_t k = ha->k;
    if (init_heap) ha->heapify ();

    const size_t block_size = hamming_batch_size;
    for (size_t j0 = 0; j0 < n2; j0 += block_size) {
      const size_t j1 = std::min(j0 + block_size, n2);
#pragma omp parallel for
      for (size_t i = 0; i < ha->nh; i++) {
        HammingComputer hc (bs1 + i * bytes_per_code, bytes_per_code);

        const uint8_t * bs2_ = bs2 + j0 * bytes_per_code;
        hamdis_t dis;
        hamdis_t * __restrict bh_val_ = ha->val + i * k;
        long * __restrict bh_ids_ = ha->ids + i * k;
        size_t j;
        for (j = j0; j < j1; j++, bs2_+= bytes_per_code) {
          dis = hc.hamming (bs2_);
          if (dis < bh_val_[0]) {
            faiss::maxheap_pop<hamdis_t> (k, bh_val_, bh_ids_);
            faiss::maxheap_push<hamdis_t> (k, bh_val_, bh_ids_, dis, j);
          }
        }
      }
    }
    if (order) ha->reorder ();
 }

/* Return closest neighbors w.r.t Hamming distance, using max count. */
template <class HammingComputer>
static
void hammings_knn_mc (
        int bytes_per_code,
        const uint8_t *a,
        const uint8_t *b,
        size_t na,
        size_t nb,
        size_t k,
        int32_t *distances,
        long *labels)
{
  const int nBuckets = bytes_per_code * 8 + 1;
  std::vector<int> all_counters(na * nBuckets, 0);
  std::unique_ptr<long[]> all_ids_per_dis(new long[na * nBuckets * k]);

  std::vector<HCounterState<HammingComputer>> cs;
  for (size_t i = 0; i < na; ++i) {
    cs.push_back(HCounterState<HammingComputer>(
                   all_counters.data() + i * nBuckets,
                   all_ids_per_dis.get() + i * nBuckets * k,
                   a + i * bytes_per_code,
                   8 * bytes_per_code,
                   k
                 ));
  }

  const size_t block_size = hamming_batch_size;
  for (size_t j0 = 0; j0 < nb; j0 += block_size) {
    const size_t j1 = std::min(j0 + block_size, nb);
#pragma omp parallel for
    for (size_t i = 0; i < na; ++i) {
      for (size_t j = j0; j < j1; ++j) {
        cs[i].update_counter(b + j * bytes_per_code, j);
      }
    }
  }

  for (size_t i = 0; i < na; ++i) {
    HCounterState<HammingComputer>& csi = cs[i];

    int nres = 0;
    for (int b = 0; b < nBuckets && nres < k; b++) {
      for (int l = 0; l < csi.counters[b] && nres < k; l++) {
        labels[i * k + nres] = csi.ids_per_dis[b * k + l];
        distances[i * k + nres] = b;
        nres++;
      }
    }
    while (nres < k) {
      labels[i * k + nres] = -1;
      distances[i * k + nres] = std::numeric_limits<int32_t>::max();
      ++nres;
    }
  }
}



// works faster than the template version
static
void hammings_knn_hc_1 (
        int_maxheap_array_t * ha,
        const uint64_t * bs1,
        const uint64_t * bs2,
        size_t n2,
        bool order = true,
        bool init_heap = true)
{
    const size_t nwords = 1;
    size_t k = ha->k;


    if (init_heap) {
        ha->heapify ();
    }

#pragma omp parallel for
    for (size_t i = 0; i < ha->nh; i++) {
        const uint64_t bs1_ = bs1 [i];
        const uint64_t * bs2_ = bs2;
        hamdis_t dis;
        hamdis_t * bh_val_ = ha->val + i * k;
        hamdis_t bh_val_0 = bh_val_[0];
        long * bh_ids_ = ha->ids + i * k;
        size_t j;
        for (j = 0; j < n2; j++, bs2_+= nwords) {
            dis = popcount64 (bs1_ ^ *bs2_);
            if (dis < bh_val_0) {
                faiss::maxheap_pop<hamdis_t> (k, bh_val_, bh_ids_);
                faiss::maxheap_push<hamdis_t> (k, bh_val_, bh_ids_, dis, j);
                bh_val_0 = bh_val_[0];
            }
        }
    }
    if (order) {
        ha->reorder ();
    }
}




/* Functions to maps vectors to bits. Assume proper allocation done beforehand,
   meaning that b should be be able to receive as many bits as x may produce. */

/*
 * dimension 0 corresponds to the least significant bit of b[0], or
 * equivalently to the lsb of the first byte that is stored.
 */
void fvec2bitvec (const float * x, uint8_t * b, size_t d)
{
    for (int i = 0; i < d; i += 8) {
        uint8_t w = 0;
        uint8_t mask = 1;
        int nj = i + 8 <= d ? 8 : d - i;
        for (int j = 0; j < nj; j++) {
            if (x[i + j] >= 0)
                w |= mask;
            mask <<= 1;
        }
        *b = w;
        b++;
    }
}



/* Same but for n vectors.
   Ensure that the ouptut b is byte-aligned (pad with 0s). */
void fvecs2bitvecs (const float * x, uint8_t * b, size_t d, size_t n)
{
    const long ncodes = ((d + 7) / 8);
#pragma omp parallel for
    for (size_t i = 0; i < n; i++)
        fvec2bitvec (x + i * d, b + i * ncodes, d);
}


/* Reverse bit (NOT a optimized function, only used for print purpose) */
static uint64_t uint64_reverse_bits (uint64_t b)
{
    int i;
    uint64_t revb = 0;
    for (i = 0; i < 64; i++) {
        revb <<= 1;
        revb |= b & 1;
        b >>= 1;
    }
    return revb;
}


/* print the bit vector */
void bitvec_print (const uint8_t * b, size_t d)
{
    size_t i, j;
    for (i = 0; i < d; ) {
        uint64_t brev = uint64_reverse_bits (* (uint64_t *) b);
        for (j = 0; j < 64 && i < d; j++, i++) {
            printf ("%d", (int) (brev & 1));
            brev >>= 1;
        }
        b += 8;
        printf (" ");
    }
}





/*----------------------------------------*/
/* Hamming distance computation and k-nn  */


#define C64(x) ((uint64_t *)x)


/* Compute a set of Hamming distances */
void hammings (
        const uint8_t * a,
        const uint8_t * b,
        size_t na, size_t nb,
        size_t ncodes,
        hamdis_t * __restrict dis)
{
    FAISS_THROW_IF_NOT (ncodes % 8 == 0);
    switch (ncodes) {
        case 8:
            faiss::hammings <64>  (C64(a), C64(b), na, nb, dis); return;
        case 16:
            faiss::hammings <128> (C64(a), C64(b), na, nb, dis); return;
        case 32:
            faiss::hammings <256> (C64(a), C64(b), na, nb, dis); return;
        case 64:
            faiss::hammings <512> (C64(a), C64(b), na, nb, dis); return;
        default:
            faiss::hammings (C64(a), C64(b), na, nb, ncodes * 8, dis); return;
    }
}

void hammings_knn(
    int_maxheap_array_t *ha,
    const uint8_t *a,
    const uint8_t *b,
    size_t nb,
    size_t ncodes,
    int order)
{
    hammings_knn_hc(ha, a, b, nb, ncodes, order);
}
void hammings_knn_hc (
        int_maxheap_array_t * ha,
        const uint8_t * a,
        const uint8_t * b,
        size_t nb,
        size_t ncodes,
        int order)
{
    switch (ncodes) {
    case 4:
        hammings_knn_hc<faiss::HammingComputer4>
            (4, ha, a, b, nb, order, true);
        break;
    case 8:
        hammings_knn_hc_1 (ha, C64(a), C64(b), nb, order, true);
        // hammings_knn_hc<faiss::HammingComputer8>
        //      (8, ha, a, b, nb, order, true);
        break;
    case 16:
        hammings_knn_hc<faiss::HammingComputer16>
            (16, ha, a, b, nb, order, true);
        break;
    case 32:
        hammings_knn_hc<faiss::HammingComputer32>
            (32, ha, a, b, nb, order, true);
        break;
    default:
        if(ncodes % 8 == 0) {
            hammings_knn_hc<faiss::HammingComputerM8>
                (ncodes, ha, a, b, nb, order, true);
        } else {
            hammings_knn_hc<faiss::HammingComputerDefault>
                (ncodes, ha, a, b, nb, order, true);

        }
    }
}

void hammings_knn_mc(
    const uint8_t * a,
    const uint8_t * b,
    size_t na,
    size_t nb,
    size_t k,
    size_t ncodes,
    int32_t *distances,
    long *labels)
{
    switch (ncodes) {
    case 4:
        hammings_knn_mc<faiss::HammingComputer4>(
          4, a, b, na, nb, k, distances, labels
        );
        break;
    case 8:
        // TODO(hoss): Write analog to hammings_knn_hc_1
        // hammings_knn_hc_1 (ha, C64(a), C64(b), nb, order, true);
        hammings_knn_mc<faiss::HammingComputer8>(
          8, a, b, na, nb, k, distances, labels
        );
        break;
    case 16:
        hammings_knn_mc<faiss::HammingComputer16>(
          16, a, b, na, nb, k, distances, labels
        );
        break;
    case 32:
        hammings_knn_mc<faiss::HammingComputer32>(
          32, a, b, na, nb, k, distances, labels
        );
        break;
    default:
        if(ncodes % 8 == 0) {
            hammings_knn_mc<faiss::HammingComputerM8>(
              ncodes, a, b, na, nb, k, distances, labels
            );
        } else {
            hammings_knn_mc<faiss::HammingComputerDefault>(
              ncodes, a, b, na, nb, k, distances, labels
            );
        }
    }
}




/* Count number of matches given a max threshold            */
void hamming_count_thres (
        const uint8_t * bs1,
        const uint8_t * bs2,
        size_t n1,
        size_t n2,
        hamdis_t ht,
        size_t ncodes,
        size_t * nptr)
{
    switch (ncodes) {
        case 8:
            faiss::hamming_count_thres <64> (C64(bs1), C64(bs2),
                                             n1, n2, ht, nptr);
            return;
        case 16:
            faiss::hamming_count_thres <128> (C64(bs1), C64(bs2),
                                              n1, n2, ht, nptr);
            return;
        case 32:
            faiss::hamming_count_thres <256> (C64(bs1), C64(bs2),
                                              n1, n2, ht, nptr);
            return;
        case 64:
            faiss::hamming_count_thres <512> (C64(bs1), C64(bs2),
                                              n1, n2, ht, nptr);
            return;
        default:
          FAISS_THROW_FMT ("not implemented for %zu bits", ncodes);
    }
}


/* Count number of cross-matches given a threshold */
void crosshamming_count_thres (
        const uint8_t * dbs,
        size_t n,
        hamdis_t ht,
        size_t ncodes,
        size_t * nptr)
{
    switch (ncodes) {
        case 8:
            faiss::crosshamming_count_thres <64>  (C64(dbs), n, ht, nptr);
            return;
        case 16:
            faiss::crosshamming_count_thres <128> (C64(dbs), n, ht, nptr);
            return;
        case 32:
            faiss::crosshamming_count_thres <256> (C64(dbs), n, ht, nptr);
            return;
        case 64:
            faiss::crosshamming_count_thres <512> (C64(dbs), n, ht, nptr);
            return;
        default:
            FAISS_THROW_FMT ("not implemented for %zu bits", ncodes);
    }
}


/* Returns all matches given a threshold */
size_t match_hamming_thres (
        const uint8_t * bs1,
        const uint8_t * bs2,
        size_t n1,
        size_t n2,
        hamdis_t ht,
        size_t ncodes,
        long * idx,
        hamdis_t * dis)
{
    switch (ncodes) {
        case 8:
          return faiss::match_hamming_thres <64> (C64(bs1), C64(bs2),
                                                  n1, n2, ht, idx, dis);
        case 16:
          return faiss::match_hamming_thres <128> (C64(bs1), C64(bs2),
                                                   n1, n2, ht, idx, dis);
        case 32:
          return faiss::match_hamming_thres <256> (C64(bs1), C64(bs2),
                                                   n1, n2, ht, idx, dis);
        case 64:
          return faiss::match_hamming_thres <512> (C64(bs1), C64(bs2),
                                                   n1, n2, ht, idx, dis);
        default:
            FAISS_THROW_FMT ("not implemented for %zu bits", ncodes);
            return 0;
    }
}


#undef C64



/*************************************
 * generalized Hamming distances
 ************************************/



template <class HammingComputer>
static void hamming_dis_inner_loop (
        const uint8_t *ca,
        const uint8_t *cb,
        size_t nb,
        size_t code_size,
        int k,
        hamdis_t * bh_val_,
        long *     bh_ids_)
{

    HammingComputer hc (ca, code_size);

    for (size_t j = 0; j < nb; j++) {
        int ndiff = hc.hamming (cb);
        cb += code_size;
        if (ndiff < bh_val_[0]) {
            maxheap_pop<hamdis_t> (k, bh_val_, bh_ids_);
            maxheap_push<hamdis_t> (k, bh_val_, bh_ids_, ndiff, j);
        }
    }
}

void generalized_hammings_knn_hc (
        int_maxheap_array_t * ha,
        const uint8_t * a,
        const uint8_t * b,
        size_t nb,
        size_t code_size,
        int ordered)
{
    int na = ha->nh;
    int k = ha->k;

    if (ordered)
        ha->heapify ();

#pragma omp parallel for
    for (int i = 0; i < na; i++) {
        const uint8_t *ca = a + i * code_size;
        const uint8_t *cb = b;

        hamdis_t * bh_val_ = ha->val + i * k;
        long *     bh_ids_ = ha->ids + i * k;

        switch (code_size) {
        case 8:
            hamming_dis_inner_loop<GenHammingComputer8>
                (ca, cb, nb, 8, k, bh_val_, bh_ids_);
            break;
        case 16:
            hamming_dis_inner_loop<GenHammingComputer16>
                (ca, cb, nb, 16, k, bh_val_, bh_ids_);
            break;
        case 32:
            hamming_dis_inner_loop<GenHammingComputer32>
                (ca, cb, nb, 32, k, bh_val_, bh_ids_);
            break;
        default:
            hamming_dis_inner_loop<GenHammingComputerM8>
                (ca, cb, nb, code_size, k, bh_val_, bh_ids_);
            break;
        }
    }

    if (ordered)
        ha->reorder ();

}


} // namespace faiss
