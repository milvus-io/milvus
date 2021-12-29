/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
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

#include <faiss/utils/hamming.h>

#include <vector>
#include <memory>
#include <stdio.h>
#include <math.h>
#include <omp.h>

#include <faiss/utils/Heap.h>
#include <faiss/utils/utils.h>
#include <faiss/impl/AuxIndexStructures.h>
#include <faiss/impl/FaissAssert.h>

static const size_t BLOCKSIZE_QUERY = 8192;
static const size_t size_1M = 1 * 1024 * 1024;

namespace faiss {

size_t hamming_batch_size = 65536;

/* Elementary Hamming distance computation: unoptimized  */
template <size_t nbits, typename T>
T hamming (const uint8_t *bs1,
           const uint8_t *bs2)
{
    const size_t nbytes = nbits / 8;
    size_t i;
    T h = 0;
    for (i = 0; i < nbytes; i++)
        h += (T) lookup8bit[bs1[i]^bs2[i]];
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
        int64_t * idx,
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
    const int64_t ncodes = ((d + 7) / 8);
#pragma omp parallel for if(n > 100000)
    for (size_t i = 0; i < n; i++)
        fvec2bitvec (x + i * d, b + i * ncodes, d);
}



void bitvecs2fvecs (
        const uint8_t * b,
        float * x,
        size_t d,
        size_t n) {

    const int64_t ncodes = ((d + 7) / 8);
#pragma omp parallel for if(n > 100000)
    for (size_t i = 0; i < n; i++) {
        binary_to_real (d, b + i * ncodes, x + i * d);
    }
}


/* Reverse bit (NOT an optimized function, only used for print purpose) */
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


void bitvec_shuffle (size_t n, size_t da, size_t db,
                     const int *order,
                     const uint8_t *a,
                     uint8_t *b)
{
    for(size_t i = 0; i < db; i++) {
        FAISS_THROW_IF_NOT (order[i] >= 0 && order[i] < da);
    }
    size_t lda = (da + 7) / 8;
    size_t ldb = (db + 7) / 8;

#pragma omp parallel for if(n > 10000)
    for (size_t i = 0; i < n; i++) {
        const uint8_t *ai = a + i * lda;
        uint8_t *bi = b + i * ldb;
        memset (bi, 0, ldb);
        for(size_t i = 0; i < db; i++) {
            int o = order[i];
            uint8_t the_bit = (ai[o >> 3] >> (o & 7)) & 1;
            bi[i >> 3] |= the_bit << (i & 7);
        }
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


template <class HammingComputer>
static
void hamming_range_search_template (
    const uint8_t * a,
    const uint8_t * b,
    size_t na,
    size_t nb,
    int radius,
    size_t code_size,
    std::vector<RangeSearchPartialResult*> &result,
    size_t buffer_size,
    const BitsetView &bitset)
{

#pragma omp parallel
    {
        RangeSearchResult *tmp_res = new RangeSearchResult(na);
        tmp_res->buffer_size = buffer_size;
        auto pres = new RangeSearchPartialResult(tmp_res);

        HammingComputer hc (a, code_size);
        const uint8_t * yi = b;
        RangeQueryResult & qres = pres->new_result (0);

#pragma omp for
        for (size_t j = 0; j < nb; j++) {
            if (bitset.empty() || !bitset.test((int64_t)j)) {
                int dis = hc.compute (yi + j * code_size);
                if (dis < radius) {
                    qres.add(dis, j);
                }
            }
//            yi += code_size;
        }
#pragma omp critical
        result.push_back(pres);
    }
}

void hamming_range_search (
    const uint8_t * a,
    const uint8_t * b,
    size_t na,
    size_t nb,
    int radius,
    size_t code_size,
    std::vector<faiss::RangeSearchPartialResult*>& result,
    size_t buffer_size,
    const BitsetView bitset)
{

#define HC(name) hamming_range_search_template<name> (a, b, na, nb, radius, code_size, result, buffer_size, bitset)

    switch(code_size) {
    case 4: HC(HammingComputer4); break;
    case 8: HC(HammingComputer8); break;
    case 16: HC(HammingComputer16); break;
    case 32: HC(HammingComputer32); break;
    default: HC(HammingComputerDefault);
    }
#undef HC
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
        int64_t * idx,
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
        int64_t *     bh_ids_)
{

    HammingComputer hc (ca, code_size);

    for (size_t j = 0; j < nb; j++) {
        int ndiff = hc.compute (cb);
        cb += code_size;
        if (ndiff < bh_val_[0]) {
            maxheap_swap_top<hamdis_t> (k, bh_val_, bh_ids_, ndiff, j);
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
        int64_t *     bh_ids_ = ha->ids + i * k;

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
