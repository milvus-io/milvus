/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

/*
 * Hamming distances. The binary vector dimensionality should be a
 * multiple of 8, as the elementary operations operate on bytes. If
 * you need other sizes, just pad with 0s (this is done by function
 * fvecs2bitvecs).
 *
 * User-defined type hamdis_t is used for distances because at this time
 * it is still uncler clear how we will need to balance
 * - flexibility in vector size (may need 16- or even 8-bit vectors)
 * - memory usage
 * - cache-misses when dealing with large volumes of data (fewer bits is better)
 *
 */

#ifndef FAISS_hamming_h
#define FAISS_hamming_h


#include <stdint.h>

#include <faiss/utils/Heap.h>


/* The Hamming distance type */
typedef int32_t hamdis_t;

namespace faiss {

/**************************************************
 * General bit vector functions
 **************************************************/

struct RangeSearchResult;

void bitvec_print (const uint8_t * b, size_t d);


/* Functions for casting vectors of regular types to compact bits.
   They assume proper allocation done beforehand, meaning that b
   should be be able to receive as many bits as x may produce.  */

/* Makes an array of bits from the signs of a float array. The length
   of the output array b is rounded up to byte size (allocate
   accordingly) */
void fvecs2bitvecs (
        const float * x,
        uint8_t * b,
        size_t d,
        size_t n);

void bitvecs2fvecs (
        const uint8_t * b,
        float * x,
        size_t d,
        size_t n);


void fvec2bitvec (const float * x, uint8_t * b, size_t d);

/** Shuffle the bits from b(i, j) := a(i, order[j])
 */
void bitvec_shuffle (size_t n, size_t da, size_t db,
                     const int *order,
                     const uint8_t *a,
                     uint8_t *b);


/***********************************************
 * Generic reader/writer for bit strings
 ***********************************************/


struct BitstringWriter {
    uint8_t *code;
    size_t code_size;
    size_t i; // current bit offset

    // code_size in bytes
    BitstringWriter(uint8_t *code, int code_size);

    // write the nbit low bits of x
    void write(uint64_t x, int nbit);
};

struct BitstringReader {
    const uint8_t *code;
    size_t code_size;
    size_t i;

    // code_size in bytes
    BitstringReader(const uint8_t *code, int code_size);

    // read nbit bits from the code
    uint64_t read(int nbit);
};

/**************************************************
 * Hamming distance computation functions
 **************************************************/



extern size_t hamming_batch_size;

inline int popcount64(uint64_t x) {
    return __builtin_popcountl(x);
}


/** Compute a set of Hamming distances between na and nb binary vectors
 *
 * @param  a             size na * nbytespercode
 * @param  b             size nb * nbytespercode
 * @param  nbytespercode should be multiple of 8
 * @param  dis           output distances, size na * nb
 */
void hammings (
        const uint8_t * a,
        const uint8_t * b,
        size_t na, size_t nb,
        size_t nbytespercode,
        hamdis_t * dis);




/** Return the k smallest Hamming distances for a set of binary query vectors,
 * using a max heap.
 * @param a       queries, size ha->nh * ncodes
 * @param b       database, size nb * ncodes
 * @param nb      number of database vectors
 * @param ncodes  size of the binary codes (bytes)
 * @param ordered if != 0: order the results by decreasing distance
 *                (may be bottleneck for k/n > 0.01) */
void hammings_knn_hc (
        int_maxheap_array_t * ha,
        const uint8_t * a,
        const uint8_t * b,
        size_t nb,
        size_t ncodes,
        int ordered);

/* Legacy alias to hammings_knn_hc. */
void hammings_knn (
  int_maxheap_array_t * ha,
  const uint8_t * a,
  const uint8_t * b,
  size_t nb,
  size_t ncodes,
  int ordered);

/** Return the k smallest Hamming distances for a set of binary query vectors,
 * using counting max.
 * @param a       queries, size na * ncodes
 * @param b       database, size nb * ncodes
 * @param na      number of query vectors
 * @param nb      number of database vectors
 * @param k       number of vectors/distances to return
 * @param ncodes  size of the binary codes (bytes)
 * @param distances output distances from each query vector to its k nearest
 *                neighbors
 * @param labels  output ids of the k nearest neighbors to each query vector
 */
void hammings_knn_mc (
  const uint8_t * a,
  const uint8_t * b,
  size_t na,
  size_t nb,
  size_t k,
  size_t ncodes,
  int32_t *distances,
  int64_t *labels);

/** same as hammings_knn except we are doing a range search with radius */
void hamming_range_search (
    const uint8_t * a,
    const uint8_t * b,
    size_t na,
    size_t nb,
    int radius,
    size_t ncodes,
    RangeSearchResult *result);


/* Counting the number of matches or of cross-matches (without returning them)
   For use with function that assume pre-allocated memory */
void hamming_count_thres (
        const uint8_t * bs1,
        const uint8_t * bs2,
        size_t n1,
        size_t n2,
        hamdis_t ht,
        size_t ncodes,
        size_t * nptr);

/* Return all Hamming distances/index passing a thres. Pre-allocation of output
   is required. Use hamming_count_thres to determine the proper size. */
size_t match_hamming_thres (
        const uint8_t * bs1,
        const uint8_t * bs2,
        size_t n1,
        size_t n2,
        hamdis_t ht,
        size_t ncodes,
        int64_t * idx,
        hamdis_t * dis);

/* Cross-matching in a set of vectors */
void crosshamming_count_thres (
        const uint8_t * dbs,
        size_t n,
        hamdis_t ht,
        size_t ncodes,
        size_t * nptr);


/* compute the Hamming distances between two codewords of nwords*64 bits */
hamdis_t hamming (
        const uint64_t * bs1,
        const uint64_t * bs2,
        size_t nwords);



} // namespace faiss

// inlined definitions of HammingComputerXX and GenHammingComputerXX

#include <faiss/utils/hamming-inl.h>

#endif /* FAISS_hamming_h */
