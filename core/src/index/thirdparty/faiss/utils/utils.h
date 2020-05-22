/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

/*
 *  A few utilitary functions for similarity search:
 * - optimized exhaustive distance and knn search functions
 * - some functions reimplemented from torch for speed
 */

#ifndef FAISS_utils_h
#define FAISS_utils_h

#include <stdint.h>

#include <faiss/utils/Heap.h>


namespace faiss {


/**************************************************
 * Get some stats about the system
**************************************************/


/// ms elapsed since some arbitrary epoch
double getmillisecs ();

/// get current RSS usage in kB
size_t get_mem_usage_kb ();


uint64_t get_cycles ();

/***************************************************************************
 * Misc  matrix and vector manipulation functions
 ***************************************************************************/


/** compute c := a + bf * b for a, b and c tables
 *
 * @param n   size of the tables
 * @param a   size n
 * @param b   size n
 * @param c   restult table, size n
 */
void fvec_madd (size_t n, const float *a,
                float bf, const float *b, float *c);


/** same as fvec_madd, also return index of the min of the result table
 * @return    index of the min of table c
 */
int fvec_madd_and_argmin (size_t n, const float *a,
                           float bf, const float *b, float *c);


/* perform a reflection (not an efficient implementation, just for test ) */
void reflection (const float * u, float * x, size_t n, size_t d, size_t nu);


/** compute the Q of the QR decomposition for m > n
 * @param a   size n * m: input matrix and output Q
 */
void matrix_qr (int m, int n, float *a);

/** distances are supposed to be sorted. Sorts indices with same distance*/
void ranklist_handle_ties (int k, int64_t *idx, const float *dis);

/** count the number of comon elements between v1 and v2
 * algorithm = sorting + bissection to avoid double-counting duplicates
 */
size_t ranklist_intersection_size (size_t k1, const int64_t *v1,
                                   size_t k2, const int64_t *v2);

/** merge a result table into another one
 *
 * @param I0, D0       first result table, size (n, k)
 * @param I1, D1       second result table, size (n, k)
 * @param keep_min     if true, keep min values, otherwise keep max
 * @param translation  add this value to all I1's indexes
 * @return             nb of values that were taken from the second table
 */
size_t merge_result_table_with (size_t n, size_t k,
                                int64_t *I0, float *D0,
                                const int64_t *I1, const float *D1,
                                bool keep_min = true,
                                int64_t translation = 0);


/// a balanced assignment has a IF of 1
double imbalance_factor (int n, int k, const int64_t *assign);

/// same, takes a histogram as input
double imbalance_factor (int k, const int *hist);


void fvec_argsort (size_t n, const float *vals,
                    size_t *perm);

void fvec_argsort_parallel (size_t n, const float *vals,
                    size_t *perm);


/// compute histogram on v
int ivec_hist (size_t n, const int * v, int vmax, int *hist);

/** Compute histogram of bits on a code array
 *
 * @param codes   size(n, nbits / 8)
 * @param hist    size(nbits): nb of 1s in the array of codes
 */
void bincode_hist(size_t n, size_t nbits, const uint8_t *codes, int *hist);


/// compute a checksum on a table.
size_t ivec_checksum (size_t n, const int *a);


/** random subsamples a set of vectors if there are too many of them
 *
 * @param d      dimension of the vectors
 * @param n      on input: nb of input vectors, output: nb of output vectors
 * @param nmax   max nb of vectors to keep
 * @param x      input array, size *n-by-d
 * @param seed   random seed to use for sampling
 * @return       x or an array allocated with new [] with *n vectors
 */
const float *fvecs_maybe_subsample (
       size_t d, size_t *n, size_t nmax, const float *x,
       bool verbose = false, int64_t seed = 1234);

/** Convert binary vector to +1/-1 valued float vector.
 *
 * @param d      dimension of the vector (multiple of 8)
 * @param x_in   input binary vector (uint8_t table of size d / 8)
 * @param x_out  output float vector (float table of size d)
 */
void binary_to_real(size_t d, const uint8_t *x_in, float *x_out);

/** Convert float vector to binary vector. Components > 0 are converted to 1,
 * others to 0.
 *
 * @param d      dimension of the vector (multiple of 8)
 * @param x_in   input float vector (float table of size d)
 * @param x_out  output binary vector (uint8_t table of size d / 8)
 */
void real_to_binary(size_t d, const float *x_in, uint8_t *x_out);


/** A reasonable hashing function */
uint64_t hash_bytes (const uint8_t *bytes, int64_t n);

/** Whether OpenMP annotations were respected. */
bool check_openmp();

} // namspace faiss


#endif /* FAISS_utils_h */
