/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_INDEX_H
#define FAISS_INDEX_H

#include <faiss/MetricType.h>
#include <cstdio>
#include <typeinfo>
#include <string>
#include <sstream>

#define FAISS_VERSION_MAJOR 1
#define FAISS_VERSION_MINOR 6
#define FAISS_VERSION_PATCH 3

/**
 * @namespace faiss
 *
 * Throughout the library, vectors are provided as float * pointers.
 * Most algorithms can be optimized when several vectors are processed
 * (added/searched) together in a batch. In this case, they are passed
 * in as a matrix. When n vectors of size d are provided as float * x,
 * component j of vector i is
 *
 *   x[ i * d + j ]
 *
 * where 0 <= i < n and 0 <= j < d. In other words, matrices are
 * always compact. When specifying the size of the matrix, we call it
 * an n*d matrix, which implies a row-major storage.
 */


namespace faiss {

/// Forward declarations see AuxIndexStructures.h
struct IDSelector;
struct RangeSearchResult;
struct DistanceComputer;

/** Abstract structure for an index, supports adding vectors and searching them.
 *
 * All vectors provided at add or search time are 32-bit float arrays,
 * although the internal representation may vary.
 */
struct Index {
    using idx_t = int64_t;  ///< all indices are this type
    using component_t = float;
    using distance_t = float;

    int d;                 ///< vector dimension
    idx_t ntotal;          ///< total nb of indexed vectors
    bool verbose;          ///< verbosity level

    /// set if the Index does not require training, or if training is
    /// done already
    bool is_trained;

    /// type of metric this index uses for search
    MetricType metric_type;
    float metric_arg;     ///< argument of the metric type

    explicit Index (idx_t d = 0, MetricType metric = METRIC_L2):
                    d(d),
                    ntotal(0),
                    verbose(false),
                    is_trained(true),
                    metric_type (metric),
                    metric_arg(0) {}

    virtual ~Index ();


    /** Perform training on a representative set of vectors
     *
     * @param n      nb of training vectors
     * @param x      training vecors, size n * d
     */
    virtual void train(idx_t n, const float* x);

    /** Add n vectors of dimension d to the index.
     *
     * Vectors are implicitly assigned labels ntotal .. ntotal + n - 1
     * This function slices the input vectors in chuncks smaller than
     * blocksize_add and calls add_core.
     * @param x      input matrix, size n * d
     */
    virtual void add (idx_t n, const float *x) = 0;

    /** Same as add, but stores xids instead of sequential ids.
     *
     * The default implementation fails with an assertion, as it is
     * not supported by all indexes.
     *
     * @param xids if non-null, ids to store for the vectors (size n)
     */
    virtual void add_with_ids (idx_t n, const float * x, const idx_t *xids);

    /** query n vectors of dimension d to the index.
     *
     * return at most k vectors. If there are not enough results for a
     * query, the result array is padded with -1s.
     *
     * @param x           input vectors to search, size n * d
     * @param labels      output labels of the NNs, size n*k
     * @param distances   output pairwise distances, size n*k
     */
    virtual void search (idx_t n, const float *x, idx_t k,
                         float *distances, idx_t *labels) const = 0;

    /** query n vectors of dimension d to the index.
     *
     * return all vectors with distance < radius. Note that many
     * indexes do not implement the range_search (only the k-NN search
     * is mandatory).
     *
     * @param x           input vectors to search, size n * d
     * @param radius      search radius
     * @param result      result table
     */
    virtual void range_search (idx_t n, const float *x, float radius,
                               RangeSearchResult *result) const;

    /** return the indexes of the k vectors closest to the query x.
     *
     * This function is identical as search but only return labels of neighbors.
     * @param x           input vectors to search, size n * d
     * @param labels      output labels of the NNs, size n*k
     */
    void assign (idx_t n, const float * x, idx_t * labels, idx_t k = 1);

    /// removes all elements from the database.
    virtual void reset() = 0;

    /** removes IDs from the index. Not supported by all
     * indexes. Returns the number of elements removed.
     */
    virtual size_t remove_ids (const IDSelector & sel);

    /** Reconstruct a stored vector (or an approximation if lossy coding)
     *
     * this function may not be defined for some indexes
     * @param key         id of the vector to reconstruct
     * @param recons      reconstucted vector (size d)
     */
    virtual void reconstruct (idx_t key, float * recons) const;

    /** Reconstruct vectors i0 to i0 + ni - 1
     *
     * this function may not be defined for some indexes
     * @param recons      reconstucted vector (size ni * d)
     */
    virtual void reconstruct_n (idx_t i0, idx_t ni, float *recons) const;

    /** Similar to search, but also reconstructs the stored vectors (or an
     * approximation in the case of lossy coding) for the search results.
     *
     * If there are not enough results for a query, the resulting arrays
     * is padded with -1s.
     *
     * @param recons      reconstructed vectors size (n, k, d)
     **/
    virtual void search_and_reconstruct (idx_t n, const float *x, idx_t k,
                                         float *distances, idx_t *labels,
                                         float *recons) const;

    /** Computes a residual vector after indexing encoding.
     *
     * The residual vector is the difference between a vector and the
     * reconstruction that can be decoded from its representation in
     * the index. The residual can be used for multiple-stage indexing
     * methods, like IndexIVF's methods.
     *
     * @param x           input vector, size d
     * @param residual    output residual vector, size d
     * @param key         encoded index, as returned by search and assign
     */
    virtual void compute_residual (const float * x,
                                   float * residual, idx_t key) const;

    /** Computes a residual vector after indexing encoding (batch form).
     * Equivalent to calling compute_residual for each vector.
     *
     * The residual vector is the difference between a vector and the
     * reconstruction that can be decoded from its representation in
     * the index. The residual can be used for multiple-stage indexing
     * methods, like IndexIVF's methods.
     *
     * @param n           number of vectors
     * @param xs          input vectors, size (n x d)
     * @param residuals   output residual vectors, size (n x d)
     * @param keys        encoded index, as returned by search and assign
     */
    virtual void compute_residual_n (idx_t n, const float* xs,
                                     float* residuals,
                                     const idx_t* keys) const;

    /** Get a DistanceComputer (defined in AuxIndexStructures) object
     * for this kind of index.
     *
     * DistanceComputer is implemented for indexes that support random
     * access of their vectors.
     */
    virtual DistanceComputer * get_distance_computer() const;


    /* The standalone codec interface */

    /** size of the produced codes in bytes */
    virtual size_t sa_code_size () const;

    /** encode a set of vectors
     *
     * @param n       number of vectors
     * @param x       input vectors, size n * d
     * @param bytes   output encoded vectors, size n * sa_code_size()
     */
    virtual void sa_encode (idx_t n, const float *x,
                                  uint8_t *bytes) const;

    /** encode a set of vectors
     *
     * @param n       number of vectors
     * @param bytes   input encoded vectors, size n * sa_code_size()
     * @param x       output vectors, size n * d
     */
    virtual void sa_decode (idx_t n, const uint8_t *bytes,
                                    float *x) const;


};

}


#endif
