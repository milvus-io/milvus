/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_PRODUCT_QUANTIZER_H
#define FAISS_PRODUCT_QUANTIZER_H

#include <stdint.h>

#include <vector>

#include "Clustering.h"
#include "Heap.h"

namespace faiss {

/** Product Quantizer. Implemented only for METRIC_L2 */
struct ProductQuantizer {

    using idx_t = Index::idx_t;

    size_t d;              ///< size of the input vectors
    size_t M;              ///< number of subquantizers
    size_t nbits;          ///< number of bits per quantization index

    // values derived from the above
    size_t dsub;           ///< dimensionality of each subvector
    size_t byte_per_idx;   ///< nb bytes per code component (1 or 2)
    size_t code_size;      ///< byte per indexed vector
    size_t ksub;           ///< number of centroids for each subquantizer
    bool verbose;          ///< verbose during training?


    /// initialization
    enum train_type_t {
        Train_default,
        Train_hot_start,   ///< the centroids are already initialized
        Train_shared,      ///< share dictionary accross PQ segments
        Train_hypercube,   ///< intialize centroids with nbits-D hypercube
        Train_hypercube_pca,   ///< intialize centroids with nbits-D hypercube
    };
    train_type_t train_type;

    ClusteringParameters cp; ///< parameters used during clustering

    /// if non-NULL, use this index for assignment (should be of size
    /// d / M)
    Index *assign_index;

    /// Centroid table, size M * ksub * dsub
    std::vector<float> centroids;

    /// return the centroids associated with subvector m
    float * get_centroids (size_t m, size_t i) {
        return &centroids [(m * ksub + i) * dsub];
    }
    const float * get_centroids (size_t m, size_t i) const {
        return &centroids [(m * ksub + i) * dsub];
    }

    // Train the product quantizer on a set of points. A clustering
    // can be set on input to define non-default clustering parameters
    void train (int n, const float *x);

    ProductQuantizer(size_t d, /* dimensionality of the input vectors */
            size_t M,          /* number of subquantizers */
            size_t nbits);     /* number of bit per subvector index */

    ProductQuantizer ();

    /// compute derived values when d, M and nbits have been set
    void set_derived_values ();

    /// Define the centroids for subquantizer m
    void set_params (const float * centroids, int m);

    /// Quantize one vector with the product quantizer
    void compute_code (const float * x, uint8_t * code) const ;

    /// same as compute_code for several vectors
    void compute_codes (const float * x,
                        uint8_t * codes,
                        size_t n) const ;

    /// speed up code assignment using assign_index
    /// (non-const because the index is changed)
    void compute_codes_with_assign_index (
                const float * x,
                uint8_t * codes,
                size_t n);

    /// decode a vector from a given code (or n vectors if third argument)
    void decode (const uint8_t *code, float *x) const;
    void decode (const uint8_t *code, float *x, size_t n) const;

    /// If we happen to have the distance tables precomputed, this is
    /// more efficient to compute the codes.
    void compute_code_from_distance_table (const float *tab,
                                           uint8_t *code) const;


    /** Compute distance table for one vector.
     *
     * The distance table for x = [x_0 x_1 .. x_(M-1)] is a M * ksub
     * matrix that contains
     *
     *   dis_table (m, j) = || x_m - c_(m, j)||^2
     *   for m = 0..M-1 and j = 0 .. ksub - 1
     *
     * where c_(m, j) is the centroid no j of sub-quantizer m.
     *
     * @param x         input vector size d
     * @param dis_table output table, size M * ksub
     */
    void compute_distance_table (const float * x,
                                 float * dis_table) const;

    void compute_inner_prod_table (const float * x,
                                   float * dis_table) const;


    /** compute distance table for several vectors
     * @param nx        nb of input vectors
     * @param x         input vector size nx * d
     * @param dis_table output table, size nx * M * ksub
     */
    void compute_distance_tables (size_t nx,
                                  const float * x,
                                  float * dis_tables) const;

    void compute_inner_prod_tables (size_t nx,
                                    const float * x,
                                    float * dis_tables) const;


    /** perform a search (L2 distance)
     * @param x        query vectors, size nx * d
     * @param nx       nb of queries
     * @param codes    database codes, size ncodes * byte_per_idx
     * @param ncodes   nb of nb vectors
     * @param res      heap array to store results (nh == nx)
     * @param init_finalize_heap  initialize heap (input) and sort (output)?
     */
    void search (const float * x,
                 size_t nx,
                 const uint8_t * codes,
                 const size_t ncodes,
                 float_maxheap_array_t *res,
                 bool init_finalize_heap = true) const;

    /** same as search, but with inner product similarity */
    void search_ip (const float * x,
                 size_t nx,
                 const uint8_t * codes,
                 const size_t ncodes,
                 float_minheap_array_t *res,
                 bool init_finalize_heap = true) const;


    /// Symmetric Distance Table
    std::vector<float> sdc_table;

    // intitialize the SDC table from the centroids
    void compute_sdc_table ();

    void search_sdc (const uint8_t * qcodes,
                     size_t nq,
                     const uint8_t * bcodes,
                     const size_t ncodes,
                     float_maxheap_array_t * res,
                     bool init_finalize_heap = true) const;

};


} // namespace faiss


#endif
