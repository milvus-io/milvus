/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_CLUSTERING_H
#define FAISS_CLUSTERING_H
#include "Index.h"

#include <vector>

namespace faiss {


/** Class for the clustering parameters. Can be passed to the
 * constructor of the Clustering object.
 */
struct ClusteringParameters {
    int niter;          ///< clustering iterations
    int nredo;          ///< redo clustering this many times and keep best

    bool verbose;
    bool spherical;     ///< do we want normalized centroids?
    bool int_centroids; ///< round centroids coordinates to integer
    bool update_index;  ///< update index after each iteration?
    bool frozen_centroids;  ///< use the centroids provided as input and do not change them during iterations

    int min_points_per_centroid; ///< otherwise you get a warning
    int max_points_per_centroid;  ///< to limit size of dataset

    int seed; ///< seed for the random number generator

    /// sets reasonable defaults
    ClusteringParameters ();
};


/** clustering based on assignment - centroid update iterations
 *
 * The clustering is based on an Index object that assigns training
 * points to the centroids. Therefore, at each iteration the centroids
 * are added to the index.
 *
 * On output, the centoids table is set to the latest version
 * of the centroids and they are also added to the index. If the
 * centroids table it is not empty on input, it is also used for
 * initialization.
 *
 * To do several clusterings, just call train() several times on
 * different training sets, clearing the centroid table in between.
 */
struct Clustering: ClusteringParameters {
    typedef Index::idx_t idx_t;
    size_t d;              ///< dimension of the vectors
    size_t k;              ///< nb of centroids

    /// centroids (k * d)
    std::vector<float> centroids;

    /// objective values (sum of distances reported by index) over
    /// iterations
    std::vector<float> obj;

    /// the only mandatory parameters are k and d
    Clustering (int d, int k);
    Clustering (int d, int k, const ClusteringParameters &cp);

    /// Index is used during the assignment stage
    virtual void train (idx_t n, const float * x, faiss::Index & index);

    /// Post-process the centroids after each centroid update.
    /// includes optional L2 normalization and nearest integer rounding
    void post_process_centroids ();

    virtual ~Clustering() {}
};


/** simplified interface
 *
 * @param d dimension of the data
 * @param n nb of training vectors
 * @param k nb of output centroids
 * @param x training set (size n * d)
 * @param centroids output centroids (size k * d)
 * @return final quantization error
 */
float kmeans_clustering (size_t d, size_t n, size_t k,
                         const float *x,
                         float *centroids);



}


#endif
